package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "gopkg.in/goracle.v2"
)

// Таблицы в СУБД, необходимые для работы.
// replicated_tables - наполняется данными по репликатам
// tmp_replicated_tables - временная таблица для упрощения DML и уменьшения количества транзакций

/*
create table replicated_tables
(
group_name varchar2(20) not null,
group_type varchar2(50) not null,
src_table_owner varchar2(128) not null,
src_table_name varchar2(128) not null,
trg_table_owner varchar2(128) not null,
trg_table_name varchar2(128) not null,
ext_params varchar2(2000),
insert_date date default on null sysdate not null
)
tablespace REP;

create global temporary table tmp_replicated_tables
(
group_name varchar2(20) not null,
group_type varchar2(50) not null,
src_table_owner varchar2(128) not null,
src_table_name varchar2(128) not null,
trg_table_owner varchar2(128) not null,
trg_table_name varchar2(128) not null,
ext_params varchar2(2000)
)
on commit preserve rows;
*/

// Автоматически создаваемый файл для хранения информации о статусе процессов с крайнего запуска
const configGroupsFile = `groups.json`

// Вручную создаваемый файл для хранения данных о подключении к БД. Структура:
// [
// 	{
// 		"tns":"repdb"
// 		"username":"fe_gg"
// 		"encr_password":"***" << вывод работы программы с флагом -encrypt
// 	}
// ]
const configCredFile = `cred.json`

// cmd flags
var fdebug bool
var fencrypt bool
var ggsciBinary string

var aliases map[string]string
var dbConns map[string]*sql.DB

// Структура для хранения данных подключения к БД, получаемых из json файла
type configCred struct {
	DbTNS       string `json:"tns"`
	Username    string `json:"username"`
	EncPassword string `json:"encr_password"`
}

// Структуры для хранения статуса и даты крайнего запуска группы
type groupLastStartAndStatus struct {
	GroupName  string `json:"groupName"`
	LastStart  string `json:"lastStart"`
	LastStatus string `json:"lastStatus"`
}
type ggGroupsLastStatus struct {
	Ggsci  string                    `json:"ggsci"`
	Groups []groupLastStartAndStatus `json:"groups"`
}

// Структура для хранения MAP statement
type repTable struct {
	srcOwner  []byte
	srcName   []byte
	tOwner    []byte
	tName     []byte
	extParams []byte
}

// Структура для хранения информации о группах
type gGroup struct {
	GroupName      string
	GroupType      string
	GroupStatus    string
	GroupLastStart string
	GroupDB        string
	GroupMaps      map[string]repTable
}

var ggGroups []gGroup

// ConfigCreds - данные для подключения к БД
var ConfigCreds []configCred
var groupsLastStatus []ggGroupsLastStatus

// seckey for enc function
var seckey []byte

func init() {
	const (
		defaultDebug    = false
		debugUsage      = "set debug=true to get output data in StdOut (and additional info) instead of sending to DB"
		defaultGgsci    = "ggsci"
		ggsciUsage      = "set full path to ggsci binary"
		defaultFencrypt = false
		fencryptUsage   = "set to perform text encryption from stdin. Use: -encrypt password-plain-text - don't use quotes"
	)
	flag.BoolVar(&fdebug, "debug", defaultDebug, debugUsage)
	flag.StringVar(&ggsciBinary, "ggsci", defaultGgsci, ggsciUsage)
	flag.BoolVar(&fencrypt, "encrypt", defaultFencrypt, fencryptUsage)
	seckey = []byte("a tiny very lovv ") // 32 bytes
}

func main() {

	start := time.Now()
	//Разворачиваем аргументы
	flag.Parse()
	// Немного security through obscurity
	part2 := []byte("some secret pic")
	seckey = append(seckey, part2...)
	seckey[30] = seckey[30] + 2

	if fencrypt {
		if len(os.Args) > 3 {
			log.Fatalln("when using encrypt flag there should be only one argument that is password to encrypt")
		}
		pwd := os.Args[2]
		encPwd, err := encrypt(seckey, []byte(pwd))
		if err != nil {
			log.Fatalln("Error encrypting text: " + pwd + "; " + err.Error())
		}
		fmt.Println(encPwd)
		return
	}

	dbConns = make(map[string]*sql.DB)

	loadGroupsLastStatus()

	loadCredentials()

	getCredStoreInfo()

	getGroupsInfo()

	// cleanRepTablesDB()

	for i, grp := range ggGroups {
		log.Printf("----------------------------------------------------\n")

		ggGroups[i].GroupLastStart = getSingleGroupInfo(grp.GroupName)

		prevLastStart, prevStatus := getLastGroupInfo(grp.GroupName)

		if grp.GroupStatus == string("RUNNING") && (prevLastStart != ggGroups[i].GroupLastStart || prevStatus != grp.GroupStatus) {
			log.Printf("Getting MAPs for group %s\n", grp.GroupName)
			out := execCmd(ggsciBinary, "view params "+grp.GroupName)
			if grp.GroupType == string("REPLICAT") {
				ggGroups[i].GroupMaps, ggGroups[i].GroupDB = processParams(out)

				if ggGroups[i].GroupDB == "" {
					continue // Пропускаем этап вставки в БД, если БД для группы не указана
				}
				updateDB(ggGroups[i])
			}
		} else {
			log.Printf("Skipping %s.. no status changes since last start\n", grp.GroupName)
		}
	}

	defer func() {
		for _, cn := range dbConns {
			cn.Close()
		}
	}()

	saveGroupsLastStatus()

	fmt.Printf("\n%s time spent\n", time.Since(start))
}

func loadGroupsLastStatus() {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	// exPath := filepath.Dir(ex)
	confPath := filepath.Dir(ex) + "/" + configGroupsFile

	// detect if file exists
	var _, pathErr = os.Stat(confPath)

	// create file if not exists
	if os.IsNotExist(pathErr) {
		if fdebug {
			log.Println("Couldn't find config file " + configGroupsFile + ". Creating one...")
		}
		file, err := os.Create(confPath)
		if err != nil {
			log.Fatalln("Error creating config file: " + confPath)
		}
		file.Close()
	}

	fileBytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatal("Error reading config file - expecting", confPath, err)
	}

	err = json.Unmarshal(fileBytes, &groupsLastStatus)
	if err != nil {
		log.Println("Error parsing config: ", err)
		groupsLastStatus = make([]ggGroupsLastStatus, 0, 10)
	}
	if fdebug {
		log.Println("Loaded Last Status data from " + configGroupsFile)
		fmt.Println(groupsLastStatus)
	}
}

func loadCredentials() {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	// exPath := filepath.Dir(ex)
	confPath := filepath.Dir(ex) + "/" + configCredFile

	// detect if file exists
	var _, pathErr = os.Stat(confPath)

	// create file if not exists
	if os.IsNotExist(pathErr) {
		if fdebug {
			log.Println("Couldn't find config file " + configCredFile + ". Creating one...")
		}
		file, err := os.Create(confPath)
		if err != nil {
			log.Fatal("Error creating config file: " + confPath)
		}
		file.Close()
	}

	fileBytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatal("Error reading config file - expecting", confPath, err)
	}

	err = json.Unmarshal(fileBytes, &ConfigCreds)
	if err != nil {
		log.Println("Error parsing config: ", err)
		ConfigCreds = make([]configCred, 0, 1)
	}
	if fdebug {
		log.Println("Loaded credentials data from " + configCredFile)
		fmt.Println(ConfigCreds)
	}
}

func getLastGroupInfo(grName string) (string, string) {
	var prevStart, prevStatus string
	for _, grs := range groupsLastStatus {
		if grs.Ggsci == ggsciBinary {
			for _, g := range grs.Groups {
				if g.GroupName == grName {
					prevStart = g.LastStart
					prevStatus = g.LastStatus
					break
				}
			}
			break
		}
	}
	return prevStart, prevStatus
}

func saveGroupsLastStatus() {
	grCurrSt := make([]groupLastStartAndStatus, 0, 10)
	for i := range ggGroups {
		grCurrSt = append(grCurrSt, groupLastStartAndStatus{ggGroups[i].GroupName, ggGroups[i].GroupLastStart, ggGroups[i].GroupStatus})
	}

	// Для текущего рабочего бинарника заменяем все старые статусы на актуальные
	isSet := false
	for i, ls := range groupsLastStatus {
		if ls.Ggsci == ggsciBinary {
			groupsLastStatus[i].Groups = grCurrSt
			isSet = true
			break
		}
	}
	if !isSet {
		groupsLastStatus = append(groupsLastStatus, ggGroupsLastStatus{ggsciBinary, grCurrSt})
	}

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)

	json, err := json.MarshalIndent(&groupsLastStatus, "", "   ")
	if err != nil {
		log.Fatal("Error making json ", err)
	}
	err = ioutil.WriteFile(exPath+"/"+configGroupsFile, json, 0644)
	if err != nil {
		log.Fatal("Error writing json ", err)
	}
	if fdebug {
		log.Println("Written Last Status data to " + configGroupsFile)
	}
}

func getGroupsInfo() {
	log.Println("Getting all groups info")
	ggGroups = make([]gGroup, 0, 10)

	out := execCmd(ggsciBinary, "info all")
	if fdebug {
		log.Printf("Got %d bytes\n", out.Len())
	}
	lines := bytes.Split(out.Bytes(), []byte("\n"))
	var ggGroup gGroup
	for _, line := range lines {
		if bytes.Contains(line, []byte("EXTRACT")) || bytes.Contains(line, []byte("REPLICAT")) {
			props := bytes.Fields(line)
			ggGroup.GroupType = string(props[0])
			ggGroup.GroupStatus = string(props[1])
			ggGroup.GroupName = string(props[2])
			ggGroups = append(ggGroups, ggGroup)
		}
	}
}

func getSingleGroupInfo(gname string) string {
	if fdebug {
		log.Println("Get simple info for group " + gname)
	}

	out := execCmd(ggsciBinary, "info "+gname)
	if fdebug {
		log.Printf("Got %d bytes\n", out.Len())
	}
	lines := bytes.Split(out.Bytes(), []byte("\n"))
	var startDate string
	for _, line := range lines {
		// Last Started 2019-01-01 00:00
		if pos := bytes.LastIndex(line, []byte("Last Started")); pos != -1 {
			startDate = string(line[pos+13 : pos+13+16])
		}
	}
	if fdebug {
		log.Println("Group " + gname + " last start: " + startDate)
	}
	return startDate
}

func getCredStoreInfo() {
	log.Println("Getting credential store info")

	out := execCmd(ggsciBinary, "info credentialstore")
	if fdebug {
		log.Printf("Got %d bytes\n", out.Len())
	}
	lines := bytes.Split(out.Bytes(), []byte("\n"))

	//Собираем пары alias-userid
	aliases = make(map[string]string)
	var currAlias string
	for _, line := range lines {
		if pos := bytes.Index(line, []byte("Alias:")); pos != -1 {
			currAlias = string(bytes.ToUpper(bytes.TrimSpace(line[pos+7:])))
			continue
		}
		if currAlias != "" {
			pos := bytes.Index(line, []byte("Userid:"))
			aliases[currAlias] = string(bytes.ToUpper(bytes.TrimSpace(line[pos+8:])))
			currAlias = ""
		}
	}
	if fdebug {
		fmt.Println(aliases)
	}
}

func execCmd(ggsciBinary string, cmdText string) bytes.Buffer {
	var out bytes.Buffer

	cmd := exec.Command(ggsciBinary)
	cmd.Stdin = bytes.NewBuffer([]byte(cmdText))
	cmd.Stdout = &out

	err := cmd.Run()

	if err != nil {
		log.Fatal(err)
	}

	return out
	// lines := bytes.Split(out.Bytes(), []byte("\n"))

	// for i, line := range lines {
	// 	fmt.Printf("%d: %s\n", i, line)
	// }

	// fmt.Printf("Output:\n%s\n", out.Bytes() )
}

func processReport(report bytes.Buffer) (map[string]repTable, string) {

	lines := bytes.Split(report.Bytes(), []byte("\n"))
	re := regexp.MustCompile(`(?i)^map[[:space:]]+([[:alnum:]_$]+)\.([[:alnum:]_$\?\*\-]+)[[:space:]]*,{0,1}[[:space:]]*target[[:space:]]+([[:alnum:]_$]+)\.([[:alnum:]_$\?\*\-]+)[[:space:]]*,{0,1}[[:space:]]*([^;]*)`)

	repTables := make(map[string]repTable, 10)
	var groupDB string
	var linesFile int
	var linesMatched int
	for _, line := range lines {
		// Ищем предложения MAP OWNER.NAME TARGET OWNER.NAME [params];
		matches := re.FindSubmatch(line)
		linesFile++
		if len(matches) > 0 {
			if fdebug {
				fmt.Printf("\t%s.%s >> %s.%s, tail: %s\n", matches[1], matches[2], matches[3], matches[4], matches[5])
			}
			repTables[strings.ToUpper(string(matches[3]))+"."+strings.ToUpper(string(matches[4]))] = repTable{matches[1], matches[2], matches[3], matches[4], matches[5]}
			linesMatched++
		}

		upperLine := bytes.ToUpper(line)
		// Получаем tns базы данных, с которой работает процесс
		if authLine := bytes.TrimSpace(upperLine); bytes.Contains(authLine, []byte("USERID")) {
			if bytes.Contains(authLine, []byte("USERIDALIAS")) {
				alias := string(bytes.TrimSpace(authLine[12:]))
				dbconn, ok := aliases[alias]
				if !ok {
					log.Fatalln("Unable to find record for " + alias + " in credentialstore map")
				}
				groupDB = strings.Split(dbconn, string("@"))[1]
			} else { // USERID type of auth

				useridRE := regexp.MustCompile(`(?i)USERID.+@([[:alnum:]_$]+).+`)
				matches = useridRE.FindSubmatch(authLine)
				groupDB = string(matches[1])
			}
			if fdebug {
				log.Printf("Group DB is %s", groupDB)
			}
		}

		if bytes.Contains(line, []byte("Run Time Messages")) {
			break
		}
	}

	if fdebug {
		fmt.Printf("\n%d lines in file\n%d lines matched\n", linesFile, linesMatched)
	}
	log.Printf("%d tables in map\n", len(repTables))
	return repTables, groupDB
}

func processParams(data bytes.Buffer) (map[string]repTable, string) {

	lines := bytes.Split(data.Bytes(), []byte("\n"))
	re := regexp.MustCompile(`(?i)^map[[:space:]]+([[:alnum:]_$]+)\."?([[:alnum:]_$\?\*\-]+)"?[[:space:]]*,{0,1}[[:space:]]*target[[:space:]]+([[:alnum:]_$]+)\.([[:alnum:]_$\?\*\-]+)[[:space:]]*,{0,1}[[:space:]]*([^;]*)`)

	repTables := make(map[string]repTable, 10)
	var groupDB string
	var linesFile int
	var linesMatched int
	for _, line := range lines {
		trimmedLine := bytes.TrimSpace(line)
		upperLine := bytes.ToUpper(line)
		if len(trimmedLine) > 1 && string(trimmedLine[:2]) == "--" { // Строки, начинающие с комментария просто пропускаем
			continue
		}

		// Рекурсивная обработка директивы obey
		if bytes.Contains(upperLine, []byte("OBEY")) {
			obeyFileN := string(bytes.TrimSpace(trimmedLine[5:]))
			if obeyFileN[:2] == "./" {
				obeyFileN = ggsciBinary[:strings.LastIndex(ggsciBinary, "/")] + obeyFileN[1:]
			} else if obeyFileN[:1] != "/" {
				obeyFileN = ggsciBinary[:strings.LastIndex(ggsciBinary, "/")+1] + obeyFileN
			}

			fileBytes, err := ioutil.ReadFile(obeyFileN)
			if err != nil {
				log.Fatal("Error reading obey file ", obeyFileN, err)
			}
			if fdebug {
				fmt.Printf("Opened obey file %s\n", obeyFileN)
			}
			buff := bytes.NewBuffer(fileBytes)
			maps, db := processParams(*buff)
			if db != "" {
				groupDB = db

			}
			for k, v := range maps {
				repTables[k] = v
			}
		}

		// Ищем предложения MAP OWNER.NAME TARGET OWNER.NAME [params];
		matches := re.FindSubmatch(trimmedLine)
		linesFile++
		if len(matches) > 0 {
			if fdebug {
				fmt.Printf("\t%s.%s >> %s.%s, tail: %s\n", matches[1], matches[2], matches[3], matches[4], matches[5])
			}
			repTables[strings.ToUpper(string(matches[3]))+"."+strings.ToUpper(string(matches[4]))] = repTable{matches[1], matches[2], matches[3], matches[4], matches[5]}
			linesMatched++
		}

		// Получаем tns базы данных, с которой работает процесс
		if authLine := bytes.TrimSpace(upperLine); bytes.Contains(authLine, []byte("USERID")) {
			if bytes.Contains(authLine, []byte("USERIDALIAS")) {
				alias := string(bytes.TrimSpace(authLine[12:]))
				dbconn, ok := aliases[alias]
				if !ok {
					log.Fatalln("Unable to find record for " + alias + " in credentialstore map")
				}
				groupDB = strings.Split(dbconn, string("@"))[1]
			} else { // USERID type of auth

				useridRE := regexp.MustCompile(`(?i)USERID.+@([[:alnum:]_$]+).+`)
				matches = useridRE.FindSubmatch(authLine)
				groupDB = string(matches[1])
			}
			if fdebug {
				log.Printf("Group DB is %s\n", groupDB)
			}
		}

	}

	if fdebug {
		fmt.Printf("\n%d lines in file\n%d lines matched\n", linesFile, linesMatched)
	}
	log.Printf("%d tables in map\n", len(repTables))
	return repTables, groupDB
}

func updateDB(group gGroup) {
	_, ok := dbConns[group.GroupDB]
	if !ok {
		dbcred := getDbCredByTns(group.GroupDB)
		if dbcred == "" {
			log.Println("No credentials for group DB specified: " + group.GroupDB + ". Skipping DB update")
			return
		}

		db, err := sql.Open("goracle", dbcred)
		if err != nil {
			log.Fatalln(err)
		}
		dbConns[group.GroupDB] = db
		if fdebug {
			log.Println("Successful DB connection")
		}
	}

	tx, err := dbConns[group.GroupDB].Begin() //db.Begin()
	if err != nil {
		log.Println("Error starting DB transaction: " + err.Error())
	}
	stmt, err := tx.Prepare("insert into tmp_replicated_tables values (:gn, :gt, :sto, :stn, :tto, :ttn, :par)")
	if err != nil {
		log.Println("Error preparing statement for GG group " + group.GroupName + ": " + err.Error())
	}

	var tmpRowsCnt int64
	for _, val := range group.GroupMaps {
		res, err := stmt.Exec(group.GroupName, group.GroupType, strings.ToUpper(string(val.srcOwner)), strings.ToUpper(string(val.srcName)), strings.ToUpper(string(val.tOwner)), strings.ToUpper(string(val.tName)), string(val.extParams)[:min(4000, len(val.extParams))])
		if err != nil {
			log.Println("Error inserting row in tmp_replicated_tables: " + err.Error())
		}
		rowsCnt, err := res.RowsAffected()
		if err != nil {
			log.Println("Error getting affected rows: " + err.Error())
		}
		tmpRowsCnt += rowsCnt
	}
	if fdebug {
		log.Println("Inserted " + strconv.FormatInt(tmpRowsCnt, 10) + " rows into tmp_replicated_tables")
	}
	err = stmt.Close()
	if err != nil {
		log.Println("Error closing statement: " + err.Error())
	}

	stmt, err = tx.Prepare(`delete from replicated_tables t
	where (not exists (select * from tmp_replicated_tables r where r.group_name = t.group_name 
																		and r.src_table_owner = t.src_table_owner 
																		and r.src_table_name = t.src_table_name 
																		and r.trg_table_owner = t.trg_table_owner 
																		and r.trg_table_name = t.trg_table_name )
	or not exists (select * from dba_tables dt where dt.owner = t.trg_table_owner and dt.table_name = t.trg_table_name))
	and t.group_name=:gn`)

	res, err := stmt.Exec(group.GroupName)
	if err != nil {
		log.Println("Error deleting row from replicated_tables: " + err.Error())
	}
	rowsCnt, err := res.RowsAffected()
	if err != nil {
		log.Println("Error getting affected rows: " + err.Error())
	}
	log.Println("   Deleted " + strconv.FormatInt(rowsCnt, 10) + " rows from replicated_tables")
	err = stmt.Close()
	if err != nil {
		log.Println("Error closing statement: " + err.Error())
	}

	stmt, err = tx.Prepare(`insert into replicated_tables 
	select group_name, group_type, src_table_owner, src_table_name, trg_table_owner, trg_table_name, ext_params, sysdate as ins_date
	from tmp_replicated_tables t
	inner join dba_tables dt on (dt.owner = t.trg_table_owner and dt.table_name = t.trg_table_name)
	where not exists (select * from replicated_tables r where r.group_name = t.group_name 
													and r.src_table_owner = t.src_table_owner 
													and r.src_table_name = t.src_table_name 
													and r.trg_table_owner = t.trg_table_owner 
													and r.trg_table_name = t.trg_table_name)`)

	res, err = stmt.Exec()
	if err != nil {
		log.Println("Error inserting row in replicated_tables: " + err.Error())
	}
	rowsCnt, err = res.RowsAffected()
	if err != nil {
		log.Println("Error getting affected rows:" + err.Error())
	}
	log.Println("   Inserted " + strconv.FormatInt(rowsCnt, 10) + " rows into replicated_tables")
	err = stmt.Close()
	if err != nil {
		log.Println("Error closing statement: " + err.Error())
	}

	_, err = tx.Exec("truncate table tmp_replicated_tables")
	if err != nil {
		log.Println("Error truncating tmp_replicated_tables: " + err.Error())
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Error commiting transaction: " + err.Error())
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func encrypt(key, text []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	b := base64.StdEncoding.EncodeToString(text)

	ciphertext := make([]byte, aes.BlockSize+len(b))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}
	cfb := cipher.NewCFBEncrypter(block, iv)

	cfb.XORKeyStream(ciphertext[aes.BlockSize:], []byte(b))
	var cipherstr string
	for _, val := range ciphertext {
		cipherstr += strconv.Itoa(int(val)) + " "
	}
	return cipherstr, nil
}

func decrypt(key []byte, textStr string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	// convert str to slice of bytes
	text := make([]byte, 0, 10)
	bytesStr := strings.Fields(textStr)
	for _, val := range bytesStr {
		if val != "" {
			num, _ := strconv.Atoi(val)
			text = append(text, byte(num))
		}
	}

	if len(text) < aes.BlockSize {
		return "", errors.New("ciphertext too short")
	}
	iv := text[:aes.BlockSize]
	text = text[aes.BlockSize:]
	cfb := cipher.NewCFBDecrypter(block, iv)
	cfb.XORKeyStream(text, text)
	data, err := base64.StdEncoding.DecodeString(string(text))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func getDbCredByTns(tns string) string {
	for _, val := range ConfigCreds {
		if strings.ToUpper(val.DbTNS) == tns {
			decPwd, err := decrypt(seckey, val.EncPassword)
			if err != nil {
				log.Fatalln("Error decrypting password: " + err.Error())
			}
			return val.Username + "/" + decPwd + "@" + val.DbTNS
		}
	}
	return ""
}
