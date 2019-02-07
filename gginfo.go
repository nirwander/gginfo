package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "gopkg.in/goracle.v2"
)

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

// const configFile = `grafana.json`

// cmd flags
var fdebug bool
var ggsciBinary string

var aliases map[string]string
var dbConns map[string]*sql.DB

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
	GroupName   string
	GroupType   string
	GroupStatus string
	GroupDB     string
	GroupMaps   map[string]repTable
}

var ggGroups []gGroup

func init() {
	const (
		defaultDebug = false
		debugUsage   = "set debug=true to get output data in StdOut (and additional info) instead of sending to DB"
		defaultGgsci = "ggsci"
		ggsciUsage   = "set full path to ggsci binary"
	)
	flag.BoolVar(&fdebug, "debug", defaultDebug, debugUsage)
	flag.StringVar(&ggsciBinary, "ggsci", defaultGgsci, ggsciUsage)
}

func main() {

	start := time.Now()
	//Разворачиваем аргументы
	flag.Parse()

	// processReport(`C:\Users\wander\go\xfecr.txt`)
	// getConfig()
	dbConns = make(map[string]*sql.DB)

	getCredStoreInfo()

	getGroupInfo()

	for i, grp := range ggGroups {
		log.Printf("Getting info for group %s\n", grp.GroupName)
		if grp.GroupStatus == string("RUNNING") {
			out := execCmd(ggsciBinary, "view report "+grp.GroupName)
			if grp.GroupType == string("REPLICAT") {
				ggGroups[i].GroupMaps, ggGroups[i].GroupDB = processReport(out)

				if ggGroups[i].GroupDB == "" || len(ggGroups[i].GroupMaps) == 0 /* защита от пустого отчета, когда иногда в отчет не попадают MAP директивы */ {
					continue // Пропускаем этап вставки в БД, если БД для группы не указана
				}
				updateDB(ggGroups[i])
			}
		}
	}

	defer func() {
		for _, cn := range dbConns {
			cn.Close()
		}
	}()

	// var cnt int64
	// err := db.QueryRow("select count(*) from replicated_tables").Scan(&cnt)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("Table records count: %v\n", cnt)

	fmt.Printf("\n%s time spent\n", time.Since(start))
}

func getGroupInfo() {
	log.Println("Getting all groups info")

	out := execCmd(ggsciBinary, "info all")
	if fdebug {
		log.Printf("Got %d bytes\n", out.Len())
	}
	ggGroups = make([]gGroup, 1)
	lines := bytes.Split(out.Bytes(), []byte("\n"))
	var ggGroup gGroup
	for _, line := range lines {
		// fmt.Printf("%s\n", line)
		if bytes.Contains(line, []byte("EXTRACT")) || bytes.Contains(line, []byte("REPLICAT")) {
			props := bytes.Fields(line)
			ggGroup.GroupType = string(props[0])
			ggGroup.GroupStatus = string(props[1])
			ggGroup.GroupName = string(props[2])
			ggGroups = append(ggGroups, ggGroup)
		}
	}
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
		// fmt.Printf("%s\n", line)
		if bytes.Contains(line, []byte("Alias:")) {
			currAlias = strings.ToUpper(string(bytes.TrimLeft(bytes.TrimSpace(line), string("Alias: "))))
			continue
		}
		if currAlias != "" {
			aliases[currAlias] = strings.ToUpper(string(bytes.TrimLeft(bytes.TrimSpace(line), string("Userid: "))))
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
	// cmd.Stdin = bytes.NewBuffer([]byte("info all"))
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

	repTables := make(map[string]repTable)
	var groupDB string
	var linesFile int
	var linesMatched int
	for _, line := range lines {
		// Ищем предложения MAP OWNER.NAME TARGET OWNER.NAME [params];
		// fmt.Printf("%d: %s", i, line)
		matches := re.FindSubmatch(line)
		linesFile++
		if len(matches) > 0 {
			// fmt.Printf("%q\n", matches)
			if fdebug {
				fmt.Printf("\t%s.%s >> %s.%s, tail: %s\n", matches[1], matches[2], matches[3], matches[4], matches[5])
			}
			repTables[strings.ToUpper(string(matches[3]))+"."+strings.ToUpper(string(matches[4]))] = repTable{matches[1], matches[2], matches[3], matches[4], matches[5]}
			// str := string(matches[3]) + "." + string(matches[4])
			// fmt.Printf("\t%s\n", str)
			linesMatched++
		}

		// Получаем tns базы данных, с которой работает процесс
		if bytes.Contains(bytes.ToUpper(line), []byte("USERID")) {
			authLine := bytes.ToUpper(line)
			if bytes.Contains(authLine, []byte("USERIDALIAS")) {
				alias := string(bytes.TrimSpace(bytes.TrimLeft(bytes.TrimSpace(authLine), string("USERIDALIAS"))))
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

func updateDB(group gGroup) {
	_, ok := dbConns[group.GroupDB]
	if !ok {
		var dbcred string
		switch group.GroupDB {
		case "REPDB_GG":
			dbcred = "fe_gg/**@repdb"
		case "STATDB":
			dbcred = "ggate/**@statdb"
		case "UAT":
			dbcred = "ggate/**@uat"
		case "DEV":
			dbcred = "ggate/**@dev"
		case "GG_STF":
			dbcred = "ggate/**@dwx"
		case "GG_KV":
			dbcred = "ggate/**@dwx"
		case "GG_FE":
			dbcred = "ggate/**@dwx"
		case "GG_NW":
			dbcred = "ggate/**@dwx"
		case "GG_GFM":
			dbcred = "ggate/**@dwx"
		case "GG_SF":
			dbcred = "ggate/**@dwx"
		case "GG_PF":
			dbcred = "ggate/**@dwx"
		case "GG_UR":
			dbcred = "ggate/**@dwx"
		default:
			log.Fatalln("No credentials for group DB specified: " + group.GroupDB)
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
	stmt, err = tx.Prepare(`insert into replicated_tables 
	select group_name, group_type, src_table_owner, src_table_name, trg_table_owner, trg_table_name, ext_params, sysdate as ins_date
	from tmp_replicated_tables t
	where not exists (select * from replicated_tables r where r.group_name = t.group_name 
																	and r.src_table_owner = t.src_table_owner 
																	and r.src_table_name = t.src_table_name 
																	and r.trg_table_owner = t.trg_table_owner 
																	and r.trg_table_name = t.trg_table_name )
	and exists (select * from dba_tables dt where dt.owner = t.trg_table_owner and dt.table_name = t.trg_table_name)`)

	res, err := stmt.Exec()
	if err != nil {
		log.Println("Error inserting row in replicated_tables: " + err.Error())
	}
	rowsCnt, err := res.RowsAffected()
	if err != nil {
		log.Println("Error getting affected rows:" + err.Error())
	}
	log.Println("   Inserted " + strconv.FormatInt(rowsCnt, 10) + " rows into replicated_tables")
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

	res, err = stmt.Exec(group.GroupName)
	if err != nil {
		log.Println("Error deleting row from replicated_tables: " + err.Error())
	}
	rowsCnt, err = res.RowsAffected()
	if err != nil {
		log.Println("Error getting affected rows: " + err.Error())
	}
	log.Println("   Deleted " + strconv.FormatInt(rowsCnt, 10) + " rows from replicated_tables")
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
