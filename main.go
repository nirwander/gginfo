package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"strings"
	"time"

	_ "gopkg.in/goracle.v2"
)

/*
 create table fe_gg.replicated_tables
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
*/

// const configFile = `grafana.json`
const bin = `/u00/ggate18/ggsci`

//const bin = `/home/oracle/app/ggate/ggsci`

const dbcred = `fe_gg/hw8mpv2vt@repdb`

//const dbcred = `ggate/ggate@orcl`

// cmd flags
var fdebug bool
var aliases map[string]string

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
	)
	flag.BoolVar(&fdebug, "debug", defaultDebug, debugUsage)
}

func main() {

	start := time.Now()
	//Разворачиваем аргументы
	flag.Parse()

	// processReplicatReport(`C:\Users\wander\go\xfecr.txt`)
	// getConfig()
	db, err := sql.Open("goracle" /*os.Args[1]*/, dbcred)
	if err != nil {
		log.Println(err)
		return
	}
	defer db.Close()
	if fdebug {
		log.Println("Successful DB connection")
	}

	getCredStoreInfo()

	getGroupInfo()

	for i, grp := range ggGroups {
		if fdebug {
			log.Printf("Getting info for group %s\n", grp.GroupName)
		}
		if grp.GroupStatus == string("RUNNING") {
			out := execCmd(bin, "view report "+grp.GroupName)
			if grp.GroupType == string("REPLICAT") {
				ggGroups[i].GroupMaps = processReplicatReport(out)
			}

		}
	}

	// if fdebug {
	// 	fmt.Println(ggGroups)
	// }

	var cnt int64
	err = db.QueryRow("select count(*) from replicated_tables").Scan(&cnt)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Table records count: %v\n", cnt)

	fmt.Printf("\n%s time spent\n", time.Since(start))
}

func getGroupInfo() {
	if fdebug {
		log.Println("Getting all groups info")
	}

	out := execCmd(bin, "info all")
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
			if fdebug {
				fmt.Println(ggGroup)
			}
		}
	}
}

func getCredStoreInfo() {
	if fdebug {
		log.Println("Getting credential store info")
	}

	out := execCmd(bin, "info credentialstore")
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
			currAlias = string(bytes.TrimLeft(bytes.TrimSpace(line), string("Alias: ")))
			continue
		}
		if currAlias != "" {
			aliases[currAlias] = string(bytes.TrimLeft(bytes.TrimSpace(line), string("Userid: ")))
			currAlias = ""
		}
	}
	if fdebug {
		fmt.Println(aliases)
	}
}

func execCmd(bin string, cmdText string) bytes.Buffer {
	var out bytes.Buffer

	cmd := exec.Command(bin)
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

func processReplicatReport(report bytes.Buffer) map[string]repTable {

	lines := bytes.Split(report.Bytes(), []byte("\n"))
	re := regexp.MustCompile(`(?i)map[[:space:]]+([[:alnum:]_$]+)\.([[:alnum:]_$\?\*\-]+)[[:space:]]*,{0,1}[[:space:]]*target[[:space:]]+([[:alnum:]_$]+)\.([[:alnum:]_$\?\*\-]+)[[:space:]]*,{0,1}[[:space:]]*(.*);`)

	repTables := make(map[string]repTable)
	var c2 int
	var c3 int
	for _, line := range lines {
		// Ищем предложения MAP OWNER.NAME TARGET OWNER.NAME [params] ;
		//fmt.Printf("%d: %s", i, line)
		matches := re.FindSubmatch(line)
		c2++
		if len(matches) > 0 {
			//fmt.Printf("%q\n", matches)
			fmt.Printf("\t%s.%s >> %s.%s, tail: %s\n", matches[1], matches[2], matches[3], matches[4], matches[5])
			repTables[strings.ToUpper(string(matches[3]))+"."+strings.ToUpper(string(matches[4]))] = repTable{matches[1], matches[2], matches[3], matches[4], matches[5]}
			//str := string(matches[3]) + "." + string(matches[4])
			//fmt.Printf("\t%s\n", str)
			c3++
		}

		if bytes.Contains(line, []byte("Run Time Messages")) {
			break
		}
	}

	if fdebug {
		// fmt.Printf("%s exists\n", repTables["BIS.PHONE_HISTORIES"].srcOwner)
		// fmt.Printf("%s not exists\n", repTables["BIS.PHONE_HISTORIES2"].srcOwner)
		// if repTables["BIS.PHONE_HISTORIES2"].srcOwner == nil {
		// 	fmt.Println("not exists")
		// }
		fmt.Printf("\n%d lines in file\n%d lines matched\n", c2, c3)
		fmt.Printf("%d tables in map\n", len(repTables))
	}
	return repTables
}
