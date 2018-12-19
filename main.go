package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"time"
)

// const configFile = `grafana.json`
const bin = `/home/oracle/app/ggate/ggsci`

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
		debugUsage   = "set debug=true to get output data in StdOut instead of sending to DB"
	)
	flag.BoolVar(&fdebug, "debug", defaultDebug, debugUsage)
}

func main() {

	start := time.Now()
	//Разворачиваем аргументы
	flag.Parse()

	// processReplicatReport(`C:\Users\wander\go\xfecr.txt`)
	// getConfig()

	getCredStoreInfo()

	getGroupInfo()

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
				log.Println(ggGroup)
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
