package main

import (
	"flag"
	"fmt"
	"time"
)

const configFile = `grafana.json`

// cmd flags
var fdebug bool

func init() {
	const (
		defaultDebug = false
		debugUsage   = "set debug=true to get output data in StdOut instead of sending to DB"
	)
	flag.BoolVar(&fdebug, "debug", defaultDebug, debugUsage)
}

func main() {

	start := time.Now()

	//processReplicatReport(`C:\Users\wander\go\xfecr.txt`)
	getConfig()

	getCredStoreInfo()

	fmt.Printf("\n%s time spent", time.Since(start))
}
