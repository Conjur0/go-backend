//////////////////////////////////////////////////////////////////////////////////
// tables.go - tables definitions
//////////////////////////////////////////////////////////////////////////////////
//
package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var tables = make(map[string]*table)

type eveDate string

func (s eveDate) toSQLDate() string {
	parse, err := time.Parse("2006-01-02T15:04:05Z", string(s))
	if err != nil {
		return "NULL"
	}
	return strconv.Itoa(int(parse.UnixNano() / int64(time.Millisecond)))
}

type boool bool

func (b boool) toSQL() int {
	if b {
		return 1
	}
	return 0
}

type sQLstring string

func (s sQLstring) escape() string {
	replace := map[string]string{"'": `\'`, "\\0": "\\\\0", "\n": "\\n", "\r": "\\r", `"`: `\"`, "\x1a": "\\Z"}
	value := strings.Replace(string(s), `\`, `\\`, -1)
	for b, a := range replace {
		value = strings.Replace(value, b, a, -1)
	}
	return "'" + value + "'"
}

type sQLenum string

func (s sQLenum) ifnull() string {
	if s == "" {
		return "NULL"
	}
	return "'" + string(s) + "'"
}

type table struct {
	database     string   //Database Name
	name         string   //Table Name
	primaryKey   string   //Primary BTREE Index (multiple fields separated with :)
	keys         []string //Other Indexes (multiple fields separated with :)
	_columnOrder []string
	duplicates   string
	proto        []string
	strategy     func(t *table, k *kjob) (error, string)
	tail         string

	handleStart       func(t *table, k *kjob) error  //Called when job started, and Pages has been populated
	handlePageData    func(t *table, k *kpage) error //Called to process NEW (200) page data
	handlePageCached  func(t *table, k *kpage) error //Called to process OLD (304) page data
	handleWriteData   func(t *table, k *kjob) error  //Called when len(Ins) > sql_ins_threshold, to INSERT data
	handleWriteCached func(t *table, k *kjob) error  //Called when len(InsIds) > sql_ins_threshold, to UPDATE data
	handleEndGood     func(t *table, k *kjob) int64  //Called when job completes successfully, returns number of DELETED records
	handleEndFail     func(t *table, k *kjob)        //Called when job fails
}

// return concatenated _columnOrder
func (t *table) columnOrder() string {
	var b strings.Builder
	comma := ""
	for it := range t._columnOrder {
		b.WriteString(comma)
		b.WriteString(t._columnOrder[it])
		comma = ","
	}
	return b.String()
}

// return CREATE TABLE IF NOT EXISTS
func (t *table) create() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n", t.database, t.name)

	//fields
	for it := range t.proto {
		fmt.Fprintf(&b, "    %s,\n", t.proto[it])
	}

	//primary key
	prim := strings.Split(t.primaryKey, ":")
	comma := ""
	if len(prim) > 0 {
		b.WriteString("    PRIMARY KEY (")
		for it := range prim {
			fmt.Fprintf(&b, "%s`%s`", comma, prim[it])
			comma = ","
		}
	}
	comma = ","
	if len(t.keys) == 0 {
		comma = ""
	}
	fmt.Fprintf(&b, ")%s\n", comma)

	//keys
	length := len(t.keys) - 1
	for it := range t.keys {
		if it == length {
			comma = ""
		}
		fmt.Fprintf(&b, "    KEY `%s` (`%s`)%s\n", t.keys[it], t.keys[it], comma)

	}
	fmt.Fprintf(&b, ")%s\n", t.tail)
	return b.String()
}

// call table_*.go init functions, create tables if needed
func tablesInit() {
	tablesInitorders()
	tablesInitcontracts()
	for it := range tables {
		safeQuery(tables[it].create())
		log(nil, fmt.Sprintf("Initialized table %s", it))
	}

	log(nil, "Initialization Complete!")
}
