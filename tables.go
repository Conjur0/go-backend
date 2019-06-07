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
	return value
}

type table struct {
	database     string
	name         string
	primaryKey   string
	keys         []string
	prune        bool
	transform    func(t *table, k *kpage) error
	purge        func(t *table, k *kjob) string
	_columnOrder []string
	duplicates   string
	proto        []string
	strategy     func(t *table, k *kjob) (error, string)
	tail         string
}

func (t *table) columnOrder() string {
	var b strings.Builder
	comma := ","
	length := len(t._columnOrder)
	b.Grow(length * 12)
	length--
	for it := range t._columnOrder {
		if it == length {
			comma = ""
		}
		fmt.Fprintf(&b, "%s%s", t._columnOrder[it], comma)
	}
	return b.String()
}
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

var tables = make(map[string]*table)

func tablesInit() {
	tablesInitorders()
	tablesInitetag()
	tablesInitspec()
	for it := range tables {
		statement, err := database.Prepare(tables[it].create())
		if err != nil {
			panic(err)
		}
		_, err = statement.Exec()
		if err != nil {
			panic(err)
		}
		log("tables.go:initTables()", fmt.Sprintf("Initialized table %s", it))
	}

	log("tables.go:initTables()", "Initialization Complete!")
}
