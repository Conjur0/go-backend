//////////////////////////////////////////////////////////////////////////////////
// tables.go - tables definitions
//////////////////////////////////////////////////////////////////////////////////
//
package main

import (
	"fmt"
	"strings"
)

type table struct {
	database     string
	name         string
	primaryKey   string
	keys         []string
	respKey      string
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
	comma := ","
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n", t.database, t.name)
	for it := range t.proto {
		fmt.Fprintf(&b, "    %s,\n", t.proto[it])
	}
	if len(t.keys) == 0 {
		comma = ""
	}
	fmt.Fprintf(&b, "    PRIMARY KEY (`%s`)%s\n", t.primaryKey, comma)
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
