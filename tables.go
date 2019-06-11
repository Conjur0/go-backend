// tables definitions

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

func (s eveDate) toktime() uint64 {
	parse, err := time.Parse("2006-01-02T15:04:05Z", string(s))
	if err != nil {
		return 0
	}
	return uint64(parse.UnixNano() / int64(time.Millisecond))
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

// table definition
type table struct {
	DB               string               `json:"db"`                    //Database Name
	Name             string               `json:"name"`                  //Table Name
	PrimaryKey       string               `json:"primary_key,omitempty"` //(uint64)Primary BTREE Index (multiple fields separated with :)
	ChangedKey       string               `json:"changed_key,omitempty"` //what to poll to see if the record needs to be updated (uint64)
	JobKey           string               `json:"job_key,omitempty"`     //what ties this row to the queried entity/source
	Keys             map[string]string    `json:"keys,omitempty"`        //Other Indexes (multiple fields separated with :)
	UniqueKeys       map[string]string    `json:"unique_keys,omitempty"` //Other other indexes (multiple fields separated with :)
	ColumnOrder      []string             `json:"column_order,omitempty"`
	Duplicates       string               `json:"duplicates,omitempty"`
	Proto            []string             `json:"proto,omitempty"`
	Tail             string               `json:"tail,omitempty"` //" ENGINE=InnoDB DEFAULT CHARSET=latin1;" appended to the end of the create table query
	handleStart      func(k *kjob) error  //Called when job started
	handlePageData   func(k *kpage) error //Called to process NEW (200) page data
	handlePageCached func(k *kpage) error //Called to process OLD (304) page data
	handleWriteIns   func(k *kjob) int64  //Called when len(Ins) > sql_ins_threshold, to INSERT data, returns number of INSERTed records
	handleWriteUpd   func(k *kjob) int64  //Called when len(InsIds) > sql_ins_threshold, to UPDATE data, returns number of UPDATEd records
	handleEndGood    func(k *kjob) int64  //Called when job completes successfully, returns number of DELETEd records
	handleEndFail    func(k *kjob)        //Called when job fails
}

// return concatenated _columnOrder
func (t *table) columnOrder() string {
	var b strings.Builder
	comma := ""
	for it := range t.ColumnOrder {
		b.WriteString(comma)
		b.WriteString(t.ColumnOrder[it])
		comma = ","
	}
	return b.String()
}

// return CREATE TABLE IF NOT EXISTS
func (t *table) create() string {
	var b strings.Builder
	comma := ""

	//create part...
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS `%s`.`%s` (", t.DB, t.Name)
	//fields
	for it := range t.Proto {
		fmt.Fprintf(&b, "%s\n    %s", comma, t.Proto[it])
		comma = ","
	}

	//primary key
	prim := strings.Split(t.PrimaryKey, ":")
	if len(prim) > 0 {
		comma := ""
		b.WriteString(",\n    PRIMARY KEY (")
		for it := range prim {
			fmt.Fprintf(&b, "%s`%s`", comma, prim[it])
			comma = ","
		}
		b.WriteString(")")
	}

	//keys
	for it := range t.Keys {
		k := strings.Split(t.Keys[it], ":")
		comma := ""
		fmt.Fprintf(&b, ",\n    KEY `%s`(", it)
		for itt := range k {
			fmt.Fprintf(&b, "%s`%s`", comma, k[itt])
			comma = ","
		}
		b.WriteString(")")
	}

	//unique keys
	for it := range t.UniqueKeys {
		k := strings.Split(t.UniqueKeys[it], ":")
		comma := ""
		fmt.Fprintf(&b, ",\n    UNIQUE KEY `%s`(", it)
		for itt := range k {
			fmt.Fprintf(&b, "%s`%s`", comma, k[itt])
			comma = ","
		}
		b.WriteString(")")
	}
	fmt.Fprintf(&b, "\n)%s\n", t.Tail)
	return b.String()
}

// call table_*.go init functions, create tables if needed
func tablesInit() {
	tablesInitorders()
	tablesInitcontracts()
	for it := range c.Tables {
		safeExec(c.Tables[it].create())
		log(nil, fmt.Sprintf("Initialized table %s", it))
	}
	//log(nil, "Initialization Complete!")
}
