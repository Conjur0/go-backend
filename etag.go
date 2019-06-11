//////////////////////////////////////////////////////////////////////////////////
// etag.go - eTag Interface
//////////////////////////////////////////////////////////////////////////////////
//  readEtags(): reads json `etagFile`, and unmarshals into `etag`
//  writeEtags(): Marshals contents of `etag` to json `etagFile` if `etagDirty`
//  getEtagData(cip): returns the stored data for the given CIP
//  setEtag(cip, tag, value): removes existing eTags for the given CIP, records the new tag and data, and marks the file as dirty
//  etagWriteTimerInit(): Timer Init (called once from main)

package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

var getetag *sql.Stmt
var getetagids *sql.Stmt

var setetag *sql.Stmt
var killetag *sql.Stmt

// initialize eTag prepared queries
func etagInit() {
	var err error
	table := c.Tables["etag"]
	getetag, err = database.Prepare(fmt.Sprintf("SELECT etag FROM `%s`.`%s` WHERE cip = ? LIMIT 1", table.DB, table.Name))
	if err != nil {
		log(nil, err)
		panic(err)
	}

	getetagids, err = database.Prepare(fmt.Sprintf("SELECT ids,len FROM `%s`.`%s` WHERE cip = ? LIMIT 1", table.DB, table.Name))
	if err != nil {
		log(nil, err)
		panic(err)
	}

	setetag, err = database.Prepare(fmt.Sprintf("INSERT INTO `%s`.`%s` (cip,etag,ids,len) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE etag=VALUES(etag),ids=VALUES(ids),len=VALUES(len)", table.DB, table.Name))
	if err != nil {
		log(nil, err)
		panic(err)
	}

	killetag, err = database.Prepare(fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE cip=?", table.DB, table.Name))
	if err != nil {
		log(nil, err)
		panic(err)
	}
}

//  returns the Entity Tag for the given CIP
func getEtag(cip string) string {
	rows, err := getetag.Query(cip)
	if err != nil {
		log(cip, err)
		return ""
	}
	rows.Next()
	defer rows.Close()
	err = rows.Err()
	var out string
	rows.Scan(&out)
	if err != nil {
		log(cip, err)
		return ""
	}
	return out
}

// returns cached list of ids, and original data length, for the given CIP
func getEtagIds(cip string) (string, int) {
	rows, err := getetagids.Query(cip)
	if err != nil {
		log(cip, err)
		return "", 0
	}
	rows.Next()
	defer rows.Close()
	var out string
	var length int
	err = rows.Scan(&out, &length)
	if err != nil {
		log(cip, err)
		return "", 0
	}
	return out, length
}

// stores list of ids, and data length, for the given CIP
func setEtag(cip string, etag string, ids string, length int) {
	if length == 0 {
		log(cip, "Invalid Data Received!")
		return
	}
	_, err := setetag.Exec(cip, etag, ids, length)
	if err != nil {
		log(cip, err)
	}
}

// removes stored data for the given CIP
func killEtag(cip string) {
	_, err := killetag.Exec(cip)
	if err != nil {
		log(cip, err)
	}
}
