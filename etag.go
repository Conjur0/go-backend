//////////////////////////////////////////////////////////////////////////////////
// etag.go - eTag Interface
//////////////////////////////////////////////////////////////////////////////////
//  readEtags(): reads json `etagFile`, and unmarshals into `etag`
//  writeEtags(): Marshals contents of `etag` to json `etagFile` if `etagDirty`
//  getEtag(cip): returns the Entity Tag for the CIP
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
var getetagdata *sql.Stmt

var setetag *sql.Stmt
var killetag *sql.Stmt

//MYSQL:
func etagInit() {
	var err error
	query := fmt.Sprintf("SELECT etag FROM `%s`.`%s` WHERE cip = ? LIMIT 1", tables["etag"].database, tables["etag"].name)
	getetag, err = database.Prepare(query)
	if err != nil {
		log("etag.go:etagInit()", err)
		panic(err)
	}

	query = fmt.Sprintf("SELECT ids,len FROM `%s`.`%s` WHERE cip = ? LIMIT 1", tables["etag"].database, tables["etag"].name)
	getetagids, err = database.Prepare(query)
	if err != nil {
		log("etag.go:etagInit()", err)
		panic(err)
	}

	query = fmt.Sprintf("SELECT data FROM `%s`.`%s` WHERE cip = ? LIMIT 1", tables["etag"].database, tables["etag"].name)
	getetagdata, err = database.Prepare(query)
	if err != nil {
		log("etag.go:etagInit()", err)
		panic(err)
	}

	query = fmt.Sprintf("INSERT INTO `%s`.`%s` (cip,etag,data,ids,len) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE etag=VALUES(etag),data=VALUES(data),ids=VALUES(ids)", tables["etag"].database, tables["etag"].name)
	setetag, err = database.Prepare(query)
	if err != nil {
		log("etag.go:etagInit()", err)
		panic(err)
	}

	query = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE cip=?", tables["etag"].database, tables["etag"].name)
	killetag, err = database.Prepare(query)
	if err != nil {
		log("etag.go:etagInit()", err)
		panic(err)
	}
}

func getEtag(cip string) string {
	rows, err := getetag.Query(cip)
	if err != nil {
		log("etag.go:getEtag("+cip+")", err)
		return ""
	}
	rows.Next()
	defer rows.Close()
	err = rows.Err()
	var out string
	rows.Scan(&out)
	if err != nil {
		log("etag.go:getEtag("+cip+")", err)
		return ""
	}
	return out
}

func getEtagData(cip string) string {
	rows, err := getetagdata.Query(cip)
	if err != nil {
		log("etag.go:getEtagData("+cip+")", err)
		return ""
	}
	rows.Next()
	defer rows.Close()
	err = rows.Err()
	var out string
	rows.Scan(&out)
	if err != nil {
		log("etag.go:getEtagData("+cip+")", err)
		return ""
	}
	return out
}
func getEtagIds(cip string) (string, int) {
	rows, err := getetagids.Query(cip)
	if err != nil {
		log("etag.go:getEtagIds("+cip+")", err)
		return "", 0
	}
	rows.Next()
	defer rows.Close()
	err = rows.Err()
	var out string
	var length int
	rows.Scan(&out, &length)
	if err != nil {
		log("etag.go:getEtagIds("+cip+")", err)
		return "", 0
	}
	return out, length
}

func setEtag(cip string, tag string, value string, ids string) {
	if len(value) == 0 || len(ids) == 0 {
		log("etag.go:setEtag("+cip+")", "Invalid Data received")
		return
	}
	_, err := setetag.Exec(cip, tag, value, ids, len(value))
	if err != nil {
		log("etag.go:setEtag("+cip+")", err)
	}
}

func killEtag(cip string) {
	_, err := killetag.Exec(cip)
	if err != nil {
		log("etag.go:killEtag("+cip+")", err)
	}
}

/*
//REDIS:

func getEtag(cip string) string {
	data, err := redisClient.Get("cip:" + cip).Result()
	if err != nil {
		return ""
	}
	go redisClient.Expire("cip:"+cip, 1*time.Hour).Result()
	return data
}

func getEtagData(cip string) []byte {
	data, err := redisClient.Get("etag:" + cip).Result()
	if err != nil {
		return []byte("")
	}
	go redisClient.Expire("etag:"+cip, 1*time.Hour).Result()
	return []byte(data)
}

func setEtag(cip string, tag string, value []byte) {
	redisClient.SetNX("cip:"+cip, tag, 1*time.Hour).Err()
	redisClient.SetNX("etag:"+cip, value, 1*time.Hour).Err()
}

func killEtag(cip string) {
	redisClient.Del("cip:" + cip).Err()
	redisClient.Del("etag:" + cip).Err()
}
*/
