//////////////////////////////////////////////////////////////////////////////////
// sql.go - sql interface
//////////////////////////////////////////////////////////////////////////////////
package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var database *sql.DB

func sqlInit() {
	var err error

	log("sql.go:sqlInit()", "attempting to connect to SQL server, user:"+c.Mariadb.User)
	database, err = sql.Open("mysql", c.Mariadb.User+":"+c.Mariadb.Pass+"@tcp(127.0.0.1:3306)/?maxAllowedPacket=0")
	if err != nil {
		panic("Unable to connect to sql!")
	}
	log("sql.go:sqlInit()", "Initialization Complete!")
	//fmt.Printf("CREATE TABLE IF NOT EXISTS ")
	// statement, _ := database.Prepare("CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, firstname TEXT, lastname TEXT)")
	// statement.Exec()
	// statement, _ = database.Prepare("INSERT INTO people (firstname, lastname) VALUES (?, ?)")
	// statement.Exec("Nic", "Raboy")
	// rows, _ := database.Query("SELECT id, firstname, lastname FROM people")
	// var id int
	// var firstname string
	// var lastname string
	// for rows.Next() {
	// 	rows.Scan(&id, &firstname, &lastname)
	// 	fmt.Println(strconv.Itoa(id) + ": " + firstname + " " + lastname)
	// }
}
func safeQuery(query string) int64 {
Again:
	statement, err := database.Prepare(query)
	defer statement.Close()
	if err != nil {
		var tries int
		var logquery string
		if len(query) > 505 {
			logquery = query[:250] + " ... " + query[len(query)-250:]
		} else {
			logquery = query
		}
		log("sql.go:safeQuery() database.Prepare", err)
		log("sql.go:safeQuery() database.Prepare", fmt.Sprintf("Query was: (%d)%s", len(query), logquery))
		tries++
		if tries < 11 {
			time.Sleep(1 * time.Second)
			goto Again
		}
		panic(query)
	} else {
		res, err := statement.Exec()
		if err != nil {
			var tries int
			var logquery string
			if len(query) > 505 {
				logquery = query[:250] + " ... " + query[len(query)-250:]
			} else {
				logquery = query
			}
			log("sql.go:safeQuery() statement.Exec", err)
			log("sql.go:safeQuery() statement.Exec", fmt.Sprintf("Query was: (%d)%s", len(query), logquery))
			tries++
			if tries < 11 {
				time.Sleep(1 * time.Second)
				goto Again
			}
			panic(query)
		} else {
			aff, err := res.RowsAffected()
			if err != nil {
				log("sql.go:safeQuery() res.RowsAffected", err)
				aff = 0
			}
			return aff
		}
	}
}
