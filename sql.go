//////////////////////////////////////////////////////////////////////////////////
// sql.go - sql interface
//////////////////////////////////////////////////////////////////////////////////
package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

var database *sql.DB

func sqlInit() {
	var err error

	log("sql.go:sqlInit()", "attempting to connect to SQL server, user:"+c.Mariadb.User)
	database, err = sql.Open("mysql", c.Mariadb.User+":"+c.Mariadb.Pass+"@tcp(127.0.0.1:3306)/")
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
