// sql interface

package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var database *sql.DB

// connect to SQL
func sqlInit() {
	var err error
	host, _ := os.Hostname()
	log("connect to SQL: " + host)
	database, err = sql.Open("mysql", c.SQL[host])
	if err != nil {
		panic("Unable to connect to SQL!")
	}
	log("SQL init complete")

}

// attempts the query 10 times, and panics if it fails
func safeExec(query string) int64 {
	var tries int
Again:
	statement, err := database.Prepare(query)
	defer statement.Close()
	if err != nil {
		var logquery string
		if len(query) > 505 {
			logquery = query[:250] + " ... " + query[len(query)-250:]
		} else {
			logquery = query
		}
		log(err, fmt.Sprintf("Query was: (%d)%s", len(query), logquery))
		tries++
		if tries < 11 {
			time.Sleep(1 * time.Second)
			goto Again
		}
		panic(query)
	} else {
		res, err := statement.Exec()
		if err != nil {
			var logquery string
			if len(query) > 505 {
				logquery = query[:250] + " ... " + query[len(query)-250:]
			} else {
				logquery = query
			}
			log(err, fmt.Sprintf("Query was: (%d)%s", len(query), logquery))
			tries++
			if tries < 11 {
				time.Sleep(1 * time.Second)
				goto Again
			}
			panic(query)
		} else {
			aff, err := res.RowsAffected()
			if err != nil {
				log(err)
				aff = 0
			}
			return aff
		}
	}
}

func safeQuery(query string) *sql.Rows {
	var tries int
Again:
	statement, err := database.Prepare(query)
	defer statement.Close()
	if err != nil {
		var logquery string
		if len(query) > 505 {
			logquery = query[:250] + " ... " + query[len(query)-250:]
		} else {
			logquery = query
		}
		log(err, fmt.Sprintf("Query was: (%d)%s", len(query), logquery))
		tries++
		if tries < 11 {
			time.Sleep(5 * time.Second)
			goto Again
		}
		panic(query)
	} else {
		res, err := statement.Query()
		if err != nil {
			var logquery string
			if len(query) > 505 {
				logquery = query[:250] + " ... " + query[len(query)-250:]
			} else {
				logquery = query
			}
			log(err, fmt.Sprintf("Query was: (%d)%s", len(query), logquery))
			tries++
			if tries < 11 {
				time.Sleep(5 * time.Second)
				goto Again
			}
			panic(query)
		} else {
			return res
		}
	}
}
func safePrepare(query string) *sql.Stmt {
	var tries int
PrepareAgain:
	statement, err := database.Prepare(query)
	if err != nil {
		var logquery string
		if len(query) > 505 {
			logquery = query[:250] + " ... " + query[len(query)-250:]
		} else {
			logquery = query
		}
		log(err, fmt.Sprintf("Query was: (%d)%s", len(query), logquery))
		tries++
		if tries < 11 {
			time.Sleep(5 * time.Second)
			goto PrepareAgain
		}
		panic(query)
	}
	return statement
}
