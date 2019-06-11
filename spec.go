// ESI Spec data management

package main

import "fmt"

type specS struct {
	invalid  bool
	security string
	cache    int
	items    int
	paged    bool
}

// get the stored spec information from SQL
func getSpec(method string, specnum string, endpoint string) specS {
	row := database.QueryRow(fmt.Sprintf("SELECT security,cache,items,paged FROM `%s`.`%s` WHERE method=? AND spec=? AND endpoint=?", c.Tables["spec"].DB, c.Tables["spec"].Name), method, specnum, endpoint)
	var specc specS
	err := row.Scan(&specc.security, &specc.cache, &specc.items, &specc.paged)
	if err != nil {
		specc.invalid = true
		return specc
	}
	return specc
}
