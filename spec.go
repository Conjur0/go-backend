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

// initialize spec tables definition
func specInit() {
	tables["spec"] = &table{
		database:   "karkinos",
		name:       "spec",
		primaryKey: "method:spec:endpoint",
		_columnOrder: []string{
			"method",
			"spec",
			"endpoint",
			"security",
			"cache",
			"items",
			"paged",
		},
		proto: []string{
			"method VARCHAR(12) NOT NULL",
			"spec VARCHAR(10) NOT NULL",
			"endpoint VARCHAR(110) NOT NULL",
			"security VARCHAR(110) NOT NULL",
			"cache INT NOT NULL",
			"items INT NOT NULL",
			"paged TINYINT NOT NULL",
		},
		tail: " ENGINE=InnoDB DEFAULT CHARSET=latin1;",
	}
}

// get the stored spec information from SQL
func getSpec(method string, specnum string, endpoint string) specS {
	row := database.QueryRow(fmt.Sprintf("SELECT security,cache,items,paged FROM `%s`.`%s` WHERE method=? AND spec=? AND endpoint=?", tables["spec"].database, tables["spec"].name), method, specnum, endpoint)
	var specc specS
	err := row.Scan(&specc.security, &specc.cache, &specc.items, &specc.paged)
	if err != nil {
		specc.invalid = true
		return specc
	}
	return specc
}
