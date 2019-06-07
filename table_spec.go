//////////////////////////////////////////////////////////////////////////////////
// table_spec.go - `spec` table definition
//////////////////////////////////////////////////////////////////////////////////
//
package main

func tablesInitspec() {
	tables["spec"] = &table{
		database:   "karkinos",
		name:       "spec",
		primaryKey: "method:spec:endpoint",
		keys:       []string{},
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

type specS struct {
	method   string
	spec     string
	endpoint string
	security string
	cache    int
	items    int
	paged    bool
	status   string
}
