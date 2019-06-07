//////////////////////////////////////////////////////////////////////////////////
// table_etag.go - `orders` table definition
//////////////////////////////////////////////////////////////////////////////////
//
package main

func tablesInitetag() {
	tables["etag"] = &table{
		database:   "karkinos",
		name:       "etag",
		primaryKey: "cip",
		keys: []string{
			"etag",
		},
		_columnOrder: []string{
			"cip",
			"etag",
			"ids",
			"len",
		},
		duplicates: "ON DUPLICATE KEY UPDATE etag=VALUES(etag)",
		proto: []string{
			"cip varchar(250) NOT NULL",
			"etag varchar(250) NOT NULL",
			"ids mediumtext NOT NULL",
			"len int(11) DEFAULT NULL",
		},
		tail: " ENGINE=InnoDB DEFAULT CHARSET=latin1;",
	}
}
