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
		respKey:    "cip",
		transform: func(t *table, k *kpage) error {
			return nil
		},
		purge: func(t *table, k *kjob) string {
			return "FALSE"
		},
		keys: []string{
			"etag",
		},
		_columnOrder: []string{
			"cip",
			"etag",
			"data",
			"ids",
		},
		duplicates: "ON DUPLICATE KEY UPDATE etag=VALUES(etag),data=VALUES(data),ids=VALUES(ids)",
		proto: []string{
			"cip varchar(250) NOT NULL",
			"etag varchar(250) NOT NULL",
			"data mediumtext NOT NULL",
			"ids mediumtext DEFAULT NULL",
			"len int(11) DEFAULT NULL",
		},
		tail: " ENGINE=InnoDB DEFAULT CHARSET=latin1;",
	}
}
