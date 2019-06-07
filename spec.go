//////////////////////////////////////////////////////////////////////////////////
// spec.go - ESI swagger.json Maintenance
//////////////////////////////////////////////////////////////////////////////////
//  lWritespec(obj, specc):  Updates `spec[]`
//  updateSpec():  Loads `spec/v*.json` into `spec[]`
//  getSpec(fName):  Checks age, and if neccessary, downloads updated swagger.json
//  readSpec(obj):  Loads spec/`obj`.json into `spec[]`

package main

func getSpec(method string, specnum string, endpoint string) specS {
	row := database.QueryRow("SELECT security,cache,items,paged FROM `karkinos`.`spec` WHERE method=? AND spec=? AND endpoint=?", method, specnum, endpoint)
	var specc specS
	err := row.Scan(&specc.security, &specc.cache, &specc.items, &specc.paged)
	if err != nil {
		specc.invalid = true
		return specc
	}
	return specc
}
