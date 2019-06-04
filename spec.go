//////////////////////////////////////////////////////////////////////////////////
// spec.go - ESI swagger.json Maintenance
//////////////////////////////////////////////////////////////////////////////////
//  lWritespec(obj, specc):  Updates `spec[]`
//  updateSpec():  Loads `spec/v*.json` into `spec[]`
//  getSpec(fName):  Checks age, and if neccessary, downloads updated swagger.json
//  readSpec(obj):  Loads spec/`obj`.json into `spec[]`

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var spec map[string]interface{}
var specMutex = sync.RWMutex{}

func lWritespec(obj string, specc interface{}) {
	specMutex.Lock()
	spec["/"+obj] = specc
	//log("spec.go:lWritespec()", fmt.Sprintf("set spec[%s] to %s", obj, specc))
	specMutex.Unlock()
}
func updateSpec() {
	//Open spec folder
	dir, err := os.Open("spec/")
	if err != nil {
		log("spec.go:updateSpec() os.Open", err)
		return
	}
	//Read contents of directory
	fileInfo, err := dir.Readdir(-1)
	if err != nil {
		log("spec.go:updateSpec() dir.Readdir", err)
		return
	}
	dir.Close()
	for _, file := range fileInfo {
		fName := file.Name()
		if matched, _ := regexp.MatchString(`^.+\.json$`, fName); matched == false {
			continue
		}
		obj := strings.TrimRight(fName, filepath.Ext(fName))
		fAge := time.Now().Unix() - file.ModTime().Unix()
		if fAge > 3600 {
			log("spec.go:updateSpec()", fmt.Sprintf("%s (%s) is %ds old, requesting new...", fName, obj, fAge))
			go getSpec(fName)
		} else {
			log("spec.go:updateSpec()", fmt.Sprintf("%s (%s) is %ds old, still ok...", fName, obj, fAge))
		}
	}
}
func getSpec(fName string) {
	obj := strings.TrimRight(fName, filepath.Ext(fName))
	downloadPath := fmt.Sprintf("/%s/swagger.json?datasource=tranquility", obj)
	addMetric(downloadPath)
	etaghdr := getEtag(downloadPath)

	req, err := http.NewRequest("GET", esiURL+downloadPath, nil)
	if err != nil {
		log("spec.go:getSpec("+fName+") http.NewRequest", err)
		return
	}
	if len(etaghdr) > 0 {
		req.Header.Add("If-None-Match", etaghdr)
	}
	resp, err := client.Do(req)
	if err != nil {
		log("getSpec("+fName+") client.Do", err)
		return
	}
	if resp.StatusCode == 200 {
		defer resp.Body.Close()
		if _, ok := resp.Header["Etag"]; ok {
			go setEtag(downloadPath, resp.Header["Etag"][0], []byte(""))
		}
		out, err := os.Create("spec/" + fName + ".tmp")
		if err != nil {
			log("spec.go:getSpec("+fName+") os.Create", err)
			return
		}
		defer out.Close()

		if _, err = io.Copy(out, resp.Body); err != nil {
			log("spec.go:getSpec("+fName+") io.Copy", err)
			return
		}
		out.Close()
		safeMove("spec/"+fName+".tmp", "spec/"+fName)
		go readSpec(obj)
		log("spec.go:getSpec("+fName+")", fmt.Sprintf("RCVD (200) %s in %dms", downloadPath, getMetric(downloadPath)))

	} else if resp.StatusCode == 304 {
		currenttime := time.Now()
		if err := os.Chtimes("spec/"+fName, currenttime, currenttime); err != nil {
			log("spec.go:getSpec("+fName+") os.Chtimes", err)
			return
		}
		log("spec.go:getSpec("+fName+")", fmt.Sprintf("RCVD (304) %s in %dms", downloadPath, getMetric(downloadPath)))
	}
}
func readSpec(obj string) {
	addMetric("spec/" + obj + ".json")
	jsonFile, err := os.Open("spec/" + obj + ".json")
	if err != nil {
		log("spec.go:readSpec("+obj+") os.Open", err)
		return
	}
	defer jsonFile.Close()
	byteValue, errr := ioutil.ReadAll(jsonFile)
	if errr != nil {
		log("spec.go:readSpec("+obj+") ioutil.ReadAll", err)
		return
	}
	var tmp interface{}
	if err = json.Unmarshal(byteValue, &tmp); err != nil {
		log("spec.go:readSpec("+obj+") json.Unmarshal", err)
		return
	}
	lWritespec(obj, tmp)
	log("spec.go:readSpec("+obj+")", fmt.Sprintf("Parsed spec/%s.json %db, in %dms", obj, len(byteValue), getMetric("spec/"+obj+".json")))

}
