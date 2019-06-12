//////////////////////////////////////////////////////////////////////////////////
// table_contracts.go - `contracts` table definition
//////////////////////////////////////////////////////////////////////////////////
//
package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

var contractStatus = map[string]uint64{`NULL`: 1, `'outstanding'`: 1, `'in_progress'`: 2, `'finished_issuer'`: 3, `'finished_contractor'`: 4, `'finished'`: 5, `'cancelled'`: 6, `'rejected'`: 7, `'failed'`: 8, `'deleted'`: 9, `'reversed'`: 10, `'expired'`: 11}

type contracts []contract
type contract struct {
	AcceptorID          int32     `json:"acceptor_id"`
	AssigneeID          int32     `json:"assignee_id"`
	Availability        sQLenum   `json:"availability"`
	Buyout              float64   `json:"buyout"`
	Collateral          float64   `json:"collateral"`
	ContractID          int32     `json:"contract_id"`
	DateAccepted        eveDate   `json:"date_accepted"`
	DateCompleted       eveDate   `json:"date_completed"`
	DateExpired         eveDate   `json:"date_expired"`
	DateIssued          eveDate   `json:"date_issued"`
	DaysToComplete      int32     `json:"days_to_complete"`
	EndLocationID       int64     `json:"end_location_id"`
	ForCorporation      boool     `json:"for_corporation"`
	IssuerCorporationID int32     `json:"issuer_corporation_id"`
	IssuerID            int32     `json:"issuer_id"`
	Price               float64   `json:"price"`
	Reward              float64   `json:"reward"`
	StartLocationID     int64     `json:"start_location_id"`
	Status              sQLenum   `json:"status"`
	Title               sQLstring `json:"title"`
	Type                sQLenum   `json:"type"`
	Volume              float64   `json:"volume"`
}

func tablesInitcontracts() {
	c.Tables["contracts"].handleStart = func(k *kjob) error { //jobMutex is already locked for us.
		numRecords, ress := k.table.getAllData(k.Source)
		k.sqldata = make(map[uint64]uint64, numRecords)
		defer ress.Close()
		var key, data uint64
		for ress.Next() {
			ress.Scan(&key, &data)
			k.sqldata[key] = data
		}
		return nil
	}
	c.Tables["contracts"].handlePageData = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		var contract contracts
		if err := json.Unmarshal(k.body, &contract); err != nil {
			log(nil, k.body)
			return err
		}
		k.recs = int64(len(contract))
		k.ins.Grow(len(contract) * 256)
		k.upd.Grow(len(contract) * 256)
		k.ids.Grow(len(contract) * 10)

		for it := range contract {
			if contract[it].Status == "" {
				contract[it].Status = "outstanding"
			}
			if contract[it].Availability == "" {
				contract[it].Availability = "public"
			}
			fmt.Fprintf(&k.ids, "%s%d", k.idscomma, contract[it].ContractID)
			k.idscomma = ","
			if ord, ok := k.job.sqldata[uint64(contract[it].ContractID)]; ok {
				if ord != contractStatus[contract[it].Status.ifnull()] {
					fmt.Fprintf(&k.upd, "%s(%s,%s,%d,%d,%s,%f,%f,%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%f,%f,%d,%s,%s,%s,%f)", k.updcomma, k.job.Source, k.job.Owner, contract[it].AcceptorID, contract[it].AssigneeID, contract[it].Availability.ifnull(), contract[it].Buyout, contract[it].Collateral, contract[it].ContractID, contract[it].DateAccepted.toSQLDate(), contract[it].DateCompleted.toSQLDate(), contract[it].DateExpired.toSQLDate(), contract[it].DateIssued.toSQLDate(), contract[it].DaysToComplete, contract[it].EndLocationID, contract[it].ForCorporation.toSQL(), contract[it].IssuerCorporationID, contract[it].IssuerID, contract[it].Price, contract[it].Reward, contract[it].StartLocationID, contract[it].Status.ifnull(), contract[it].Title.escape(), contract[it].Type.ifnull(), contract[it].Volume)
					k.updcomma = ","
					k.updrecs++
				} else {
					// exists in database and order has not changed, no-op
				}
				delete(k.job.sqldata, uint64(contract[it].ContractID)) //remove matched items from the map
			} else {
				fmt.Fprintf(&k.ins, "%s(%s,%s,%d,%d,%s,%f,%f,%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%f,%f,%d,%s,%s,%s,%f)", k.inscomma, k.job.Source, k.job.Owner, contract[it].AcceptorID, contract[it].AssigneeID, contract[it].Availability.ifnull(), contract[it].Buyout, contract[it].Collateral, contract[it].ContractID, contract[it].DateAccepted.toSQLDate(), contract[it].DateCompleted.toSQLDate(), contract[it].DateExpired.toSQLDate(), contract[it].DateIssued.toSQLDate(), contract[it].DaysToComplete, contract[it].EndLocationID, contract[it].ForCorporation.toSQL(), contract[it].IssuerCorporationID, contract[it].IssuerID, contract[it].Price, contract[it].Reward, contract[it].StartLocationID, contract[it].Status.ifnull(), contract[it].Title.escape(), contract[it].Type.ifnull(), contract[it].Volume)
				k.inscomma = ","
				k.insrecs++
			}
		}
		k.pageMutex.Unlock()
		k.job.jobMutex.Unlock()
		return nil
	}
	c.Tables["contracts"].handlePageCached = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		defer k.pageMutex.Unlock()
		defer k.job.jobMutex.Unlock()
		contract := strings.Split(k.ids.String(), ",")
		k.recs = int64(len(contract))
		var contractID int
		var contractspurged int
		for it := range contract {
			contractID, _ = strconv.Atoi(contract[it])
			if _, ok := k.job.sqldata[uint64(contractID)]; ok {
				delete(k.job.sqldata, uint64(contractID)) //remove matched items from the map
				contractspurged++
			} else {
				// return fmt.Errorf("etag data does not match table %d not found in table", contractID)
			}

		}
		return nil
	}
	c.Tables["contracts"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
	c.Tables["contracts"].handleWriteUpd = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.updJob.String(), k.table.Duplicates))
	}
	c.Tables["contracts"].handleEndGood = func(k *kjob) int64 { //jobMutex is already locked for us.
		var delrecords int64
		if len(k.sqldata) > 0 {
			var b strings.Builder
			comma := ""
			for it := range k.sqldata {
				fmt.Fprintf(&b, "%s%d", comma, it)
				comma = ","
			}
			query := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN (%s)", k.table.DB, k.table.Name, k.table.PrimaryKey, b.String())
			delrecords = safeExec(query)
		}
		k.sqldata = make(map[uint64]uint64)
		return delrecords
	}
	c.Tables["contracts"].handleEndFail = func(k *kjob) { //jobMutex is already locked for us.
		k.sqldata = make(map[uint64]uint64)
		return
	}
}
