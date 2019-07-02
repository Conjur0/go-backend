// `contracts` table definition

package main

import (
	"encoding/json"
	"fmt"
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
	c.Tables["contracts"].handlePageData = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		var contract contracts
		if err := json.Unmarshal(k.body, &contract); err != nil {
			log(err)
			return err
		}
		k.recs = int64(len(contract))
		k.ins.Grow(len(contract) * 256)
		k.ids.Grow(len(contract) * 10)

		for it := range contract {
			k.records++
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
					k.recordsChanged++
					fmt.Fprintf(&k.ins, "%s(%s,%s,%d,%d,%s,%f,%f,%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%f,%f,%d,%s,%s,%s,%f)", k.inscomma, k.job.Source, k.job.Owner, contract[it].AcceptorID, contract[it].AssigneeID, contract[it].Availability.ifnull(), contract[it].Buyout, contract[it].Collateral, contract[it].ContractID, contract[it].DateAccepted.toSQLDate(), contract[it].DateCompleted.toSQLDate(), contract[it].DateExpired.toSQLDate(), contract[it].DateIssued.toSQLDate(), contract[it].DaysToComplete, contract[it].EndLocationID, contract[it].ForCorporation.toSQL(), contract[it].IssuerCorporationID, contract[it].IssuerID, contract[it].Price, contract[it].Reward, contract[it].StartLocationID, contract[it].Status.ifnull(), contract[it].Title.escape(), contract[it].Type.ifnull(), contract[it].Volume)
					k.inscomma = ","
					k.insrecs++
				} else {
					k.recordsStale++
					// exists in database and order has not changed, no-op
				}
				delete(k.job.sqldata, uint64(contract[it].ContractID)) //remove matched items from the map
			} else {
				k.recordsNew++
				k.job.allsqldata[uint64(contract[it].ContractID)] = contractStatus[contract[it].Status.ifnull()]
				fmt.Fprintf(&k.ins, "%s(%s,%s,%d,%d,%s,%f,%f,%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%f,%f,%d,%s,%s,%s,%f)", k.inscomma, k.job.Source, k.job.Owner, contract[it].AcceptorID, contract[it].AssigneeID, contract[it].Availability.ifnull(), contract[it].Buyout, contract[it].Collateral, contract[it].ContractID, contract[it].DateAccepted.toSQLDate(), contract[it].DateCompleted.toSQLDate(), contract[it].DateExpired.toSQLDate(), contract[it].DateIssued.toSQLDate(), contract[it].DaysToComplete, contract[it].EndLocationID, contract[it].ForCorporation.toSQL(), contract[it].IssuerCorporationID, contract[it].IssuerID, contract[it].Price, contract[it].Reward, contract[it].StartLocationID, contract[it].Status.ifnull(), contract[it].Title.escape(), contract[it].Type.ifnull(), contract[it].Volume)
				k.inscomma = ","
				k.insrecs++
			}
		}
		k.pageMutex.Unlock()
		k.job.jobMutex.Unlock()
		return nil
	}
	c.Tables["contracts"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
}
