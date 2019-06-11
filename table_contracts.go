//////////////////////////////////////////////////////////////////////////////////
// table_contracts.go - `contracts` table definition
//////////////////////////////////////////////////////////////////////////////////
//
package main

import (
	"encoding/json"
	"errors"
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
	tables["contracts"] = &table{
		database:   "karkinos",
		name:       "contracts",
		primaryKey: "contract_id",
		changedKey: "status+0",
		jobKey:     "source",
		keys: map[string]string{
			"source":                "source",
			"owner":                 "owner",
			"acceptor_id":           "acceptor_id",
			"assignee_id":           "assignee_id",
			"end_location_id":       "end_location_id",
			"issuer_corporation_id": "issuer_corporation_id",
			"issuer_id":             "issuer_id",
			"start_location_id":     "start_location_id",
			"status":                "status",
			"type":                  "type",
			"date_accepted_hour":    "date_accepted_hour",
			"date_completed_hour":   "date_completed_hour",
			"date_expired_hour":     "date_expired_hour",
			"date_issued_hour":      "date_issued_hour",
		},
		_columnOrder: []string{
			"source",
			"owner",
			"acceptor_id",
			"assignee_id",
			"availability",
			"buyout",
			"collateral",
			"contract_id",
			"date_accepted",
			"date_completed",
			"date_expired",
			"date_issued",
			"days_to_complete",
			"end_location_id",
			"for_corporation",
			"issuer_corporation_id",
			"issuer_id",
			"price",
			"reward",
			"start_location_id",
			"status",
			"title",
			"type",
			"volume",
		},
		duplicates: "ON DUPLICATE KEY UPDATE acceptor_id=VALUES(acceptor_id),date_accepted=VALUES(date_accepted),date_completed=VALUES(date_completed),status=VALUES(status)",
		proto: []string{
			"source bigint(20) NOT NULL",
			"owner bigint(20) NULL",
			"position int(11) NOT NULL DEFAULT -1000",
			"acceptor_id bigint(20) DEFAULT NULL",
			"assignee_id bigint(20) NOT NULL",
			"availability enum('public','personal','corporation','alliance') DEFAULT NULL",
			"buyout decimal(22,2) DEFAULT NULL",
			"collateral decimal(22,2) DEFAULT NULL",
			"contract_id bigint(20) NOT NULL",
			"date_accepted bigint(20) DEFAULT NULL",
			"date_accepted_hour int(11) GENERATED ALWAYS AS (floor(date_accepted / 3600000)) STORED",
			"date_completed bigint(20) DEFAULT NULL",
			"date_completed_hour int(11) GENERATED ALWAYS AS (floor(date_completed / 3600000)) STORED",
			"date_expired bigint(20) NOT NULL",
			"date_expired_hour int(11) GENERATED ALWAYS AS (floor(date_expired / 3600000)) STORED",
			"date_issued bigint(20) NOT NULL",
			"date_issued_hour int(11) GENERATED ALWAYS AS (floor(date_issued / 3600000)) STORED",
			"days_to_complete int(11) DEFAULT NULL",
			"end_location_id bigint(20) DEFAULT NULL",
			"for_corporation tinyint(1) NOT NULL",
			"issuer_corporation_id bigint(20) NOT NULL",
			"issuer_id bigint(20) NOT NULL",
			"price decimal(22,2) DEFAULT NULL",
			"reward decimal(22,2) DEFAULT NULL",
			"start_location_id bigint(20) DEFAULT NULL",
			"status enum('outstanding','in_progress','finished_issuer','finished_contractor','finished','cancelled','rejected','failed','deleted','reversed','expired') NOT NULL",
			"title tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL",
			"type enum('unknown','item_exchange','auction','courier','loan') NOT NULL",
			"volume decimal(22,3) DEFAULT NULL",
			"created timestamp NOT NULL DEFAULT current_timestamp()",
			"last_update timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()",
		},
		tail: " ENGINE=InnoDB DEFAULT CHARSET=latin1;",
		handleStart: func(k *kjob) error { //jobMutex is already locked for us.
			res := safeQuery(fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` WHERE %s=%s", tables["contracts"].database, tables["contracts"].name, tables["contracts"].jobKey, k.Source))
			defer res.Close()
			if !res.Next() {
				k.sqldata = make(map[uint64]uint64)
				return nil
			}
			var numRecords int
			res.Scan(&numRecords)
			k.sqldata = make(map[uint64]uint64, numRecords)
			ress := safeQuery(fmt.Sprintf("SELECT %s,%s FROM `%s`.`%s` WHERE %s=%s", tables["contracts"].primaryKey, tables["contracts"].changedKey, tables["contracts"].database, tables["contracts"].name, tables["contracts"].jobKey, k.Source))
			defer ress.Close()
			var key, data uint64
			for ress.Next() {
				ress.Scan(&key, &data)
				k.sqldata[key] = data
			}
			return nil
		},
		handlePageData: func(k *kpage) error {
			k.pageMutex.Lock()
			k.job.jobMutex.Lock()
			var contract contracts
			if err := json.Unmarshal(k.body, &contract); err != nil {
				return err
			}
			k.recs = int64(len(contract))
			k.ins.Grow(len(contract) * 256)
			k.upd.Grow(len(contract) * 256)
			k.ids.Grow(len(contract) * 10)
			inscomma := ""
			updcomma := ""
			idscomma := ""

			for it := range contract {
				if contract[it].Status == "" {
					contract[it].Status = "outstanding"
				}
				if contract[it].Availability == "" {
					contract[it].Availability = "public"
				}
				fmt.Fprintf(&k.ids, "%s%d", idscomma, contract[it].ContractID)
				idscomma = ","
				if ord, ok := k.job.sqldata[uint64(contract[it].ContractID)]; ok {
					if ord != contractStatus[contract[it].Status.ifnull()] {
						fmt.Fprintf(&k.upd, "%s(%s,%s,%d,%d,%s,%f,%f,%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%f,%f,%d,%s,%s,%s,%f)", updcomma, k.job.Source, k.job.Owner, contract[it].AcceptorID, contract[it].AssigneeID, contract[it].Availability.ifnull(), contract[it].Buyout, contract[it].Collateral, contract[it].ContractID, contract[it].DateAccepted.toSQLDate(), contract[it].DateCompleted.toSQLDate(), contract[it].DateExpired.toSQLDate(), contract[it].DateIssued.toSQLDate(), contract[it].DaysToComplete, contract[it].EndLocationID, contract[it].ForCorporation.toSQL(), contract[it].IssuerCorporationID, contract[it].IssuerID, contract[it].Price, contract[it].Reward, contract[it].StartLocationID, contract[it].Status.ifnull(), contract[it].Title.escape(), contract[it].Type.ifnull(), contract[it].Volume)
						updcomma = ","
						k.updrecs++
					} else {
						// exists in database and order has not changed, no-op
					}
					delete(k.job.sqldata, uint64(contract[it].ContractID)) //remove matched items from the map
				} else {
					fmt.Fprintf(&k.ins, "%s(%s,%s,%d,%d,%s,%f,%f,%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%f,%f,%d,%s,%s,%s,%f)", inscomma, k.job.Source, k.job.Owner, contract[it].AcceptorID, contract[it].AssigneeID, contract[it].Availability.ifnull(), contract[it].Buyout, contract[it].Collateral, contract[it].ContractID, contract[it].DateAccepted.toSQLDate(), contract[it].DateCompleted.toSQLDate(), contract[it].DateExpired.toSQLDate(), contract[it].DateIssued.toSQLDate(), contract[it].DaysToComplete, contract[it].EndLocationID, contract[it].ForCorporation.toSQL(), contract[it].IssuerCorporationID, contract[it].IssuerID, contract[it].Price, contract[it].Reward, contract[it].StartLocationID, contract[it].Status.ifnull(), contract[it].Title.escape(), contract[it].Type.ifnull(), contract[it].Volume)
					inscomma = ","
					k.insrecs++
				}
			}
			k.pageMutex.Unlock()
			k.job.jobMutex.Unlock()
			return nil
		},
		handlePageCached: func(k *kpage) error {
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
					return errors.New("etag data does not match table")
				}

			}
			return nil
		},
		handleWriteIns: func(k *kjob) int64 { //jobMutex is already locked for us.
			return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.database, k.table.name, k.table.columnOrder(), k.insJob.String(), k.table.duplicates))
		},
		handleWriteUpd: func(k *kjob) int64 { //jobMutex is already locked for us.
			return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.database, k.table.name, k.table.columnOrder(), k.updJob.String(), k.table.duplicates))
		},
		handleEndGood: func(k *kjob) int64 { //jobMutex is already locked for us.
			var delrecords int64
			if len(k.sqldata) > 0 {
				var b strings.Builder
				comma := ""
				for it := range k.sqldata {
					fmt.Fprintf(&b, "%s%d", comma, it)
					comma = ","
				}
				query := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN (%s)", tables["contracts"].database, tables["contracts"].name, tables["contracts"].primaryKey, b.String())
				delrecords = safeExec(query)
			}
			k.sqldata = make(map[uint64]uint64)
			return delrecords
		},
		handleEndFail: func(k *kjob) { //jobMutex is already locked for us.
			k.sqldata = make(map[uint64]uint64)
			return
		},
	}

}
