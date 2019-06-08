//////////////////////////////////////////////////////////////////////////////////
// table_contracts.go - `contracts` table definition
//////////////////////////////////////////////////////////////////////////////////
//
package main

import (
	"encoding/json"
	"errors"
	"fmt"
)

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
		transform: func(t *table, k *kpage) error {
			var contract contracts
			if err := json.Unmarshal(k.body, &contract); err != nil {
				return err
			}

			length := len(contract)
			k.pageMutex.Lock()
			k.Ins.Grow(length * 104)
			comma := ""
			for it := range contract {
				if contract[it].Status == "" {
					contract[it].Status = "outstanding"
				}
				if contract[it].Availability == "" {
					contract[it].Availability = "public"
				}
				fmt.Fprintf(&k.Ins, "%s(%d,%d,%s,%f,%f,%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%f,%f,%d,%s,%s,%s,%f)", comma,
					contract[it].AcceptorID, contract[it].AssigneeID, contract[it].Availability.ifnull(),
					contract[it].Buyout, contract[it].Collateral, contract[it].ContractID, contract[it].DateAccepted.toSQLDate(),
					contract[it].DateCompleted.toSQLDate(), contract[it].DateExpired.toSQLDate(), contract[it].DateIssued.toSQLDate(),
					contract[it].DaysToComplete, contract[it].EndLocationID, contract[it].ForCorporation.toSQL(),
					contract[it].IssuerCorporationID, contract[it].IssuerID, contract[it].Price, contract[it].Reward,
					contract[it].StartLocationID, contract[it].Status.ifnull(), contract[it].Title.escape(), contract[it].Type.ifnull(), contract[it].Volume)
				comma = ","
			}
			if k.dead || !k.job.running {
				return errors.New("transform finished a dead job")
			}
			k.InsReady = true
			k.pageMutex.Unlock()
			k.job.LockJob("table_contracts.go:90")
			k.job.InsLength += length
			k.job.UnlockJob()
			k.pageMutex.Lock()
			defer k.pageMutex.Unlock()
			return nil
		},
		purge: func(t *table, k *kjob) string {
			//			return fmt.Sprintf("UPDATE `%s`.`%s` SET status='' WHERE source = %s AND NOT last_seen = %d", t.database, t.name, k.RunTag)
			return ""
		},
		keys: []string{
			"acceptor_id",
			"assignee_id",
			"end_location_id",
			"issuer_corporation_id",
			"issuer_id",
			"start_location_id",
			"status",
			"type",
			"date_accepted_hour",
			"date_completed_hour",
			"date_expired_hour",
			"date_issued_hour",
		},
		_columnOrder: []string{
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
	}

}
