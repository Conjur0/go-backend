// `sovereignty` table definition

package main

import (
	"encoding/json"
	"fmt"
)

type sovereignties []sovereignty

type sovereignty struct {
	SystemID      uint64 `json:"system_id"`
	FactionID     uint64 `json:"faction_id,omitempty"`
	AllianceID    uint64 `json:"alliance_id,omitempty"`
	CorporationID uint64 `json:"corporation_id,omitempty"`
}

func tablesInitsovereignty() {
	c.Tables["sovereignty"].handlePageData = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		var sovereignty sovereignties
		if err := json.Unmarshal(k.body, &sovereignty); err != nil {
			return err
		}
		k.recs = int64(len(sovereignty))
		k.ins.Grow(len(sovereignty) * 60)
		k.ids.Grow(len(sovereignty) * 10)

		for it := range sovereignty {
			fmt.Fprintf(&k.ids, "%s%d", k.idscomma, sovereignty[it].SystemID)
			k.idscomma = ","
			if ord, ok := k.job.sqldata[sovereignty[it].SystemID]; ok {
				if ord != sovereignty[it].CorporationID {
					fmt.Fprintf(&k.ins, "%s(%d,%d,%d,%d)", k.inscomma, sovereignty[it].SystemID, sovereignty[it].AllianceID, sovereignty[it].CorporationID, sovereignty[it].FactionID)
					k.inscomma = ","
					k.insrecs++
				} else {
					// exists in database and order has not changed, no-op
				}
				delete(k.job.sqldata, uint64(sovereignty[it].SystemID)) //remove matched items from the map
			} else {
				k.job.allsqldata[sovereignty[it].SystemID] = sovereignty[it].CorporationID
				fmt.Fprintf(&k.ins, "%s(%d,%d,%d,%d)", k.inscomma, sovereignty[it].SystemID, sovereignty[it].AllianceID, sovereignty[it].CorporationID, sovereignty[it].FactionID)
				k.inscomma = ","
				k.insrecs++
			}
		}
		k.pageMutex.Unlock()
		k.job.jobMutex.Unlock()
		return nil
	}
	c.Tables["sovereignty"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
}
