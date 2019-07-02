// `prices` table definition

package main

import (
	"encoding/json"
	"fmt"
)

type prices []price

type price struct {
	TypeID        uint64  `json:"type_id"`
	AveragePrice  float64 `json:"average_price"`
	AdjustedPrice float64 `json:"adjusted_price"`
}

func tablesInitprices() {
	c.Tables["prices"].handlePageData = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		var price prices
		if err := json.Unmarshal(k.body, &price); err != nil {
			return err
		}
		k.recs = int64(len(price))
		k.ins.Grow(len(price) * 60)
		k.ids.Grow(len(price) * 10)

		for it := range price {
			k.records++
			fmt.Fprintf(&k.ids, "%s%d", k.idscomma, price[it].TypeID)
			k.idscomma = ","
			if _, ok := k.job.sqldata[price[it].TypeID]; ok {
				k.recordsChanged++
				delete(k.job.sqldata, uint64(price[it].TypeID)) //remove matched items from the map
			} else {
				k.recordsNew++
				k.job.allsqldata[price[it].TypeID] = 1
			}
			fmt.Fprintf(&k.ins, "%s(%d,%f,%f)", k.inscomma, price[it].TypeID, price[it].AveragePrice, price[it].AdjustedPrice)
			k.inscomma = ","
			k.insrecs++

		}
		k.pageMutex.Unlock()
		k.job.jobMutex.Unlock()
		return nil
	}
	c.Tables["prices"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
}
