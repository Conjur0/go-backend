// `orders` table definition

package main

import (
	"encoding/json"
	"fmt"
)

type orders []order

type order struct {
	Duration     uint32  `json:"duration"`
	IsBuyOrder   boool   `json:"is_buy_order"`
	Issued       eveDate `json:"issued"`
	LocationID   uint64  `json:"location_id"`
	MinVolume    uint32  `json:"min_volume"`
	OrderID      uint64  `json:"order_id"`
	Price        float64 `json:"price"`
	Range        string  `json:"range"`
	TypeID       uint32  `json:"type_id"`
	VolumeRemain uint32  `json:"volume_remain"`
	VolumeTotal  uint32  `json:"volume_total"`
}

func tablesInitorders() {
	c.Tables["orders"].handlePageData = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		var order orders
		if err := json.Unmarshal(k.body, &order); err != nil {
			return err
		}
		k.recs = int64(len(order))
		k.ins.Grow(len(order) * 120)
		k.ids.Grow(len(order) * 10)

		for it := range order {
			fmt.Fprintf(&k.ids, "%s%d", k.idscomma, order[it].OrderID)
			k.idscomma = ","
			if ord, ok := k.job.sqldata[order[it].OrderID]; ok {
				if ord != order[it].Issued.toktime() {
					fmt.Fprintf(&k.ins, "%s(%s,%s,%d,%d,%s,%d,%d,%d,%f,'%s',%d,%d,%d)", k.inscomma, k.job.Source, k.job.Owner, order[it].Duration, order[it].IsBuyOrder.toSQL(), order[it].Issued.toSQLDate(), order[it].LocationID, order[it].MinVolume, order[it].OrderID, order[it].Price, order[it].Range, order[it].TypeID, order[it].VolumeRemain, order[it].VolumeTotal)
					k.inscomma = ","
					k.insrecs++
				} else {
					// exists in database and order has not changed, no-op
				}
				delete(k.job.sqldata, order[it].OrderID) //remove matched items from the map
			} else {
				k.job.allsqldata[order[it].OrderID] = order[it].Issued.toktime()
				fmt.Fprintf(&k.ins, "%s(%s,%s,%d,%d,%s,%d,%d,%d,%f,'%s',%d,%d,%d)", k.inscomma, k.job.Source, k.job.Owner, order[it].Duration, order[it].IsBuyOrder.toSQL(), order[it].Issued.toSQLDate(), order[it].LocationID, order[it].MinVolume, order[it].OrderID, order[it].Price, order[it].Range, order[it].TypeID, order[it].VolumeRemain, order[it].VolumeTotal)
				k.inscomma = ","
				k.insrecs++
			}

		}
		k.pageMutex.Unlock()
		k.job.jobMutex.Unlock()
		return nil
	}
	c.Tables["orders"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
}
