//////////////////////////////////////////////////////////////////////////////////
// table_orders.go - `orders` table definition
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
	c.Tables["orders"].handleStart = func(k *kjob) error { //jobMutex is already locked for us.
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
	c.Tables["orders"].handlePageData = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		var order orders
		if err := json.Unmarshal(k.body, &order); err != nil {
			return err
		}
		k.recs = int64(len(order))
		k.ins.Grow(len(order) * 120)
		k.upd.Grow(len(order) * 120)
		k.ids.Grow(len(order) * 10)

		for it := range order {
			fmt.Fprintf(&k.ids, "%s%d", k.idscomma, order[it].OrderID)
			k.idscomma = ","
			if ord, ok := k.job.sqldata[uint64(order[it].OrderID)]; ok {
				if ord != order[it].Issued.toktime() {
					fmt.Fprintf(&k.upd, "%s(%s,%s,%d,%d,%s,%d,%d,%d,%f,'%s',%d,%d,%d)", k.updcomma, k.job.Source, k.job.Owner, order[it].Duration, order[it].IsBuyOrder.toSQL(), order[it].Issued.toSQLDate(), order[it].LocationID, order[it].MinVolume, order[it].OrderID, order[it].Price, order[it].Range, order[it].TypeID, order[it].VolumeRemain, order[it].VolumeTotal)
					k.updcomma = ","
					k.updrecs++
				} else {
					// exists in database and order has not changed, no-op
				}
				delete(k.job.sqldata, uint64(order[it].OrderID)) //remove matched items from the map
			} else {
				fmt.Fprintf(&k.ins, "%s(%s,%s,%d,%d,%s,%d,%d,%d,%f,'%s',%d,%d,%d)", k.inscomma, k.job.Source, k.job.Owner, order[it].Duration, order[it].IsBuyOrder.toSQL(), order[it].Issued.toSQLDate(), order[it].LocationID, order[it].MinVolume, order[it].OrderID, order[it].Price, order[it].Range, order[it].TypeID, order[it].VolumeRemain, order[it].VolumeTotal)
				k.inscomma = ","
				k.insrecs++
			}

		}
		k.pageMutex.Unlock()
		k.job.jobMutex.Unlock()
		return nil
	}
	c.Tables["orders"].handlePageCached = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		defer k.pageMutex.Unlock()
		defer k.job.jobMutex.Unlock()
		order := strings.Split(k.ids.String(), ",")
		k.recs = int64(len(order))
		var orderID int
		var orderspurged int
		for it := range order {
			orderID, _ = strconv.Atoi(order[it])
			if _, ok := k.job.sqldata[uint64(orderID)]; ok {
				delete(k.job.sqldata, uint64(orderID)) //remove matched items from the map
				orderspurged++
			} else {
				return errors.New("etag data does not match table")
			}

		}
		return nil
	}
	c.Tables["orders"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
	c.Tables["orders"].handleWriteUpd = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.updJob.String(), k.table.Duplicates))
	}
	c.Tables["orders"].handleEndGood = func(k *kjob) int64 { //jobMutex is already locked for us.
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
	c.Tables["orders"].handleEndFail = func(k *kjob) { //jobMutex is already locked for us.
		k.sqldata = make(map[uint64]uint64)
		return
	}
}
