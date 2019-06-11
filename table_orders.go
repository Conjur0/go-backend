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
	tables["orders"] = &table{
		database:   "karkinos",
		name:       "orders",
		primaryKey: "order_id",
		changedKey: "issued",
		jobKey:     "source",
		keys: map[string]string{
			"location_id":  "location_id",
			"type_id":      "type_id",
			"is_buy_order": "is_buy_order",
			"source":       "source",
			"owner":        "owner",
		},
		_columnOrder: []string{
			"source",
			"owner",
			"duration",
			"is_buy_order",
			"issued",
			"location_id",
			"min_volume",
			"order_id",
			"price",
			"`range`",
			"type_id",
			"volume_remain",
			"volume_total",
		},
		duplicates: "ON DUPLICATE KEY UPDATE source=IF(ISNULL(VALUES(owner)),VALUES(source),source),owner=IF(ISNULL(VALUES(owner)),owner,VALUES(owner)),issued=VALUES(issued),price=VALUES(price),volume_remain=VALUES(volume_remain)",
		proto: []string{
			"source bigint(20) NOT NULL",
			"owner bigint(20) NULL",
			"duration int(4) NOT NULL",
			"is_buy_order tinyint(1) NOT NULL",
			"issued bigint(20) NOT NULL",
			"location_id bigint(20) NOT NULL",
			"min_volume int(11) NOT NULL",
			"order_id bigint(20) NOT NULL",
			"price decimal(22,2) NOT NULL",
			"`range` enum('station','region','solarsystem','1','2','3','4','5','10','20','30','40')",
			"type_id int(11) NOT NULL",
			"volume_remain bigint(20) NOT NULL",
			"volume_total bigint(20) NOT NULL",
		},
		tail: " ENGINE=InnoDB DEFAULT CHARSET=latin1;",
		handleStart: func(k *kjob) error { //jobMutex is already locked for us.
			// log(k.CI, fmt.Sprintf("tables[\"%s\"].handleStart called", k.table.name))
			res := safeQuery(fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` WHERE %s=%s", k.table.database, k.table.name, k.table.jobKey, k.Source))
			defer res.Close()
			if !res.Next() {
				k.sqldata = make(map[uint64]uint64)
				return nil
			}
			var numRecords int
			res.Scan(&numRecords)
			// log(k.CI, fmt.Sprintf("tables[\"%s\"].handleStart got %d records", k.table.name, numRecords))
			k.sqldata = make(map[uint64]uint64, numRecords)

			ress := safeQuery(fmt.Sprintf("SELECT %s,%s FROM `%s`.`%s` WHERE %s=%s", k.table.primaryKey, k.table.changedKey, k.table.database, k.table.name, k.table.jobKey, k.Source))
			defer ress.Close()
			var key, data uint64
			for ress.Next() {
				ress.Scan(&key, &data)
				k.sqldata[key] = data
			}
			// log(k.CI, fmt.Sprintf("tables[\"%s\"].handleStart have %d records", k.table.name, len(k.sqldata)))
			return nil
		},
		handlePageData: func(k *kpage) error {
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
			inscomma := ""
			updcomma := ""
			idscomma := ""

			for it := range order {
				fmt.Fprintf(&k.ids, "%s%d", idscomma, order[it].OrderID)
				idscomma = ","
				if ord, ok := k.job.sqldata[uint64(order[it].OrderID)]; ok {
					if ord != order[it].Issued.toktime() {
						fmt.Fprintf(&k.upd, "%s(%s,%s,%d,%d,%s,%d,%d,%d,%f,'%s',%d,%d,%d)", updcomma, k.job.Source, k.job.Owner, order[it].Duration, order[it].IsBuyOrder.toSQL(), order[it].Issued.toSQLDate(), order[it].LocationID, order[it].MinVolume, order[it].OrderID, order[it].Price, order[it].Range, order[it].TypeID, order[it].VolumeRemain, order[it].VolumeTotal)
						updcomma = ","
						k.updrecs++
					} else {
						// exists in database and order has not changed, no-op
					}
					delete(k.job.sqldata, uint64(order[it].OrderID)) //remove matched items from the map
				} else {
					fmt.Fprintf(&k.ins, "%s(%s,%s,%d,%d,%s,%d,%d,%d,%f,'%s',%d,%d,%d)", inscomma, k.job.Source, k.job.Owner, order[it].Duration, order[it].IsBuyOrder.toSQL(), order[it].Issued.toSQLDate(), order[it].LocationID, order[it].MinVolume, order[it].OrderID, order[it].Price, order[it].Range, order[it].TypeID, order[it].VolumeRemain, order[it].VolumeTotal)
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
			//log(k.cip, fmt.Sprintf("tables[\"%s\"].handlePageCached called with %d from etag, %d purged.", k.job.table.name, len(contract), contractspurged))
			return nil
		},
		handleWriteIns: func(k *kjob) int64 { //jobMutex is already locked for us.
			return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.database, k.table.name, k.table.columnOrder(), k.ins.String(), k.table.duplicates))
		},
		handleWriteUpd: func(k *kjob) int64 { //jobMutex is already locked for us.
			return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.database, k.table.name, k.table.columnOrder(), k.upd.String(), k.table.duplicates))
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
				query := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN (%s)", k.table.database, k.table.name, k.table.primaryKey, b.String())
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
