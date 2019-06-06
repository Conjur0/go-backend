//////////////////////////////////////////////////////////////////////////////////
// table_orders.go - `orders` table definition
//////////////////////////////////////////////////////////////////////////////////
//
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type orders []ordersElement

type ordersElement struct {
	Duration     uint32  `json:"duration"`
	IsBuyOrder   bool    `json:"is_buy_order"`
	Issued       string  `json:"issued"`
	LocationID   uint64  `json:"location_id"`
	MinVolume    uint32  `json:"min_volume"`
	OrderID      uint64  `json:"order_id"`
	Price        float64 `json:"price"`
	Range        string  `json:"range"`
	SystemID     uint32  `json:"system_id"`
	TypeID       uint32  `json:"type_id"`
	VolumeRemain uint32  `json:"volume_remain"`
	VolumeTotal  uint32  `json:"volume_total"`
}

func tablesInitorders() {
	tables["orders"] = &table{
		database:   "karkinos",
		name:       "orders",
		primaryKey: "order_id",
		respKey:    "order_id",
		transform: func(t *table, k *kpage) error {
			var jsonData orders
			if err := json.Unmarshal(k.body, &jsonData); err != nil {
				return err
			}
			var entity string
			var ok bool
			owner := "NULL"
			k.job.LockJob("table_orders.go:47")
			tmp := k.job.Entity
			runtag := k.job.RunTag
			k.job.UnlockJob()

			if entity, ok = tmp["region_id"]; !ok {
				if entity, ok = tmp["structure_id"]; !ok {
					if entity, ok = tmp["character_id"]; ok {
						owner = entity
					} else {
						return errors.New("unable to resolve entity")
					}
				}
			}
			length := len(jsonData)
			k.pageMutex.Lock()
			k.Ins.Grow(length * 104)
			comma := ","
			length--
			for it := range jsonData {
				//fmt.Printf("Record %d of %d: order_id:%d\n", it, length, jsonData[it].OrderID)
				if length == it {
					comma = ""
				}
				issued, err := time.Parse("2006-01-02T15:04:05Z", jsonData[it].Issued)
				if err != nil {
					return errors.New("unable to parse issued time")
				}
				var ibo int8
				if jsonData[it].IsBuyOrder {
					ibo = 1
				}
				fmt.Fprintf(&k.Ins, "(%s,%s,%d,%d,%d,%d,%d,%d,%f,'%s',%d,%d,%d,%d)%s", entity, owner, jsonData[it].Duration, ibo, issued.UnixNano()/int64(time.Millisecond), jsonData[it].LocationID, jsonData[it].MinVolume, jsonData[it].OrderID, jsonData[it].Price, jsonData[it].Range, jsonData[it].TypeID, jsonData[it].VolumeRemain, jsonData[it].VolumeTotal, runtag, comma)
				fmt.Fprintf(&k.InsIds, "%d%s", jsonData[it].OrderID, comma)
			}
			if k.dead || !k.job.running {
				return errors.New("transform finished a dead job")
			}
			k.InsReady = true
			k.pageMutex.Unlock()
			k.job.LockJob("table_orders.go:90")
			k.job.InsLength += length + 1
			k.job.UnlockJob()
			k.pageMutex.Lock()
			defer k.pageMutex.Unlock()
			return nil
		},
		purge: func(t *table, k *kjob) string {
			var entity string
			var ok bool
			if entity, ok = k.Entity["region_id"]; !ok {
				if entity, ok = k.Entity["structure_id"]; !ok {
					if entity, ok = k.Entity["character_id"]; !ok {
						return "FALSE"
					}
				}
			}
			return fmt.Sprintf("source = %s AND NOT last_seen = %d", entity, k.RunTag)

		},
		keys: []string{
			"location_id",
			"type_id",
			"is_buy_order",
			"source",
			"owner",
			"last_seen",
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
			"last_seen",
		},
		duplicates: "ON DUPLICATE KEY UPDATE source=IF(ISNULL(VALUES(owner)),VALUES(source),source),owner=VALUES(owner),issued=VALUES(issued),price=VALUES(price),volume_remain=VALUES(volume_remain)",
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
			"last_seen bigint(20) NOT NULL",
		},
		tail: " ENGINE=InnoDB DEFAULT CHARSET=latin1;",
	}

}
