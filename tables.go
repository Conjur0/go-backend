package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type table struct {
	name         string
	key          string
	respKey      string
	transform    func(t *table, k *kpage) error
	purge        func(t *table, k *kjob) (error, string)
	_columnOrder []string
	duplicates   string
	proto        []string
	strategy     func(t *table, k *kjob) (error, string)
}

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

func (t *table) columnOrder() string {
	out := ""
	first := true
	for it := range t._columnOrder {
		if first {
			out = t._columnOrder[it]
			first = false
		} else {
			out = fmt.Sprintf("%s,%s", out, t._columnOrder[it])
		}
	}
	return out
}

var tables = make(map[string]*table)

func initTables() {
	tables["orders"] = &table{
		name:    "k.orders",
		key:     "order_id",
		respKey: "order_id",
		transform: func(t *table, k *kpage) error {
			//return t.name + " transform function output: "
			var jsonData orders
			if err := json.Unmarshal(k.body, &jsonData); err != nil {
				return err
			}
			var entity string
			var ok bool
			owner := "NULL"
			if entity, ok = k.job.Entity["region_id"]; !ok {
				if entity, ok = k.job.Entity["structure_id"]; !ok {
					if entity, ok = k.job.Entity["character_id"]; !ok {
						return errors.New("unable to resolve entity")
					} else {
						owner = entity
					}
				}
			}

			length := len(jsonData)
			var ins strings.Builder
			var insIds strings.Builder
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
				fmt.Fprintf(&ins, "(%s,%s,%d,%d,%d,%d,%d,%d,%f,'%s',%d,%d,%d)%s", entity, owner, jsonData[it].Duration, ibo, issued.UnixNano()/int64(time.Millisecond), jsonData[it].LocationID, jsonData[it].MinVolume, jsonData[it].OrderID, jsonData[it].Price, jsonData[it].Range, jsonData[it].TypeID, jsonData[it].VolumeRemain, jsonData[it].VolumeTotal, comma)
				fmt.Fprintf(&insIds, "%d%s", jsonData[it].OrderID, comma)
			}
			k.job.Ins[k.page-1] = ins.String()
			k.job.InsIds[k.page-1] = insIds.String()
			fmt.Printf("%s\n%s\n\n", k.job.Ins[k.page-1], k.job.InsIds[k.page-1])
			return nil
		},
		purge: func(t *table, k *kjob) (error, string) {
			var entity string
			var ok bool
			if entity, ok = k.Entity["region_id"]; !ok {
				if entity, ok = k.Entity["structure_id"]; !ok {
					if entity, ok = k.Entity["character_id"]; !ok {
						return errors.New("unable to resolve entity"), ""
					}
				}
			}
			var b strings.Builder
			comma := ","
			length := len(k.InsIds) - 1
			for it := range k.InsIds {
				if it == length {
					comma = ""
				}
				fmt.Fprintf(&b, "%s%s", k.InsIds[it], comma)
			}
			return nil, fmt.Sprintf("source = %s AND NOT order_id IN (%s)", entity, b.String())

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
		},
	}
	log("tables.go:initTables()", "Initialization Complete!")
}
