package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type table struct {
	name         string
	key          string
	respKey      string
	transform    func(t *table, k *kpage) error
	purge        func(t *table, k *kpage) string
	_columnOrder []string
	duplicates   string
	proto        []string
	strategy     func(t *table, k string) error
}

type Orders []OrdersElement

type OrdersElement struct {
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
		name:    "orders",
		key:     "order_id",
		respKey: "order_id",
		transform: func(t *table, k *kpage) error {
			//return t.name + " transform function output: "
			var jsonData Orders
			if err := json.Unmarshal(k.body, &jsonData); err != nil {
				log("tables.go:orders->transform("+k.cip+") json.Unmarshal", err)
				return err
			}
			var entity string
			var ok bool
			owner := "NULL"
			if entity, ok = k.job.Entity["region_id"]; !ok {
				if entity, ok = k.job.Entity["character_id"]; !ok {
					if entity, ok = k.job.Entity["structure_id"]; !ok {
						err := errors.New("unable to resolve entity")
						log("tables.go:orders->transform("+k.cip+") resolve entity", err)
						return err
					}
				} else {
					owner = entity
				}
			}

			length := len(jsonData)
			fmt.Printf("Processing %d Records entity:%s, owner:%s...\n", length, entity, owner)
			var ins = ""
			var insIds = ""
			comma := ","
			length--
			for it := range jsonData {
				fmt.Printf("Record %d of %d: order_id:%d    ", it, length, jsonData[it].OrderID)
				if length == it {
					comma = ""
				}
				issued, err := time.Parse("2006-01-02T15:04:05Z", jsonData[it].Issued)
				if err != nil {
					err := errors.New("unable to parse issued time")
					log("tables.go:orders->transform("+k.cip+") parse issued time", err)
					return err
				}
				var ibo int8
				if jsonData[it].IsBuyOrder {
					ibo = 1
				}
				out := fmt.Sprintf("(%s,%s,%d,%d,%d,%d,%d,%d,%f,'%s',%d,%d,%d)%s",
					entity,
					owner,
					jsonData[it].Duration,
					ibo,
					issued.UnixNano()/int64(time.Millisecond),
					jsonData[it].LocationID,
					jsonData[it].MinVolume,
					jsonData[it].OrderID,
					jsonData[it].Price,
					jsonData[it].Range,
					jsonData[it].TypeID,
					jsonData[it].VolumeRemain,
					jsonData[it].VolumeTotal,
					comma,
				)
				ins = ins + out
				insIds = fmt.Sprintf("%s%d%s", insIds, jsonData[it].OrderID, comma)
				fmt.Printf("PARSED: %s\n", out)
			}
			/*
				transform: (d, treq) => {
					let ibo = d.is_buy_order ? 1 : 0;
					let min = d.min_volume ? d.min_volume : 0;
					return `${treq.entity},${treq.char_id > 0 ? treq.char_id : 'NULL'},${d.duration},${ibo},UNIX_TIMESTAMP(STR_TO_DATE('${d.issued}','%Y-%m-%dT%H:%i:%sZ')),${d.location_id},${min},
					${d.order_id},${d.price},'${d.range}',${d.type_id},${d.volume_remain},${d.volume_total}`;
				},
			*/
			k.job.Ins[k.page-1] = ins
			k.job.InsIds[k.page-1] = insIds
			fmt.Printf("%s\n%s\n\n", ins, insIds)
			return nil
		},
		purge: func(t *table, k *kpage) string {
			return fmt.Sprintf("source = %s AND NOT order_id IN (%s)", k, k)

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
