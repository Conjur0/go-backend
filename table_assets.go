// `assets` table definition

package main

import (
	"encoding/json"
	"fmt"
)

type assets []asset

type asset struct {
	IsSingleton     boool   `json:"is_singleton"`
	ItemID          uint64  `json:"item_id"`
	LocationFlag    sQLenum `json:"location_flag"`
	LocationID      uint64  `json:"location_id"`
	LocationType    sQLenum `json:"location_type"`
	Quantity        uint64  `json:"quantity"`
	TypeID          uint64  `json:"type_id"`
	IsBlueprintCopy boool   `json:"is_blueprint_copy,omitempty"`
}

func tablesInitassets() {
	c.Tables["assets"].handlePageData = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		var asset assets
		if err := json.Unmarshal(k.body, &asset); err != nil {
			log(err)
			return err
		}
		k.recs = int64(len(asset))
		k.ins.Grow(len(asset) * 256)
		k.ids.Grow(len(asset) * 10)
		/*
		           "source",
		           "owner",
		           "is_blueprint_copy",
		           "is_singleton",
		           "item_id",
		           "location_flag",
		           "location_id",
		           "location_type",
		           "quantity",
		   				"type_id"
		*/
		for it := range asset {
			fmt.Fprintf(&k.ids, "%s%d", k.idscomma, asset[it].ItemID)
			k.idscomma = ","
			if _, ok := k.job.sqldata[asset[it].ItemID]; ok {
				delete(k.job.sqldata, uint64(asset[it].ItemID)) //remove matched items from the map
			} else {
				k.job.allsqldata[asset[it].ItemID] = 1
			}
			fmt.Fprintf(&k.ins, "%s(%s,%s,%d,%d,%d,%s,%d,%s,%d,%d)", k.inscomma, k.job.Source, k.job.Owner, asset[it].IsBlueprintCopy.toSQL(), asset[it].IsSingleton.toSQL(), asset[it].ItemID, asset[it].LocationFlag.ifnull(), asset[it].LocationID, asset[it].LocationType.ifnull(), asset[it].Quantity, asset[it].TypeID)
			k.inscomma = ","
			k.insrecs++
		}
		k.pageMutex.Unlock()
		k.job.jobMutex.Unlock()
		return nil
	}
	c.Tables["assets"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
}
