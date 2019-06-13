// `corp_members` table definition

package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type corpMember []uint64

func tablesInitcorpMembers() {
	c.Tables["corp_members"].handleStart = func(k *kjob) error { //jobMutex is already locked for us.
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
	c.Tables["corp_members"].handlePageData = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		var corpMember corpMember
		if err := json.Unmarshal(k.body, &corpMember); err != nil {
			return err
		}
		k.recs = int64(len(corpMember))
		k.ins.Grow(len(corpMember) * 30)
		k.ids.Grow(len(corpMember) * 10)

		for it := range corpMember {
			fmt.Fprintf(&k.ids, "%s%d", k.idscomma, corpMember[it])
			k.idscomma = ","
			if _, ok := k.job.sqldata[corpMember[it]]; !ok {
				fmt.Fprintf(&k.ins, "%s(%s,%d)", k.inscomma, k.job.Source, corpMember[it])
				k.inscomma = ","
				k.insrecs++
			} else {
				delete(k.job.sqldata, corpMember[it]) //remove matched items from the map
			}
		}
		k.pageMutex.Unlock()
		k.job.jobMutex.Unlock()
		return nil
	}
	c.Tables["corp_members"].handlePageCached = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		defer k.pageMutex.Unlock()
		defer k.job.jobMutex.Unlock()
		skill := strings.Split(k.ids.String(), ",")
		k.recs = int64(len(skill))
		return nil
	}
	c.Tables["corp_members"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT IGNORE INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
	c.Tables["corp_members"].handleWriteUpd = func(k *kjob) int64 { //jobMutex is already locked for us.
		return 0
	}
	c.Tables["corp_members"].handleEndGood = func(k *kjob) int64 { //jobMutex is already locked for us.
		return 0
	}
	c.Tables["corp_members"].handleEndFail = func(k *kjob) { //jobMutex is already locked for us.
		return
	}
}
