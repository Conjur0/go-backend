// `corp_members` table definition

package main

import (
	"encoding/json"
	"fmt"
)

type corpMember []uint64

func tablesInitcorpMembers() {
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
				k.job.allsqldata[corpMember[it]] = 1
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
	c.Tables["corp_members"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT IGNORE INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
}
