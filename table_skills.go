// `skills` table definition

package main

import (
	"encoding/json"
	"fmt"
)

type skills struct {
	Skills        []skill `json:"skills"`
	TotalSP       int64   `json:"total_sp"`
	UnallocatedSP int64   `json:"unallocated_sp"`
}

type skill struct {
	ActiveSkillLevel   uint64 `json:"active_skill_level"`
	SkillID            uint64 `json:"skill_id"`
	SkillpointsInSkill uint64 `json:"skillpoints_in_skill"`
	TrainedSkillLevel  uint64 `json:"trained_skill_level"`
}

func tablesInitskills() {
	c.Tables["skills"].handlePageData = func(k *kpage) error {
		k.pageMutex.Lock()
		k.job.jobMutex.Lock()
		var skill skills
		if err := json.Unmarshal(k.body, &skill); err != nil {
			return err
		}
		k.recs = int64(len(skill.Skills))
		k.ins.Grow(len(skill.Skills) * 60)
		k.ids.Grow(len(skill.Skills) * 10)

		for it := range skill.Skills {
			fmt.Fprintf(&k.ids, "%s%d", k.idscomma, skill.Skills[it].SkillID)
			k.idscomma = ","
			if ord, ok := k.job.sqldata[skill.Skills[it].SkillID]; ok {
				if ord != skill.Skills[it].ActiveSkillLevel {
					fmt.Fprintf(&k.ins, "%s(%s,%d,%d,%d,%d)", k.inscomma, k.job.Source, skill.Skills[it].SkillID, skill.Skills[it].SkillpointsInSkill, skill.Skills[it].ActiveSkillLevel, skill.Skills[it].TrainedSkillLevel)
					k.inscomma = ","
					k.insrecs++
				} else {
					// exists in database and order has not changed, no-op
				}
				delete(k.job.sqldata, uint64(skill.Skills[it].SkillID)) //remove matched items from the map
			} else {
				k.job.allsqldata[skill.Skills[it].SkillID] = skill.Skills[it].ActiveSkillLevel
				fmt.Fprintf(&k.ins, "%s(%s,%d,%d,%d,%d)", k.inscomma, k.job.Source, skill.Skills[it].SkillID, skill.Skills[it].SkillpointsInSkill, skill.Skills[it].ActiveSkillLevel, skill.Skills[it].TrainedSkillLevel)
				k.inscomma = ","
				k.insrecs++
			}
		}
		k.pageMutex.Unlock()
		k.job.jobMutex.Unlock()
		return nil
	}
	c.Tables["skills"].handleWriteIns = func(k *kjob) int64 { //jobMutex is already locked for us.
		return safeExec(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s %s", k.table.DB, k.table.Name, k.table.columnOrder(), k.insJob.String(), k.table.Duplicates))
	}
}
