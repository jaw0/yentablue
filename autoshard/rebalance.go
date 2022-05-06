// Copyright (c) 2022
// Author: Jeff Weisberg <tcp4me.com!jaw>
// Created: 2022-Apr-21 15:25 (EDT)
// Function: rebalance

package autoshard

import (
	"math"
	"sort"
	"time"

	"github.com/jaw0/yentablue/ring"
	ringcf "github.com/jaw0/yentablue/ring/config"
)

type shardInfo struct {
	servers []string
	shard   uint32
	width   uint32
}

const rebalTimeLimit = uint64(47 * time.Minute)

func rebalance(rcf *ringcf.Ring, rcver uint64, info map[string]*serverInfo) bool {
	if someHaveNoCapacity(info) {
		return false
	}

	if Now()-rcver < rebalTimeLimit {
		// check timestamp, only update slowly
		return false
	}

	// figure current config
	// determine which servers need more, less
	// ...

	changed := false
	for _, dc := range datacenters(rcf) {
		parts := mkRebalanceInfo(dc, rcf)
		adjust := determineServerAdjustment(dc, rcf.RingBits, info)
		idx, dir, found := findBestChange(rcf, parts, adjust)

		if found {
			dl.Debug("sliding shards [%d] %x", idx, parts[idx].shard)
			changed = true
			slideShards(rcf, dir, dc, parts[idx].shard)
		}

	}

	return changed
}

func slideShards(rcf *ringcf.Ring, dir bool, dc string, shard uint32) {

	delta := ring.PartIdx2Shard(rcf.RingBits, 1)

	if !dir {
		delta = -delta
	}

	for i := range rcf.Parts {
		if rcf.Parts[i].Datacenter == dc {
			for sn, s := range rcf.Parts[i].Shard {
				if ring.PartShard2Idx(rcf.RingBits, s) == ring.PartShard2Idx(rcf.RingBits, shard) {
					rcf.Parts[i].Shard[sn] += delta
				}
			}
		}
	}

}

func findBestChange(rcf *ringcf.Ring, parts []*shardInfo, adjust map[string]float64) (int, bool, bool) {

	aveWidth := uint32((1 << 16) / len(parts))
	var bestScore float64
	var bestIdx int
	var bestDir bool
	var found bool

	for i, si := range parts {
		// previous part
		j := (i + len(parts) - 1) % len(parts)
		sj := parts[j]

		scoreFwd := resizePairScore(1, adjust, si.servers, sj.servers)
		scoreRev := resizePairScore(-1, adjust, si.servers, sj.servers)

		if scoreFwd > bestScore && si.width < 2*aveWidth && sj.width > aveWidth/2 {
			bestScore = scoreFwd
			bestIdx = i
			bestDir = false
			found = true
		}
		if scoreRev > bestScore && sj.width < 2*aveWidth && si.width > aveWidth/2 {
			bestScore = scoreRev
			bestIdx = i
			bestDir = true
			found = true
		}
	}

	return bestIdx, bestDir, found
}

// grow one + shrink the adjacent
func resizePairScore(dir int, adjust map[string]float64, serversL, serversR []string) float64 {
	return resizeScore(dir, adjust, serversR) + resizeScore(-dir, adjust, serversL)
}

func resizeScore(dir int, adjust map[string]float64, servers []string) float64 {

	var score float64

	for _, server := range servers {
		score += float64(dir) * adjust[server]
	}

	return score
}

func datacenters(rcf *ringcf.Ring) []string {
	var dcs []string
	have := make(map[string]struct{})

	for _, p := range rcf.Parts {
		if _, ok := have[p.Datacenter]; !ok {
			have[p.Datacenter] = struct{}{}
			dcs = append(dcs, p.Datacenter)
		}
	}
	return dcs
}

func mkRebalanceInfo(dc string, rcf *ringcf.Ring) []*shardInfo {

	var info []*shardInfo
	xcf := &ring.ConstrCf{MyDC: dc}
	parts := xcf.ConfigureParts(rcf)

	for pIdx, p := range parts {
		dpt := p.DCpart(dc)
		if dpt == nil || !dpt.IsBoundary {
			continue
		}
		info = append(info, &shardInfo{
			servers: dpt.Servers,
			shard:   ring.PartIdx2Shard(rcf.RingBits, pIdx),
		})
	}

	sort.Slice(info, func(i, j int) bool {
		return info[i].shard < info[j].shard
	})

	// measure
	for i, si := range info {
		if i < len(info)-1 {
			si.width = info[i+1].shard - si.shard
		} else {
			si.width = info[0].shard - si.shard // NB: 32 bits overflows to correct result ( 1^32 + s0 - s )
		}
		si.width = si.width >> 16
	}

	return info
}

func determineServerAdjustment(dc string, ringbits int, info map[string]*serverInfo) map[string]float64 {
	adjust := make(map[string]float64)

	ringsz := float64(int(1) << ringbits)
	// determine average across all servers
	var totalSpace int32
	var totalAvail int32

	for _, si := range info {
		totalSpace += si.totalSpace
		totalAvail += si.availSpace
	}

	goal := float64(totalAvail) / float64(totalSpace)
	var min float64
	var max float64

	for name, si := range info {
		if si.datacenter != dc {
			continue
		}
		if si.totalSpace != 0 {
			m := float64(si.availSpace)/float64(si.totalSpace) - goal
			a := m * float64(si.totalSpace) / float64(totalSpace) * ringsz
			adjust[name] = a

			if a < 0 && a < min {
				min = a
			}
			if a > 0 && a > max {
				max = a
			}
		}
	}

	// and normalize to min/max
	k := max
	if math.Abs(min) > max {
		k = math.Abs(min)
	}

	k = k
	//for name := range adjust {
	//	//adjust[name] /= k
	//}

	// NB - sum of all adjustments == 0
	return adjust
}
