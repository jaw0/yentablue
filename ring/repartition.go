// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-08 10:34 (EDT)
// Function: move data to new servers if the partitioning changes

package ring

import (
	"time"

	"github.com/jaw0/yentablue/soty"
)

const TSAVE = uint64(10 * time.Second)

func (p *P) repartitioner() {

	defer p.redone.Done()
	chk, _ := p.getCheckpoint()
	tchk := soty.Now()

	for {
		done := p.repartition(chk)

		if done {
			if p.stableVer != p.currVer {
				p.lock.Lock()
				p.stableVer = p.currVer
				p.lock.Unlock()
				dl.Verbose("database %s partitions are now stable", p.name)
				chk = &checkpoint{ringver: p.currVer, ringbits: p.ringbits}
			}
			p.saveCheckpoint(chk)
			tchk = soty.Now()
			break
		}

		if soty.Now() > tchk+TSAVE {
			p.saveCheckpoint(chk)
			tchk = soty.Now()
		}

		if waitDelayOrReturn(p.restop, done) {
			return
		}
	}
}

func (p *P) repartition(chk *checkpoint) bool {

	if chk.ringver == 0 {
		return true
	}

	newLoc := p.GetLoc(PartTreeID2Shard(p.ringbits, chk.chktree))

	if chk.chktree == newLoc.TreeID && newLoc.IsLocal {
		// these keys belong here, no need to analyze
		// skip to next tree
		chk.chkver = 0
		chk.chktree++
		dl.Debug("repart local/ok %x", newLoc.TreeID)
	} else {
		chkLoc := &soty.Loc{
			Shard:   PartTreeID2Shard(chk.ringbits, chk.chktree),
			TreeID:  chk.chktree,
			PartIdx: PartTreeID2Idx(chk.ringbits, chk.chktree),
		}

		dl.Debug("repart %x -> %x", chk.chktree, newLoc.TreeID)

		mdone, nver := p.db.Repartition(chkLoc, chk.chkver)

		chk.chkver = nver + 1

		if mdone {
			// next tree
			chk.chkver = 0
			chk.chktree++
		}
	}

	// reached end?
	if chk.chktree == 0 && chk.chkver == 0 {
		// we are done
		return true
	}

	return false
}

func waitDelayOrReturn(stop chan struct{}, delay bool) bool {

	if delay {
		select {
		case <-stop:
			return true
		case <-time.After(30 * time.Second):
			return false
		}
	} else {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	return false
}
