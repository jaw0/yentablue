// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-25 13:30 (EDT)
// Function:

package ring

import (
	"math/rand"
	"sync"

	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/soty"
)

var lock sync.RWMutex
var allsrvr = make(map[string]*rserver)

func GetPeerAddr(id string) string {

	lock.Lock()
	defer lock.Unlock()

	rs, ok := allsrvr[id]
	if !ok || !rs.isUp {
		return ""
	}

	return rs.bestAddr
}

// receive status updates from peerdb
func (p *P) PeerUpdate(id string, isup bool, iscf bool, first bool, pd *kibitz.Export, dat *acproto.ACPHeartBeat) {

	dl.Debug("ring: %v %s is %v; %s %v %v", first, id, isup, pd.BestAddr, pd.IsSameRack, pd.IsSameDC)

	if first {
		p.updateAllSrvr(id, isup, pd, dat)
	}

	// update ring
	if iscf {
		p.addServer(id, pd.Datacenter)
	} else {
		p.removeServer(id)
	}
}

func (p *P) updateAllSrvr(id string, isup bool, pd *kibitz.Export, dat *acproto.ACPHeartBeat) {

	// update allservers
	lock.Lock()
	defer lock.Unlock()

	rs, ok := allsrvr[id]

	dl.Debug("ok %v rs %v pd %v", ok, rs, pd)

	if !ok {
		rs = &rserver{
			id:     id,
			sameDC: pd.IsSameDC,
			dc:     pd.Datacenter,
		}
		allsrvr[id] = rs
		dl.Debug("add allsrvr %s", id)
	}

	rs.sameRack = pd.IsSameRack
	rs.rack = pd.Rack
	rs.bestAddr = pd.BestAddr
	rs.isUp = isup
	rs.isUpToDate = dat.GetUptodate()
	rs.sortmetric = dat.GetSortMetric()
	rs.cpumetric = dat.GetCpuMetric()
	rs.capacity = dat.GetCapacityMetric()
}

func (p *P) addServer(id string, dc string) {

	lock.RLock()
	defer lock.RUnlock()
	rs, ok := allsrvr[id]

	if !ok {
		dl.Bug("cannot find entry for server '%s'", id)
	}

	dl.Debug("+ %s; %v %v %v", id, rs.isUpToDate, rs.sameRack, rs.sameDC)
	p.lock.Lock()
	defer p.lock.Unlock()

	p.all.add(id, dc)

}

func (p *P) removeServer(id string) {

	p.lock.Lock()
	defer p.lock.Unlock()

	p.all.remove(id)
}

func removeFrom(list []string, id string) []string {

	idx := -1
	for i, l := range list {
		if l == id {
			idx = i
		}
	}
	if idx == -1 {
		// not found
		return list
	}

	// delete
	copy(list[idx:], list[idx+1:])
	list[len(list)-1] = ""
	list = list[:len(list)-1]
	return list
}

func (p *Part) add(id string, dc string) {

	_, ok := p.dcidx[id]
	if ok {
		// already got it
		return
	}

	for i, d := range p.dc {
		if d.dcname == dc {
			p.dcidx[id] = i
			p.dc[i].servers = append(p.dc[i].servers, id)
			return
		}
	}

	// add new dc
	p.dc = append(p.dc, &DCPart{
		dcname:  dc,
		servers: []string{id},
	})
	p.dcidx[id] = len(p.dc) - 1
}

func (p *Part) remove(id string) {

	i, ok := p.dcidx[id]
	if !ok {
		// nothing here to remove
		return
	}

	p.dc[i].servers = removeFrom(p.dc[i].servers, id)
	// QQQ - remove dc if empty?
}

// ################################################################

func (p *P) peerIsFaraway(id string) bool {

	lock.RLock()
	defer lock.RUnlock()

	pd, ok := allsrvr[id]
	if !ok {
		return false
	}
	return pd.sameDC
}

// ################################################################

func (p *P) RandomAEPeer(loc *soty.Loc) string {

	// look for an up to date peer, (usually) prefer local
	// use an out-of-date peer only if we have to
	// NB: if we never used ood peers, we'd have a bootstrap deadlock

	lock.RLock()
	defer lock.RUnlock()

	p.lock.RLock()
	defer p.lock.RUnlock()

	part := p.all

	if len(p.part) != 0 {
		if loc.PartIdx >= len(p.part) {
			return ""
		}
		part = p.part[loc.PartIdx]
	}

	local := &randString{}
	faraway := &randString{}
	ood := &randString{}

	for peer, _ := range part.dcidx {
		pd, ok := allsrvr[peer]
		if !ok || !pd.isUp {
			continue
		}
		addr := pd.bestAddr

		if !pd.isUpToDate {
			ood.maybe(addr)
			continue
		}
		if pd.sameDC {
			local.maybe(addr)
			continue
		}
		faraway.maybe(addr)
	}

	if random_n(8) == 0 && faraway.len() != 0 {
		return faraway.use()
	}

	if local.len() != 0 {
		return local.use()
	}

	return ood.use()
}

func (p *P) RandomDistPeer(loc *soty.Loc) string {

	// prefer most local

	lock.RLock()
	defer lock.RUnlock()

	p.lock.RLock()
	defer p.lock.RUnlock()

	part := p.all

	if len(p.part) != 0 {
		if loc.PartIdx >= len(p.part) {
			return ""
		}
		part = p.part[loc.PartIdx]
	}

	local := &randString{}
	faraway := &randString{}

	for peer, _ := range part.dcidx {
		pd, ok := allsrvr[peer]
		if !ok || !pd.isUp {
			continue
		}
		addr := pd.bestAddr

		if pd.sameRack {
			local.maybe(addr)
			local.maybe(addr)
			continue
		}
		if pd.sameDC {
			local.maybe(addr)
			continue
		}
		faraway.maybe(addr)
	}

	if local.len() != 0 {
		return local.use()
	}

	return faraway.use()
}

// get peers for specified dc.
// local dc can optionally be split for rack

func (p *P) DistPeers(loc *soty.Loc, dcidx int, splitRack bool, butnot string) ([]string, []string) {

	lock.RLock()
	defer lock.RUnlock()

	p.lock.RLock()
	defer p.lock.RUnlock()

	part := p.all

	if len(p.part) != 0 {
		if loc.PartIdx >= len(p.part) {
			// invalid loc?
			return nil, nil
		}
		part = p.part[loc.PartIdx]
	}

	if dcidx >= len(part.dc) {
		// invalid dcidx
		return nil, nil
	}

	dc := part.dc[dcidx]

	var rack []string
	var other []string

	for _, peer := range dc.servers {
		pd, ok := allsrvr[peer]
		if !ok || !pd.isUp || peer == butnot {
			continue
		}
		addr := pd.bestAddr

		if splitRack && pd.sameRack && dcidx == 0 {
			rack = append(rack, addr)
		} else {
			other = append(other, addr)
		}
	}

	return other, rack
}

func (p *P) AltPeers(loc *soty.Loc) []string {

	lock.RLock()
	defer lock.RUnlock()

	p.lock.RLock()
	defer p.lock.RUnlock()

	part := p.all

	if len(p.part) != 0 {
		if loc.PartIdx >= len(p.part) {
			// invalid loc?
			return nil
		}
		part = p.part[loc.PartIdx]
	}

	var dist []string

	for dcidx, dc := range part.dc {

		for _, peer := range dc.servers {
			pd, ok := allsrvr[peer]
			if !ok || !pd.isUp {
				continue
			}
			addr := pd.bestAddr

			dist = append(dist, addr)
		}

		// local or all of the faraway
		if len(dist) > 0 && dcidx == 0 {
			return dist
		}
	}

	return dist
}

func random_n(n int) int {
	return int(rand.Int31n(int32(n)))
}
