// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-07 11:06 (EDT)
// Function: build the ring from the config

package ring

import (
	"time"

	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/ring/config"
	"github.com/jaw0/yentablue/soty"
)

func (p *P) configure() {

	p.maybeReconfigure()
	// go maint { reconfig, repart }

}

func (p *P) maybeReconfigure() {

	cftxt, cfver, cfok := p.getConfig()
	dl.Debug("cf %s ver %x", p.name, cfver)

	if !cfok || cfver <= p.currVer {
		return
	}
	// there are often several changes made in quick succession, wait a bit
	if cfver > soty.Now()-uint64(60*time.Second) {
		return
	}

	cf, err := config.FromBytes(cftxt)
	if err != nil || cf.Version != config.VERSION {
		dl.Problem("cannot parse ring config for %s", p.name)
		return
	}

	if cf.RingBits > 16 {
		cf.RingBits = 16
	}
	if cf.Replicas < 0 {
		cf.Replicas = 0
	}
	if cf.Replicas == 0 && p.replicas == 0 {
		return
	}

	// tell repartitioner to stop
	close(p.restop)

	dl.Debug("bits %d, replicas %d", cf.RingBits, cf.Replicas)

	var newparts []*Part

	if cf.Replicas > 0 && len(cf.Parts) > 0 {
		newparts = p.configureParts(cf)
	}

	// make sure repartitioner finished
	p.redone.Wait()

	// repart_init

	// lock+swap
	p.lock.Lock()
	p.part = newparts
	p.replicas = cf.Replicas
	p.ringbits = cf.RingBits
	p.currVer = cfver
	p.restop = make(chan struct{})
	p.lock.Unlock()

	// restart repartitioner
	p.redone.Add(1)
	go p.repartitioner()

	dl.Verbose("database %s reconfigured", p.name)
}

func (p *P) configureParts(cf *config.Ring) []*Part {

	slots := 1 << uint(cf.RingBits)
	newparts := make([]*Part, slots)
	dcsrv := make(map[string]map[string]bool)

	for i := 0; i < slots; i++ {
		newparts[i] = newPart(p.mydc)
	}
	// add the shards as sonfigured
	for _, cfp := range cf.Parts {
		for _, shard := range cfp.Shard {
			p.partInsert(newparts, cf.RingBits, cfp.Server, cfp.Datacenter, cfp.Rack, shard)

			// keep track of servers in each dc.
			if _, ok := dcsrv[cfp.Datacenter]; !ok {
				dcsrv[cfp.Datacenter] = make(map[string]bool)
			}
			dcsrv[cfp.Datacenter][cfp.Server] = true
		}
	}

	// add replicas
	start := 0
	for pi, pt := range newparts {
		for di, dc := range pt.dc {
			if !dc.isBoundary {
				continue
			}

			// 1st try to be rack aware
			start = p.addReplicas(newparts, cf.Replicas, pi, start, di, true, false)
			start = p.addReplicas(newparts, cf.Replicas, pi, start, di, false, false)

			// add remainder of servers as hot spares
			start = p.addReplicas(newparts, len(dcsrv[dc.dcname]), pi, start, di, false, true)
		}
	}

	// interpolate the reminaing slots
	interpolateParts(newparts)

	return newparts
}

func (p *P) addReplicas(parts []*Part, replicas int, pn int, start int, dn int, tryrack bool, spare bool) int {

	pt := parts[pn]
	dc := pt.dc[dn]

	// have enough servers?
	if len(dc.servers) >= replicas {
		return start
	}

	// walk forwards, looking for suitable servers
	size := len(parts)
	for i := 0; i < size; i++ {
		pos := (start + i + 1) % size
		tdc := parts[pos].dc[dn]
		if !tdc.isBoundary {
			continue
		}
		server := tdc.servers[0]
		rack := tdc.rack[server]
		// can we use this server?
		if !replicaIsCompatHere(server, rack, dc, tryrack) {
			continue
		}
		dl.Debug("slot %x + %s %v", pn, server, spare)
		dc.servers = append(dc.servers, server)
		dc.rack[server] = rack
		if server == p.myid && !spare {
			pt.isLocal = true
		}
		if len(dc.servers) >= replicas {
			return pos
		}
	}
	return start
}

func replicaIsCompatHere(server string, rack string, dc *DCPart, tryrack bool) bool {

	for _, s := range dc.servers {
		if s == server {
			return false
		}
		if tryrack && rack == dc.rack[s] {
			return false
		}
	}
	return true
}

func interpolateParts(parts []*Part) {

	size := len(parts)

	for i, p := range parts {
		for d, dc := range p.dc {
			if dc.isBoundary {
				continue
			}

			// walk backwards until we find something
			for j := 0; j < size; j++ {
				lp := parts[(i-j+size)%size]
				ldc := lp.dc[d]
				if len(ldc.servers) == 0 {
					continue
				}
				// copy
				dc.servers = ldc.servers
				dc.rack = ldc.rack
				p.isLocal = lp.isLocal
			}
		}
	}
}

func newPart(mydc string) *Part {
	p := &Part{
		dcidx: make(map[string]int),
	}

	p.addDC(mydc)

	return p
}

func (pt *Part) addDC(dc string) {

	dp := &DCPart{
		dcname: dc,
		rack:   make(map[string]string),
	}
	pt.dc = append(pt.dc, dp)
	pt.dcidx[dc] = len(pt.dc) - 1
}

func addDC(parts []*Part, dc string) {

	for _, p := range parts {
		p.addDC(dc)
	}
}

func (p *P) partInsert(parts []*Part, bits int, server string, dc string, rack string, shard uint32) {

	slot := partShard2Idx(bits, shard)
	pt := parts[slot]

	if server == p.myid {
		pt.isLocal = true
	}

	idx, ok := pt.dcidx[dc]
	if !ok {
		addDC(parts, dc)
		idx = pt.dcidx[dc]
	}

	d := pt.dc[idx]
	d.servers = append(pt.dc[idx].servers, server)
	d.rack[server] = rack
	d.isBoundary = true
}

func (p *P) getConfig() ([]byte, uint64, bool) {

	if p.name == "_conf" {
		return nil, 0, false
	}

	key := p.name + ".cf"

	rec := &acproto.ACPY2MapDatum{
		Map: "_conf",
		Key: key,
	}
	ok, _ := p.cfdb.Get(rec)

	if !ok {
		return nil, 0, false
	}

	return rec.GetValue(), rec.GetVersion(), true
}
