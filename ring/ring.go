// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-14 13:53 (EDT)
// Function: sharding

package ring

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/soty"
	"github.com/jaw0/acgo/diag"
)

type DCPart struct {
	dcname     string
	servers    []string
	rack       map[string]string // only used for configuring
	isBoundary bool
}

type Part struct {
	dc        []*DCPart      // [0] is the local dc
	dcidx     map[string]int // which dc[i] is this server in?
	isLocal   bool
	stableVer uint64
}

type P struct {
	name      string
	myid      string
	mydc      string
	myrack    string
	db        Databaser
	cfdb      ConfDBer
	lock      sync.RWMutex
	currVer   uint64
	stableVer uint64
	ringbits  int
	replicas  int
	part      []*Part // build from config
	all       *Part   // dynamically maintained
	stop      chan struct{}
	done      sync.WaitGroup
	restop    chan struct{} // coordinate reconfig/repartition
	redone    sync.WaitGroup
}

type rserver struct {
	id         string
	bestAddr   string
	dc         string
	rack       string
	capacity   int32
	cpumetric  int32
	sortmetric int32
	sameDC     bool
	sameRack   bool
	isUp       bool
	isUpToDate bool
}

// access to our underlying database
type Databaser interface {
	Get(*acproto.ACPY2MapDatum) (bool, error)
	GetInternal(string, string) ([]byte, bool)
	SetInternal(string, string, []byte)
	DelInternal(string, string)
	Repartition(*soty.Loc, uint64) (bool, uint64)
}

// access to _conf database (store sdb)
type ConfDBer interface {
	Get(*acproto.ACPY2MapDatum) (bool, error)
}

var dl = diag.Logger("ring")

func New(name string, db Databaser, myid string, mydc string, myrack string, sdb ConfDBer, grpcp *gclient.Defaults) *P {

	p := &P{
		name:   name,
		db:     db,
		cfdb:   sdb,
		myid:   myid,
		mydc:   mydc,
		myrack: myrack,
		all:    newPart(mydc),
	}

	distGrpcp = grpcp
	p.configure()
	go p.periodicReconfig()
	return p
}

func (p *P) Close() {
	close(p.stop)
	p.done.Wait()
}

// ################################################################

func (p *P) GetLoc(shard uint32) *soty.Loc {

	p.lock.RLock()
	defer p.lock.RUnlock()

	id := partShard2TreeID(p.ringbits, shard)
	idx := partShard2Idx(p.ringbits, shard)
	isl := true

	if len(p.part) != 0 {
		isl = p.part[idx].isLocal
	}

	return &soty.Loc{
		Shard:   shard,
		TreeID:  id,
		PartIdx: idx,
		IsLocal: isl,
	}
}

func (p *P) GetLocN(partidx int) *soty.Loc {

	p.lock.RLock()
	defer p.lock.RUnlock()

	if partidx < len(p.part) {
		x := p.part[partidx]

		return &soty.Loc{
			Shard:   partIdx2Shard(p.ringbits, partidx),
			TreeID:  partIdx2TreeID(p.ringbits, partidx),
			PartIdx: partidx,
			IsLocal: x.isLocal,
		}
	}
	return &soty.Loc{
		Shard:   0,
		TreeID:  0,
		PartIdx: 0,
		IsLocal: true,
	}
}

func (p *P) NumParts() int {

	p.lock.RLock()
	defer p.lock.RUnlock()

	if len(p.part) != 0 {
		return len(p.part)
	}

	return 1
}

func (p *P) NumReplicas() int {

	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.replicas
}

func (p *P) NumDC(loc *soty.Loc) int {

	p.lock.RLock()
	defer p.lock.RUnlock()

	if len(p.part) == 0 {
		return len(p.all.dc)
	}

	if loc.PartIdx >= len(p.part) {
		return 0
	}

	part := p.part[loc.PartIdx]

	return len(part.dc)

}

func (p *P) CurrVer() uint64 {

	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.currVer
}

// dc == "" => local
// dc == "*" all
func (p *P) GetConf(dcreq string) *acproto.ACPY2RingConfReply {

	p.lock.RLock()
	defer p.lock.RUnlock()

	res := &acproto.ACPY2RingConfReply{
		Version:  proto.Uint64(p.currVer),
		IsStable: proto.Bool(p.currVer == p.stableVer),
	}

	if len(p.part) == 0 {
		return res
	}

	for pidx, pt := range p.part {
		for dcidx, dc := range pt.dc {
			if dcreq != "*" {
				if dcidx != 0 && dcreq != "" {
					continue
				}
				if dcreq != "" && dcreq != dc.dcname {
					continue
				}
			}

			if !dc.isBoundary {
				continue
			}

			sh := partIdx2Shard(p.ringbits, pidx)

			res.Part = append(res.Part, &acproto.ACPY2RingPart{Shard: proto.Uint32(sh), Server: dc.servers})

		}
	}

	return res
}

// ##############################################################

func (p *P) periodicReconfig() {

	for {
		p.maybeReconfigure()

		select {
		case <-p.stop:
			// stop the repartitioner
			close(p.restop)
			p.redone.Wait()
			// we are now done
			p.done.Done()
			return
		case <-time.After(15 * time.Second):
			continue
		}
	}
}
