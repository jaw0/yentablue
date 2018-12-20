// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-14 12:09 (EDT)
// Function:

package store

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/database"
	"github.com/jaw0/yentablue/merkle"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/putstatus"
	"github.com/jaw0/yentablue/ring"
	"github.com/jaw0/yentablue/soty"
)

type SDB struct {
	dbs      map[string]*database.DB
	Uptodate bool
	stop     chan struct{}
	done     sync.WaitGroup
	// all configured dbs for all servers - for hinted handoff
	lock    sync.RWMutex
	dbconfd map[string]map[string]bool
}

func New() *SDB {

	r := &SDB{
		dbs:     make(map[string]*database.DB),
		dbconfd: make(map[string]map[string]bool),
	}

	r.done.Add(1)
	go r.maint()

	return r
}

func (db *SDB) Close() {

	for _, d := range db.dbs {
		d.Close()
	}

	close(db.stop)
	db.done.Wait()
}

func (db *SDB) Add(name string, d *database.DB) {
	db.dbs[name] = d
}

func (db *SDB) find(name string) (*database.DB, bool) {
	d, ok := db.dbs[name]
	return d, ok
}

func (db *SDB) Put(rec *acproto.ACPY2MapDatum) (int, *soty.Loc) {
	d, ok := db.find(rec.GetMap())
	if !ok {
		return putstatus.NOTME, nil
	}
	return d.Put(rec)
}

func (db *SDB) Get(rec *acproto.ACPY2MapDatum) (bool, error) {
	d, ok := db.find(rec.GetMap())
	if !ok {
		return false, fmt.Errorf("no such map")
	}
	return d.Get(rec)
}

func (db *SDB) GetRange(req *acproto.ACPY2GetRange, res *acproto.ACPY2GetSet) (bool, error) {
	d, ok := db.find(req.GetMap())
	if !ok {
		return false, fmt.Errorf("no such map")
	}
	return d.GetRange(req, res)
}

func (db *SDB) Del(rec *acproto.ACPY2MapDatum) error {
	d, ok := db.find(rec.GetMap())
	if !ok {
		return fmt.Errorf("no such map")
	}
	return d.Del(rec)
}

func (db *SDB) Distrib(loc *soty.Loc, rec *acproto.ACPY2DistRequest, onDone func(int)) {

	r := rec.Data
	if r == nil {
		return
	}

	d, ok := db.find(r.GetMap())
	if !ok {
		return
	}

	d.Distrib(loc, rec, onDone)
}

func (db *SDB) Redirect(rec *acproto.ACPY2MapDatum) {

	name := rec.GetMap()
	d, ok := db.find(name)
	if ok {
		d.Redirect(rec)
		return
	}

	// who has this map?
	for id, dbm := range db.dbconfd {
		if dbm[name] {
			addr := ring.GetPeerAddr(id)
			if addr != "" {
				rec.Location = append(rec.Location, addr)
			}
		}
	}

}

func (db *SDB) Handoff(rec *acproto.ACPY2DistRequest) {

	r := rec.Data
	if r == nil {
		return
	}
	name := r.GetMap()

	db.lock.RLock()
	defer db.lock.RUnlock()

	var dist []string

	// figure out where we can send this
	for id, dbm := range db.dbconfd {
		if dbm[name] {
			addr := ring.GetPeerAddr(id)
			if addr != "" {
				dist = append(dist, addr)
			}
		}
	}

	ring.Handoff(rec, dist)
}

func (db *SDB) GetMerkle(name string, level int, treeid uint16, ver uint64) ([]*merkle.Result, error) {

	d, ok := db.find(name)
	if !ok {
		return nil, fmt.Errorf("no such map")
	}

	res := d.GetMerkle(level, treeid, ver)
	return res, nil
}

func (db *SDB) PeerUpdate(id string, isup bool, pd *kibitz.Export, dat *acproto.ACPHeartBeat) {

	dbcf := make(map[string]bool)

	for _, n := range dat.Database {
		// server reports that it has this db
		dbcf[n] = true
	}

	first := true

	for n, d := range db.dbs {
		d.PeerUpdate(id, isup, dbcf[n], first, pd, dat)
		first = false
	}

	db.lock.Lock()
	defer db.lock.Unlock()
	db.dbconfd[id] = dbcf
}

func (db *SDB) GetRingConf(name string, dc string) (*acproto.ACPY2RingConfReply, error) {

	d, ok := db.find(name)
	if !ok {
		return nil, fmt.Errorf("no such map")
	}

	res := d.GetRingConf(dc)
	return res, nil
}

func (db *SDB) maint() {

	time.Sleep(20 * time.Second) // so we can discover some peers

	for {
		db.ae()
		randomDelay()

		delay := 5 * time.Second
		if db.Uptodate {
			// we're good, no need to check too often
			delay = 30 * time.Second
		}

		select {
		case <-db.stop:
			return
		case <-time.After(delay):
			continue
		}
	}
}

func (db *SDB) ae() {

	ok := true

	for _, d := range db.dbs {
		if !d.AE() {
			ok = false
		}
	}

	if ok {
		db.Uptodate = true
	}
}

func randomDelay() {
	// 0.5 sec, to prevent falling into lockstep
	time.Sleep(time.Duration(rand.Intn(500000000)) * time.Nanosecond)
}
