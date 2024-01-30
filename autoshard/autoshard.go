// Copyright (c) 2022
// Author: Jeff Weisberg <tcp4me.com!jaw>
// Created: 2022-Apr-20 13:58 (EDT)
// Function: autoconfigure/reconfigure sharding

package autoshard

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"time"

	"github.com/jaw0/acdiag"
	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/config"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/raftish"
	"github.com/jaw0/yentablue/ring"
	ringcf "github.com/jaw0/yentablue/ring/config"
	"github.com/jaw0/yentablue/store"
)

var dl = diag.Logger("autoshard")

type AS struct {
	rft  *raftish.R
	sdb  *store.SDB
	pdb  *kibitz.DB
	dbs  []string
	stop chan struct{}
}

type serverInfo struct {
	datacenter string
	rack       string
	availSpace int32
	totalSpace int32
	isUp       bool
}

func Start(rft *raftish.R, sdb *store.SDB, pdb *kibitz.DB) *AS {

	cf := config.Cf()
	var dbs []string

	for _, db := range cf.Database {
		dbs = append(dbs, db.Name)
	}

	as := &AS{
		rft:  rft,
		sdb:  sdb,
		pdb:  pdb,
		dbs:  dbs,
		stop: make(chan struct{}),
	}

	go as.Run()
	return as
}

func (as *AS) Stop() {
	as.stop <- struct{}{}
}

func (as *AS) Run() {

	tick := time.NewTicker(time.Minute)
	for {
		select {
		case <-as.stop:
			tick.Stop()
			return
		case <-tick.C:
			as.maybeAutoshard()
		}
	}
}

func (as *AS) maybeAutoshard() {

	// get data on all servers
	sinfo := as.getServers()

	// autoshard each db where I am the elected leader
	for _, dbname := range as.dbs {
		if as.rft.AmLeader(dbname) {
			as.autoshard(dbname, sinfo[dbname])
		}
	}
}

func (as *AS) autoshard(dbname string, si map[string]*serverInfo) {

	rcf, ver, _ := as.getRingConfig(dbname)

	if rcf == nil || si == nil || rcf.Replicas == 0 || !rcf.AutoShard {
		return
	}
	if !as.sdb.RingIsStable(dbname) {
		dl.Debug("skipping autoshard '%s' - not yet stable", dbname)
		return
	}
	// skip if some servers report down - wait until they are up or forgotten
	if someAreDown(si) {
		dl.Debug("skipping autoshard '%s' - servers down", dbname)
		return
	}

	dl.Debug("autosharding %s", dbname)

	// add / remove servers
	changed := addRemoveServers(rcf, si)

	// rebalance shards
	if !changed {
		changed = rebalance(rcf, ver, si)
	}

	if changed {
		dl.Verbose("adjusted config for '%s'", dbname)
		as.setRingConfig(dbname, rcf, ver)
	}
}

func addRemoveServers(rcf *ringcf.Ring, info map[string]*serverInfo) bool {

	const numNums = 4
	dbServers := copyServerInfo(info)
	rgServers := serversOnRing(rcf)
	changed := false

	var newParts []ringcf.Part
	var numUsed []uint32

	dl.Debug("dbS %#v", dbServers)
	dl.Debug("rgS %#v", rgServers)

	for name, si := range dbServers {
		if ri, ok := rgServers[name]; ok {
			ri.Datacenter = si.datacenter // update to current value
			ri.Rack = si.rack
			newParts = append(newParts, *ri)
			delete(dbServers, name)
			delete(rgServers, name)
			numUsed = append(numUsed, ri.Shard...)

			dl.Debug("matched %s", name)
		}
	}

	// reuse the shard#s from removed servers - less turmoil
	var useNums []uint32

	for name, pi := range rgServers {
		useNums = append(useNums, pi.Shard...)
		dl.Debug("removing %s", name)
		changed = true
	}

	// add new servers
	for name, si := range dbServers {
		dl.Debug("adding %s", name)
		changed = true

		nums := make([]uint32, numNums)
		for i := 0; i < numNums; i++ {
			if len(useNums) != 0 {
				nums[i] = useNums[0]
				useNums = useNums[1:]
			} else {
				nums[i] = newShardNum(numUsed, rcf.RingBits)
				numUsed = append(numUsed, nums[i])
			}
		}

		newParts = append(newParts, ringcf.Part{
			Server:     name,
			Datacenter: si.datacenter,
			Rack:       si.rack,
			Shard:      nums,
		})
	}

	// update number of shards to proper number
	for i := range newParts {
		p := &newParts[i]
		for len(p.Shard) < numNums {
			p.Shard = append(p.Shard, random())
		}
		p.Shard = p.Shard[:numNums]
	}

	rcf.Parts = newParts

	// how many bits?
	ns := len(newParts) * numNums * 4
	rbits := int(math.Ceil(math.Log2(float64(ns))))
	if rbits < 8 {
		rbits = 8
	}
	rcf.RingBits = rbits

	return changed
}

// ################################################################

func serversOnRing(rcf *ringcf.Ring) map[string]*ringcf.Part {

	res := make(map[string]*ringcf.Part)

	for _, p := range rcf.Parts {
		cp := p
		res[p.Server] = &cp
	}

	return res
}

func (as *AS) getRingConfig(dbname string) (*ringcf.Ring, uint64, error) {
	rec := &acproto.ACPY2MapDatum{
		Map: "_conf",
		Key: dbname + ".cf",
	}

	ok, _ := as.sdb.Get(rec)

	if !ok {
		return nil, 0, nil
	}

	rcf, err := ringcf.FromBytes(rec.Value)

	return rcf, rec.Version, err
}

func Now() uint64 {
	return uint64(time.Now().UnixNano())
}
func (as *AS) setRingConfig(dbname string, rcf *ringcf.Ring, ver uint64) {

	val, err := ringcf.ToBytes(rcf)
	if err != nil {
		return
	}
	rec := &acproto.ACPY2MapDatum{
		Map:       "_conf",
		Key:       dbname + ".cf",
		Value:     val,
		Version:   Now(),
		IfVersion: ver,
	}

	as.sdb.Put(rec)
}

func (as *AS) getServers() map[string]map[string]*serverInfo {

	// map[dbname][serverid]
	dbsim := make(map[string]map[string]*serverInfo)

	as.pdb.ForAllData(func(id string, isUp bool, v interface{}) {

		hb, ok := v.(*acproto.ACPHeartBeat)
		if !ok {
			return
		}

		pi := hb.PeerInfo

		for _, db := range hb.Database {

			si := &serverInfo{
				isUp:       isUp,
				rack:       pi.Rack,
				datacenter: pi.Datacenter,
				availSpace: db.SpaceAvail,
				totalSpace: db.SpaceTotal,
			}

			dbm := dbsim[db.Name]
			if dbm == nil {
				dbm = make(map[string]*serverInfo)
				dbsim[db.Name] = dbm
			}
			dbm[id] = si
		}
	})

	return dbsim
}

func someAreDown(sinfo map[string]*serverInfo) bool {

	for _, si := range sinfo {
		if !si.isUp {
			return true
		}
	}
	return false
}

// because some systems don't report the info, not because of full
func someHaveNoCapacity(sinfo map[string]*serverInfo) bool {

	for _, si := range sinfo {
		if si.totalSpace == 0 {
			return true
		}
	}
	return false
}

func copyServerInfo(info map[string]*serverInfo) map[string]*serverInfo {
	m := make(map[string]*serverInfo)

	for k, v := range info {
		m[k] = v
	}
	return m
}

func newShardNum(used []uint32, ringbits int) uint32 {

	for i := 0; i < 100; i++ {
		n := random()
		// make sure it is unused

		found := false
		for _, s := range used {
			if ring.PartShard2Idx(ringbits, s) == ring.PartShard2Idx(ringbits, n) {
				found = true
				break
			}
		}
		if !found {
			return n
		}
	}

	return random()
}

func random() uint32 {

	b := make([]byte, 4)
	if _, err := rand.Reader.Read(b); err != nil {
		dl.Fatal("%v", err)
	}

	return binary.LittleEndian.Uint32(b)
}
