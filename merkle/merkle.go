// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-14 10:59 (EDT)
// Function: merkle tree

package merkle

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/jaw0/acgo/diag"
	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/soty"
)

type D struct {
	name   string
	db     MStorer
	grpcp  *gclient.Defaults
	expire uint64
	nlock  [NLOCK]sync.Mutex // to protect on disk nodes
	cache  [NLOCK]leafCache
	chch   chan *change
	stop   chan struct{}
	done   sync.WaitGroup
}

type Result struct {
	Map      string
	Level    int
	TreeID   uint16
	Version  uint64
	Shard    uint32
	Key      string
	Hash     []byte
	KeyCount int64
	Children int
	IsValid  bool
}

type leafCache struct {
	ver    uint64
	mkey   string
	treeID uint16
	dirty  bool
	fixme  bool
	leaves *LeafSaves
}

type change struct {
	keyCount int64
	ver      uint64
	children int32
	treeID   uint16
	level    int
	fixme    bool
	hash     []byte
}
type changes []*change

type MStorer interface {
	Get(rec *acproto.ACPY2MapDatum) (bool, error)
	Put(rec *acproto.ACPY2MapDatum) (int, *soty.Loc)
	Del(rec *acproto.ACPY2MapDatum) error
	MGet(string) ([]byte, bool)
	MPut(string, []byte)
	MDel(string)
	Range(string, string, string, func(string, []byte) bool)
	DistribLocal(*soty.Loc, *acproto.ACPY2DistRequest, func(int))
	//...
}

type Ringerizer interface {
	RandomAEPeer(*soty.Loc) string
	GetLoc(uint32) *soty.Loc
}

var dl = diag.Logger("merkle")

const (
	F16       = 0xFFFFFFFFFFFFFFFF
	HEIGHT    = 10
	NLOCK     = 137
	HASHLEN   = 16
	CHQLEN    = 10000
	MAXTODO   = 1000
	MAXREPART = 10240
)

func New(name string, db MStorer, expire uint64, grpcp *gclient.Defaults) *D {

	m := &D{
		name:   name,
		db:     db,
		expire: expire,
		grpcp:  grpcp,
		chch:   make(chan *change, CHQLEN),
		stop:   make(chan struct{}),
	}

	m.done.Add(2)
	go m.flusher()
	go m.periodic()

	return m
}

func (m *D) Close() {
	close(m.stop)
	m.done.Wait()
	m.leafCacheFlushAll()
	m.updater(true)
}

func (m *D) flusher() {

	defer m.done.Done()

	for {
		randomDelay()
		m.leafCacheFlushAll()

		select {
		case <-m.stop:
			return
		case <-time.After(5 * time.Second):
			continue
		}
	}
}

func (m *D) periodic() {

	defer m.done.Done()

	for {
		m.updater(false)

		if len(m.chch) > MAXTODO {
			// process now, don't wait
			continue
		}

		randomDelay()

		select {
		case <-m.stop:
			return
		case <-time.After(5 * time.Second):
			continue
		}
	}
}

//################################################################

func (m *D) Add(key string, ver uint64, loc *soty.Loc, shard uint32, fixme bool) {

	mkey := merkleKey(HEIGHT, loc.TreeID, ver)
	lockno := merkleLockNo(HEIGHT, loc.TreeID, ver)
	dl.Debug("leaf %d %016X => %s lock %d; %s", loc.TreeID, ver, mkey, lockno, key)

	m.nlock[lockno].Lock()
	defer m.nlock[lockno].Unlock()

	leafs := m.leafCacheGet(lockno, mkey)

	// check
	found := false

	for _, l := range leafs.Save {
		if l.Version == ver && string(l.Key) == key {
			found = true
			break
		}
	}

	if found && !fixme {
		dl.Debug("found")
		return
	}

	if !found {
		// add
		leafs.Save = append(leafs.Save, &LeafSave{
			Key:     key,
			Shard:   shard,
			Version: ver,
		})
	}

	sort.Sort(leafs)

	m.leafCacheSet(lockno, loc.TreeID, ver, fixme, leafs)
}

func (m *D) Del(key string, ver uint64, loc *soty.Loc) {

	mkey := merkleKey(HEIGHT, loc.TreeID, ver)
	lockno := merkleLockNo(HEIGHT, loc.TreeID, ver)
	dl.Debug("leaf %d %016X => %s lock %d; %s", loc.TreeID, ver, mkey, lockno, key)

	m.nlock[lockno].Lock()
	defer m.nlock[lockno].Unlock()

	leafs := m.leafCacheGet(lockno, mkey)

	// find
	idx := -1
	for i, l := range leafs.Save {
		if l.Version == ver && string(l.Key) == key {
			idx = i
		}
	}
	if idx == -1 {
		// not found
		return
	}

	// delete
	copy(leafs.Save[idx:], leafs.Save[idx+1:])
	leafs.Save[len(leafs.Save)-1] = nil
	leafs.Save = leafs.Save[:len(leafs.Save)-1]

	m.leafCacheSet(lockno, loc.TreeID, ver, false, leafs)
}

func (m *D) FixLeaf(ver uint64, treeid uint16) {

	mkey := merkleKey(HEIGHT, treeid, ver)
	lockno := merkleLockNo(HEIGHT, treeid, ver)
	dl.Debug("leaf %d %016X => %s lock %d", treeid, ver, mkey, lockno)

	m.nlock[lockno].Lock()
	defer m.nlock[lockno].Unlock()

	leafs := m.leafCacheGet(lockno, mkey)

	m.leafCacheSet(lockno, treeid, ver, true, leafs)
	m.leafCacheFlush(lockno)
}

func (m *D) Move(key string, ver uint64, shard uint32, oldLoc *soty.Loc, newLoc *soty.Loc) {

	if newLoc.TreeID == oldLoc.TreeID {
		return
	}

	m.Add(key, ver, newLoc, shard, false)
	m.Del(key, ver, oldLoc)
}

func (m *D) haveVer(key string) (uint64, bool) {

	r := acproto.ACPY2MapDatum{
		Map: m.name,
		Key: key,
	}

	ok, err := m.db.Get(&r)

	if !ok || err != nil {
		return 0, false
	}

	return r.GetVersion(), true
}

func (m *D) Xfer(key string, ver uint64, oldLoc *soty.Loc, newLoc *soty.Loc) {

	// send to new server
	req := &acproto.ACPY2DistRequest{
		Hop:    10, // prevent wide distribution
		Expire: soty.Now() + uint64(10*time.Second),
		Data: &acproto.ACPY2MapDatum{
			Map: m.name,
			Key: key,
		}}
	m.db.Get(req.Data)

	m.db.DistribLocal(newLoc, req, func(unused int) {
		m.Del(key, ver, oldLoc)
		m.db.Del(req.Data)
	})
}

//################################################################

func (m *D) GetResponse(level int, treeid uint16, ver uint64) []*Result {

	if level == HEIGHT {
		return m.getLeafResponse(level, treeid, ver)
	} else {
		return m.getNodeResponse(level, treeid, ver)
	}
}

func (m *D) getLeafResponse(level int, treeid uint16, ver uint64) []*Result {

	mkey := merkleKey(level, treeid, ver)
	leaves := m.leafGet(mkey)
	var res []*Result

	for _, n := range leaves.Save {
		res = append(res, &Result{
			Map:      m.name,
			TreeID:   treeid,
			Shard:    n.Shard,
			Level:    level + 1,
			Key:      string(n.Key),
			Version:  n.Version,
			KeyCount: 1,
			IsValid:  true,
		})
	}

	return res
}

func (m *D) getNodeResponse(level int, treeid uint16, ver uint64) []*Result {

	mkey := merkleKey(level, treeid, ver)
	nodes := m.nodeGet(mkey)
	var res []*Result

	for _, n := range nodes.Save {
		res = append(res, &Result{
			Map:      m.name,
			TreeID:   treeid,
			Level:    level + 1,
			KeyCount: n.KeyCount,
			Children: int(n.Children),
			Hash:     []byte(n.Hash),
			Version:  merkleNextVersion(level, ver, int(n.Slot)),
			// Isvalid: ring->is_stable
		})
	}

	return res
}

//################################################################

func (m *D) Repartition(ring Ringerizer, loc *soty.Loc, ver uint64) (bool, uint64) {

	// iterate 10/tree/ver - end | maxiter
	//  iterate keys
	//   part1 = new part
	//   still local?
	//     next if part0 == part1
	//     add( part1, ... )
	//     del( part0, ... )
	//   else
	//     distrib to new server

	start := merkleKey(HEIGHT, loc.TreeID, ver)
	end := merkleKey(HEIGHT, loc.TreeID+1, 0)
	count := 0
	done := true

	m.db.Range("m", start, end, func(key string, val []byte) bool {
		leafs := bytes2leaf(val)
		for _, l := range leafs.Save {
			count++
			m.repart(ring, loc, l)
			ver = l.Version
		}

		if count >= MAXREPART {
			done = false
			return false
		}
		return true
	})

	return done, ver
}

func (m *D) repart(ring Ringerizer, loc *soty.Loc, l *LeafSave) {

	newLoc := ring.GetLoc(l.Shard)

	if newLoc.IsLocal {
		m.Move(l.Key, l.Version, l.Shard, loc, newLoc)
		return
	}

	m.Xfer(l.Key, l.Version, loc, newLoc)
}

//################################################################

func (m *D) queueLeafNext(treeid uint16, ver uint64, keyct int, fixme bool, leaves *LeafSaves) {

	c := &change{
		keyCount: int64(keyct),
		ver:      merkleLevelVersion(HEIGHT, ver),
		treeID:   treeid,
		level:    HEIGHT,
		children: 0,
		fixme:    fixme,
	}

	if keyct != 0 {
		c.children = 1
	}

	if leaves != nil {
		c.hash = leafHash(leaves)
	}

	dl.Debug("qln: %#v", c)

	m.chch <- c
}

// ################################################################

func (m *D) fix(r *Result) {

	dl.Debug("fixing node %d/%x/%X", r.Level, r.TreeID, r.Version)

	mkey := merkleKey(r.Level, r.TreeID, r.Version)
	lockno := merkleLockNo(r.Level, r.TreeID, r.Version)
	dl.Debug("node %s", mkey)

	m.nlock[lockno].Lock()
	defer m.nlock[lockno].Unlock()

	ns := m.nodeGet(mkey)

	// RSN - verify keycount, children, ?

	c := &change{
		keyCount: r.KeyCount,
		children: int32(r.Children),
		treeID:   r.TreeID,
		level:    r.Level,
		ver:      r.Version,
		fixme:    true,
	}

	if c.children != 0 {
		c.hash = nodeHash(ns)
	}

	m.chch <- c
}

func (m *D) fixkeys(ver uint64, treeid uint16) {

	m.FixLeaf(ver, treeid)
	m.updater(false)
}

//################################################################

func (m *D) IterVersion(treeid uint16, startVer uint64, endVer uint64, lambda func(string, uint64, uint32) bool) {

	start := merkleKey(HEIGHT, treeid, startVer)
	end := merkleKey(HEIGHT, treeid+1, 0)

	if endVer != 0 {
		end = merkleKey(HEIGHT, treeid, endVer)
	}

	dl.Debug("iter %s %x [%x - %x] : %s - %s", m.name, treeid, startVer, endVer, start, end)

	m.db.Range("m", start, end, func(key string, val []byte) bool {
		dl.Debug(" + node %s", key)
		leafs := bytes2leaf(val)
		for _, l := range leafs.Save {
			ok := lambda(l.Key, l.Version, l.Shard)
			if !ok {
				return ok
			}
		}
		return true
	})
}

//################################################################

// merkle number masked for specified level
func merkleLevelVersion(level int, ver uint64) uint64 {
	mask := uint64(F16 << uint((16-level)<<2))
	return ver & mask
}

func merkleKey(level int, treeid uint16, ver uint64) string {
	return fmt.Sprintf("%02X/%04X/%0*X", level, treeid, HEIGHT, merkleLevelVersion(level, ver)>>((16-HEIGHT)<<2))
}

func merkleLockNo(level int, treeid uint16, ver uint64) int {
	return int((merkleLevelVersion(level, ver) ^ uint64(treeid)) % NLOCK)
}

func merkleSlot(level int, ver uint64) int32 {
	return int32((ver >> uint((16-level)<<2)) & 0xF)
}

func merkleNextVersion(level int, ver uint64, slot int) uint64 {
	slsh := (16 - level - 1) << 2
	return merkleLevelVersion(level, ver) | uint64(slot<<uint(slsh))
}

func randomDelay() {
	// 0.1 sec, to prevent falling into lockstep
	time.Sleep(time.Duration(rand.Intn(100000000)) * time.Nanosecond)
}

//################################################################
func (l changes) Len() int {
	return len(l)
}

// sort by level descending, version ascending
func (l changes) Less(i int, j int) bool {

	if l[i].level > l[j].level {
		return true
	}
	if l[i].level < l[j].level {
		return false
	}
	if l[i].ver < l[j].ver {
		return true
	}
	return false
}
func (l changes) Swap(i int, j int) {
	l[i], l[j] = l[j], l[i]
}
