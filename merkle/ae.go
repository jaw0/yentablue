// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-28 14:39 (EDT)
// Function: anti-entropy

package merkle

import (
	"expvar"
	"golang.org/x/net/context"
	"sync"
	"time"

	"github.com/jaw0/acgo/diag"
	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/putstatus"
	"github.com/jaw0/yentablue/soty"
)

const (
	MAXAE    = 20
	BUFSZ    = 1000
	MINBUF   = 4
	MAXFETCH = 256
	TIMEOUT  = 30 * time.Second
	TOONEW   = uint64(60 * time.Second)
)

var dlae = diag.Logger("ae")
var dlwk = diag.Logger("aework")
var aeCount = expvar.NewInt("ae_count")
var aeErrs = expvar.NewInt("ae_errs")
var aeMismatch = expvar.NewInt("ae_mismatch")
var aeFetch = expvar.NewInt("ae_fetch")
var aeFixed = expvar.NewInt("ae_fixed")
var aeXfered = expvar.NewInt("ae_xfered")

type todo struct {
	ver   uint64
	level int
}
type needkey struct {
	key string
	ver uint64
}

type aeWork struct {
	merk     *D
	loc      *soty.Loc
	ring     Ringerizer
	lock     sync.Mutex
	work     [HEIGHT + 1][]todo
	workc    chan todo
	keys     []needkey
	keysc    chan needkey
	produce  sync.WaitGroup
	done     sync.WaitGroup
	missing  int64
	mismatch int64
	fetched  int64
	failed   bool
}

func (m *D) AE(loc *soty.Loc, peer string, ring Ringerizer) (bool, int64, int64) {

	if !loc.IsLocal {
		dlae.Debug("AE check %s[%d] - not local", m.name, loc.TreeID)
		m.RemoveTree(ring, loc, peer)
		return true, 0, 0
	}

	if peer == "" {
		dlae.Debug("AE check %s[%d] - no peer", m.name, loc.TreeID)
		return true, 0, 0
	}

	dlae.Debug("AE check %s[%d] with %s", m.name, loc.TreeID, peer)

	wd := &aeWork{
		merk:   m,
		loc:    loc,
		ring:   ring,
		workc:  make(chan todo, BUFSZ),
		keysc:  make(chan needkey, BUFSZ),
		failed: false,
	}

	// start at the root
	wd.pushWork(0, 0)

	// start worker(s)

	if MAXAE < 2 {
		wd.done.Add(1)
		wd.aeWorker(peer)
	} else {
		wd.startWorkers(peer)
	}

	aeCount.Add(1)
	aeMismatch.Add(wd.missing + wd.mismatch)
	aeFetch.Add(wd.fetched)

	if wd.failed {
		dlae.Debug("ae done failed; missing %d mismatch %d fetched %d", wd.missing, wd.mismatch, wd.fetched)
	} else {
		dlae.Debug("ae done sucess: missing %d mismatch %d fetched %d", wd.missing, wd.mismatch, wd.fetched)
	}
	return !wd.failed, wd.missing + wd.mismatch, wd.fetched
}

func (wd *aeWork) startWorkers(peer string) {

	// start 1st worker
	wd.done.Add(1)
	go wd.aeWorker(peer)

	alldone := make(chan struct{})

	go func() {
		// break the loop if the workers all finish
		wd.done.Wait()
		close(alldone)
	}()

	// start more workers
	for i := 1; i < MAXAE; i++ {
		delay := 60 * time.Second

		if i > 0 && i < MAXAE/2 {
			delay = 30 * time.Second
		}

		select {
		case <-alldone:
			dlwk.Debug("all done")
			break
		case <-time.After(delay):
			break
		}

		if wd.queuesEmpty() {
			dlwk.Debug("queues empty")
			break
		}

		p := peer

		if (i > 2 && i&1 == 0) || (i > MAXAE/2) {
			// spread it out over some additional peers
			p = wd.ring.RandomAEPeer(wd.loc)
		}

		dlwk.Debug("starting ae worker %d with %s", i, p)
		wd.done.Add(1)
		go wd.aeWorker(p)
	}

	// wait for done
	dlwk.Debug("waiting for workers to finish")
	wd.done.Wait()
}

// ################################################################

func (wd *aeWork) aeWorker(peer string) {
	defer wd.done.Done()

	ac, closer, err := gclient.GrpcClient(&gclient.GrpcConfig{
		Addr: []string{peer, wd.ring.RandomAEPeer(wd.loc)},
	}, wd.merk.grpcp)
	if err != nil {
		aeErrs.Add(1)
		return
	}
	defer closer()

	ok := true

	pc := &produceConsume{wd, false}
	pc.producing()
	defer pc.consuming()

	done := make(chan struct{})

	go func() {
		// if there are no workers still producing, stop
		wd.produce.Wait()
		close(done)
	}()

	for {
		dlwk.Debug("shifting")
		wd.shiftKeys()
		wd.shiftWork()

		if wd.queuesEmpty() {
			pc.consuming()
		}

		dlwk.Debug("waiting...")
		select {
		case k := <-wd.keysc:
			pc.producing()
			dlwk.Debug("fetching keys")
			ok = wd.fetchKeys(k, ac)
		case t := <-wd.workc:
			pc.producing()
			dlwk.Debug("checking merk")
			ok = wd.checkMerk(t, ac)
		case <-done:
			dlwk.Debug("done")
			return
		}
		if !ok {
			dlwk.Debug("!ok")
			return
		}
	}
}

func (wd *aeWork) checkMerk(t todo, ac acproto.ACrpcClient) bool {

	now := soty.Now()

	if t.ver > now-TOONEW {
		// too new, don't bother
		dlae.Debug("too new %d/%X", t.level, t.ver)
		return true
	}
	if t.level >= HEIGHT-1 && wd.merk.expire != 0 && t.ver < now-wd.merk.expire {
		// about to expire, skip
		dlae.Debug("expires soon %d/%X %d %d", t.level, t.ver, now, wd.merk.expire)
		return true
	}

	// fetch results from peer + self. compare
	dlae.Debug("checking %d/%d/%X", t.level, wd.loc.TreeID, t.ver)

	res, ok := wd.aeFetchMerkle(ac, t.level, t.ver)
	if !ok {
		return wd.failure()
	}

	ours := wd.merk.GetResponse(t.level, wd.loc.TreeID, t.ver)

	dlae.Debug("  rcvd %d, have %d", len(res), len(ours))

	if len(res) == 0 && len(ours) == 0 {
		// other side sent us wack results
		return true
	}

	if t.level == HEIGHT {
		wd.checkLeaf(t, res, ours)
	} else {
		wd.checkNode(t, res, ours)
	}
	return true
}

func (wd *aeWork) checkLeaf(t todo, res []*acproto.ACPY2CheckValue, ours []*Result) {

	var missing int64
	var newer int

	have := make(map[string]int)

	for i, r := range ours {
		have[r.Key] = i
	}

	for _, r := range res {
		k := r.GetKey()
		v := r.GetVersion()

		i, h := have[k]
		var ov uint64

		if h {
			ov = ours[i].Version
		} else {
			// do we have another (newer) version?
			ov, _ = wd.merk.haveVer(k)
			newer++
		}

		if ov >= v {
			// got it. cool.
			dlae.Debug("  ok %s %x", k, v)
			delete(have, k)
			continue
		}

		dlae.Debug("  missing key %s %x", k, v)
		wd.pushKey(k, v)
		missing++
	}

	if missing != 0 {
		wd.lock.Lock()
		defer wd.lock.Unlock()
		wd.missing += missing
	}

	if len(have) == 0 && missing == 0 && newer == 0 {
		// nothing missing, nothing extra. why are we looking here?
		// hash might be wrong - fix
		dlae.Debug("  bad hash? L/%X", t.ver)
		aeFixed.Add(1)
		wd.merk.fixkeys(t.ver, wd.loc.TreeID)
		return
	}

	for k, i := range have {
		v := ours[i].Version

		// do we have a newer ver?
		ov, _ := wd.merk.haveVer(k)

		if ov > v {
			// remove stale data
			dlae.Debug("  del stale [%X %s]", v, k)
			aeFixed.Add(1)
			wd.merk.Del(k, v, wd.loc)
			continue
		}
		if ov == v {
			// is this supposed to be here?
			correctloc := wd.ring.GetLoc(ours[i].Shard)

			if correctloc.TreeID != wd.loc.TreeID {
				dlae.Debug("  moving merkle [%X %s]", v, k)
				aeFixed.Add(1)
				wd.merk.Move(k, v, ours[i].Shard, wd.loc, correctloc)
				continue
			}
			if !correctloc.IsLocal {
				// this belongs on a different server
				dlae.Debug("  relocating [%X %s]", v, k)
				aeXfered.Add(1)
				wd.merk.Xfer(k, v, wd.loc, correctloc)
				continue
			}
		}
		dlae.Debug("  extra key? [%X %s]", v, k)

		// else - other side is missing this key
	}
}

func (wd *aeWork) checkNode(t todo, res []*acproto.ACPY2CheckValue, ours []*Result) {

	var mismatch int64
	var missing int64

	have := make(map[uint64]int)

	for i, r := range ours {
		have[r.Version] = i
	}

	for _, r := range res {
		v := r.GetVersion()
		i, found := have[v]

		if !found {
			dlae.Debug("  missing node %d %x", r.GetLevel(), v)
			wd.pushWork(int(r.GetLevel()), v)
			missing++
			continue
		}

		delete(have, v)

		if !cmpHash(r.Hash, ours[i].Hash) {
			dlae.Debug("  mismatch %d %x", r.GetLevel(), v)
			wd.pushWork(int(r.GetLevel()), v)
			mismatch++
			continue
		}
		dlae.Debug("  ok %d %X", r.GetLevel(), v)
	}

	if mismatch+missing != 0 {
		wd.lock.Lock()
		defer wd.lock.Unlock()
		wd.missing += missing
		wd.mismatch += mismatch
	}

	if len(have) == 0 && mismatch+missing == 0 && t.level > 0 {
		// hash might be wrong - fix
		dlae.Debug("  bad hash? %d/%X", t.level, t.ver)
		for _, r := range ours {
			aeFixed.Add(1)
			wd.merk.fix(r)
		}
	}

	// we have nodes the other side does not,
	// - is the other side out of date?
	// - or are they leftovers that should have been deleted?
	for v, _ := range have {
		dlae.Debug("  extra node %d/%X", t.level+1, v)
		cv, _ := wd.dfsCheckNode(t.level+1, v)
		if cv != 0 {
			dlae.Debug("  candidate %X", cv)
			wd.pushWork(HEIGHT, cv)
		}
	}
}

func (wd *aeWork) dfsCheckNode(level int, ver uint64) (uint64, bool) {

	ours := wd.merk.GetResponse(level, wd.loc.TreeID, ver)

	if len(ours) == 0 {
		// we have an entry that has no children nodes - fix
		dlae.Debug("empty %d/%X", level, ver)
		wd.merk.fix(&Result{
			KeyCount: 0,
			Children: 0,
			TreeID:   wd.loc.TreeID,
			Level:    level,
			Version:  ver,
		})
		// we found a problem, stop searching
		aeFixed.Add(1)
		return 0, true
	}

	if level != HEIGHT {
		var bcv uint64
		tok := false
		for _, r := range ours {
			cv, ok := wd.dfsCheckNode(r.Level, r.Version)
			if ok {
				tok = true
				if level < HEIGHT-2 {
					return 0, true
				}
			}
			if cv != 0 {
				bcv = cv
			}
		}
		if tok {
			return 0, true
		}
		return bcv, false
	}

	for _, r := range ours {
		ov, _ := wd.merk.haveVer(r.Key)
		mv := merkleLevelVersion(HEIGHT, ov)

		if mv == ver {
			correctloc := wd.ring.GetLoc(r.Shard)

			if correctloc.TreeID == wd.loc.TreeID {
				// found valid data here. other side must be ood
				dlae.Debug("    found valid data. ending search [%X %s] = %X", ov, r.Key, ver)
				return 0, true
			}
			dlae.Debug("    moving merkle [%X %s]", r.Version, r.Key)
			aeFixed.Add(1)
			wd.merk.Move(r.Key, r.Version, r.Shard, wd.loc, correctloc)
		}
		if mv > ver {
			// remove stale data
			dlae.Debug("    removing stale merkle [%X %s]", r.Version, r.Key)
			aeFixed.Add(1)
			wd.merk.Del(r.Key, r.Version, wd.loc)
		}
		if mv < ver {
			dlae.Debug("    time travel? [%X %s] > %X", ov, r.Key, ver)
		}
	}

	return ver, false
}

// ################################################################

func (wd *aeWork) addKeyToReq(k needkey, req *acproto.ACPY2GetSet) {

	req.Data = append(req.Data, &acproto.ACPY2MapDatum{
		Map:     wd.merk.name,
		Key:     k.key,
		Version: k.ver,
	})
}

func (wd *aeWork) addMoreKeysToReq(req *acproto.ACPY2GetSet) {
	// gather more keys
	for {
		select {
		case k := <-wd.keysc:
			wd.addKeyToReq(k, req)
		default:
			return
		}
		if len(req.Data) > MAXFETCH {
			return
		}
	}
}

func (wd *aeWork) fetchKeys(k needkey, ac acproto.ACrpcClient) bool {

	req := &acproto.ACPY2GetSet{}
	wd.addKeyToReq(k, req)
	wd.addMoreKeysToReq(req)

	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	res, err := ac.Get(ctx, req)
	cancel()

	if err != nil {
		aeErrs.Add(1)
		dlae.Debug("fetch failed: %v", err)
		return wd.failure()
	}

	var fetched int64

	for _, r := range res.Data {
		rp, _ := wd.merk.db.Put(r)
		fetched++

		if rp == putstatus.HAVE {
			// we requested this, but already have it? is it missing from the merkle tree?
			dlae.Debug("fix key %s", r.GetKey())
			wd.merk.Add(r.GetKey(), r.GetVersion(), wd.loc, r.GetShard(), true)
		}
	}

	wd.lock.Lock()
	defer wd.lock.Unlock()
	wd.fetched += fetched

	return true
}

// ################################################################

func (wd *aeWork) pushWork(l int, ver uint64) {
	wd.lock.Lock()
	defer wd.lock.Unlock()

	wd.work[l] = append(wd.work[l], todo{level: l, ver: ver})
	wd.shiftWorkL()
}

func (wd *aeWork) pushKey(k string, ver uint64) {
	wd.lock.Lock()
	defer wd.lock.Unlock()

	select {
	case wd.keysc <- needkey{key: k, ver: ver}:
		wd.shiftKeysL()
		break
	default:
		wd.keys = append(wd.keys, needkey{key: k, ver: ver})
	}
}

func (wd *aeWork) shiftWorkL() {

	maxc := BUFSZ

	// move the highest level work we have

	for l := HEIGHT; l >= 0; l-- {
		n := 0

	FOR:
		for len(wd.workc) < maxc && n < len(wd.work[l]) {
			select {
			case wd.workc <- wd.work[l][n]:
				n++
			default:
				break FOR
			}
		}

		wd.work[l] = wd.work[l][n:]

		maxc = min(MINBUF, MAXAE)
	}
}

func (wd *aeWork) shiftWork() {
	wd.lock.Lock()
	defer wd.lock.Unlock()
	wd.shiftWorkL()
}

func (wd *aeWork) shiftKeys() {
	wd.lock.Lock()
	defer wd.lock.Unlock()
	wd.shiftKeysL()
}
func (wd *aeWork) shiftKeysL() {

	n := 0

FOR:
	for len(wd.keysc) < BUFSZ && n < len(wd.keys) {
		select {
		case wd.keysc <- wd.keys[n]:
			n++
		default:
			break FOR
		}
	}

	wd.keys = wd.keys[n:]
}

func (wd *aeWork) queuesEmpty() bool {

	if len(wd.keysc) > 0 || len(wd.workc) > 0 {
		return false
	}

	wd.lock.Lock()
	defer wd.lock.Unlock()

	if len(wd.keys) > 0 {
		return false
	}

	for l := 0; l <= HEIGHT; l++ {
		if len(wd.work[l]) > 0 {
			return false
		}
	}

	return true
}

// ################################################################

// keep track of whether workers are producing or consuming
// if all workers are done producing, they are done

func (wd *aeWork) producing() {
	wd.produce.Add(1)
}
func (wd *aeWork) consuming() {
	wd.produce.Done()
}

type produceConsume struct {
	wd          *aeWork
	isProducing bool
}

func (pc *produceConsume) producing() {
	if !pc.isProducing {
		pc.wd.producing()
		pc.isProducing = true
	}
}
func (pc *produceConsume) consuming() {
	if pc.isProducing {
		pc.wd.consuming()
		pc.isProducing = false
	}
}

func (wd *aeWork) failure() bool {
	wd.lock.Lock()
	defer wd.lock.Unlock()
	wd.failed = true
	return false
}

// ################################################################

func (wd *aeWork) aeFetchMerkle(ac acproto.ACrpcClient, level int, ver uint64) ([]*acproto.ACPY2CheckValue, bool) {

	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	res, err := ac.GetMerkle(ctx, &acproto.ACPY2CheckRequest{
		Map:     wd.merk.name,
		Treeid:  uint32(wd.loc.TreeID),
		Level:   int32(level),
		Version: ver,
	})
	cancel()

	if err != nil {
		aeErrs.Add(1)
		return nil, false
	}

	return res.Check, true
}

func cmpHash(a []byte, b []byte) bool {

	if len(a) != len(b) {
		return false
	}

	for i, c := range a {
		if c != b[i] {
			return false
		}
	}
	return true
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
