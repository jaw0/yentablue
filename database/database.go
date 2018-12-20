// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-13 14:06 (EDT)
// Function:

package database

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/hashicorp/golang-lru"

	"github.com/jaw0/acgo/diag"
	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/crypto"
	"github.com/jaw0/yentablue/expire"
	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/merkle"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/putstatus"
	"github.com/jaw0/yentablue/ring"
	"github.com/jaw0/yentablue/soty"
)

type Conf struct {
	Name      string
	Expire    uint64
	MyId      string
	DC        string
	Rack      string
	Secret    string
	CacheSize int
	SDB       ring.ConfDBer
	Grpcp     *gclient.Defaults
}

type BackEnder interface {
	Close()
	Get(sub string, key string) ([]byte, bool, error)
	Put(sub string, key string, val []byte) error
	Del(sub string, key string) error
	Range(sub string, start string, end string, lambda func(string, []byte) bool) error
}

const (
	DATASUB   = "d"
	NDLOCK    = 2053
	CACHESIZE = 1000
)

const (
	FLAG_ENCRYPTED_V1 = (1 << 0)
)

type DB struct {
	name     string
	myid     string
	be       BackEnder
	merk     *merkle.D
	expire   *expire.D
	ring     *ring.P
	enigma   *crypto.Box
	cache    *lru.TwoQueueCache
	datalock [NDLOCK]sync.Mutex
}

var dl = diag.Logger("database")

// ################################################################

func Open(be BackEnder, c Conf) *DB {

	db := &DB{
		name: c.Name,
		myid: c.MyId,
		be:   be,
	}

	exp := c.Expire * uint64(time.Second)

	db.merk = merkle.New(c.Name, db, exp, c.Grpcp)
	db.expire = expire.New(c.Name, db, exp)
	db.ring = ring.New(c.Name, db, c.MyId, c.DC, c.Rack, c.SDB, c.Grpcp)

	if c.Secret != "" {
		db.enigma = crypto.New(c.Secret)
	}

	sz := c.CacheSize
	if sz == 0 {
		sz = CACHESIZE
	}
	if sz > 0 {
		var err error
		db.cache, err = lru.New2Q(sz)

		dl.Debug("cache sz %d", sz)
		if err != nil {
			dl.Fatal("cannot instantiate cache: %v", err)
		}
	}

	return db
}

func (db *DB) Close() {
	db.merk.Close()
	db.expire.Close()
	db.ring.Close()
	db.be.Close()
}

// ################################################################

func (db *DB) GetInternal(sub string, key string) ([]byte, bool) {
	r, found, _ := db.be.Get(sub, key)
	return r, found
}
func (db *DB) SetInternal(sub string, key string, val []byte) {
	db.be.Put(sub, key, val)
}
func (db *DB) DelInternal(sub string, key string) {
	db.be.Del(sub, key)
}

func (db *DB) MGet(key string) ([]byte, bool) {
	return db.GetInternal("m", key)
}
func (db *DB) MPut(key string, val []byte) {
	db.SetInternal("m", key, val)
}
func (db *DB) MDel(key string) {
	db.DelInternal("m", key)
}
func (db *DB) ExGet(key string) ([]byte, bool) {
	return db.GetInternal("e", key)
}
func (db *DB) ExPut(key string, val []byte) {
	db.SetInternal("e", key, val)
}
func (db *DB) Range(sub string, start string, end string, f func(string, []byte) bool) {
	db.be.Range(sub, start, end, f)
}

// ################################################################
func (db *DB) Get(rec *acproto.ACPY2MapDatum) (bool, error) {

	var dr *Record
	cached := false
	key := rec.GetKey()

	// check cache
	if db.cache != nil {
		cval, ok := db.cache.Get(key)
		if ok {
			dr = cval.(*Record)
			cached = true
		}
	}

	if dr == nil {
		// fetch from backend db
		val, found, err := db.be.Get(DATASUB, key)
		dl.Debug("get '%s' -> %d", key, len(val))

		if err != nil {
			return false, err
		}

		if !found || len(val) == 0 {
			dl.Debug("not found")
			return false, nil
		}

		dr = db.bytes2record(val)
	}

	ver := rec.GetVersion()

	if ver != 0 && ver != dr.GetVersion() {
		// someone asked for this particular version, but we do not have.
		dl.Debug("ver not found")
		return false, nil
	}

	now := soty.Now()
	if exp := dr.GetExpire(); exp != 0 && exp < now {
		dl.Debug("expired")
		return false, nil
	}

	rec.Version = &dr.Version
	rec.Shard = &dr.Shard
	rec.Value = dr.Value

	ringVer := db.ring.CurrVer()
	rec.ConfTime = &ringVer

	if dr.Expire != 0 {
		rec.Expire = &dr.Expire
	}

	if db.cache != nil && !cached {
		db.cache.Add(key, dr)
	}

	return true, nil
}

func (db *DB) Put(rec *acproto.ACPY2MapDatum) (int, *soty.Loc) {

	now := soty.Now()
	rloc := db.ring.GetLoc(rec.GetShard())

	if rec.GetExpire() != 0 && rec.GetExpire() < now {
		dl.Debug("already expired")
		return putstatus.OLD, rloc
	}

	if !rloc.IsLocal {
		dl.Debug("not local")
		// return putstatus.NOTME, rloc
		// for safety, save it anyway. remove it after it has been redistributed
	}

	lockno := rec.GetShard() % NDLOCK
	db.datalock[lockno].Lock()
	defer db.datalock[lockno].Unlock()

	old, found, err := db.be.Get(DATASUB, rec.GetKey())
	if found {
		pr := db.bytes2record(old)

		if pr.GetVersion() == rec.GetVersion() {
			dl.Debug("have this version")
			return putstatus.HAVE, rloc
		}
		if pr.GetVersion() >= rec.GetVersion() {
			dl.Debug("have newer version")
			return putstatus.NEWER, rloc
		}
		if rec.IfVersion != nil {
			// simulated atomic transactions
			if rec.GetIfVersion() != pr.GetVersion() {
				dl.Debug("if_version mismatch")
				return putstatus.CONDFAIL, rloc
			}
		}

		// RSN - run supplied program

		// remove old merkle
		db.merk.Del(rec.GetKey(), pr.GetVersion(), rloc)
	}

	buf := db.record2bytes(&Record{
		Version: rec.GetVersion(),
		Expire:  rec.GetExpire(),
		Shard:   rec.GetShard(),
		Value:   rec.Value,
	})

	dl.Debug("put '%s' %d", rec.GetKey(), len(buf))
	err = db.be.Put(DATASUB, rec.GetKey(), buf)

	if err != nil {
		return putstatus.FAIL, rloc
	}

	if db.cache != nil {
		db.cache.Remove(rec.GetKey())
	}
	db.merk.Add(rec.GetKey(), rec.GetVersion(), rloc, rec.GetShard(), false)
	if rec.GetExpire() != 0 {
		db.expire.Add(rec.GetKey(), rec.GetShard(), rec.GetExpire())
	}

	return putstatus.DONE, rloc
}

func (db *DB) filterAppendRes(req *acproto.ACPY2GetRange, res *acproto.ACPY2GetSet, key string, val []byte) {

	dr := db.bytes2record(val)

	if v := req.GetVersion0(); v > dr.GetVersion() {
		return
	}

	if v := req.GetVersion0(); v != 0 && v < dr.GetVersion() {
		return
	}

	dat := &acproto.ACPY2MapDatum{
		Map:     req.Map,
		Key:     &key,
		Value:   dr.Value,
		Version: &dr.Version,
		Shard:   &dr.Shard,
		Expire:  &dr.Expire,
	}

	res.Data = append(res.Data, dat)
}

func (db *DB) GetRange(req *acproto.ACPY2GetRange, res *acproto.ACPY2GetSet) (bool, error) {

	if req.GetKey0() != "" || req.GetKey1() != "" {
		// range over keys, filter by vers
		db.be.Range(DATASUB, req.GetKey0(), req.GetKey1(), func(key string, val []byte) bool {
			db.filterAppendRes(req, res, key, val)
			return true
		})
		return true, nil
	}

	if req.GetVersion0() != 0 || req.GetVersion1() != 0 {
		// range over ver, fetch values
		db.IterVersion(req.GetVersion0(), req.GetVersion1(), func(key string, ver uint64, shard uint32) bool {
			// fetch, filter, append
			val, found, err := db.be.Get(DATASUB, key)
			if found {
				db.filterAppendRes(req, res, key, val)
			}
			if err != nil {
				return false
			}
			return true
		})
		return true, nil
	}

	return false, nil
}

// actually remove, not tombstone
// used primarily for key expiration
func (db *DB) Del(rec *acproto.ACPY2MapDatum) error {

	val, found, err := db.be.Get(DATASUB, rec.GetKey())

	if err != nil {
		return err
	}

	if !found || len(val) == 0 {
		dl.Debug("not found")
		return nil
	}

	dr := db.bytes2record(val)

	if rec.GetVersion() != 0 && rec.GetVersion() != dr.GetVersion() {
		return nil
	}

	rloc := db.ring.GetLoc(rec.GetShard())
	lockno := rec.GetShard() % NDLOCK

	db.datalock[lockno].Lock()
	defer db.datalock[lockno].Unlock()

	dl.Debug("del %s [%x %x]", rec.GetKey(), dr.Shard, dr.Version)
	db.be.Del(DATASUB, rec.GetKey())

	if db.cache != nil {
		db.cache.Remove(rec.GetKey())
	}

	db.merk.Del(rec.GetKey(), dr.GetVersion(), rloc)

	return nil
}

// verify the expiration + delete
func (db *DB) DelExpired(key string, shard uint32, exp uint64) {

	lockno := shard % NDLOCK
	db.datalock[lockno].Lock()
	defer db.datalock[lockno].Unlock()

	val, found, err := db.be.Get(DATASUB, key)

	if err != nil {
		return
	}

	if !found || len(val) == 0 {
		return
	}

	dr := db.bytes2record(val)

	if dr.GetExpire() > exp {
		return
	}

	dl.Debug("del %s", key)
	db.be.Del(DATASUB, key)

	if db.cache != nil {
		db.cache.Remove(key)
	}

	rloc := db.ring.GetLoc(shard)
	db.merk.Del(key, dr.GetVersion(), rloc)
}

// ################################################################

func (db *DB) GetMerkle(level int, treeid uint16, ver uint64) []*merkle.Result {
	return db.merk.GetResponse(level, treeid, ver)
}
func (db *DB) GetRingConf(dc string) *acproto.ACPY2RingConfReply {
	return db.ring.GetConf(dc)
}

func (db *DB) PeerUpdate(id string, isup bool, iscf bool, first bool, pd *kibitz.Export, dat *acproto.ACPHeartBeat) {
	db.ring.PeerUpdate(id, isup, iscf, first, pd, dat)
}

func (db *DB) Distrib(loc *soty.Loc, req *acproto.ACPY2DistRequest, onDone func(int)) {
	db.ring.Distrib(loc, req, onDone)
}

func (db *DB) DistribLocal(loc *soty.Loc, req *acproto.ACPY2DistRequest, onDone func(int)) {
	db.ring.DistribLocal(loc, req, onDone)
}

func (db *DB) Repartition(loc *soty.Loc, ver uint64) (bool, uint64) {
	return db.merk.Repartition(db.ring, loc, ver)
}

func (db *DB) Redirect(rec *acproto.ACPY2MapDatum) {

	shard := rec.GetShard()
	loc := db.ring.GetLoc(shard)

	if loc.IsLocal {
		return
	}

	dist := db.ring.AltPeers(loc)

	for _, s := range dist {
		rec.Location = append(rec.Location, s)
	}
}

// ################################################################

func (db *DB) IterVersion(startVer uint64, endVer uint64, lambda func(string, uint64, uint32) bool) {

	npart := db.ring.NumParts()

	dl.Debug("iter %x - %x", startVer, endVer)

	for p := 0; p < npart; p++ {
		rloc := db.ring.GetLocN(p)
		if !rloc.IsLocal {
			continue
		}

		db.merk.IterVersion(rloc.TreeID, startVer, endVer, lambda)
	}
}

// ################################################################

func (db *DB) AE() bool {

	ok := true
	var mismatch int64
	var synced int64

	npart := db.ring.NumParts()
	dl.Debug("ae %s %d", db.name, npart)

	for p := 0; p < npart; p++ {
		rloc := db.ring.GetLocN(p)
		if !rloc.IsLocal {
			continue
		}
		peer := db.ring.RandomAEPeer(rloc)
		dl.Debug("peer %s", peer)
		if peer == "" {
			continue
		}

		mok, mism, sync := db.merk.AE(rloc, peer, db.ring)
		mismatch += mism
		synced += sync

		if !mok {
			ok = false
		}
	}

	if mismatch+synced > 0 {
		dl.Verbose("ae %s mismatch %d synced %d", db.name, mismatch, synced)
	}

	return ok
}

// ################################################################

func cryptoData(shard uint32, ver uint64) ([]byte, []byte) {
	tweak := &bytes.Buffer{}
	addtl := &bytes.Buffer{}

	binary.Write(addtl, binary.LittleEndian, ver)
	binary.Write(addtl, binary.LittleEndian, shard)
	binary.Write(tweak, binary.LittleEndian, shard^uint32(ver))

	return tweak.Bytes(), addtl.Bytes()
}

func (db *DB) bytes2record(b []byte) *Record {

	d := &Record{}
	d.Unmarshal(b)

	// decrypt, uncompress
	if d.GetFlags()&FLAG_ENCRYPTED_V1 != 0 {
		if db.enigma == nil {
			dl.Problem("cannot decrypt: bad config")
			return d
		}

		// decrypt
		var err interface{}
		tweak, addt := cryptoData(d.GetShard(), d.GetVersion())
		d.Value, err = db.enigma.Decrypt(d.Value, tweak, addt)

		if err != nil {
			dl.Bug("crypto fail: %v", err)
		}
		// decompress
		d.Value, err = snappy.Decode(nil, d.Value)
		if db.enigma == nil {
			dl.Problem("cannot decompress: %v", err)
			return d
		}
	}

	return d
}

func (db *DB) record2bytes(d *Record) []byte {

	if db.enigma != nil {
		// compress + encrypt
		var err interface{}
		tweak, addt := cryptoData(d.GetShard(), d.GetVersion())
		d.Value = snappy.Encode(nil, d.Value)
		d.Value, err = db.enigma.Encrypt(d.Value, tweak, addt)
		if err != nil {
			dl.Bug("crypto fail: %v", err)
		}
		d.Flags = FLAG_ENCRYPTED_V1
	}

	buf, err := d.Marshal()
	if err != nil {
		dl.Problem("corrupt data! %v", err)
	}

	return buf
}
