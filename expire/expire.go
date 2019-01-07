// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-14 10:59 (EDT)
// Function:

package expire

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/jaw0/acgo/diag"

	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/soty"
)

type zilch struct{}

type D struct {
	name   string
	expire uint64
	db     ExStorer
	lock   sync.Mutex
	queue  map[uint64]map[string]uint32 // [expire & mask]->[key]->shard
	stop   chan struct{}
	done   sync.WaitGroup
}

type ExStorer interface {
	Del(rec *acproto.ACPY2MapDatum) error
	GetInternal(string, string) ([]byte, bool)
	SetInternal(string, string, []byte)
	DelInternal(string, string)
	DelExpired(string, uint32, uint64)
	Range(string, string, string, func(string, []byte) bool)
	IterVersion(uint64, uint64, func(string, uint64, uint32) bool)
}

const TBUCK = 0x3fffffffff // ~5 mins
const EXPSUB = "e"

var dl = diag.Logger("expire")

func New(name string, db ExStorer, exp uint64) *D {

	d := &D{
		name:   name,
		expire: exp,
		db:     db,
		stop:   make(chan struct{}),
		queue:  make(map[uint64]map[string]uint32),
	}

	d.done.Add(1)
	go d.periodic()

	return d
}

func (d *D) Close() {
	close(d.stop)
	d.done.Wait()
	d.flush()
}

func (d *D) Add(key string, shard uint32, exp uint64) {
	d.lock.Lock()
	defer d.lock.Unlock()

	dl.Debug("+ %x %s", exp&^TBUCK, key)
	q, ok := d.queue[exp&^TBUCK]
	if ok {
		q[key] = shard
		return
	}

	q = make(map[string]uint32)
	q[key] = shard
	d.queue[exp&^TBUCK] = q
}

func (d *D) Expire() uint64 {
	return d.expire
}

// ################################################################

func (d *D) tasQueue() map[uint64]map[string]uint32 {

	// keep the lock quick - swap in a new empty queue
	d.lock.Lock()
	defer d.lock.Unlock()

	queue := d.queue

	if len(queue) != 0 {
		d.queue = make(map[uint64]map[string]uint32)
		return queue
	}
	return nil
}

func (d *D) flush() {

	queue := d.tasQueue()
	if queue == nil {
		return
	}

	for exp, m := range queue {
		d.flushBucket(exp, m)
	}
}

func (d *D) flushBucket(exp uint64, m map[string]uint32) {

	k := fmt.Sprintf("%016x", exp)

	// fetch current record
	buf, ok := d.db.GetInternal(EXPSUB, k)
	if ok {
		// unmarshal + merge
		w := bytes2wire(buf)

		for _, p := range w.Pairs {
			m[string(p.Key)] = p.Shard
		}
	}

	// marshal + save
	w := &Wire{
		Pairs: make([]*KeyShardPair, 0, len(m)),
	}
	for key, shard := range m {
		w.Pairs = append(w.Pairs, &KeyShardPair{Key: key, Shard: shard})
	}
	buf = wire2bytes(w)
	d.db.SetInternal(EXPSUB, k, buf)
}

// expire records off of the trailing edge of the merkle tree
func (d *D) expireEdge() {

	if d.expire == 0 {
		return
	}

	exp := soty.Now() - d.expire

	dl.Debug("expire edge %s %x (%x %x)", d.name, exp, soty.Now(), d.expire)

	d.db.IterVersion(0, exp, func(key string, ver uint64, shard uint32) bool {

		dl.Debug("expire %s", key)
		d.db.Del(&acproto.ACPY2MapDatum{
			Key:     key,
			Shard:   shard,
			Version: ver,
		})
		return true
	})
}

// expire records with explicitly set expire times
func (d *D) expireSpec() {

	now := soty.Now()
	end := fmt.Sprintf("%016x", now)
	d.db.Range(EXPSUB, "", end, func(kexp string, buf []byte) bool {
		// unmarshal + delete the expired records
		w := bytes2wire(buf)
		for _, p := range w.Pairs {
			dl.Debug("expire %s", p.Key)
			d.db.DelExpired(string(p.Key), p.Shard, now)
		}

		// and delete the expiration data
		d.db.DelInternal(EXPSUB, kexp)
		return true
	})
}

func (d *D) periodic() {

	for {
		select {
		case <-d.stop:
			return
		case <-time.After(60 * time.Second):
			break
		}

		randomDelay()
		d.flush()
		d.expireEdge()
		d.expireSpec()
	}
}

func randomDelay() {
	// to prevent falling into lockstep
	time.Sleep(time.Duration(rand.Intn(60)) * time.Second)
}

func bytes2wire(b []byte) *Wire {
	d := &Wire{}
	d.Unmarshal(b)
	return d
}

func wire2bytes(d *Wire) []byte {
	buf, err := d.Marshal()
	if err != nil {
		dl.Problem("corrupt data! %v", err)
	}
	return buf
}
