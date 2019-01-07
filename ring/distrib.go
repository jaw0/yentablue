// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-02 13:36 (EDT)
// Function: distribute data to others

package ring

import (
	"expvar"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/jaw0/acgo/diag"
	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/putstatus"
	"github.com/jaw0/yentablue/soty"
)

const (
	MAXWORKQLEN = 1000
	MAXWORKERPP = 5
	MAXWORKERS  = 100
	KEEPALIVE   = 30 * time.Second
)

type distreq struct {
	req  *acproto.ACPY2DistRequest
	next distNexter
}

type distworkinfo struct {
	workq    chan distreq
	nworkers int
}

type distwork struct {
	lock     sync.Mutex
	nworkers int
	pmap     map[string]*distworkinfo
}

type distNexter interface {
	next(int)
	fail()
}

type distNextAndMore struct {
	dw      *distwork
	req     *acproto.ACPY2DistRequest
	dist    []string
	maxseen int
	n       int
	onDone  func(int)
}
type distNextAndStop struct {
	dw     *distwork
	req    *acproto.ACPY2DistRequest
	dist   []string
	n      int
	onDone func(int)
}

type distNextEnough struct {
	dw       *distwork
	req      *acproto.ACPY2DistRequest
	dist     []string
	replicas int
	copies   int
	n        int
	onDone   func(int)
}

var dld = diag.Logger("distrib")
var gdw = newDistWork()
var distGrpcp *gclient.Defaults

var statDist = expvar.NewInt("distrib_sent")
var statErrs = expvar.NewInt("distrib_errs")
var statSeen = expvar.NewInt("distrib_seen")
var statWorkers = expvar.NewInt("distrib_workers")

//################################################################

// RSN - if sharded && (hops == 0 || fromfar && hops == 1) => i am coordinator
//       => send to faraway, send to #replicas or spares

func (p *P) Distrib(loc *soty.Loc, req *acproto.ACPY2DistRequest, onDone func(int)) {

	npart := p.NumParts()
	if npart > 1 {
		p.distribSharded(loc, req, onDone)
		return
	}

	hops := req.GetHop()
	ndc := p.NumDC(loc)
	sender := req.GetSender()
	fromFaraway := p.peerIsFaraway(sender)
	p.updateReq(req)

	maxsee := 1
	if hops < 4 {
		maxsee = 2
	}

	// only the 1st hop sends to faraway
	switch hops {
	case 0:
	case 1:
		if !fromFaraway {
			ndc = 1
		}
	default:
		ndc = 1
	}

	dld.Debug("dist ffar %v, hops %d, ndc %d, ms %d", fromFaraway, hops, ndc, maxsee)

	for i := 0; i < ndc; i++ {
		local, rack := p.DistPeers(loc, i, true, sender)

		shuffle(local)
		shuffle(rack)

		dld.Debug(" dc %d dist %v, %v", i, local, rack)
		if i > 0 {
			// far datacenter - send to one per DC (stop after 1st success)
			x := &distNextAndStop{gdw, req, local, 0, noop}
			x.start()
		} else {
			x := &distNextAndMore{gdw, req, local, maxsee, 0, onDone}
			// within the rack, always be aggressive
			y := &distNextAndMore{gdw, req, rack, 2, 0, onDone}
			x.start()
			y.start()
		}
	}
	dld.Debug("/distrib")
}

func (p *P) distribSharded(loc *soty.Loc, req *acproto.ACPY2DistRequest, onDone func(int)) {

	hops := req.GetHop()
	ndc := p.NumDC(loc)
	sender := req.GetSender()
	fromFaraway := p.peerIsFaraway(sender)
	nrepl := p.NumReplicas()
	p.updateReq(req)

	switch hops {
	case 0:
		if loc.IsLocal {
			// because we count
			nrepl--
		}
	case 1:
		if !fromFaraway {
			return
		}
		// only the 1st hop sends to faraway
		ndc = 1
	default:
		return
	}

	dld.Debug("dist ffar %v, hops %d, ndc %d, nrep %d", fromFaraway, hops, ndc, nrepl)

	for i := 0; i < ndc; i++ {
		dist, _ := p.DistPeers(loc, i, false, sender)

		// the list includes spares, shuffle them seperately
		shuffle(dist[:nrepl])
		shuffle(dist[nrepl:])

		if i > 0 {
			// far datacenter - send to one per DC (stop after 1st success)
			x := &distNextAndStop{gdw, req, dist, 0, noop}
			x.start()
		} else {
			x := &distNextAndMore{gdw, req, dist, nrepl, 0, onDone}
			x.start()
		}
	}
}

func (p *P) DistribLocal(loc *soty.Loc, req *acproto.ACPY2DistRequest, onDone func(int)) {

	p.updateReq(req)
	nrepl := p.NumReplicas()
	npart := p.NumParts()
	dist, _ := p.DistPeers(loc, 0, false, "")

	if npart > 1 {
		// do not include the spares
		dist = dist[:nrepl]
	}

	dld.Debug("dist cb")

	dister := &distNextEnough{
		dw:       gdw,
		req:      req,
		replicas: nrepl,
		onDone:   onDone,
		dist:     dist,
	}

	dister.start()
}

func Handoff(req *acproto.ACPY2DistRequest, dist []string) {

	// get it to one server that can better handle it
	x := &distNextAndStop{gdw, req, dist, 0, noop}
	x.start()
}

//################################################################

func (dw *distwork) distrib(peer string, waitForIt bool, req *acproto.ACPY2DistRequest, next distNexter) {

	dld.Debug("dist to %s", peer)

	if peer == "" {
		next.fail()
		return
	}

	expire := req.GetExpire()
	if expire <= soty.Now() {
		dld.Debug("expired %d", expire)
		next.fail()
		return
	}

	statDist.Add(1)

	dw.lock.Lock()
	defer dw.lock.Unlock()
	wi := dw.findOrCreate(peer)

	if len(wi.workq) >= MAXWORKQLEN && !waitForIt {
		// queue is full - drop request
		dld.Debug("queue full")
		next.fail()
		return
	}

	dld.Debug("distributing req")

	wi.workq <- distreq{
		req:  req,
		next: next,
	}

	dw.startWorkers(peer, wi, waitForIt)
}

func newDistWork() *distwork {

	return &distwork{
		pmap: make(map[string]*distworkinfo),
	}
}

func (dw *distwork) distWorker(peer string, wi *distworkinfo) {

	defer func() {
		dld.Debug("worker done")
		dw.lock.Lock()
		wi.nworkers--
		dw.nworkers--
		statWorkers.Set(int64(dw.nworkers))
		dw.lock.Unlock()
	}()

	dld.Debug("starting worker on %s", peer)

	ac, closer, err := gclient.GrpcClient(&gclient.GrpcConfig{
		Addr: []string{peer},
	}, distGrpcp)

	if err != nil {
		statErrs.Add(1)
		return
	}
	defer closer()

	for {
		select {
		case w := <-wi.workq:
			res, err := ac.Put(context.Background(), w.req)
			if err != nil {
				statErrs.Add(1)
				dld.Debug("dist err %v", err)
				w.next.next(putstatus.FAIL)
			} else {
				switch res.GetResultCode() {
				case putstatus.HAVE, putstatus.NEWER:
					statSeen.Add(1)
				}
				dld.Debug("dist done res %d,%d", res.GetStatusCode(), res.GetResultCode())
				w.next.next(int(res.GetResultCode()))
			}

		case <-time.After(KEEPALIVE):
			return
		}

	}
}

func (dw *distwork) startWorkers(peer string, wi *distworkinfo, force bool) {

	if dw.nworkers >= MAXWORKERS && !force {
		// at maximum
		return
	}

	want := (len(wi.workq)*MAXWORKERPP + MAXWORKQLEN - 1) / MAXWORKQLEN
	dld.Debug("want %d have %d", want, wi.nworkers)

	if want <= wi.nworkers {
		return
	}

	wi.nworkers++
	dw.nworkers++
	statWorkers.Set(int64(dw.nworkers))

	go dw.distWorker(peer, wi)
}

func (dw *distwork) findOrCreate(peer string) *distworkinfo {

	wi, ok := dw.pmap[peer]

	if ok {
		return wi
	}

	wi = &distworkinfo{
		workq: make(chan distreq, MAXWORKQLEN),
	}

	dw.pmap[peer] = wi
	return wi
}

// ################################################################

func (d *distNextEnough) start() {
	d.next(putstatus.FAIL)
}

func (d *distNextEnough) fail() {
	d.onDone(putstatus.FAIL)
}

func (d *distNextEnough) next(status int) {
	// keep sending until enough replicas exist
	// or we run out of peers

	if d.n >= len(d.dist) {
		d.onDone(putstatus.FAIL)
		return
	}

	switch status {
	case putstatus.DONE, putstatus.HAVE, putstatus.NEWER:
		d.copies++
	}

	if d.copies >= d.replicas {
		d.onDone(putstatus.DONE)
		return
	}

	d.dw.distrib(d.dist[d.n], true, d.req, d)
	d.n++
}

// ################################################################

func (d *distNextAndMore) start() {
	d.next(putstatus.FAIL)
}

func (d *distNextAndMore) fail() {
	d.onDone(putstatus.FAIL)
}
func (d *distNextAndMore) done() {
	if d.maxseen == 0 {
		d.onDone(putstatus.DONE)
	} else {
		d.onDone(putstatus.FAIL)
	}
}
func (d *distNextAndMore) next(status int) {
	// keep sending it until it is well distributed (others already saw it)

	if d.n >= len(d.dist) {
		d.done()
		return
	}

	if status == putstatus.DONE || status == putstatus.FAIL {
		d.dw.distrib(d.dist[d.n], false, d.req, d)
		d.n++
		return
	}

	d.maxseen--

	if d.maxseen > 0 {
		// skip ahead
		d.n++
		if d.n >= len(d.dist) {
			d.done()
			return
		}
		d.dw.distrib(d.dist[d.n], false, d.req, d)
		d.n++
		return
	}
	d.done()
}

// ################################################################

func (d *distNextAndStop) start() {
	d.next(putstatus.FAIL)
}

func (d *distNextAndStop) fail() {
	d.onDone(putstatus.FAIL)
}

func (d *distNextAndStop) next(status int) {
	// send until 1st success, then stop

	if d.n >= len(d.dist) {
		d.onDone(putstatus.FAIL)
		return
	}

	switch status {
	case putstatus.DONE, putstatus.HAVE, putstatus.NEWER:
		d.onDone(putstatus.DONE)
		return
	}

	d.dw.distrib(d.dist[d.n], true, d.req, d)
	d.n++
}

//################################################################

func shuffle(l []string) {
	// Fisher-Yates
	for i := len(l) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		l[i], l[j] = l[j], l[i]
	}
}

func (p *P) updateReq(req *acproto.ACPY2DistRequest) {
	req.Sender = p.myid
	req.Hop = req.GetHop() + 1
}

//################################################################

func noop(int) {}
