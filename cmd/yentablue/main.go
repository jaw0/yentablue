// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jun-16 14:08 (EDT)
// Function:

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"google.golang.org/grpc/grpclog"
	gpeer "google.golang.org/grpc/peer"

	"github.com/jaw0/acgo/daemon"
	"github.com/jaw0/acgo/diag"
	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/autoshard"
	"github.com/jaw0/yentablue/config"
	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/monitor"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/raftish"
	"github.com/jaw0/yentablue/store"
)

const (
	SUBSYS = "yenta"
)

type peerChange struct {
	id       string
	isup     bool
	ischange bool
}

var dl = diag.Logger("main")
var pdb *kibitz.DB
var sdb *store.SDB
var rft *raftish.R
var peerChan = make(chan peerChange, 5)
var shutdown = make(chan bool)
var sigchan = make(chan os.Signal, 5)

var configfile = ""
var proffile = ""
var tlsConfig *tls.Config

type myServer struct{}
type pinfo struct{}

func main() {
	var foreground bool

	flag.StringVar(&configfile, "c", "", "config file")
	flag.StringVar(&proffile, "profile", "", "write cpu profile `file`")
	flag.BoolVar(&foreground, "f", false, "run in foreground")
	flag.Parse()

	if proffile != "" {
		f, err := os.Create(proffile)
		if err != nil {
			dl.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			dl.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if configfile == "" {
		dl.Fatal("no config specified!\ntry -c config\n")
	}

	if !foreground {
		daemon.Ize()
	}

	// grpclog.SetLogger(&peerdb.Stfu{})
	runtime.GOMAXPROCS(128)
	diag.Init("yentablue")
	config.Init(configfile)
	dl.Verbose("starting...")

	tlsConfig = configureTLS()

	cf := config.Cf()

	pdb = kibitz.New(&kibitz.Conf{
		System:      SUBSYS,
		Datacenter:  cf.Datacenter,
		Rack:        cf.Rack,
		Port:        cf.Port_Server,
		Seed:        cf.Seedpeer,
		Environment: cf.Environment,
		// Id...
		Promiscuous: true,
		Iface:       &pinfo{},
	})

	sdb = initDBs(cf, pdb.Id(), pdb.Datacenter(), pdb.Rack())

	rft = raftish.New(raftish.Conf{
		Id: pdb.Id(),
		DB: dbnames(),
	})

	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR2)

	go peerChanger()
	pdb.Start()
	srv := startServer()
	go periodicSaveServerInfo(cf.Save_status)
	go monitor.Run(pdb)
	as := autoshard.Start(rft, sdb, pdb)

	block()

	dl.Verbose("beginning graceful shutdown")

	as.Stop()
	srv.Shutdown()
	pdb.Stop()
}

//################################################################

func dbnames() []string {

	cf := config.Cf()

	var dbNames []string
	for _, db := range cf.Database {
		dbNames = append(dbNames, db.Name)
	}
	return dbNames
}

func block() {

	for {
		select {
		case <-shutdown:
			return
		case n := <-sigchan:
			if n == syscall.SIGUSR2 {
				trace()
				continue
			}
			return
		}
	}
}

func statsInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {

	p, _ := gpeer.FromContext(ctx)
	dl.Debug("connection from %v", p.Addr)

	t0 := time.Now()
	res, err := handler(ctx, req)

	dt := time.Now().Sub(t0)
	reqStatsCounter(dt)
	return res, err
}

// ################################################################
// interface with peerdb

func (*pinfo) Myself(info *kibitz.PeerInfo) kibitz.PeerImport {

	cf := config.Cf()
	var dbl []*acproto.ACPDatabaseInfo

	for _, c := range cf.Database {
		dbSpace := spaceTotal(c.FullPathName)
		dbAvail := spaceAvail(c.FullPathName)

		dbl = append(dbl, &acproto.ACPDatabaseInfo{
			Name:       c.Name,
			Raft:       rft.RaftInfo(c.Name),
			SpaceTotal: dbSpace,
			SpaceAvail: dbAvail,
		})
		// RSN - more info per database
	}

	load := currentLoad()
	avail := spaceAvail(cf.Basedir)

	res := &acproto.ACPHeartBeat{
		PeerInfo:       info,
		ProcessId:      int32(os.Getpid()),
		SortMetric:     load,
		CapacityMetric: avail,
		CpuMetric:      int32(runtime.NumCPU())*1000 - load,
		Uptodate:       sdb.Uptodate,
		Database:       dbl,
	}

	return res
}

func (*pinfo) Update(id string, isup bool, isMySys bool) {

	if !isMySys {
		return
	}

	pc := peerChange{
		id:       id,
		isup:     isup,
		ischange: false,
	}

	// non-blocking. drop if backlogged
	select {
	case peerChan <- pc:
	default:
	}
}

func (*pinfo) Change(id string, isup bool, isMySys bool) {

	if !isMySys {
		return
	}

	pc := peerChange{
		id:       id,
		isup:     isup,
		ischange: true,
	}

	// non-blocking. drop if backlogged
	select {
	case peerChan <- pc:
	default:
	}

}

func peerChanger() {

	for c := range peerChan {

		pd := pdb.Get(c.id)

		if pd == nil {
			return
		}

		switch wd := pd.GetData().(type) {
		case *acproto.ACPHeartBeat:
			if c.ischange {
				sdb.PeerUpdate(c.id, c.isup, pd.GetExport(), wd)
			} else {
				rft.PeerUpdate(c.id, c.isup, wd)
			}
		}
	}
}

func (x *pinfo) Send(addr string, timeout time.Duration, myself kibitz.PeerImport) ([]kibitz.PeerImport, error) {

	t0 := time.Now()

	ac, closer, err := gclient.GrpcClient(&gclient.GrpcConfig{
		Addr:    []string{addr},
		Timeout: timeout,
		TLS:     tlsConfig,
	}, nil)

	if err != nil {
		dl.Debug(" => down err %v", err)
		return nil, err
	}
	defer closer()

	if ac == nil {
		dl.Debug("unable to connect")
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, err := ac.SendHB(ctx, &acproto.ACPHeartBeatRequest{
		Myself: myself.(*acproto.ACPHeartBeat),
	})

	td := time.Now().Sub(t0)

	if err != nil {
		dl.Debug(" => down err %v", err)
		return nil, err
	}

	if res.GetStatusCode() != 200 {
		dl.Debug(" => down code %d; %s; %#v, %s", res.GetStatusCode(), addr, ctx.Err(), td)
		return nil, fmt.Errorf("status %d", res.GetStatusCode())
	}

	dl.Debug("recv %v", res)

	// process results
	hbinfos := res.GetHbinfo()
	respi := make([]kibitz.PeerImport, len(hbinfos))
	for i, hb := range hbinfos {
		respi[i] = kibitz.PeerImport(hb)
	}

	return respi, nil

}

// ################################################################

func trace() {

	var stack = make([]byte, 65535)
	stack = stack[:runtime.Stack(stack, true)]
	fmt.Printf("\n%s\n\n", stack)
}
