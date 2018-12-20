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

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"google.golang.org/grpc/grpclog"
	gpeer "google.golang.org/grpc/peer"

	"github.com/jaw0/acgo/daemon"
	"github.com/jaw0/acgo/diag"
	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/config"
	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/store"
)

const (
	SUBSYS = "fgdb"
)

type peerChange struct {
	id   string
	isup bool
}

var dl = diag.Logger("main")
var pdb *kibitz.DB
var sdb *store.SDB
var peerChan = make(chan peerChange, 2)
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
	config.Init(configfile)
	diag.Init("prog")
	dl.Verbose("starting...")

	tlsConfig = configureTLS()

	cf := config.Cf()

	pdb = kibitz.New(&kibitz.Conf{
		System:     SUBSYS,
		Datacenter: cf.Datacenter,
		Rack:       cf.Rack,
		Port:       cf.Port_Server,
		Seed:       cf.Seedpeer,
		Monitor:    cf.Monitor,
		// Id...
		Promiscuous: true,
		Iface:       &pinfo{},
	})

	sdb = initDBs(cf, pdb.Id(), pdb.Datacenter(), pdb.Rack())

	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR2)

	go peerChanger()
	pdb.Start()
	srv := startServer()
	go periodicSaveServerInfo(cf.Save_status)

	block()

	dl.Verbose("beginning graceful shutdown")

	srv.Shutdown()
	pdb.Stop()
}

//################################################################

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

func Myself(info *kibitz.PeerInfo) *acproto.ACPHeartBeat {

	cf := config.Cf()
	var dbl []string

	for _, c := range cf.Database {
		dbl = append(dbl, c.Name)
	}

	load := currentLoad()
	avail := spaceAvail(cf.Basedir)

	return &acproto.ACPHeartBeat{
		PeerInfo:       info,
		ProcessId:      proto.Int32(int32(os.Getpid())),
		SortMetric:     proto.Int32(load),
		CapacityMetric: proto.Int32(avail),
		CpuMetric:      proto.Int32(int32(runtime.NumCPU())*1000 - load),
		Uptodate:       proto.Bool(sdb.Uptodate),
		Database:       dbl,
	}
}

func (*pinfo) Notify(id string, isup bool, isMySys bool) {

	if !isMySys {
		return
	}

	peerChan <- peerChange{
		id:   id,
		isup: isup,
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
			sdb.PeerUpdate(c.id, c.isup, pd.GetExport(), wd)
		}
	}
}

func (x *pinfo) Send(addr string, timeout time.Duration, info *kibitz.PeerInfo) (bool, error) {

	ac, closer, err := gclient.GrpcClient(&gclient.GrpcConfig{
		Addr:    []string{addr},
		Timeout: timeout,
		TLS:     tlsConfig,
	}, nil)

	if err != nil {
		dl.Debug(" => down err %v", err)
		return false, err
	}

	if ac == nil {
		dl.Verbose("wtf")
	}

	defer closer()

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	res, err := ac.SendHB(ctx, &acproto.ACPHeartBeatRequest{
		Myself: Myself(info),
	})

	if err != nil {
		dl.Debug(" => down err %v", err)
		return false, err
	}

	if res.GetStatusCode() != 200 {
		dl.Debug(" => down code %d", res.GetStatusCode)
		return false, fmt.Errorf("status %d", res.GetStatusCode())
	}

	dl.Debug("recv %v", res)

	// process results
	for _, hb := range res.GetHbinfo() {
		pdb.Update(hb)
	}

	return true, nil
}

// ################################################################

func trace() {

	var stack = make([]byte, 65535)
	stack = stack[:runtime.Stack(stack, true)]
	fmt.Printf("\n%s\n\n", stack)
}
