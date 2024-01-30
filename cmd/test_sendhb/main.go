// Copyright (c) 2022
// Author: Jeff Weisberg <tcp4me.com!jaw>
// Created: 2022-May-06 11:42 (EDT)
// Function: send hb

package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/jaw0/acdiag"
	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/proto"
)

var dl = diag.Logger("main")

func main() {
	var concurrency int
	var server string
	flag.IntVar(&concurrency, "c", 1, "concurrency")
	flag.StringVar(&server, "h", "127.0.0.1:5301", "server")

	flag.Parse()
	diag.Init("test")

	concurSema := make(chan struct{}, concurrency)

	for {
		concurSema <- struct{}{}
		go func() {
			err := Send(server)
			if err != nil {
				dl.Verbose("error: %v", err)
			}
			<-concurSema
		}()

	}
}

func Send(addr string) error {

	t0 := time.Now()

	timeout := 15 * time.Second

	ac, closer, err := gclient.GrpcClient(&gclient.GrpcConfig{
		Addr:    []string{addr},
		Timeout: timeout,
	}, nil)

	if err != nil {
		dl.Verbose(" => down err %v", err)
		return err
	}
	defer closer()

	if ac == nil {
		dl.Verbose("unable to connect")
		return err
	}

	now := uint64(t0.UnixNano())
	request := &acproto.ACPHeartBeatRequest{
		Myself: &acproto.ACPHeartBeat{
			CpuMetric:      100,
			SortMetric:     100,
			CapacityMetric: 100,
			PeerInfo: &kibitz.PeerInfo{
				StatusCode:  2,
				Subsystem:   "testy",
				Environment: "dev",
				ServerId:    "test/sendhb/127.0.0.1",
				Hostname:    "localhost",
				TimeCreated: now,
				TimeConf:    now,
				TimeUpSince: now,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, err := ac.SendHB(ctx, request)

	td := time.Now().Sub(t0)

	if err != nil {
		dl.Problem(" => down err %v", err)
		return err
	}

	if res.GetStatusCode() != 200 {
		dl.Verbose(" => down code %d; %s; %#v, %s", res.GetStatusCode(), addr, ctx.Err(), td)
		return fmt.Errorf("status %d", res.GetStatusCode())
	}

	dl.Debug("recv %v", res)

	dl.Verbose("=> %d %s, %s", res.StatusCode, res.StatusMessage, td)

	return nil

}
