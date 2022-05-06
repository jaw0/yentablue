// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Dec-22 11:46 (EST)
// Function: native monitoring protocol

package monitor

import (
	"time"

	"golang.org/x/net/context"

	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/proto"
)

const (
	TIMEOUT = 5 * time.Second
)

func (mon *monDat) monitor_yb() {

	dl.Debug("checking %s", mon.id)

	client, err := gclient.New(&gclient.GrpcConfig{
		Addr:    []string{mon.endpoint},
		Timeout: TIMEOUT,
	})
	if err != nil {
		dl.Problem("client error: %v", err)
		mon.result(false, nil)
		return
	}

	ac := client.RPC()
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	res, err := ac.SendHB(ctx, &acproto.ACPHeartBeatRequest{})

	if err != nil {
		dl.Debug("cannot connect to %s: %v", mon.cf.Id, err)
		mon.result(false, nil)
		return
	}

	for _, hb := range res.GetHbinfo() {
		dl.Debug("res: %v", hb)
		mon.result(true, hb)
	}

}
