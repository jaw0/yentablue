// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-03 11:43 (EST)
// Function: save server info

package main

import (
	"time"

	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/info"
	"github.com/jaw0/yentablue/proto"
)

func addServerInfo(res []*info.Server, w *acproto.ACPHeartBeat, isUp bool) []*info.Server {

	ii := w.PeerInfo

	r := &info.Server{
		Subsystem:      ii.GetSubsystem(),
		Environment:    ii.GetEnvironment(),
		Id:             ii.GetServerId(),
		Datacenter:     ii.GetDatacenter(),
		Rack:           ii.GetRack(),
		IsLocal:        (ii.GetDatacenter() == pdb.Datacenter()),
		Hostname:       ii.GetHostname(),
		TimeLastUp:     ii.GetTimeLastUp(),
		TimeUpSince:    ii.GetTimeUpSince(),
		IsUp:           isUp,
		SortMetric:     w.GetSortMetric(),
		CpuMetric:      w.GetCpuMetric(),
		CapacityMetric: w.GetCapacityMetric(),
		Database:       w.Database,
		Uptodate:       w.GetUptodate(),
	}

	for _, n := range ii.NetInfo {
		r.NetInfo = append(r.NetInfo, info.Net{Addr: n.GetAddr(), Dom: n.GetNatdom()})
	}

	return append(res, r)
}

func SaveServers(file string) error {

	pwd := pdb.GetAll()
	res := []*info.Server{}

	for _, p := range pwd {

		pd := p.GetData()
		w := pd.(*acproto.ACPHeartBeat)

		isUp := w.PeerInfo.GetStatusCode() == int32(kibitz.STATUS_UP)
		res = addServerInfo(res, w, isUp)
	}

	res = addServerInfo(res, Myself(pdb.MyInfo()), true)

	return info.SaveJson(file, res)
}

func periodicSaveServerInfo(file string) {

	if file == "" {
		return
	}

	for {
		time.Sleep(60 * time.Second)
		SaveServers(file)
	}
}
