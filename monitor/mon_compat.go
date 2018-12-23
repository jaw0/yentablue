// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Dec-22 14:29 (EST)
// Function: AC::Yenta compat monitoring

package monitor

import (
	"github.com/jaw0/kibitz"

	"github.com/jaw0/acgo/acrpc"
	"github.com/jaw0/yentablue/proto"
)

const (
	PHMT_HEARTBEATREQ = 2
)

func (mon *monDat) monitor_ac() {

	dl.Debug("checking %s", mon.id)

	req := &ACP1HeartBeatRequest{}
	res := &ACP1HeartBeat{}

	client := &acrpc.APC{
		Addr:    mon.endpoint,
		Timeout: TIMEOUT,
	}

	_, err := client.Call(PHMT_HEARTBEATREQ, req, res, nil)

	if err != nil {
		dl.Debug("call failed to %s: %v", mon.cf.Id, err)
		mon.result(false, nil)
		return
	}

	dl.Debug("recvd %+v", res)

	// convert
	isup := true

	if res.GetStatusCode() != 200 {
		isup = false
	}

	info := &acproto.ACPHeartBeat{
		PeerInfo: &kibitz.PeerInfo{
			Subsystem:   res.Subsystem,
			Environment: res.Environment,
			Hostname:    res.Hostname,
			ServerId:    res.ServerId,
		},
		SortMetric:     res.SortMetric,
		CapacityMetric: res.CapacityMetric,
		ProcessId:      res.ProcessId,
	}

	mon.fillHeartbeat(isup, info)
	mon.result(isup, info)
}
