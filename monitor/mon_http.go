// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Dec-23 10:49 (EST)
// Function: http monitoring

package monitor

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/proto"
)

// maybe the server can return something like:
type jsonDat struct {
	Subsystem      string
	Environment    string
	Hostname       string
	Datacenter     string
	Rack           string
	ServerId       string
	SortMetric     int32
	CpuMetric      int32
	CapacityMetric int32
}

func (mon *monDat) monitor_http() {
	dl.Debug("checking %s", mon.id)

	if mon.cf.URL == "" {
		dl.Problem("url not specified")
		return
	}

	c := &http.Client{
		Timeout: TIMEOUT,
	}
	res, err := c.Get(mon.cf.URL)

	info := &acproto.ACPHeartBeat{
		PeerInfo: &kibitz.PeerInfo{},
	}

	if err != nil {
		dl.Debug("GET failed: %v", err)
		mon.fillHeartbeat(false, info)
		mon.result(false, info)
		return
	}

	content, err := ioutil.ReadAll(res.Body)
	res.Body.Close()

	if res.StatusCode != 200 {
		dl.Debug("GET failed: %d", res.StatusCode)
		mon.fillHeartbeat(false, info)
		mon.result(false, info)
		return
	}

	dl.Debug("res: %v", res)

	if res.Header.Get("Content-Type") != "application/json" {
		// great.
		mon.fillHeartbeat(true, info)
		mon.result(true, info)
		return
	}

	// try to smoosh returned json into usable info
	jsinfo := &jsonDat{}
	err = json.Unmarshal(content, jsinfo)
	if err == nil {
		pi := info.PeerInfo

		if jsinfo.Subsystem != "" {
			pi.Subsystem = jsinfo.Subsystem
		}
		if jsinfo.Environment != "" {
			pi.Environment = jsinfo.Environment
		}
		if jsinfo.Hostname != "" {
			pi.Hostname = jsinfo.Hostname
		}
		if jsinfo.Datacenter != "" {
			pi.Datacenter = jsinfo.Datacenter
		}
		if jsinfo.Rack != "" {
			pi.Rack = jsinfo.Rack
		}
		if jsinfo.ServerId != "" {
			pi.ServerId = jsinfo.ServerId
		}
		if jsinfo.SortMetric != 0 {
			info.SortMetric = jsinfo.SortMetric
		}
		if jsinfo.CpuMetric != 0 {
			info.CpuMetric = jsinfo.CpuMetric
		}
		if jsinfo.CapacityMetric != 0 {
			info.CapacityMetric = jsinfo.CapacityMetric
		}
	}

	mon.fillHeartbeat(true, info)
	mon.result(true, info)

}
