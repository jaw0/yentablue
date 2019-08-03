// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Dec-22 10:18 (EST)
// Function: monitor other systems

package monitor

import (
	"expvar"
	"fmt"
	"net"
	"time"

	"github.com/jaw0/acgo/diag"
	"github.com/jaw0/kibitz"
	"github.com/jaw0/kibitz/myinfo"

	"github.com/jaw0/yentablue/config"
	"github.com/jaw0/yentablue/proto"
)

type monDat struct {
	id       string
	pdb      *kibitz.DB
	cf       *config.Monitor
	endpoint string
	timeup   uint64
	timedn   uint64
	netinfo  []*kibitz.NetInfo
}

const (
	LOCALHOST = "127.0.0.1"
	DEFSYS    = "netjawn"
)

var viaMon = "(mon)"
var dl = diag.Logger("monitor")
var monchks = expvar.NewInt("monitor_checks")

func Run(pdb *kibitz.DB) {
	cf := config.Cf()

	mons := []*monDat{}

	for _, mcf := range cf.Monitor {
		mon := &monDat{cf: mcf, pdb: pdb}
		mons = append(mons, mon)
		mon.init()
	}

	for {
		time.Sleep(10 * time.Second)

		for _, mon := range mons {
			mon.monitor()
		}
	}
}

// fill in defaults, validate
func (mon *monDat) init() {

	mcf := mon.cf

	if mcf.Port == 0 {
		dl.Fatal("monitor port not specified")
	}
	if mcf.Protocol == "" {
		mcf.Protocol = "yb"
	}
	if mcf.System == "" {
		mcf.System = DEFSYS
	}
	if mcf.Addr == "" {
		mcf.Addr = LOCALHOST
	}
	if mcf.Environment == "" {
		mcf.Environment = mon.pdb.Env()
	}

	info := myinfo.GetInfo(mcf.Hostname)

	if mcf.Hostname == "" {
		mcf.Hostname = info.Hostname
	}
	if mcf.Datacenter == "" {
		mcf.Datacenter = info.Datacenter
	}
	if mcf.Rack == "" {
		mcf.Rack = info.Rack
	}
	if mcf.Id == "" {
		mcf.Id = info.ServerId(mcf.System, mcf.Environment, mcf.Port)
	}

	mon.id = mcf.Id
	mon.endpoint = net.JoinHostPort(mcf.Addr, fmt.Sprintf("%d", mcf.Port))

	if mcf.Addr == LOCALHOST {
		ninfo := myinfo.Network(mcf.Datacenter, mcf.Port)

		for _, ni := range ninfo {
			dl.Debug("intf %s [%s]", ni.Addr, ni.Dom)

			a := ni.Addr
			d := ni.Dom

			mon.netinfo = append(mon.netinfo, &kibitz.NetInfo{
				Addr:   a,
				Natdom: d,
			})
		}
	} else {
		a := mon.endpoint
		d := ""
		mon.netinfo = []*kibitz.NetInfo{{Addr: a, Natdom: d}}
	}

	switch mcf.Protocol {
	case "yb":
	case "ac":
	case "http":
		if mcf.URL == "" {
			dl.Fatal("monitor url required for protocol http")
		}
	default:
		dl.Fatal("unknown monitor protocol '%s'", mcf.Protocol)
	}
}

func (mon *monDat) monitor() {

	// don't talk to self. any of my addrs.
	if mon.pdb.IsOwnAddr(mon.endpoint) {
		dl.Debug("monitor - skipping self - %s", mon.cf.Id)
		return
	}

	dl.Debug("checking %s %s", mon.id, mon.cf.Protocol)
	switch mon.cf.Protocol {
	case "yb":
		mon.monitor_yb()
	case "ac":
		mon.monitor_ac()
	case "http":
		mon.monitor_http()
	}
}

func (mon *monDat) fillHeartbeat(isup bool, info *acproto.ACPHeartBeat) {

	if info.PeerInfo == nil {
		info.PeerInfo = &kibitz.PeerInfo{}
	}
	pi := info.PeerInfo
	mcf := mon.cf

	if pi.StatusCode == 0 {
		status := int32(kibitz.STATUS_DOWN)
		if isup {
			status = int32(kibitz.STATUS_UP)
		}
		pi.StatusCode = status
	}
	if pi.Subsystem == "" {
		pi.Subsystem = mcf.System
	}
	if pi.Environment == "" {
		pi.Environment = mcf.Environment
	}
	if pi.ServerId == "" {
		pi.ServerId = mcf.Id
	}
	if pi.Datacenter == "" {
		pi.Datacenter = mcf.Datacenter
	}
	if pi.Rack == "" {
		pi.Rack = mcf.Rack
	}
	if pi.Via == "" {
		pi.Via = viaMon
	}
	if len(pi.NetInfo) == 0 {
		pi.NetInfo = mon.netinfo
	}
}

func (mon *monDat) result(isup bool, info *acproto.ACPHeartBeat) {

	now := mon.pdb.ClockNow()
	boot := mon.pdb.ClockBoot()

	monchks.Add(1)

	if info != nil {
		pi := info.PeerInfo

		if mon.timedn == 0 || !isup {
			mon.timedn = now
		}
		if isup {
			mon.timeup = now
		}

		pi.TimeChecked = now
		pi.TimeCreated = now
		pi.TimeConf = boot
		pi.TimeLastUp = mon.timeup
		pi.TimeUpSince = mon.timedn

		mon.id = pi.GetServerId() // save best known Id
		mon.pdb.Update(info)
	}
	if isup {
		mon.pdb.PeerUp(mon.id)
	} else {
		mon.pdb.PeerDn(mon.id)
	}
}
