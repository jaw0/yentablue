// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-11 10:49 (EDT)
// Function:

package main

import (
	"expvar"
	"fmt"
	"runtime"
	"syscall"
	"time"

	insight "github.com/jaw0/streaminginsight"
)

var requests = expvar.NewInt("net_reqs")

var histo = insight.New(100, 60*time.Second)

func init() {

	expvar.Publish("net_reqtime_mean", &reqPct{-1, histo})
	expvar.Publish("net_reqtime_median", &reqPct{50, histo})
	expvar.Publish("net_reqtime_80th", &reqPct{80, histo})
	expvar.Publish("net_reqtime_90th", &reqPct{90, histo})
	expvar.Publish("net_reqtime_95th", &reqPct{95, histo})
	expvar.Publish("net_reqtime_99th", &reqPct{99, histo})

	l := &load{}
	l.String() // init
	expvar.Publish("load", l)
}

func reqStatsCounter(dt time.Duration) {
	requests.Add(1)
	histo.Add(int(dt.Nanoseconds() / 1000)) // microsec
}

// ################################################################

type load struct {
	t0  int64
	cpu int64
}

func (l *load) String() string {

	t := time.Now().UnixNano()

	var usage syscall.Rusage
	syscall.Getrusage(0, &usage)
	cpu := usage.Utime.Nano() + usage.Stime.Nano()

	ave := float64(cpu-l.cpu) / float64(t-l.t0) / float64(runtime.NumCPU()) * 100.0

	l.t0 = t
	l.cpu = cpu

	return fmt.Sprintf("%f", ave)
}

// ################################################################

type reqPct struct {
	p float64
	s *insight.S
}

func (s *reqPct) String() string {
	if s.p < 0 {
		return fmt.Sprintf("%f", s.s.Mean())
	}
	return fmt.Sprintf("%f", s.s.Percentile(s.p))
}
