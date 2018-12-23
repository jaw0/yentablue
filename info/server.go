// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-03 11:41 (EST)
// Function: save file data

package info

import (
	"math/rand"
	"os"
	"sort"
	"time"
)

type Net struct {
	Dom  string
	Addr string
}

type Server struct {
	Hostname       string
	Datacenter     string
	Environment    string
	Rack           string
	Id             string
	Subsystem      string
	Env            string
	IsUp           bool
	IsLocal        bool
	Uptodate       bool
	SortMetric     int32
	CpuMetric      int32
	CapacityMetric int32
	TimeUp         uint64
	Database       []string
	NetInfo        []Net
}

func LoadServerInfo(file string) ([]*Server, error) {

	var res []*Server

	err := LoadJson(file, &res)
	return res, err
}

// ################################################################

type SD struct {
	file    string
	loaded  time.Time
	checked time.Time
	info    []*Server
}

const CHECKTIME = 10 * time.Second
const STABLETIME = 30 * time.Second

func NewServerInfo(file string) (*SD, error) {

	sd := &SD{file: file}
	err := sd.maybeReadFile()

	return sd, err
}

func (sd *SD) maybeReadFile() error {

	now := time.Now()
	defer func() { sd.checked = now }()

	if now.Before(sd.checked.Add(CHECKTIME)) {
		return nil
	}

	st, err := os.Stat(sd.file)
	if err != nil {
		return err
	}

	if !st.ModTime().After(sd.loaded) {
		return nil
	}

	return sd.readFile()
}

func (sd *SD) readFile() error {

	info, err := LoadServerInfo(sd.file)
	if err != nil {
		return err
	}

	sd.info = info
	sd.loaded = time.Now()
	return nil
}

func (sd *SD) GetServers(sys, env string, nofar bool) []*Server {

	sd.maybeReadFile()

	res := []*Server{}

	for _, s := range sd.info {
		if !s.IsUp {
			continue
		}
		if sys != "" && sys != s.Subsystem {
			continue
		}
		if env != "" && env != s.Env {
			continue
		}
		if nofar && !s.IsLocal {
			continue
		}

		res = append(res, s)
	}

	return res
}

func (sd *SD) GetServersOrdered(sys, env string, nofar bool) []*Server {

	// prefer stable, local
	stable := time.Now().UnixNano() - int64(STABLETIME)

	all := sd.GetServers(sys, env, nofar)
	stable_local := []*Server{}
	all_local := []*Server{}

	for _, s := range all {
		if !s.IsLocal {
			continue
		}
		all_local = append(all_local, s)

		if s.TimeUp < uint64(stable) {
			continue
		}

		stable_local = append(stable_local, s)
	}

	if len(stable_local) > 0 {
		all = stable_local
	} else if len(all_local) > 0 {
		all = all_local
	}

	if len(all) == 0 {
		return nil
	}

	// sort by metric
	sort.Slice(all, func(i, j int) bool {
		return all[i].SortMetric < all[j].SortMetric
	})

	// semi-randomize
	// pick a pivot point, shuffle the 2 halves

	i := 3*len(all)/4 - 1
	if i < 0 {
		i = 0
	}
	limit := all[i].SortMetric

	left := []*Server{}
	right := []*Server{}

	for _, s := range all {
		if s.SortMetric <= limit {
			left = append(left, s)
		} else {
			right = append(right, s)
		}
	}

	shuffle(left)
	shuffle(right)
	return append(left, right...)
}

func shuffle(a []*Server) {

	i := len(a)

	for i > 1 {
		j := rand.Int31n(int32(i))
		i--
		a[i], a[j] = a[j], a[i]
	}
}
