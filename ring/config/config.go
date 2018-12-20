// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-06 14:36 (EDT)
// Function: ring config format

package config

import (
	"encoding/json"
)

type Part struct {
	Server     string
	Datacenter string
	Rack       string
	Shard      []uint32
}
type Ring struct {
	Version  int
	Replicas int
	RingBits int
	Parts    []Part
}

const VERSION = 1

func ToBytes(cf *Ring) ([]byte, error) {
	return json.Marshal(cf)

}

func FromBytes(b []byte) (*Ring, error) {
	r := &Ring{}
	err := json.Unmarshal(b, r)
	return r, err
}
