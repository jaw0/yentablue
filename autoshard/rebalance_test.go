// Copyright (c) 2022
// Author: Jeff Weisberg <tcp4me.com!jaw>
// Created: 2022-Apr-21 15:03 (EDT)
// Function: test

package autoshard

import (
	"fmt"
	"testing"

	"github.com/jaw0/yentablue/ring/config"
)

func TestRebalance(t *testing.T) {
	rcf, err := config.FromBytes([]byte(cftxt))
	if err != nil {
		fmt.Printf("err: %v\n", err)
	}

	rebalance(rcf, 0, sinfo)
	fmt.Printf(">> %#v\n", rcf.Parts)

	rbi := mkRebalanceInfo("dc1", rcf)
	for _, i := range rbi {
		fmt.Printf(">  %#v\n", i)
	}

}

func TestDetermineAdjust(t *testing.T) {

	adjust := determineServerAdjustment("dc1", 8, sinfo)

	for name, adj := range adjust {
		fmt.Printf("%s %f\n", name, adj)
	}
	if adjust["s2"] < 0 {
		t.Fail()
	}
	if adjust["s3"] > 0 {
		t.Fail()
	}
}

var sinfo = map[string]*serverInfo{
	"s1": {datacenter: "dc1", totalSpace: 1000, availSpace: 500},
	"s2": {datacenter: "dc1", totalSpace: 1000, availSpace: 750},
	"s3": {datacenter: "dc1", totalSpace: 2000, availSpace: 700},
	"s4": {datacenter: "dc1", totalSpace: 1000, availSpace: 400},
}

var cftxt = `
{
  "Version": 1,
  "Replicas": 2,
  "RingBits": 8,
  "Parts": [
     { "Server": "s1", "Datacenter": "dc1", "Rack": "r1", "Shard": [1685892766, 3364293257, 2444096260, 3070921507] },
     { "Server": "s2", "Datacenter": "dc1", "Rack": "r1", "Shard": [ 715167895, 3612019622, 2246265240, 1747858774] },
     { "Server": "s3", "Datacenter": "dc1", "Rack": "r1", "Shard": [ 360714155,  677537270, 2779897733, 3841108652] },
     { "Server": "s4", "Datacenter": "dc1", "Rack": "r1", "Shard": [ 110890611,  233745421, 3104310162, 3416388008] }
  ]
}
`
