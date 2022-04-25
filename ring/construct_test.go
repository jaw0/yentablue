// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-07 15:17 (EDT)
// Function:

package ring

import (
	"fmt"
	"testing"

	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/ring/config"
)

func TestConstruct(t *testing.T) {

	cf, err := config.FromBytes([]byte(cftxt))
	if err != nil {
		fmt.Printf("err: %v\n", err)
	}
	//fmt.Printf("cf %v\n", cf)

	p := &P{
		myid:   "s1",
		mydc:   "dc1",
		myrack: "r1",
		all:    newPart("dc1"),
	}

	pcf := &cfCf{
		myid:   "s1",
		mydc:   "dc1",
		myrack: "r1",
	}
	np := pcf.configureParts(cf)

	for pi, pt := range np {
		fmt.Printf("Part %x\t%v\t", pi, pt.isLocal)

		for _, dc := range pt.dc {
			//fmt.Printf("  DC %s\t%v\n", dc.dcname, dc.isBoundary)
			fmt.Printf("    %v\n", dc.servers)
			dc = dc
		}

	}

	p.part = np
	p.replicas = cf.Replicas
	p.ringbits = cf.RingBits

	p.PeerUpdate("s2", true, true, true, &kibitz.Export{
		IsSameDC: true,
		BestAddr: "127.0.0.23:1234",
	}, &acproto.ACPHeartBeat{
		Uptodate: true,
	})

	loc := p.GetLoc(0x01)
	peer := p.RandomAEPeer(loc)
	fmt.Printf("lov %#v -> %s\n", loc, peer)

	if peer != "127.0.0.23:1234" {
		t.Fail()
	}

	loc = p.GetLoc(0xA1001234) //0x01)
	peer = p.RandomAEPeer(loc)
	fmt.Printf("lov %#v -> %s\n", loc, peer)

	if peer != "" {
		t.Fail()
	}
}

var cftxt = `
{
  "Version": 1,
  "Replicas": 1,
  "RingBits": 8,
  "Parts": [
     { "Server": "s1", "Datacenter": "dc1", "Rack": "r1", "Shard": [1685892766, 3364293257, 2444096260, 3070921507] },
     { "Server": "s2", "Datacenter": "dc1", "Rack": "r1", "Shard": [ 715167895, 3612019622, 2246265240, 1747858774] },
     { "Server": "s3", "Datacenter": "dc1", "Rack": "r1", "Shard": [ ] }
  ]
}
`
