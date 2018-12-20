// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-07 15:17 (EDT)
// Function:

package ring

import (
	"fmt"
	"testing"

	"github.com/jaw0/yentablue/store/ring/config"
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
	}

	np := p.configureParts(cf)

	for pi, pt := range np {
		fmt.Printf("Part %d\t%v\n", pi, pt.isLocal)

		for _, dc := range pt.dc {
			fmt.Printf("  DC %s\t%v\n", dc.dcname, dc.isBoundary)
			fmt.Printf("    %v\n", dc.servers)

		}

	}
}

var cftxt = `
{
  "Version": 1,
  "Replicas": 2,
  "RingBits": 8,
  "Parts": [
     { "Server": "s1", "Datacenter": "dc1", "Rack": "r1", "Shard": [0] },
     { "Server": "s2", "Datacenter": "dc1", "Rack": "r1", "Shard": [536870912] },
     { "Server": "s3", "Datacenter": "dc1", "Rack": "r1", "Shard": [1073741824] },
     { "Server": "s4", "Datacenter": "dc1", "Rack": "r1", "Shard": [1610612736] },
     { "Server": "s5", "Datacenter": "dc1", "Rack": "r2", "Shard": [2147483648] },
     { "Server": "s6", "Datacenter": "dc1", "Rack": "r2", "Shard": [2684354560] },
     { "Server": "s7", "Datacenter": "dc1", "Rack": "r2", "Shard": [3221225472] },
     { "Server": "s8", "Datacenter": "dc1", "Rack": "r2", "Shard": [3758096384] }
  ]
}
`
