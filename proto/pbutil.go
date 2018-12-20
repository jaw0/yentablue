// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Nov-06 08:32 (EST)
// Function:

package acproto

import (
	"github.com/jaw0/kibitz"
)

func (p *ACPHeartBeat) SetPeerInfo(pi *kibitz.PeerInfo) {
	p.PeerInfo = pi
}
