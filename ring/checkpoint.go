// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-06 17:05 (EDT)
// Function: save+restore repartitioning checkpoint data

package ring

import (
	"fmt"
)

type checkpoint struct {
	ringver  uint64
	ringbits int
	chktree  uint16
	chkver   uint64
}

func (p *P) getCheckpoint() (*checkpoint, bool) {

	c := &checkpoint{}

	d, ok := p.db.GetInternal("m", "checkpoint")
	if !ok {
		return c, false
	}

	_, err := fmt.Sscan(string(d), &c.ringver, &c.ringbits, &c.chktree, &c.chkver)
	if err != nil {
		dl.Problem("%v", err)
	}
	return c, true
}

func (p *P) saveCheckpoint(c *checkpoint) {

	d := fmt.Sprintf("%d %d %d %d", c.ringver, c.ringbits, c.chktree, c.chkver)
	p.db.SetInternal("m", "checkpoint", []byte(d))
}
