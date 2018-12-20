// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-02 20:41 (EDT)
// Function:

package soty

import (
	"time"
)

type Loc struct {
	Shard   uint32 // 0x12345678
	TreeID  uint16 // 0x1230
	PartIdx int    // 0x0123
	IsLocal bool
}

// ################################################################

func Now() uint64 {
	return uint64(time.Now().UnixNano())
}
