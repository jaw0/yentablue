// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-03 10:35 (EDT)
// Function:

package ring

import (
	"testing"
)

func TestConvert(t *testing.T) {

	checkCvt(t, 8, 0x12345678, 0x1200, 0x12)
	checkCvt(t, 2, 0x12345678, 0x0000, 0x0)
	checkCvt(t, 2, 0xF2345678, 0xC000, 0x3)
	checkCvt(t, 0, 0x12345678, 0, 0)
}

func checkCvt(t *testing.T, bits int, shard uint32, treeid uint16, partidx int) {

	checkRes(t, partShard2TreeID(bits, shard) == treeid)
	checkRes(t, partShard2Idx(bits, shard) == partidx)
	checkRes(t, partTreeID2Idx(bits, treeid) == partidx)
	checkRes(t, partIdx2TreeID(bits, partidx) == treeid)
}

func checkRes(t *testing.T, ok bool) {

	if !ok {
		t.Errorf("fail")
	}
}
