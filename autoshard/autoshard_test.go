// Copyright (c) 2022
// Author: Jeff Weisberg <tcp4me.com!jaw>
// Created: 2022-Apr-22 11:30 (EDT)
// Function: test

package autoshard

import (
	"fmt"
	"testing"
)

func TestNewShard(t *testing.T) {

	// leave the top half available
	var used []uint32
	for i := 0; i < 16; i++ {
		used = append(used, uint32(i<<27))
	}

	n := newShardNum(used, 4)

	if n < 0x80000000 {
		fmt.Printf("%x\n", n)
		t.Fail()
	}
}
