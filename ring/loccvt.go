// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-03 10:34 (EDT)
// Function: simple conversions

package ring

func PartShard2TreeID(bits int, shard uint32) uint16 {
	return uint16((shard >> 16) & (0xFFFF << uint(16-bits)))
}

func PartShard2Idx(bits int, shard uint32) int {
	return int(shard >> uint(32-bits))
}

func PartTreeID2Shard(bits int, treeid uint16) uint32 {
	return uint32(treeid) << 16
}

func PartTreeID2Idx(bits int, treeid uint16) int {
	return int(treeid >> uint(16-bits))
}

func PartIdx2TreeID(bits int, partidx int) uint16 {
	return uint16(partidx << uint(16-bits))
}

func PartIdx2Shard(bits int, partidx int) uint32 {
	return uint32(partidx << uint(32-bits))
}
