// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Nov-15 10:03 (EST)
// Function: system info - netbsd

// +build netbsd

package main

import (
	"github.com/mackerelio/go-osstat/loadavg"
	"golang.org/x/sys/unix"
)

func spaceAvail(dir string) int32 {

	// has statvfs instead of statfs
	var st unix.Statvfs_t
	unix.Statvfs(dir, &st)
	return int32(st.Bavail * st.Bsize / 1000000) // MB avail
}

func currentLoad() int32 {

	aves, err := loadavg.Get()
	if err != nil {
		return 0
	}
	return int32(1000 * aves.Loadavg1)
}
