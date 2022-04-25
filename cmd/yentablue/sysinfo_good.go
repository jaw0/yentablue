// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Nov-15 10:03 (EST)
// Function: system info

// +build !netbsd

package main

import (
	"syscall"
)

func spaceAvail(dir string) int32 {

	var st syscall.Statfs_t
	syscall.Statfs(dir, &st)
	return int32(st.Bavail * st.Frsize / 1000000) // MB avail
}

func spaceTotal(dir string) int32 {

	var st syscall.Statfs_t
	syscall.Statfs(dir, &st)
	return int32(st.Blocks * st.Frsize / 1000000) // MB avail
}

func currentLoad() int32 {

	var info syscall.Sysinfo_t
	syscall.Sysinfo(&info)
	return int32(1000 * float32(info.Loads[0]) / 65536)
}
