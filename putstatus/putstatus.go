// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-31 11:30 (EDT)
// Function:

package putstatus

const (
	DONE     = 0 // data was accepted, and saved
	WANT     = 0
	BAD      = 1 // invalid
	OLD      = 2 // expired
	NOTME    = 3 // wrong server
	HAVE     = 4 // already have this
	NEWER    = 5 // already have a newer version
	FAIL     = 6 // tried, failed
	CONDFAIL = 7 // condition failed to match
)
