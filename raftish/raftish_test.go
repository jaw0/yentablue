// Copyright (c) 2022
// Author: Jeff Weisberg <tcp4me.com!jaw>
// Created: 2022-Apr-19 14:06 (EDT)
// Function: test

package raftish

import (
	"fmt"
	"testing"

	"github.com/jaw0/acgo/diag"
	"github.com/jaw0/kibitz/lamport"

	"github.com/jaw0/yentablue/proto"
)

const basePort = 4400

func TestBallot(t *testing.T) {
	bb := NewBallotBox()

	bb.cast("a")
	bb.cast("a")
	bb.cast("a")
	bb.cast("b")
	bb.cast("b")
	bb.cast("c")

	winner, n := bb.winner()
	if winner != "a" && n != 3 {
		t.Fail()
	}

	if bb.random() == "" {
		t.Fail()
	}
}

func TestVote(t *testing.T) {

	clock := lamport.New()
	myid := "self"
	rft := New(Conf{Id: myid, DB: []string{"db1"}})

	diag.SetDebugAll(false)

	rft.RaftInfo("db1")

	hb := &acproto.ACPHeartBeat{
		Database: []*acproto.ACPDatabaseInfo{{
			Name: "db1",
			Raft: &acproto.ACPRaftish{
				Leader:    "a",
				Vote:      "",
				TermStart: clock.Inc().Uint64(),
				TimeVoted: clock.Now().Uint64(),
			},
		}},
	}

	rft.PeerUpdate("a", true, hb)
	rft.PeerUpdate("b", true, hb)
	rft.PeerUpdate("c", true, hb)
	//fmt.Printf(">> %v\n", rft.RaftInfo("db1"))

	res := rft.RaftInfo("db1")

	if res.Leader != "a" || res.Vote != "" {
		fmt.Printf("%v\n", res)
		t.Fail()
	}
}
