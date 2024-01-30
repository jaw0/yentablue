// Copyright (c) 2022
// Author: Jeff Weisberg <tcp4me.com!jaw>
// Created: 2022-Apr-19 13:41 (EDT)
// Function: leader election, per db, using lamport clocks

package raftish

import (
	"math/rand"
	"sync"

	"github.com/jaw0/acdiag"
	"github.com/jaw0/kibitz/lamport"

	"github.com/jaw0/yentablue/proto"
)

var dl = diag.Logger("raftish")

type Conf struct {
	Id string
	DB []string
}

type R struct {
	lock sync.Mutex
	myId string
	dbr  map[string]*dbR // [dbname]
}

type dbR struct {
	clock     *lamport.Clock
	leader    string
	voteFor   string
	termStart lamport.Time
	peers     map[string]*peerState // [serverId]
}

type peerState struct {
	leader    string
	voteFor   string
	termStart lamport.Time
	timeVoted lamport.Time
	isUp      bool
	downSince lamport.Time
}

func New(cf Conf) *R {

	r := &R{
		myId: cf.Id,
		dbr:  make(map[string]*dbR),
	}

	for _, dbname := range cf.DB {
		r.dbr[dbname] = &dbR{
			clock: lamport.New(),
			peers: make(map[string]*peerState),
		}
	}

	return r
}

func (r *R) PeerUpdate(id string, isup bool, hb *acproto.ACPHeartBeat) {

	r.lock.Lock()
	defer r.lock.Unlock()

	for _, db := range hb.Database {

		if db.Raft == nil {
			// no data. skip
			continue
		}

		ballot := db.Raft
		dbr := r.dbr[db.Name]
		if dbr == nil {
			// unknown db - skip
			continue
		}
		if dbr.peers[id] == nil {
			dbr.peers[id] = &peerState{}
		}
		ps := dbr.peers[id]

		if ballot.TimeVoted < ps.timeVoted.Uint64() {
			// out of date - skip
			dl.Debug("out of date: %s", id)
			continue
		}

		if isup {
			dbr.vote(id, r.myId, db.Name, ballot)
		} else {
			dl.Debug("raftish down: %#v", hb.GetPeerInfo())
			dbr.down(id, r.myId, db.Name, ballot)
		}
	}
}

func (r *R) CurrentLeader(dbname string) string {
	r.lock.Lock()
	defer r.lock.Unlock()

	dbr := r.dbr[dbname]
	if dbr == nil {
		dl.Debug("unknown db '%s'", dbname)
		return ""
	}

	return dbr.leader
}

// am I the leader?
func (r *R) AmLeader(dbname string) bool {
	return r.CurrentLeader(dbname) == r.myId
}

// provide our current state to peers
func (r *R) RaftInfo(dbname string) *acproto.ACPRaftish {

	r.lock.Lock()
	defer r.lock.Unlock()

	dbr := r.dbr[dbname]
	if dbr == nil {
		dl.Debug("unknown db '%s'", dbname)
		return nil
	}

	if dbr.leader == "" && dbr.voteFor == "" {
		dl.Debug("nominating self '%s', %s", dbname, r.myId)
		// nominate myself
		dbr.voteFor = r.myId
		dbr.termStart = dbr.clock.Inc()
	}

	return &acproto.ACPRaftish{
		Leader:    dbr.leader,
		Vote:      dbr.voteFor,
		TermStart: dbr.termStart.Uint64(),
		TimeVoted: dbr.clock.Inc().Uint64(),
	}
}

func (dbr *dbR) vote(id, myid, dbname string, ballot *acproto.ACPRaftish) {
	dl.Debug("up %s %s; %#v", id, dbname, ballot)

	ps := dbr.peers[id]

	// copy data
	ps.isUp = true
	ps.leader = ballot.Leader
	ps.voteFor = ballot.Vote
	ps.termStart = lamport.Time(ballot.TermStart)
	ps.timeVoted = lamport.Time(ballot.TimeVoted)
	ps.downSince = 0

	dbr.clock.Update(ps.timeVoted)
	dbr.election(myid, dbname)
}

func (dbr *dbR) down(id, myid, dbname string, ballot *acproto.ACPRaftish) {
	dl.Debug("dn %s %s", id, dbname)

	ps := dbr.peers[id]

	ps.termStart = lamport.Time(ballot.TermStart)
	ps.timeVoted = lamport.Time(ballot.TimeVoted)
	dbr.clock.Update(ps.timeVoted)

	if ps.isUp {
		ps.downSince = dbr.clock.Now()
	}
	ps.isUp = false

	if id == dbr.leader {
		// leader is down - start a new election
		dbr.leader = ""
		dbr.voteFor = ""
		dbr.termStart = dbr.clock.Inc()
	}
	dbr.election(myid, dbname)
}

func (dbr *dbR) election(myid, dbname string) {

	nDown := 0
	nUp := 1 // count myself
	leaderBallot := NewBallotBox()
	candidBallot := NewBallotBox()
	maxTerm := dbr.termStart

	// cast my votes
	if dbr.leader == "" && dbr.voteFor == "" {
		dbr.voteFor = myid
	}
	leaderBallot.cast(dbr.leader)
	if dbr.voteFor != "" {
		candidBallot.cast(dbr.voteFor)
	} else {
		candidBallot.cast(dbr.leader)
	}

	// count the ballots
	for _, ps := range dbr.peers {
		if !ps.isUp {
			nDown++
			continue
		}
		nUp++

		if ps.termStart >= dbr.termStart {
			// only count votes in the current term
			leaderBallot.cast(ps.leader)
			if ps.voteFor != "" {
				candidBallot.cast(ps.voteFor)
			} else {
				candidBallot.cast(ps.leader)
			}
		} else {
			dl.Debug("%x < %x : %#v", ps.termStart, dbr.termStart, ps)
		}

		if ps.termStart > maxTerm {
			maxTerm = ps.termStart
		}
	}

	// QQQ - what constitutes a majority?
	// servers can come+go, so "all servers" is unknown.
	// perhaps up+down, and remove down after a timeout?

	numTotal := nUp

	// is there a consensus on the leader
	winner, nVotes := leaderBallot.winner()
	dl.Debug("leader ballots %s %d; %#v", winner, nVotes, leaderBallot)

	if nVotes*2 > numTotal && numTotal > 1 {
		dbr.newLeader(winner, maxTerm, dbname)
		if winner != "" {
			dl.Debug("winner: %s %s %d %d", dbname, dbr.leader, maxTerm, dbr.termStart)
			return
		}
	}

	// is there a consensus on votes
	best, nVotes := candidBallot.winner()
	dl.Debug("candidate ballots %s %d; %#v", best, nVotes, candidBallot)

	if nVotes*2 > numTotal && numTotal > 1 {
		dbr.newLeader(best, maxTerm, dbname)
		if best != "" {
			dl.Debug("winner: %s %s %x %x", dbname, dbr.leader, maxTerm, dbr.termStart)
			return
		}
	}

	dbr.termStart = maxTerm
	dbr.voteFor = best
	dl.Debug("voting for %s %x", best, maxTerm)
}

func (dbr *dbR) newLeader(leader string, maxTerm lamport.Time, dbname string) {

	if (leader != dbr.leader) && (leader != "") {
		dl.Verbose("new leader for '%s' : %s", dbname, leader)
	}
	if leader != "" {
		dbr.voteFor = ""
		dbr.termStart = maxTerm
	}
	dbr.leader = leader
}

// ################################################################

type ballotBox struct {
	count map[string]int
}

func NewBallotBox() *ballotBox {
	return &ballotBox{
		count: make(map[string]int),
	}
}

func (bb *ballotBox) cast(id string) {
	bb.count[id]++
}

func (bb *ballotBox) winner() (string, int) {

	var names []string
	best := 0

	for n, v := range bb.count {
		if v == best {
			// tie
			names = append(names, n)
		}
		if v > best {
			best = v
			names = []string{n}
		}
	}

	// break ties randomly
	rnd := rand.Intn(len(names))
	return names[rnd], best
}

// pick anything
func (bb *ballotBox) random() string {

	rnd := rand.Intn(len(bb.count))

	for k := range bb.count {
		if rnd == 0 {
			return k
		}
		rnd--
	}

	return ""
}
