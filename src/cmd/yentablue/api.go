// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-20 13:55 (EDT)
// Function:

package main

import (
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/putstatus"
	"github.com/jaw0/yentablue/shard"
)

func (*myServer) SendHB(ctx context.Context, hb *acproto.ACPHeartBeatRequest) (*acproto.ACPHeartBeatReply, error) {
	//fmt.Printf("sendHB %v\n", hb)

	if hb.Myself != nil {
		pdb.UpdateSceptical(hb.Myself)
	}

	res := &acproto.ACPHeartBeatReply{StatusCode: proto.Int32(200), StatusMessage: proto.String("OK")}
	pwd := pdb.GetAll()

	for _, p := range pwd {
		pd := p.GetData()
		w := pd.(*acproto.ACPHeartBeat)
		res.Hbinfo = append(res.Hbinfo, w)
	}

	res.Hbinfo = append(res.Hbinfo, Myself(pdb.MyInfo()))

	return res, nil
}

func addServer(res *acproto.ACPY2ServerReply, w *acproto.ACPHeartBeat, isUp bool) {

	ii := w.PeerInfo
	isLocal := ii.GetDatacenter() == pdb.Datacenter()

	r := &acproto.ACPY2ServerData{
		Subsystem:      ii.Subsystem,
		Environment:    ii.Environment,
		ServerId:       ii.ServerId,
		Datacenter:     ii.Datacenter,
		Rack:           ii.Rack,
		Hostname:       ii.Hostname,
		TimeUp:         ii.TimeUp,
		NetInfo:        ii.NetInfo,
		IsUp:           &isUp,
		IsLocal:        &isLocal,
		SortMetric:     w.SortMetric,
		CpuMetric:      w.CpuMetric,
		CapacityMetric: w.CapacityMetric,
		Database:       w.Database,
		Uptodate:       w.Uptodate,
	}

	res.Data = append(res.Data, r)
}

func (*myServer) Servers(ctx context.Context, req *acproto.ACPY2ServerRequest) (*acproto.ACPY2ServerReply, error) {

	pwd := pdb.GetAll()
	res := &acproto.ACPY2ServerReply{}

	for _, p := range pwd {
		pd := p.GetData()
		w := pd.(*acproto.ACPHeartBeat)
		ii := w.PeerInfo

		if req.Subsystem != nil && req.GetSubsystem() != ii.GetSubsystem() {
			continue
		}
		if req.Environment != nil && req.GetEnvironment() != ii.GetEnvironment() {
			continue
		}
		if req.Hostname != nil && req.GetHostname() != ii.GetHostname() {
			continue
		}
		if req.Datacenter != nil && req.GetDatacenter() != ii.GetDatacenter() {
			continue
		}
		if req.ServerId != nil && req.GetServerId() != ii.GetServerId() {
			continue
		}

		isUp := w.PeerInfo.GetStatusCode() == int32(kibitz.STATUS_UP)

		addServer(res, w, isUp)
	}

	addServer(res, Myself(pdb.MyInfo()), true)

	return res, nil
}

func (*myServer) GetMerkle(ctx context.Context, hb *acproto.ACPY2CheckRequest) (*acproto.ACPY2CheckReply, error) {

	mt, err := sdb.GetMerkle(hb.GetMap(), int(hb.GetLevel()), uint16(hb.GetTreeid()), hb.GetVersion())
	if err != nil {
		return nil, err
	}

	res := &acproto.ACPY2CheckReply{}

	for _, m := range mt {
		v := &acproto.ACPY2CheckValue{
			Map:      proto.String(m.Map),
			Level:    proto.Int(m.Level),
			Treeid:   proto.Uint32(uint32(m.TreeID)),
			Shard:    proto.Uint32(m.Shard),
			Version:  proto.Uint64(m.Version),
			Keycount: proto.Int64(m.KeyCount),
			Children: proto.Int(m.Children),
			Isvalid:  proto.Bool(m.IsValid),
		}

		if len(m.Hash) != 0 {
			v.Hash = m.Hash
		}
		if len(m.Key) != 0 {
			v.Key = proto.String(m.Key)
		}

		res.Check = append(res.Check, v)
	}

	return res, nil
}

func (*myServer) Get(ctx context.Context, req *acproto.ACPY2GetSet) (*acproto.ACPY2GetSet, error) {

	for _, r := range req.Data {
		_, err := sdb.Get(r)

		if err != nil {
			return req, err
		}

		// not found? redirect to a better server
		if r.GetVersion() == 0 && r.Shard != nil {
			sdb.Redirect(r)
		}
	}

	return req, nil
}

func (*myServer) Range(ctx context.Context, req *acproto.ACPY2GetRange) (*acproto.ACPY2GetSet, error) {

	res := &acproto.ACPY2GetSet{}

	_, err := sdb.GetRange(req, res)
	return res, err
}

func (*myServer) Put(ctx context.Context, req *acproto.ACPY2DistRequest) (*acproto.ACPY2DistReply, error) {

	// runmode.is_stopping => 500

	r := req.Data

	if r.Shard == nil {
		// fill in missing shard
		r.Shard = proto.Uint32(shard.Hash(r.GetKey()))
	}
	status, loc := sdb.Put(r)

	switch status {
	case putstatus.DONE:
		if loc.IsLocal {
			sdb.Distrib(loc, req, func(int) {})
		} else {
			// this does not belong on this server.
			// save + distribute, after others have it, delete
			sdb.Distrib(loc, req, func(status int) {
				if status == putstatus.DONE {
					sdb.Del(r)
				}
			})
			// ...
		}
		// RSN - push update to other clients, chans, ...
	case putstatus.NOTME:
		// try to handoff to the correct server
		// RSN - configurable option - handoff or return 302 redirect
		// or write to disk, or ...
		sdb.Handoff(req)
	}

	return &acproto.ACPY2DistReply{
		StatusCode:    proto.Int(200),
		StatusMessage: proto.String("OK"),
		ResultCode:    proto.Int(status),
	}, nil
}

func (*myServer) RingConf(ctx context.Context, req *acproto.ACPY2RingConfReq) (*acproto.ACPY2RingConfReply, error) {

	res, err := sdb.GetRingConf(req.GetMap(), req.GetDatacenter())

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (*myServer) ShutDown(ctx context.Context, req *acproto.ACPY2Empty) (*acproto.ACPHeartBeatReply, error) {

	close(shutdown)
	res := &acproto.ACPHeartBeatReply{StatusCode: proto.Int32(200), StatusMessage: proto.String("OK")}
	return res, nil
}
