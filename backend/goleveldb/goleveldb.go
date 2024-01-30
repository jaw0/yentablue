// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-10 16:30 (EDT)
// Function:

package goleveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/jaw0/acdiag"
)

type Be struct {
	name string
	path string
	db   *leveldb.DB
}

var dl = diag.Logger("leveldb")

func Open(name string, path string) (*Be, error) {

	dl.Debug("opening leveldb '%s'", path)

	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	be := &Be{
		name: name,
		path: path,
		db:   db,
	}

	return be, nil
}

func (be *Be) Close() {
	be.db.Close()
}

func (be *Be) Get(sub string, key string) ([]byte, bool, error) {
	dl.Debug("get %s/%s/%s", be.name, sub, key)

	val, err := be.db.Get([]byte(sub+key), nil)

	if err != nil {
		return nil, false, nil
	}

	return val, true, nil
}

func (be *Be) Put(sub string, key string, val []byte) error {
	dl.Debug("put %s/%s/%s", be.name, sub, key)

	err := be.db.Put([]byte(sub+key), val, nil)
	dl.Debug("err %v", err)
	return err
}

func (be *Be) Del(sub string, key string) error {

	dl.Debug("del %s/%s/%s", be.name, sub, key)
	err := be.db.Delete([]byte(sub+key), nil)

	return err
}

func (be *Be) Range(sub string, start string, end string, lambda func(string, []byte) bool) error {

	rang := &util.Range{Start: []byte(sub + start)}
	if end != "" {
		rang.Limit = []byte(sub + end)
	}

	iter := be.db.NewIterator(rang, nil)
	defer iter.Release()

	for iter.Next() {
		k := iter.Key()
		v := iter.Value()

		ok := lambda(string(k[1:]), v)

		if !ok {
			break
		}
	}

	return nil
}
