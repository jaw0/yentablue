// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-10 16:30 (EDT)
// Function:

package badger

import (
	"time"

	"github.com/dgraph-io/badger"

	"github.com/jaw0/acgo/diag"
)

type Be struct {
	name string
	path string
	db   *badger.DB
}

type beLog struct{}

var dl = diag.Logger("badger")

func Open(name string, path string) (*Be, error) {

	dl.Debug("opening badgerdb '%s'", path)

	opts := badger.DefaultOptions(path)
	opts.Logger = beLog{}
	db, err := badger.Open(opts)

	if err != nil {
		return nil, err
	}

	be := &Be{
		name: name,
		path: path,
		db:   db,
	}

	go be.badgerMaint()
	return be, nil
}

func (be *Be) Close() {
	be.db.Close()
}

func (be *Be) Get(sub string, key string) ([]byte, bool, error) {
	dl.Debug("get %s/%s/%s", be.name, sub, key)

	txn := be.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get([]byte(sub + key))

	if err != nil {
		return nil, false, err
	}

	var val []byte
	err = item.Value(func(val []byte) error {
		val = append([]byte{}, val...)
		return nil
	})

	if err != nil {
		return nil, false, err
	}

	return val, true, nil
}

func (be *Be) Put(sub string, key string, val []byte) error {
	dl.Debug("put %s/%s/%s", be.name, sub, key)

	txn := be.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Set([]byte(sub+key), val)

	dl.Debug("err %v", err)
	txn.Commit()
	return err
}

func (be *Be) Del(sub string, key string) error {

	dl.Debug("del %s/%s/%s", be.name, sub, key)

	txn := be.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Delete([]byte(sub + key))

	dl.Debug("err %v", err)
	txn.Commit()
	return err
}

func (be *Be) Range(sub string, start string, end string, lambda func(string, []byte) bool) error {

	txn := be.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	defer it.Close()

	prefix := []byte(sub + start)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		var ok bool

		err := item.Value(func(v []byte) error {
			ok = lambda(string(k[1:]), v)
			return nil
		})
		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}

	return nil
}

func (be *Be) badgerMaint() {
	ticker := time.NewTicker(4 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
	again:
		err := be.db.RunValueLogGC(0.7)
		if err == nil {
			goto again
		}
	}
}

func (x beLog) Errorf(msg string, args ...interface{})   { dl.Verbose(msg, args...) }
func (x beLog) Infof(msg string, args ...interface{})    { dl.Debug(msg, args...) }
func (x beLog) Debugf(msg string, args ...interface{})   { dl.Debug(msg, args...) }
func (x beLog) Warningf(msg string, args ...interface{}) { dl.Verbose(msg, args...) }
