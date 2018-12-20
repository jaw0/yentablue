// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-13 10:53 (EDT)
// Function:

package sqlite

import (
	"database/sql"
	"encoding/base64"
	_ "github.com/mattn/go-sqlite3"
	"strings"

	"github.com/jaw0/acgo/diag"
)

type Be struct {
	name string
	path string
	db   *sql.DB
}

var dl = diag.Logger("sqlite")

func Open(name string, path string) (*Be, error) {

	dl.Debug("opening sqlite '%s'", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	be := &Be{
		name: name,
		path: path,
		db:   db,
	}

	be.init()

	return be, nil
}

func (be *Be) Close() {
	be.db.Close()
}

func (be *Be) Get(sub string, key string) ([]byte, bool, error) {
	dl.Debug("get %s/%s/%s", be.name, sub, key)

	rows, err := be.db.Query("select value, 1 from ykv where map = ? and sub = ? and key = ?", be.name, sub, key)
	if err != nil {
		return nil, false, err
	}

	var val string
	var found bool
	rows.Next()
	rows.Scan(&val, &found)
	rows.Close()

	if !found {
		return nil, false, nil
	}

	v, _ := base64.StdEncoding.DecodeString(val)

	return v, true, nil
}

func (be *Be) Put(sub string, key string, val []byte) error {
	dl.Debug("put %s/%s/%s", be.name, sub, key)

	tx, _ := be.db.Begin()

	_, err := tx.Exec("delete from ykv where map = ? and sub = ? and key = ?", be.name, sub, key)
	n, err := tx.Exec("insert into ykv (map,sub,key,value) values (?,?,?,?)",
		be.name, sub, key, base64.StdEncoding.EncodeToString(val))

	tx.Commit()

	dl.Debug("err %v", n, err)
	return err
}

func (be *Be) Del(sub string, key string) error {

	dl.Debug("del %s/%s/%s", be.name, sub, key)
	_, err := be.db.Exec("delete from ykv where map = ? and sub = ? and key = ?", be.name, sub, key)
	return err
}

func (be *Be) Range(sub string, start string, end string, lambda func(string, []byte) bool) error {

	var rows *sql.Rows
	var err error

	if end != "" {
		rows, err = be.db.Query("select key, value from ykv where map = ? and sub = ? and key >= ? and key < ?", be.name, sub, start, end)
	} else {
		rows, err = be.db.Query("select key, value from ykv where map = ? and sub = ? and key >= ?", be.name, sub, start)
	}

	if err != nil {
		return err
	}

	for rows.Next() {
		var k string
		var v string

		rows.Scan(&k, &v)

		val, _ := base64.StdEncoding.DecodeString(v)
		ok := lambda(k, val)

		if !ok {
			break
		}

	}

	rows.Close()
	return nil
}

//################################################################

func (be *Be) init() {

	for _, sql := range strings.Split(initsql, ";") {
		sql = strings.Trim(sql, " \t\n\r")
		if sql != "" {
			dl.Debug("sql: %s", sql)
			_, err := be.db.Exec(sql)
			if err != nil {
				dl.Fatal("err %v", err)
			}
		}
	}
}

const initsql = `
create table if not exists ykv (
	map	text 	not null,
	sub	text	not null,
	key	text	not null,
	value	text,

	unique(map,sub,key)
);

create index if not exists ykvidx on ykv(map, sub, key);

pragma synchronous = 1;

pragma cache_size  = 100000;

vacuum;

analyze;
`
