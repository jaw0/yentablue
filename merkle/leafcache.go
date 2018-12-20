// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-17 12:20 (EDT)
// Function: cache merkle leaf nodes

package merkle

import ()

func (m *D) leafCacheGet(lockno int, mkey string) *LeafSaves {

	c := &m.cache[lockno]

	if c.mkey == mkey {
		// cached
		return c.leaves
	}

	if c.mkey != "" {
		// something else here? flush it
		m.leafCacheFlush(lockno)
	}

	// fetch
	c.mkey = mkey
	c.dirty = false

	v := m.leafGet(mkey)

	dl.Debug("fetch %s -> %d", mkey, len(v.Save))

	if v != nil {
		c.leaves = v
	} else {
		c.leaves = &LeafSaves{}
	}

	return c.leaves
}

func (m *D) leafCacheSet(lockno int, treeid uint16, ver uint64, fixme bool, leafs *LeafSaves) {

	c := &m.cache[lockno]

	c.treeID = treeid
	c.ver = ver
	c.dirty = true
	c.fixme = fixme
	c.leaves = leafs
	dl.Debug("lock %d [%d] %s", lockno, len(leafs.Save), c.mkey)
}

func (m *D) leafCacheFlush(lockno int) {

	c := &m.cache[lockno]

	if c.mkey == "" {
		return
	}
	if !c.dirty && !c.fixme {
		return
	}

	if len(c.leaves.Save) == 0 {
		m.db.MDel(c.mkey)
		dl.Debug("del leaf [0] %s", c.mkey)
		m.queueLeafNext(c.treeID, c.ver, 0, c.fixme, nil)
	} else {
		val := leaf2bytes(c.leaves)
		dl.Debug("put leaf [%d %d] %s; %#v -> %#v", len(c.leaves.Save), len(val), c.mkey, c.leaves, val)
		m.db.MPut(c.mkey, val)
		m.queueLeafNext(c.treeID, c.ver, len(c.leaves.Save), c.fixme, c.leaves)
	}

	c.dirty = false
	c.fixme = false
	c.mkey = ""
}

func (m *D) leafCacheFlushAll() {

	for i := 0; i < NLOCK; i++ {
		m.nlock[i].Lock()
		m.leafCacheFlush(i)
		m.nlock[i].Unlock()
	}
}

//################################################################

func (l *LeafSaves) Len() int {
	return len(l.Save)
}
func (l *LeafSaves) Less(i int, j int) bool {

	if l.Save[i].Version < l.Save[j].Version {
		return true
	}
	if l.Save[i].Version > l.Save[j].Version {
		return false
	}
	if l.Save[i].Key < l.Save[j].Key {
		return true
	}
	return false
}
func (l *LeafSaves) Swap(i int, j int) {
	l.Save[i], l.Save[j] = l.Save[j], l.Save[i]
}

func (m *D) leafGet(mkey string) *LeafSaves {

	val, found := m.db.MGet(mkey)
	if !found {
		return &LeafSaves{}
	}
	return bytes2leaf(val)
}

func bytes2leaf(b []byte) *LeafSaves {
	d := &LeafSaves{}
	d.Unmarshal(b)
	return d
}

func leaf2bytes(d *LeafSaves) []byte {
	buf, err := d.Marshal()
	if err != nil {
		dl.Problem("corrupt data! %v", err)
	}
	return buf
}
