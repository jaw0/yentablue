// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-18 16:11 (EDT)
// Function:

package merkle

import (
	"sort"
	"time"
)

func (m *D) updater(final bool) {

	var todo []*change

	if final {
		todo = m.accumuChangesFinal()
	} else {
		todo = m.accumuChanges()
	}

	if len(todo) == 0 {
		return
	}

	sort.Stable(changes(todo))
	dl.Debug("todo %d", len(todo))

	for len(todo) != 0 {
		todo = m.applyUpdates(todo)
	}
}

func (m *D) accumuChanges() []*change {

	var todo []*change

	// suck up a bunch of changes to process
	for {
		select {
		case c := <-m.chch:
			todo = append(todo, c)
			if len(todo) >= MAXTODO {
				return todo
			}
			continue
		case <-m.stop:
			return todo
		case <-time.After(10 * time.Second):
			return todo
		}
	}
	// not reached
	return nil
}

func (m *D) accumuChangesFinal() []*change {

	var todo []*change

	// suck up the rest of the changes to process
	for {
		select {
		case c := <-m.chch:
			todo = append(todo, c)
		case <-time.After(time.Second / 4):
			return todo
		}
	}
	// not reached
	return nil
}

// process one clump of notes
// add result back to list
// list should already be properly sorted
func (m *D) applyUpdates(todo []*change) []*change {

	if len(todo) == 0 {
		return nil
	}

	i := 0
	no := todo[i]
	i++
	level := no.level - 1 // move up
	if level < 0 {
		return todo[i:len(todo)]
	}

	mkey := merkleKey(level, no.treeID, no.ver)
	lockno := merkleLockNo(level, no.treeID, no.ver)
	aver := merkleLevelVersion(level, no.ver)
	dl.Debug("node %s", mkey)

	m.nlock[lockno].Lock()
	defer m.nlock[lockno].Unlock()

	ns := m.nodeGet(mkey)

	fixme := no.fixme

	ns, changed := m.updateNode(ns, no)

	// process anything else that lands in the same next-up node
	for ; i != len(todo); i++ {
		nx := todo[i]

		if nx.level != no.level ||
			nx.treeID != no.treeID ||
			merkleLevelVersion(level, nx.ver) != aver {
			break
		}

		dl.Debug("+node %s", mkey)

		var c bool
		ns, c = m.updateNode(ns, nx)

		changed = changed || c
		fixme = fixme || nx.fixme
	}

	if changed {
		ns = cleanNodes(ns)
		m.nodePut(mkey, ns)
	}

	if !changed && !fixme {
		return todo[i:]
	}

	c := m.aggrNodes(level, no.treeID, no.ver, fixme, ns)

	dl.Debug("=> %#v", c)

	return append(todo[i:], c)
}

// apply change to on disk node
func (m *D) updateNode(mn *NodeSaves, no *change) (*NodeSaves, bool) {

	slot := merkleSlot(no.level, no.ver)

	// search for slot
	found := false
	var i int

	for i = 0; i < len(mn.Save); i++ {
		if mn.Save[i].Slot == slot {
			found = true
			break
		}
	}

	dl.Debug("slot %d, i %d; %#v", slot, i, no)

	if found {
		// apply change (if keycount == 0, it will be removed by clean)
		mn.Save[i].Children = no.children
		mn.Save[i].KeyCount = no.keyCount
		mn.Save[i].Hash = no.hash

		dl.Debug("found %#v", mn)
		return mn, true
	}

	if no.keyCount != 0 {
		// add new node
		n := &NodeSave{
			Slot:     slot,
			KeyCount: no.keyCount,
			Children: no.children,
			Hash:     no.hash,
		}

		mn.Save = append(mn.Save, n)
		dl.Debug("add %#v", mn)
		return mn, true
	}

	// else (node to delete is not found): nop
	dl.Debug("nop %#v", mn)
	return mn, false
}

func (m *D) aggrNodes(level int, treeid uint16, ver uint64, fixme bool, no *NodeSaves) *change {

	c := &change{
		level:  level,
		treeID: treeid,
		fixme:  fixme,
		ver:    merkleLevelVersion(level, ver),
	}

	for _, n := range no.Save {
		c.children++
		c.keyCount += n.KeyCount
	}

	if c.children != 0 {
		c.hash = nodeHash(no)
	}

	if fixme {
		dl.Debug("fix %d/%x/%X -> %X", level, treeid, ver, c.hash)
	}

	return c
}

//################################################################

func cleanNodes(mn *NodeSaves) *NodeSaves {

	// mark empty slots, so they sort to the front
	ndel := 0
	for _, n := range mn.Save {
		if n.KeyCount == 0 {
			n.Slot = -1
			ndel++
		}
	}

	sort.Sort(mn)

	// slice the marked slots off
	mn.Save = mn.Save[ndel:]
	return mn
}

func (m *D) nodeGet(mkey string) *NodeSaves {

	val, found := m.db.MGet(mkey)
	if !found {
		return &NodeSaves{}
	}
	return bytes2node(val)
}
func (m *D) nodePut(mkey string, no *NodeSaves) {

	if no == nil || len(no.Save) == 0 {
		m.db.MDel(mkey)
		return
	}
	m.db.MPut(mkey, node2bytes(no))
}

func bytes2node(b []byte) *NodeSaves {
	d := &NodeSaves{}
	d.Unmarshal(b)
	return d
}
func node2bytes(d *NodeSaves) []byte {
	buf, err := d.Marshal()
	if err != nil {
		dl.Problem("corrupt data! %v", err)
	}
	return buf
}

func (l *NodeSaves) Len() int {
	return len(l.Save)
}
func (l *NodeSaves) Less(i int, j int) bool {

	if l.Save[i].Slot < l.Save[j].Slot {
		return true
	}
	return false
}
func (l *NodeSaves) Swap(i int, j int) {
	l.Save[i], l.Save[j] = l.Save[j], l.Save[i]
}
