/*
 * Copyright 2026 Rapid Loop, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package collector

import (
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/rapidloop/pgmetrics"
)

// errGroupDomainNotRun is returned for group domains that were skipped
// because the group aborted or reached its hold limit.
var errGroupDomainNotRun = errors.New("snapshot group domain not run")

// checkpoint captures the lengths of every model slice that a snapshot group
// may append to, so that an aborted group can roll partial results back.
type checkpoint struct {
	databases        int
	tablespaces      int
	roles            int
	replicationSlots int

	tables           int
	indexes          int
	sequences        int
	userFunctions    int
	extensions       int
	disabledTriggers int
	publications     int
	subscriptions    int
	citusDBs         []string
}

func (c *collector) checkpointModel() checkpoint {
	r := &c.result
	cp := checkpoint{
		databases:        len(r.Databases),
		tablespaces:      len(r.Tablespaces),
		roles:            len(r.Roles),
		replicationSlots: len(r.ReplicationSlots),
		tables:           len(r.Tables),
		indexes:          len(r.Indexes),
		sequences:        len(r.Sequences),
		userFunctions:    len(r.UserFunctions),
		extensions:       len(r.Extensions),
		disabledTriggers: len(r.DisabledTriggers),
		publications:     len(r.Publications),
		subscriptions:    len(r.Subscriptions),
	}
	if r.Citus != nil {
		cp.citusDBs = make([]string, 0, len(r.Citus))
		for k := range r.Citus {
			cp.citusDBs = append(cp.citusDBs, k)
		}
	}
	return cp
}

// rollback restores the model to the state captured by the checkpoint.
func (cp checkpoint) rollback(c *collector) {
	r := &c.result
	r.Databases = r.Databases[:cp.databases]
	r.Tablespaces = r.Tablespaces[:cp.tablespaces]
	r.Roles = r.Roles[:cp.roles]
	r.ReplicationSlots = r.ReplicationSlots[:cp.replicationSlots]
	r.Tables = r.Tables[:cp.tables]
	r.Indexes = r.Indexes[:cp.indexes]
	r.Sequences = r.Sequences[:cp.sequences]
	r.UserFunctions = r.UserFunctions[:cp.userFunctions]
	r.Extensions = r.Extensions[:cp.extensions]
	r.DisabledTriggers = r.DisabledTriggers[:cp.disabledTriggers]
	r.Publications = r.Publications[:cp.publications]
	r.Subscriptions = r.Subscriptions[:cp.subscriptions]
	if r.Citus != nil {
		keep := map[string]bool{}
		for _, k := range cp.citusDBs {
			keep[k] = true
		}
		for k := range r.Citus {
			if !keep[k] {
				delete(r.Citus, k)
			}
		}
	}
}

// groupRunner presents one snapshot consistency group to the collection
// functions: every domain runs in the same read-only REPEATABLE READ
// transaction and shares one snapshot id and one data time.
type groupRunner struct {
	c        *collector
	source   string
	tx       *sql.Tx
	prevQ    querier
	cp       checkpoint
	snapID   string
	dataTime time.Time
	start    time.Time
	hold     time.Duration

	open    bool // transaction is usable
	aborted bool // a statement failed: rolled back + checkpoint restored
	cutoff  bool // hold limit reached / canceled between domains: committed

	// recs are the timing records of domains this group registered; on
	// rollback exactly these are retracted (never keyed by snapshot id text)
	recs []*domainRec
}

// snapshotGroup opens a read-only REPEATABLE READ transaction, takes one
// statistics snapshot (clock_timestamp + pg_current_snapshot) and runs fn
// with a groupRunner. On normal completion the transaction is COMMITted and
// every domain shares a single snapshot; when MaxSnapshotHold is reached
// between domains (or the session is canceled), completed domains are
// committed and the rest are recorded as skipped; a statement error rolls
// the transaction and the model slices back.
func (c *collector) snapshotGroup(name string, fn func(g *groupRunner)) {
	if c.session == nil || c.session.Done() {
		return
	}

	hold := c.session.Config().MaxSnapshotHold
	// The transaction is bound to the session context only; the hold budget
	// is enforced as a clock gate between domains (see effCtx comment).
	tx, err := c.db.BeginTx(c.session.Ctx(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		log.Fatalf("failed to begin snapshot transaction: %v", err)
	}

	g := &groupRunner{
		c:      c,
		source: c.sourceName,
		tx:     tx,
		prevQ:  c.q,
		cp:     c.checkpointModel(),
		start:  c.session.Now(),
		hold:   hold,
		open:   true,
	}
	c.q = tx

	// Take the shared snapshot marker in a single round trip. Failures are
	// not fatal: domains simply get an empty snapshot id and the local clock
	// time, but they still run in one serializable-to-statement snapshot
	// because the REPEATABLE READ transaction itself holds one.
	var snapT time.Time
	var snapID sql.NullString
	if err := tx.QueryRowContext(c.session.Ctx(),
		`SELECT clock_timestamp(), COALESCE(pg_current_snapshot()::text, '')`).
		Scan(&snapT, &snapID); err == nil {
		g.dataTime = snapT
		if snapID.Valid {
			g.snapID = snapID.String
		}
	} else {
		g.dataTime = c.session.Now()
	}

	defer func() {
		c.q = g.prevQ
	}()

	fn(g)

	if !g.open {
		// already committed (cutoff) or rolled back (aborted)
		if g.cutoff {
			c.session.markHoldExceeded()
		}
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("warning: failed to commit snapshot group %q: %v", name, err)
		_ = tx.Rollback()
		g.cp.rollback(c)
		g.retractRecorded()
		c.session.MarkIncomplete()
	}
}

// holdReached reports whether the group has already consumed its hold budget.
func (g *groupRunner) holdReached() bool {
	return g.hold > 0 && g.c.session.Now().Sub(g.start) >= g.hold
}

// closeCommit commits the work done so far and releases the transaction.
func (g *groupRunner) closeCommit() {
	if !g.open {
		return
	}
	if err := g.tx.Commit(); err != nil {
		log.Printf("warning: failed to commit partial snapshot group: %v", err)
		_ = g.tx.Rollback()
		g.cp.rollback(g.c)
		g.retractRecorded()
		g.aborted = true
	}
	g.open = false
	g.cutoff = !g.aborted
	g.c.q = g.prevQ
}

// retractRecorded marks exactly this group's domains as errored after a
// rollback; other groups are never touched.
func (g *groupRunner) retractRecorded() {
	g.c.session.RetractDomains(g.recs)
}

// abort rolls the transaction and the model back after a statement error.
func (g *groupRunner) abort() {
	if !g.open {
		return
	}
	_ = g.tx.Rollback()
	g.cp.rollback(g.c)
	g.retractRecorded()
	g.open = false
	g.aborted = true
	g.c.q = g.prevQ
}

// domain runs one collection domain inside the group. After an abort, a hold
// cutoff or a cancellation, further domains are recorded as skipped without
// executing fn.
func (g *groupRunner) domain(name string, fn func() error) error {
	c := g.c

	if g.open && (g.holdReached() || c.session.Done()) {
		g.closeCommit()
	}

	if !g.open {
		c.session.SkipDomain(g.source, name, pgmetrics.ClassSnapshot, g.snapID)
		return errGroupDomainNotRun
	}

	err := c.session.ObserveSnapshot(g.source, name, g.snapID, g.dataTime, fn)
	if err == nil {
		if rec := c.session.DomainRec(g.source, name); rec != nil {
			g.recs = append(g.recs, rec)
		}
	}
	if err != nil {
		g.abort()
		// ensure integrity reflects the failure even if fn returned an error
		// instead of exiting the process
		c.session.SetSourceStatus(g.source, pgmetrics.StatusError)
		return err
	}
	return nil
}

// observed runs one live-view domain outside any transaction; its window is
// the server-clock observation interval derived from the source offset.
func (c *collector) observed(name string, fn func()) {
	if c.session == nil {
		fn()
		return
	}
	_ = c.session.Observe(c.sourceName, name, func() error {
		fn()
		return nil
	})
}
