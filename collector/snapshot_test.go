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
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rapidloop/pgmetrics"
)

// domainQuery runs one dummy query through the active querier, exactly like a
// real collection domain would.
func domainQuery(c *collector, label string) func() error {
	return func() error {
		ctx, cancel := context.WithTimeout(c.effCtx(), 5*time.Second)
		defer cancel()
		rows, err := c.q.QueryContext(ctx, "SELECT 1 AS "+label)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
		}
		return rows.Err()
	}
}

// TR-4.1/TR-4.2: one connection, two REPEATABLE READ READ ONLY transactions,
// all domain queries run on the transaction, each transaction adds only BEGIN
// / snapshot SELECT / COMMIT beyond the domain statements.
func TestSnapshotGroupsShape(t *testing.T) {
	c, sess, cancel := newFakeCollector(t, 5*time.Second)
	defer cancel()

	c.snapshotGroup("cluster_catalog", func(g *groupRunner) {
		if err := g.domain("databases", domainQuery(c, "databases")); err != nil {
			t.Fatal(err)
		}
		if err := g.domain("roles", domainQuery(c, "roles")); err != nil {
			t.Fatal(err)
		}
	})
	c.snapshotGroup("db_fakedb", func(g *groupRunner) {
		if err := g.domain("tables", domainQuery(c, "tables")); err != nil {
			t.Fatal(err)
		}
	})

	out := sess.Finalize()

	sharedFake.mu.Lock()
	defer sharedFake.mu.Unlock()

	if sharedFake.connects != 1 {
		t.Fatalf("connects = %d, want 1", sharedFake.connects)
	}
	var begins, commits, rollbacks, snapSelects, domainQueries int
	for _, e := range sharedFake.events {
		switch e.kind {
		case "begin":
			begins++
			if !strings.Contains(e.stmt, "ISOLATION LEVEL REPEATABLE READ") ||
				!strings.Contains(e.stmt, "READ ONLY") {
				t.Fatalf("BEGIN clause wrong: %q", e.stmt)
			}
			if e.onTx {
				t.Fatal("BEGIN must run on the pool, not a tx")
			}
		case "commit":
			commits++
		case "rollback":
			rollbacks++
		case "query":
			if strings.Contains(strings.ToLower(e.stmt), "pg_current_snapshot") {
				snapSelects++
				if !e.onTx {
					t.Fatal("snapshot SELECT must run on the tx")
				}
			} else {
				domainQueries++
				if !e.onTx {
					t.Fatalf("domain query ran outside its tx: %q", e.stmt)
				}
			}
		}
	}
	if begins != 2 || commits != 2 || rollbacks != 0 {
		t.Fatalf("begins=%d commits=%d rollbacks=%d, want 2/2/0", begins, commits, rollbacks)
	}
	if snapSelects != 2 {
		t.Fatalf("snapshot selects = %d, want 2", snapSelects)
	}
	if domainQueries != 3 {
		t.Fatalf("domain queries = %d, want exactly 3", domainQueries)
	}
	if sharedFake.maxTxOpen > 1 {
		t.Fatalf("concurrent open tx = %d, want <= 1", sharedFake.maxTxOpen)
	}

	// domains share the snapshot id within each group and carry the snapshot
	// time as their window
	if out.Status != pgmetrics.StatusOK {
		t.Fatalf("status = %q, want ok", out.Status)
	}
	ids := map[string]int{}
	for _, d := range out.Domains {
		if d.Class != pgmetrics.ClassSnapshot {
			t.Fatalf("domain %s class = %s", d.Name, d.Class)
		}
		if d.SnapshotID != "0:2600:10," {
			t.Fatalf("domain %s snap id = %q", d.Name, d.SnapshotID)
		}
		if d.WindowStart == 0 || d.WindowStart != d.WindowEnd || d.WindowStart != d.DataTime {
			t.Fatalf("domain %s window must equal snapshot time: %+v", d.Name, d)
		}
		ids[d.SnapshotID]++
	}
	if len(out.Domains) != 3 {
		t.Fatalf("domains = %d, want 3", len(out.Domains))
	}
	if ids["0:2600:10,"] != 3 {
		t.Fatalf("snapshot groups must share one id per run: %v", ids)
	}
}

// TR-4.3: when MaxSnapshotHold elapses between domains, the transaction is
// committed promptly, remaining domains are skipped and the contract is
// incomplete.
func TestSnapshotHoldCutoff(t *testing.T) {
	const hold = 80 * time.Millisecond
	c, sess, cancel := newFakeCollector(t, hold)
	defer cancel()
	sess.withHooks(Hooks{
		AfterDomain: func(source, name, class, status string) {
			if name == "slow" {
				time.Sleep(hold + 40*time.Millisecond)
			}
		},
	})

	var beginAt, commitAt time.Time
	c.snapshotGroup("g", func(g *groupRunner) {
		g.domain("slow", func() error {
			sharedFake.mu.Lock()
			beginAt = sharedFake.events[len(sharedFake.events)-1].at
			sharedFake.mu.Unlock()
			return domainQuery(c, "slow")()
		})
		g.domain("never", domainQuery(c, "never"))
		g.domain("never2", domainQuery(c, "never2"))
	})

	out := sess.Finalize()

	sharedFake.mu.Lock()
	for _, e := range sharedFake.events {
		if e.kind == "commit" {
			commitAt = e.at
		}
		if e.kind == "query" && strings.Contains(e.stmt, "never") {
			t.Fatalf("post-cutoff domain query was executed: %q", e.stmt)
		}
	}
	queryCount := 0
	for _, e := range sharedFake.events {
		if e.kind == "query" && !strings.Contains(strings.ToLower(e.stmt), "pg_current_snapshot") {
			queryCount++
		}
	}
	sharedFake.mu.Unlock()

	if queryCount != 1 {
		t.Fatalf("domain queries = %d, want 1 (skipped domains must not query)", queryCount)
	}
	if elapsed := commitAt.Sub(beginAt); elapsed > hold+100*time.Millisecond {
		t.Fatalf("tx held for %v, want <= %v", elapsed, hold+100*time.Millisecond)
	}
	if out.Complete || out.Status != pgmetrics.StatusIncomplete {
		t.Fatalf("complete=%v status=%q, want false/incomplete", out.Complete, out.Status)
	}
	var ok, skipped int
	for _, d := range out.Domains {
		switch d.Status {
		case pgmetrics.StatusOK:
			ok++
		case pgmetrics.StatusSkipped:
			skipped++
		}
	}
	if ok != 1 || skipped != 2 {
		t.Fatalf("domain statuses ok=%d skipped=%d, want 1/2", ok, skipped)
	}
}

// TR-4.4: a statement error rolls the transaction and the model slices back
// to their checkpoint; later domains are not executed.
func TestSnapshotRollbackCheckpoint(t *testing.T) {
	c, sess, cancel := newFakeCollector(t, 5*time.Second)
	defer cancel()

	c.result.Roles = append(c.result.Roles, pgmetrics.Role{Name: "preexisting"})

	c.snapshotGroup("g", func(g *groupRunner) {
		g.domain("good", func() error {
			c.result.Roles = append(c.result.Roles, pgmetrics.Role{Name: "new"})
			return domainQuery(c, "good")()
		})
		g.domain("bad", func() error {
			c.result.Roles = append(c.result.Roles, pgmetrics.Role{Name: "partial"})
			return errBoom
		})
		g.domain("after", domainQuery(c, "after"))
	})

	out := sess.Finalize()

	if len(c.result.Roles) != 1 || c.result.Roles[0].Name != "preexisting" {
		t.Fatalf("checkpoint rollback failed: %+v", c.result.Roles)
	}
	sharedFake.mu.Lock()
	var rollbacks int
	afterRan := false
	for _, e := range sharedFake.events {
		if e.kind == "rollback" {
			rollbacks++
		}
		if strings.Contains(e.stmt, "after") {
			afterRan = true
		}
	}
	sharedFake.mu.Unlock()
	if rollbacks != 1 {
		t.Fatalf("rollbacks = %d, want 1", rollbacks)
	}
	if afterRan {
		t.Fatal("domain after abort must not run")
	}
	if out.Status != pgmetrics.StatusIncomplete {
		t.Fatalf("status = %q, want incomplete", out.Status)
	}
	// rolled-back snapshot data must not be presented as collected: the
	// earlier "good" domain is retracted, none of the group stays ok
	for _, d := range out.Domains {
		if d.Class == pgmetrics.ClassSnapshot && d.Status == pgmetrics.StatusOK {
			t.Fatalf("domain %s stayed ok after rollback", d.Name)
		}
	}
}

// TR-4.4: rolling back one group must never retract domains of another group
// that already committed, even when the groups share a snapshot id text (the
// fake driver returns the same id for every group; PostgreSQL < 13 yields an
// empty id).
func TestSnapshotRetractIsGroupScoped(t *testing.T) {
	c, sess, cancel := newFakeCollector(t, 5*time.Second)
	defer cancel()

	c.snapshotGroup("a", func(g *groupRunner) {
		g.domain("a1", domainQuery(c, "a1"))
	})
	c.snapshotGroup("b", func(g *groupRunner) {
		g.domain("b1", domainQuery(c, "b1"))
		g.domain("b2", func() error { return errBoom })
	})

	out := sess.Finalize()
	st := map[string]string{}
	for _, d := range out.Domains {
		st[d.Name] = d.Status
	}
	if st["a1"] != pgmetrics.StatusOK {
		t.Fatalf("committed group domain a1 = %q, want ok (cross-group retraction)", st["a1"])
	}
	if st["b1"] != pgmetrics.StatusError {
		t.Fatalf("rolled-back b1 = %q, want error", st["b1"])
	}
	if st["b2"] != pgmetrics.StatusError {
		t.Fatalf("failed b2 = %q, want error", st["b2"])
	}
	// retracted domains carry no window, so their data cannot enter the
	// overall window via group b
	for _, d := range out.Domains {
		if d.Name == "b1" && d.WindowStart != 0 {
			t.Fatalf("retracted b1 kept window %d", d.WindowStart)
		}
	}
}

// TR-3.3: a canceled session starts no connection to the next database.
func TestCancelGatesNewConnection(t *testing.T) {
	c, sess, cancel := newFakeCollector(t, 5*time.Second)
	cancel()

	c.collectFromDB("dbname=second", DefaultCollectConfig(), "second")

	_ = sess.Finalize()
	sharedFake.mu.Lock()
	defer sharedFake.mu.Unlock()
	if sharedFake.connects != 0 {
		t.Fatalf("connects after cancel = %d, want 0", sharedFake.connects)
	}
}

var errBoom = context.DeadlineExceeded
