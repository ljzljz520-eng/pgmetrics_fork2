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
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

// fakeEvent records one statement seen by the fake driver.
type fakeEvent struct {
	stmt   string
	onTx   bool // executed on a transaction rather than the pool
	at     time.Time
	kind   string // "begin", "commit", "rollback", "query", "exec"
	fail   bool   // force an error
}

// fakeDriver is a minimal database/sql/driver used by the snapshot tests.
// Every query returns one row of NULLs (NULL scans into the zero value of
// every Scan destination), so domain functions that merely run a query work
// without real catalogs.
type fakeDriver struct {
	mu        sync.Mutex
	connects  int
	events    []fakeEvent
	txOpen    int
	maxTxOpen int
	// failQuery, when set, makes queries containing this substring fail.
	failQuery string
}

func (d *fakeDriver) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.connects = 0
	d.events = nil
	d.txOpen = 0
	d.maxTxOpen = 0
	d.failQuery = ""
}

func (d *fakeDriver) Open(name string) (driver.Conn, error) {
	d.mu.Lock()
	d.connects++
	d.mu.Unlock()
	return &fakeConn{d: d}, nil
}

type fakeConn struct {
	d  *fakeDriver
	tx *fakeTx
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{c: c, query: query}, nil
}
func (c *fakeConn) Close() error { return nil }

// Begin is required by driver.Conn but never used because ConnBeginTx is
// implemented.
func (c *fakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	stmt := "BEGIN"
	if opts.ReadOnly {
		stmt += " READ ONLY"
	}
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelRepeatableRead:
		stmt += " ISOLATION LEVEL REPEATABLE READ"
	case sql.LevelSerializable:
		stmt += " ISOLATION LEVEL SERIALIZABLE"
	}
	if err := c.record(stmt, "begin", false); err != nil {
		return nil, err
	}
	tx := &fakeTx{c: c}
	c.tx = tx
	c.d.mu.Lock()
	c.d.txOpen++
	if c.d.txOpen > c.d.maxTxOpen {
		c.d.maxTxOpen = c.d.txOpen
	}
	c.d.mu.Unlock()
	return tx, nil
}

func (c *fakeConn) record(stmt, kind string, onTx bool) error {
	c.d.mu.Lock()
	defer c.d.mu.Unlock()
	if c.d.failQuery != "" && strings.Contains(stmt, c.d.failQuery) {
		return errors.New("fakeDriver forced error: " + c.d.failQuery)
	}
	c.d.events = append(c.d.events, fakeEvent{
		stmt: stmt, at: time.Now(), kind: kind, onTx: onTx,
	})
	return nil
}

func (c *fakeConn) Ping(ctx context.Context) error { return nil }

func (c *fakeConn) ExecContext(ctx context.Context, query string,
	args []driver.NamedValue) (driver.Result, error) {
	onTx := c.tx != nil
	if err := c.record(query, "exec", onTx); err != nil {
		return nil, err
	}
	return fakeResult{}, nil
}

func (c *fakeConn) QueryContext(ctx context.Context, query string,
	args []driver.NamedValue) (driver.Rows, error) {
	onTx := c.tx != nil
	if err := c.record(query, "query", onTx); err != nil {
		return nil, err
	}
	r := &fakeRows{cols: countColumns(query)}
	// provide real values for the snapshot marker query
	lq := strings.ToLower(query)
	if strings.Contains(lq, "clock_timestamp") && strings.Contains(lq, "pg_current_snapshot") {
		r.values = []driver.Value{
			time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
			[]byte("0:2600:10,"),
		}
	}
	return r, nil
}

type fakeResult struct{}

func (fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (fakeResult) RowsAffected() (int64, error) { return 0, nil }

type fakeStmt struct {
	c     *fakeConn
	query string
}

func (s *fakeStmt) Close() error                                  { return nil }
func (s *fakeStmt) NumInput() int                                 { return -1 }
func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	return fakeResult{}, nil
}
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &fakeRows{cols: countColumns(s.query)}, nil
}

type fakeTx struct{ c *fakeConn }

func (t *fakeTx) Commit() error {
	if err := t.c.record("COMMIT", "commit", true); err != nil {
		return err
	}
	t.c.tx = nil
	t.c.d.mu.Lock()
	t.c.d.txOpen--
	t.c.d.mu.Unlock()
	return nil
}

func (t *fakeTx) Rollback() error {
	_ = t.c.record("ROLLBACK", "rollback", true)
	t.c.tx = nil
	t.c.d.mu.Lock()
	t.c.d.txOpen--
	t.c.d.mu.Unlock()
	return nil
}

type fakeRows struct {
	cols   []string
	values []driver.Value
	done   bool
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }

// Next returns exactly one row of NULLs, then EOF.
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	for i := range dest {
		if r.values != nil && i < len(r.values) {
			dest[i] = r.values[i]
		} else {
			dest[i] = nil
		}
	}
	return nil
}

// countColumns guesses the number of output columns of a SELECT by counting
// top-level commas between SELECT and the first top-level FROM. It only has to
// be right for the queries used by the snapshot tests.
func countColumns(q string) []string {
	s := strings.ToUpper(q)
	si := strings.Index(s, "SELECT")
	if si < 0 {
		return []string{"v"}
	}
	depth := 0
	fi := -1
	for i := si + 6; i < len(q); i++ {
		switch q[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && i+4 <= len(q) && strings.EqualFold(q[i:i+4], "FROM") {
				// word boundary
				if (i == si+6 || q[i-1] == ' ' || q[i-1] == '\n' || q[i-1] == '\t') &&
					(i+4 == len(q) || q[i+4] == ' ' || q[i+4] == '\n' || q[i+4] == '\t' || q[i+4] == '(') {
					fi = i
				}
			}
		}
		if fi >= 0 {
			break
		}
	}
	list := strings.TrimSpace(q[si+6:])
	if fi >= 0 {
		list = strings.TrimSpace(q[si+6 : fi])
	}
	depth = 0
	n := 1
	for i := 0; i < len(list); i++ {
		switch list[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				n++
			}
		}
	}
	cols := make([]string, n)
	for i := range cols {
		cols[i] = "v"
	}
	return cols
}

const fakeDriverName = "pgmetrics-fake"

var (
	fakeDriverOnce sync.Once
	sharedFake     = &fakeDriver{}
)

func registerFake() {
	fakeDriverOnce.Do(func() {
		sql.Register(fakeDriverName, sharedFake)
	})
}

// newFakeCollector builds a collector wired to the fake driver with a fresh
// session of the given hold budget.
func newFakeCollector(t testingT, hold time.Duration) (*collector, *CollectionSession, context.CancelFunc) {
	registerFake()
	sharedFake.reset()
	db, err := sql.Open(fakeDriverName, "")
	if err != nil {
		t.Fatalf("open fake: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	sess := newSession(ctx, SessionConfig{
		MaxSkew:         0, // disable skew checks for mechanics tests
		MaxStale:        0,
		MaxSnapshotHold: hold,
	}, nil)
	c := &collector{
		db:         db,
		q:          db,
		session:    sess,
		sourceName: "fakedb",
		timeout:    10 * time.Second,
	}
	return c, sess, cancel
}

// testingT is the subset of *testing.T used by the helper.
type testingT interface {
	Helper()
	Fatalf(format string, args ...interface{})
	Cleanup(f func())
}
