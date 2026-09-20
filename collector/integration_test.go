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
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/rapidloop/pgmetrics"
)

// domains of the cluster_catalog snapshot group (the rest belong to the
// per-database db_<name> group).
var clusterOverheadNames = map[string]bool{
	"databases": true, "tablespaces": true,
	"replication_slots": true, "roles": true,
}

// These tests need Docker and are skipped unless PGMETRICS_INTEGRATION=1:
//
//	PGMETRICS_INTEGRATION=1 go test ./collector/ -run Integration -v
const integrationImage = "postgres:17-alpine"

type intEnv struct {
	container string
	port      int
	dsn       string
}

func skipIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("PGMETRICS_INTEGRATION") != "1" {
		t.Skip("set PGMETRICS_INTEGRATION=1 to run Docker integration tests")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found in PATH")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func startIntPostgres(t *testing.T) *intEnv {
	t.Helper()
	skipIntegration(t)

	port := freePort(t)
	container := fmt.Sprintf("pgmetrics-int-%d", time.Now().UnixNano())
	run := exec.Command("docker", "run", "-d", "--name", container,
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust",
		"-p", fmt.Sprintf("%d:5432", port),
		integrationImage)
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("docker run failed: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", container).Run()
	})

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ready := exec.Command("docker", "exec", container,
			"pg_isready", "-U", "postgres", "-h", "127.0.0.1")
		if ready.Run() == nil {
			goto up
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("postgres container did not become ready in 90s")

up:
	env := &intEnv{
		container: container,
		port:      port,
		dsn:       fmt.Sprintf("host=127.0.0.1 port=%d user=postgres", port),
	}

	// two user databases with baseline tables/indexes/stats
	setup := `
CREATE DATABASE intdb1;
CREATE DATABASE intdb2;
\c intdb1
CREATE TABLE t1 (id int primary key, note text);
CREATE INDEX idx_t1_note ON t1(note);
INSERT INTO t1 SELECT g, 'note-'||g FROM generate_series(1,100) g;
ANALYZE t1;
\c intdb2
CREATE TABLE t2 (id int primary key);
INSERT INTO t2 SELECT g FROM generate_series(1,50) g;
ANALYZE t2;
`
	if out := env.psql(t, "", setup); out != "" {
		t.Logf("setup output: %s", out)
	}
	return env
}

// psql runs SQL through psql inside the container and returns stdout.
func (e *intEnv) psql(t *testing.T, db, sql string) string {
	t.Helper()
	args := []string{"exec", "-i", e.container, "psql", "-U", "postgres",
		"-At", "-v", "ON_ERROR_STOP=1"}
	if db != "" {
		args = append(args, "-d", db)
	}
	cmd := exec.Command("docker", args...)
	cmd.Stdin = strings.NewReader(sql)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("psql failed: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func (e *intEnv) collectCfg() CollectConfig {
	o := DefaultCollectConfig()
	o.TimeoutSec = 10
	o.Host = "127.0.0.1"
	o.Port = uint16(e.port)
	o.User = "postgres"
	return o
}

func findDomains(m *pgmetrics.Model, source string) []pgmetrics.DomainTiming {
	var out []pgmetrics.DomainTiming
	if m.Collection == nil {
		return out
	}
	for _, d := range m.Collection.Domains {
		if d.Source == source {
			out = append(out, d)
		}
	}
	return out
}

// TR-8.1/AC-1/AC-2: catalog/statistics domains declared as one database
// snapshot share one snapshot id and one snapshot time while catalog objects
// and statistics change mid-flight; observed domains carry their own windows.
func TestIntegrationSnapshot(t *testing.T) {
	env := startIntPostgres(t)

	var blocked atomic.Bool
	var oldHooks Hooks
	oldHooks, testSessionHooks = testSessionHooks, Hooks{
		AfterDomain: func(source, name, class, status string) {
			// block once, inside the db_intdb1 snapshot group, right after
			// the "tables" domain: the tx stays open meanwhile
			if source == "intdb1" && name == "tables" && !blocked.Swap(true) {
				// TR-8.2: a REPEATABLE READ tx sits idle in transaction
				cnt := env.psql(t, "intdb1",
					`SELECT count(*) FROM pg_stat_activity
					 WHERE application_name='pgmetrics'
					   AND state='idle in transaction'
					   AND xact_start IS NOT NULL`)
				if n, _ := strconv.Atoi(cnt); n < 1 {
					t.Errorf("expected >=1 idle-in-transaction pgmetrics tx, got %s", cnt)
				}
				// mutate catalog objects AND statistics counts mid-snapshot
				env.psql(t, "intdb1", `
CREATE TABLE int_sneaky (id int);
CREATE INDEX idx_int_sneaky_id ON int_sneaky(id);
INSERT INTO int_sneaky SELECT g FROM generate_series(1,200) g;
ANALYZE int_sneaky;
`)
				time.Sleep(300 * time.Millisecond)
			}
		},
	}
	t.Cleanup(func() { testSessionHooks = oldHooks })

	o := env.collectCfg()
	m := CollectWithContext(context.Background(), o, []string{"intdb1", "intdb2"})
	if m.Collection == nil {
		t.Fatal("no integrity contract")
	}
	if !blocked.Load() {
		t.Fatal("block hook never fired")
	}

	// snapshot domains of intdb1 split into exactly the two designed groups.
	// NOTE: pg_current_snapshot() text can repeat across transactions on a
	// quiet server, so groups are identified by their designed domain set.
	clusterNames := map[string]bool{
		"databases": true, "tablespaces": true,
		"replication_slots": true, "roles": true,
	}
	groupByName := map[string][]pgmetrics.DomainTiming{
		"cluster_catalog": nil, "db_intdb1": nil,
	}
	for _, d := range findDomains(m, "intdb1") {
		if d.Class != pgmetrics.ClassSnapshot {
			continue
		}
		if d.Status != pgmetrics.StatusOK {
			t.Fatalf("snapshot domain %s status = %s", d.Name, d.Status)
		}
		if d.SnapshotID == "" {
			t.Fatalf("snapshot domain %s has empty snapshot id", d.Name)
		}
		if d.WindowStart != d.WindowEnd || d.WindowStart == 0 {
			t.Fatalf("snapshot domain %s window must be a single point: [%d,%d]",
				d.Name, d.WindowStart, d.WindowEnd)
		}
		g := "db_intdb1"
		if clusterNames[d.Name] {
			g = "cluster_catalog"
		}
		groupByName[g] = append(groupByName[g], d)
	}
	for g, ds := range groupByName {
		if len(ds) == 0 {
			t.Fatalf("snapshot group %s collected no domains", g)
		}
		id, w := ds[0].SnapshotID, ds[0].WindowStart
		for _, d := range ds[1:] {
			if d.SnapshotID != id {
				t.Fatalf("group %s mixes snapshot ids: %q vs %q", g, id, d.SnapshotID)
			}
			if d.WindowStart != w {
				t.Fatalf("group %s mixes snapshot times: %d vs %d", g, w, d.WindowStart)
			}
		}
	}

	// the table/index created mid-snapshot must be absent from BOTH catalog
	// and statistics views collected in the same snapshot group
	for _, tbl := range m.Tables {
		if tbl.DBName == "intdb1" && tbl.Name == "int_sneaky" {
			t.Fatal("snapshot leaked mid-flight table into Tables")
		}
	}
	for _, idx := range m.Indexes {
		if idx.DBName == "intdb1" && strings.Contains(idx.Name, "int_sneaky") {
			t.Fatal("snapshot leaked mid-flight index into Indexes")
		}
	}
	// sanity: the change really committed on the server
	if got := env.psql(t, "intdb1", "SELECT count(*) FROM int_sneaky"); got != "200" {
		t.Fatalf("mid-flight change did not commit: %s", got)
	}

	// observed domains: class + non-degenerate window
	nObserved := 0
	for _, d := range findDomains(m, "intdb1") {
		if d.Class == pgmetrics.ClassObserved && d.Status == pgmetrics.StatusOK {
			if d.WindowStart == 0 || d.WindowEnd < d.WindowStart {
				t.Fatalf("observed domain %s bad window [%d,%d]",
					d.Name, d.WindowStart, d.WindowEnd)
			}
			if d.SnapshotID != "" {
				t.Fatalf("observed domain %s must not carry a snapshot id", d.Name)
			}
			nObserved++
		}
	}
	if nObserved < 3 {
		t.Fatalf("want >=3 healthy observed domains, got %d", nObserved)
	}
}

// fakeCloud returns an awsCollector factory backed by the unit-test fakes,
// with one metric point at the given offset from now.
func fakeAWSFactory(t *testing.T, offset time.Duration) (*fakeCW, *fakeRDSInst, *fakeCWL, func()) {
	t.Helper()
	ts := time.Now().Add(-offset)
	val := 42.0
	fcw := &fakeCW{
		names: []string{"CPUUtilization"},
		results: []*cloudwatch.MetricDataResult{{
			Id:         aws.String("id0"),
			Timestamps: []*time.Time{&ts},
			Values:     []*float64{&val},
		}},
	}
	frds := &fakeRDSInst{}
	fcwl := &fakeCWL{}
	old := newAwsCollectorFn
	newAwsCollectorFn = func() (*awsCollector, error) {
		return newAWSCollectorWith(fcw, fcwl, frds), nil
	}
	return fcw, frds, fcwl, func() { newAwsCollectorFn = old }
}

func fakeAzureListerAt(offset time.Duration) *fakeAzureLister {
	fl := &fakeAzureLister{}
	region := "eastus"
	fl.resp.Resourceregion = &region
	fl.resp.Value = []*armmonitor.Metric{
		azMetric("cpu_percent", azPoint(time.Now().Add(-offset), nil, fptr(8), nil)),
	}
	return fl
}

// TR-8.1/AC-3/AC-4: multiple databases plus cloud APIs returning samples at
// different times produce per-source actual times, an overall window, a max
// skew and degraded status when the configured bound is exceeded.
func TestIntegrationMultiSource(t *testing.T) {
	env := startIntPostgres(t)

	// AWS -20s, Azure -80s
	fcw, _, _, restoreAWS := fakeAWSFactory(t, 20*time.Second)
	defer restoreAWS()
	fl := fakeAzureListerAt(80 * time.Second)
	restoreAzure := withAzureFakes(t, fl)
	defer restoreAzure()

	cfg := env.collectCfg()
	cfg.Omit = []string{"log"}
	cfg.MaxSkewSec = 30 // 80s azure sample exceeds this
	cfg.MaxStaleSec = 300
	cfg.RDSDBIdentifier = "db-fake"
	cfg.AzureResourceID = testAzureResourceID

	m := CollectWithContext(context.Background(), cfg, []string{"intdb1", "intdb2"})
	ci := m.Collection
	if ci == nil {
		t.Fatal("no integrity contract")
	}
	if ci.Status != pgmetrics.StatusDegraded {
		t.Fatalf("status=%s, want degraded", ci.Status)
	}
	if !ci.Complete {
		t.Fatal("a skew-driven degraded run still completed all domains")
	}
	foundOOB := false
	for _, oob := range ci.OutOfBounds {
		if oob == "azure/metrics" {
			foundOOB = true
		}
	}
	if !foundOOB {
		t.Fatalf("azure/metrics missing from out_of_bounds: %v", ci.OutOfBounds)
	}
	if ci.WindowStart == 0 || ci.WindowEnd <= ci.WindowStart {
		t.Fatalf("bad overall window [%d,%d]", ci.WindowStart, ci.WindowEnd)
	}
	wantMinSkew := int64((70 * time.Second).Microseconds())
	if ci.MaxSkew < wantMinSkew {
		t.Fatalf("max skew %d < %d", ci.MaxSkew, wantMinSkew)
	}
	// provider timestamps retained per metric
	// every PostgreSQL source carries its own actual data range and a
	// measured request duration (not just cloud sources)
	for _, name := range []string{"intdb1", "intdb2"} {
		var src *pgmetrics.SourceTiming
		for i := range ci.Sources {
			if ci.Sources[i].Name == name {
				src = &ci.Sources[i]
			}
		}
		if src == nil {
			t.Fatalf("source %s missing", name)
		}
		if src.DataStart == 0 || src.DataEnd < src.DataStart {
			t.Fatalf("source %s bad data range: [%d,%d]", name, src.DataStart, src.DataEnd)
		}
		if src.RequestDuration == 0 {
			t.Fatalf("source %s request duration not measured", name)
		}
	}
	if m.RDS == nil || m.RDS.BasicTimes["CPUUtilization"] == 0 {
		t.Fatal("RDS per-metric provider timestamp missing")
	}
	if fcw.calls == 0 || fl.calls == 0 {
		t.Fatal("cloud fakes were not called")
	}
	if m.Azure == nil || m.Azure.MetricTimes["cpu_percent"] == 0 {
		t.Fatal("Azure per-metric provider timestamp missing")
	}
	// the overall window must reach the -80s Azure sample (and no further
	// than ~-100s); compare against the run anchor, not time.Now() (the run
	// itself takes time)
	anchor := time.UnixMicro(ci.AnchorTime)
	if d := anchor.UnixMicro() - ci.WindowStart; d < (70*time.Second).Microseconds() ||
		d > (100*time.Second).Microseconds() {
		t.Fatalf("window start is %s before anchor, want ~80s",
			time.Duration(d)*time.Microsecond)
	}
	if d := anchor.UnixMicro() - ci.WindowEnd; d > (30 * time.Second).Microseconds() {
		t.Fatalf("window end is %s before anchor, want the ~-20s AWS sample",
			time.Duration(d)*time.Microsecond)
	}

	// loosen the bound: same timestamps now yield an ok run
	cfg2 := cfg
	cfg2.MaxSkewSec = 600
	cfg2.MaxStaleSec = 600
	m2 := CollectWithContext(context.Background(), cfg2, []string{"intdb1", "intdb2"})
	if m2.Collection.Status != pgmetrics.StatusOK || !m2.Collection.Complete {
		t.Fatalf("loose bounds: status=%s complete=%v", m2.Collection.Status, m2.Collection.Complete)
	}
}

// TR-8.1/AC-5: cancellation after the first source stops all later database
// and cloud requests; finished domains keep windows; contract is incomplete.
func TestIntegrationCancel(t *testing.T) {
	env := startIntPostgres(t)

	fcw, frds, fcwl, restoreAWS := fakeAWSFactory(t, 20*time.Second)
	defer restoreAWS()
	fl := fakeAzureListerAt(20 * time.Second)
	restoreAzure := withAzureFakes(t, fl)
	defer restoreAzure()

	ctx, cancel := context.WithCancel(context.Background())
	var fired atomic.Bool
	var oldHooks Hooks
	oldHooks, testSessionHooks = testSessionHooks, Hooks{
		AfterDomain: func(source, name, class, status string) {
			// last domain of the cluster snapshot of the first database
			if source == "intdb1" && name == "roles" && !fired.Swap(true) {
				cancel()
				time.Sleep(200 * time.Millisecond) // let gate take effect
			}
		},
	}
	t.Cleanup(func() { testSessionHooks = oldHooks })

	cfg := env.collectCfg()
	cfg.Omit = []string{"log"}
	cfg.RDSDBIdentifier = "db-fake"
	cfg.AzureResourceID = testAzureResourceID
	m := CollectWithContext(ctx, cfg, []string{"intdb1", "intdb2"})
	ci := m.Collection
	if ci.Complete || ci.Status != pgmetrics.StatusIncomplete {
		t.Fatalf("complete=%v status=%s, want incomplete", ci.Complete, ci.Status)
	}
	if ci.CancelReason == "" {
		t.Fatal("cancel reason must be recorded")
	}

	have := map[string]bool{}
	for _, s := range ci.Sources {
		have[s.Name] = true
	}
	if !have["intdb1"] {
		t.Fatal("first source should be present")
	}
	for _, later := range []string{"intdb2", "aws-rds", "azure"} {
		if have[later] {
			t.Fatalf("source %s must not start after cancellation", later)
		}
	}
	if fcw.calls+frds.calls+fcwl.calls+fl.calls != 0 {
		t.Fatalf("cloud SDK calls after cancel: aws=%d azure=%d",
			fcw.calls+frds.calls+fcwl.calls, fl.calls)
	}
	// finished domains keep accurate windows
	sawFinished := false
	for _, d := range findDomains(m, "intdb1") {
		if d.Name == "databases" && d.Status == pgmetrics.StatusOK &&
			d.WindowStart != 0 && d.RequestEnd != 0 {
			sawFinished = true
		}
	}
	if !sawFinished {
		t.Fatal("completed domain lost its window after cancellation")
	}
}

// TR-8.1/AC-6: overhead is bounded: exactly one snapshot transaction group
// per designed group, server-side commit increment stays bounded by the
// observed-domain count, and no long transaction leaks past the run.
func TestIntegrationOverhead(t *testing.T) {
	env := startIntPostgres(t)

	before := env.psql(t, "intdb1",
		`SELECT xact_commit FROM pg_stat_database WHERE datname='intdb1'`)
	beforeN, _ := strconv.ParseInt(before, 10, 64)

	cfg := env.collectCfg()
	m := CollectWithContext(context.Background(), cfg, []string{"intdb1", "intdb2"})
	ci := m.Collection
	if ci == nil || !ci.Complete || ci.Status != pgmetrics.StatusOK {
		t.Fatalf("collection should complete ok: %+v", ci)
	}

	after := env.psql(t, "intdb1",
		`SELECT xact_commit FROM pg_stat_database WHERE datname='intdb1'`)
	afterN, _ := strconv.ParseInt(after, 10, 64)

	nSnapshot, nObserved := 0, 0
	hitGroup := map[string]bool{"cluster_catalog": false, "db_intdb1": false}
	for _, d := range findDomains(m, "intdb1") {
		switch d.Class {
		case pgmetrics.ClassSnapshot:
			nSnapshot++
			if clusterOverheadNames[d.Name] {
				hitGroup["cluster_catalog"] = true
			} else {
				hitGroup["db_intdb1"] = true
			}
		case pgmetrics.ClassObserved:
			nObserved++
		}
	}
	for g, hit := range hitGroup {
		if !hit {
			t.Fatalf("snapshot group %s collected no domains", g)
		}
	}
	if nSnapshot < 10 {
		t.Fatalf("too few snapshot domains: %d", nSnapshot)
	}

	// weak upper bound: each observed domain runs a handful of autocommit
	// statements; each of the two snapshot groups adds one commit; psql
	// probes add slack.
	bound := int64(3*nObserved + 2 + 10)
	delta := afterN - beforeN
	if delta <= 0 {
		t.Fatalf("xact_commit delta = %d", delta)
	}
	if delta > bound {
		t.Fatalf("xact_commit grew by %d, bound %d (snapshot=%d observed=%d)",
			delta, bound, nSnapshot, nObserved)
	}

	// no leftover open transaction from pgmetrics
	left := env.psql(t, "intdb1",
		`SELECT count(*) FROM pg_stat_activity
		 WHERE application_name='pgmetrics' AND state LIKE 'idle in transaction%'`)
	if left != "0" {
		t.Fatalf("pgmetrics left %s open transactions behind", left)
	}
}
