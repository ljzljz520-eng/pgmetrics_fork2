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
	"testing"
	"time"

	"github.com/rapidloop/pgmetrics"
)

// fakeClock starts at a fixed wall time and only advances when advanced.
type fakeClock struct {
	t time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time { return c.t }
func (c *fakeClock) Advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

func testCfg() SessionConfig {
	return SessionConfig{
		MaxSkew:         300 * time.Second,
		MaxStale:        300 * time.Second,
		MaxSnapshotHold: 10 * time.Second,
	}
}

// TR-2.1: anchor comes from the injected clock; request intervals and
// observed windows are derived from clock readings.
func TestSessionAnchorAndObservedWindow(t *testing.T) {
	clk := newFakeClock()
	s := newSession(context.Background(), testCfg(), clk.Now)

	if !s.Anchor().Equal(clk.t) {
		t.Fatalf("anchor = %v, want %v", s.Anchor(), clk.t)
	}

	if !s.BeginSource("db1", "postgres") {
		t.Fatal("BeginSource should succeed")
	}
	// server clock is 2 seconds ahead of the local clock
	s.SetSourceOffset("db1", 2*time.Second)

	if err := s.Observe("db1", "activity", func() error {
		clk.Advance(50 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(100 * time.Millisecond)
	s.EndSource("db1", "")

	out := s.Finalize()
	if out.Status != pgmetrics.StatusOK {
		t.Fatalf("status = %q, want ok", out.Status)
	}
	if out.AnchorTime != clk.t.Add(-150*time.Millisecond).UnixMicro() {
		t.Fatalf("anchor time mismatch: %d", out.AnchorTime)
	}
	var dom *pgmetrics.DomainTiming
	for i := range out.Domains {
		if out.Domains[i].Name == "activity" {
			dom = &out.Domains[i]
		}
	}
	if dom == nil {
		t.Fatal("activity domain missing")
	}
	// observed window must be local interval shifted by the 2s offset
	wantLo := clk.t.Add(-150*time.Millisecond + 2*time.Second).UnixMicro()
	wantHi := clk.t.Add(-100*time.Millisecond + 2*time.Second).UnixMicro()
	if dom.WindowStart != wantLo || dom.WindowEnd != wantHi {
		t.Fatalf("window = [%d,%d], want [%d,%d]", dom.WindowStart, dom.WindowEnd, wantLo, wantHi)
	}
	if dom.SnapshotID != "" {
		t.Fatalf("observed domain must not carry snapshot id: %q", dom.SnapshotID)
	}
	if dom.RequestDuration < 49000 || dom.RequestDuration > 51000 {
		t.Fatalf("duration = %d us, want ~50000", dom.RequestDuration)
	}
	// the source range must be aggregated from its domains and the request
	// duration measured with the monotonic clock
	if len(out.Sources) != 1 {
		t.Fatalf("sources = %d, want 1", len(out.Sources))
	}
	src := out.Sources[0]
	if src.DataStart != wantLo || src.DataEnd != wantHi {
		t.Fatalf("source data range = [%d,%d], want [%d,%d]",
			src.DataStart, src.DataEnd, wantLo, wantHi)
	}
	if src.RequestDuration < 49000 || src.RequestDuration > 160000 {
		t.Fatalf("source request duration = %d us, want ~50-150ms", src.RequestDuration)
	}

	// Finalize is idempotent
	if again := s.Finalize(); again != out {
		t.Fatal("repeated Finalize must return the same contract")
	}
}

// TR-2.2: snapshot domains share snapshot id and data time.
func TestSessionSnapshotGroup(t *testing.T) {
	clk := newFakeClock()
	s := newSession(context.Background(), testCfg(), clk.Now)
	s.BeginSource("db1", "postgres")
	snapT := clk.Now().Add(10 * time.Millisecond)
	for _, name := range []string{"databases", "tables", "indexes"} {
		n := name
		if err := s.ObserveSnapshot("db1", n, "0/1234567:10", snapT, func() error {
			clk.Advance(time.Millisecond)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	s.EndSource("db1", "")
	out := s.Finalize()
	for _, d := range out.Domains {
		if d.Class != pgmetrics.ClassSnapshot || d.SnapshotID != "0/1234567:10" {
			t.Fatalf("domain %s not in group: %+v", d.Name, d)
		}
		if d.WindowStart != snapT.UnixMicro() || d.WindowEnd != snapT.UnixMicro() {
			t.Fatalf("domain %s window must equal snapshot time", d.Name)
		}
		if d.Status != pgmetrics.StatusOK {
			t.Fatalf("domain %s status %s", d.Name, d.Status)
		}
	}
}

// TR-2.3: skew across sources is computed and out-of-bounds sources are
// flagged; status becomes degraded.
func TestSessionSkewAndDegraded(t *testing.T) {
	clk := newFakeClock()
	s := newSession(context.Background(), testCfg(), clk.Now)
	anchor := s.Anchor()

	s.BeginSource("db1", "postgres")
	s.SetSourceOffset("db1", 0)
	s.Observe("db1", "activity", func() error { return nil })
	s.EndSource("db1", "")

	// cloud sample 600 seconds from anchor (> 300s max skew)
	s.BeginSource("aws-rds", "aws")
	sampleT := anchor.Add(600 * time.Second)
	s.SetSourceDataRange("aws-rds", sampleT, sampleT)
	s.RecordDomain("aws-rds", "metrics", pgmetrics.ClassObserved, pgmetrics.StatusOK,
		sampleT, sampleT, sampleT)
	s.EndSource("aws-rds", "")

	out := s.Finalize()
	if out.Status != pgmetrics.StatusDegraded {
		t.Fatalf("status = %q, want degraded", out.Status)
	}
	if out.MaxSkew != int64(600*time.Second/time.Microsecond) {
		t.Fatalf("max skew = %d us, want 600s", out.MaxSkew)
	}
	found := false
	sourceFound := false
	for _, l := range out.OutOfBounds {
		if l == "aws-rds/metrics" {
			found = true
		}
		if l == "aws-rds (source)" {
			sourceFound = true
		}
	}
	if !found {
		t.Fatalf("out_of_bounds = %v, want aws-rds/metrics", out.OutOfBounds)
	}
	if !sourceFound {
		t.Fatalf("out_of_bounds = %v, want source-level label", out.OutOfBounds)
	}
	if out.WindowEnd-out.WindowStart != 600_000_000 {
		t.Fatalf("window span = %d us, want 600s", out.WindowEnd-out.WindowStart)
	}
}

// TR-2.4: stale and unavailable cloud statuses degrade without pretending
// current data.
func TestSessionStaleAndUnavailable(t *testing.T) {
	clk := newFakeClock()
	s := newSession(context.Background(), testCfg(), clk.Now)
	anchor := s.Anchor()

	s.BeginSource("aws-rds", "aws")
	old := anchor.Add(-600 * time.Second)
	s.SetSourceDataRange("aws-rds", old, old)
	s.RecordDomain("aws-rds", "metrics", pgmetrics.ClassObserved, pgmetrics.StatusStale,
		old, old, old)
	s.SetSourceStatus("aws-rds", pgmetrics.StatusStale)
	s.EndSource("aws-rds", "")

	s.BeginSource("azure", "azure")
	s.EndSource("azure", pgmetrics.StatusUnavailable)

	out := s.Finalize()
	if out.Status != pgmetrics.StatusDegraded {
		t.Fatalf("status = %q, want degraded", out.Status)
	}
	for _, src := range out.Sources {
		switch src.Name {
		case "aws-rds":
			if src.Status != pgmetrics.StatusStale {
				t.Fatalf("aws status = %q, want stale", src.Status)
			}
			if src.DataEnd == 0 {
				t.Fatal("aws data time must be preserved even when stale")
			}
		case "azure":
			if src.Status != pgmetrics.StatusUnavailable {
				t.Fatalf("azure status = %q, want unavailable", src.Status)
			}
		}
	}
	// each degraded source gets exactly one explanatory source-level label:
	// the staleness/availability label must not be duplicated by a bare
	// "(source)" skew label
	has := func(l string) bool {
		for _, x := range out.OutOfBounds {
			if x == l {
				return true
			}
		}
		return false
	}
	if !has("aws-rds (source:stale)") || !has("azure (source:unavailable)") {
		t.Fatalf("missing source status labels in %v", out.OutOfBounds)
	}
	for _, l := range []string{"aws-rds (source)", "azure (source)", "azure (source:stale)"} {
		if has(l) {
			t.Fatalf("redundant/incorrect label %q in %v", l, out.OutOfBounds)
		}
	}
}

// TR-2.5: parent cancellation gates new sources/domains; completed domains
// keep accurate windows; final status is incomplete with a cancel reason.
func TestSessionCancelGating(t *testing.T) {
	clk := newFakeClock()
	ctx, cancel := context.WithCancel(context.Background())
	s := newSession(ctx, testCfg(), clk.Now)

	ran := false
	s.BeginSource("db1", "postgres")
	if err := s.Observe("db1", "settings", func() error {
		ran = true
		clk.Advance(10 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("first domain should run")
	}

	cancel()

	if s.BeginSource("aws-rds", "aws") {
		t.Fatal("BeginSource must return false after cancellation")
	}
	if err := s.Observe("db1", "activity", func() error {
		t.Fatal("fn must not run after cancellation")
		return nil
	}); err == nil {
		t.Fatal("Observe after cancellation must return an error")
	}
	s.EndSource("db1", "")

	out := s.Finalize()
	if out.Status != pgmetrics.StatusIncomplete || out.Complete {
		t.Fatalf("status=%q complete=%v, want incomplete/false", out.Status, out.Complete)
	}
	if out.CancelReason != "canceled" {
		t.Fatalf("cancel reason = %q", out.CancelReason)
	}
	var settings, activity *pgmetrics.DomainTiming
	for i := range out.Domains {
		switch out.Domains[i].Name {
		case "settings":
			settings = &out.Domains[i]
		case "activity":
			activity = &out.Domains[i]
		}
	}
	if settings == nil || settings.Status != pgmetrics.StatusOK || settings.WindowEnd == 0 {
		t.Fatalf("completed domain must keep its window: %+v", settings)
	}
	if activity == nil || activity.Status != pgmetrics.StatusSkipped {
		t.Fatalf("post-cancel domain must be skipped: %+v", activity)
	}
}

// TR-2.6: error domains make the collection incomplete; selectClosest picks
// the sample nearest the anchor regardless of scan order.
func TestSessionErrorAndSelectClosest(t *testing.T) {
	clk := newFakeClock()
	s := newSession(context.Background(), testCfg(), clk.Now)
	s.BeginSource("db1", "postgres")
	err := s.Observe("db1", "locks", func() error {
		return context.DeadlineExceeded
	})
	if err == nil {
		t.Fatal("expected error to propagate")
	}
	s.EndSource("db1", "")
	out := s.Finalize()
	if out.Status != pgmetrics.StatusIncomplete {
		t.Fatalf("status = %q, want incomplete", out.Status)
	}

	anchor := time.Unix(1_000_000, 0)
	cands := []time.Time{
		anchor.Add(40 * time.Second),
		anchor.Add(-10 * time.Second),
		anchor.Add(20 * time.Second),
		anchor.Add(-5 * time.Second),
	}
	got, idx, ok := selectClosest(anchor, cands)
	if !ok || idx != 3 || !got.Equal(anchor.Add(-5*time.Second)) {
		t.Fatalf("selectClosest = %v %d %v, want idx 3", got, idx, ok)
	}
	if _, _, ok := selectClosest(anchor, nil); ok {
		t.Fatal("empty candidates must return false")
	}
}
