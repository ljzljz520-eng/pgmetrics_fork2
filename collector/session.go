/*
 * Copyright 2026 RapidLoop, Inc.
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
	"math"
	"sync"
	"time"

	"github.com/rapidloop/pgmetrics"
)

// Clock returns the current time. The default implementation is time.Now,
// whose results carry monotonic clock readings. Tests may inject a
// controllable clock.
type Clock func() time.Time

// SessionConfig holds the timing contracts of a collection session.
type SessionConfig struct {
	// MaxSkew is the largest permitted absolute deviation of any source data
	// time from the common anchor. Exceeding it degrades the collection.
	MaxSkew time.Duration
	// MaxStale is the largest permitted age (anchor - data time) of a cloud
	// sample. Older samples mark the source stale; no samples mark it
	// unavailable.
	MaxStale time.Duration
	// MaxSnapshotHold bounds how long a snapshot consistency group may keep
	// its read-only transaction open.
	MaxSnapshotHold time.Duration
}

// Hooks are optional test/instrumentation callbacks. AfterDomain is invoked
// immediately after every domain finished (or was skipped); tests use it to
// block the collection at a deterministic point. The callback must not be
// changed while the session is running.
type Hooks struct {
	AfterDomain  func(source, name, class, status string)
	BeforeDomain func(source, name, class string)
}

// CollectionSession carries the parent context, the monotonic clock, the
// common anchor and the per-domain/per-source timing ledger for one run of
// collection.
type CollectionSession struct {
	// gateCtx is derived from the parent context and only drives the
	// "do not start new work" gates (BeginSource/Done/Observe) and cloud
	// API calls, whose errors are handled gracefully.
	gateCtx    context.Context
	gateCancel context.CancelFunc

	// workCtx is intentionally NOT derived from the parent context: SQL
	// requests already in flight are allowed to finish (they are bounded by
	// the per-query timeout and statement_timeout) so that parent
	// cancellation cannot turn an in-flight query into a fatal exit. It is
	// canceled only when the session itself ends.
	workCtx    context.Context
	workCancel context.CancelFunc

	clock  Clock
	anchor time.Time // wall clock, common anchor
	start  time.Time // carries a monotonic reading
	cfg    SessionConfig
	hooks  Hooks

	mu          sync.Mutex
	sources     map[string]*sourceRec
	sourceOrder []string
	domains     []*domainRec

	holdExceeded bool // a snapshot group ran past MaxSnapshotHold

	finalOnce sync.Once
	final     *pgmetrics.CollectionIntegrity
}

type sourceRec struct {
	s pgmetrics.SourceTiming

	reqStart  time.Time     // carries a monotonic reading
	offset    time.Duration // source clock - local clock, measured once
	hasOffset bool
	hasData   bool
	dataLo    time.Time
	dataHi    time.Time
	finished  bool
}

type domainRec struct {
	d pgmetrics.DomainTiming

	skew        time.Duration
	hasData     bool
	dataTime    time.Time
	outOfBounds bool
}

// newSession creates a session anchored at the current clock time.
func newSession(parent context.Context, cfg SessionConfig, clock Clock) *CollectionSession {
	if clock == nil {
		clock = time.Now
	}
	if parent == nil {
		parent = context.Background()
	}
	gateCtx, gateCancel := context.WithCancel(parent)
	workCtx, workCancel := context.WithCancel(context.Background())
	now := clock()
	return &CollectionSession{
		gateCtx:    gateCtx,
		gateCancel: gateCancel,
		workCtx:    workCtx,
		workCancel: workCancel,
		clock:      clock,
		anchor:     now,
		start:      now,
		cfg:        cfg,
		sources:    make(map[string]*sourceRec),
	}
}

// withHooks attaches hooks and returns the session (builder style).
func (s *CollectionSession) withHooks(h Hooks) *CollectionSession {
	s.hooks = h
	return s
}

// Ctx returns the WORK context for in-flight SQL requests: it is not
// canceled by the parent context, only by session end. In-flight work stays
// bounded by per-query timeouts and cannot be turned into a fatal error by
// parent cancellation.
func (s *CollectionSession) Ctx() context.Context { return s.workCtx }

// GateCtx returns the parent-derived context used to gate new work and to
// drive cloud API calls (whose errors are handled, not fatal).
func (s *CollectionSession) GateCtx() context.Context { return s.gateCtx }

// Done reports whether the session must stop starting new work.
func (s *CollectionSession) Done() bool {
	select {
	case <-s.gateCtx.Done():
		return true
	default:
		return false
	}
}

// Anchor returns the common anchor time.
func (s *CollectionSession) Anchor() time.Time { return s.anchor }

// Now returns the current (possibly injected) clock time.
func (s *CollectionSession) Now() time.Time { return s.clock() }

// renameSource renames a registered source, keeping all its timing data.
func (s *CollectionSession) renameSource(old, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.sources[old]
	if !ok {
		return
	}
	delete(s.sources, old)
	rec.s.Name = name
	s.sources[name] = rec
	for i, n := range s.sourceOrder {
		if n == old {
			s.sourceOrder[i] = name
		}
	}
	for _, d := range s.domains {
		if d.d.Source == old {
			d.d.Source = name
		}
	}
}

// Config returns the session timing configuration.
func (s *CollectionSession) Config() SessionConfig { return s.cfg }

// Cancel ends the session (in addition to parent-context cancellation).
func (s *CollectionSession) Cancel() { s.gateCancel(); s.workCancel() }

//------------------------------------------------------------------------------
// sources
//------------------------------------------------------------------------------

// BeginSource gates and starts timing one collection source. It returns false
// when the session context has already ended, in which case no request to the
// source may be started.
func (s *CollectionSession) BeginSource(name, kind string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gateCtx.Err() != nil {
		return false
	}
	if _, ok := s.sources[name]; ok {
		return true // re-entrant for the same source
	}
	now := s.clock()
	rec := &sourceRec{
		reqStart: now,
		s: pgmetrics.SourceTiming{
			Name:         name,
			Kind:         kind,
			RequestStart: now.UnixMicro(),
			Status:       pgmetrics.StatusOK,
		},
	}
	s.sources[name] = rec
	s.sourceOrder = append(s.sourceOrder, name)
	return true
}

// SetSourceOffset records the measured source-clock offset (source - local).
func (s *CollectionSession) SetSourceOffset(name string, offset time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec := s.sources[name]; rec != nil {
		rec.offset = offset
		rec.hasOffset = true
		rec.s.ClockOffset = offset.Microseconds()
	}
}

// SetSourceDataRange records an explicit data time range (cloud providers).
func (s *CollectionSession) SetSourceDataRange(name string, lo, hi time.Time) {
	if lo.IsZero() && hi.IsZero() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.sources[name]
	if rec == nil {
		return
	}
	if !lo.IsZero() {
		if !rec.hasData || lo.Before(rec.dataLo) {
			rec.dataLo = lo
		}
		rec.hasData = true
	}
	if !hi.IsZero() {
		if !rec.hasData || hi.After(rec.dataHi) {
			rec.dataHi = hi
		}
		rec.hasData = true
	}
	if rec.hasData {
		rec.s.DataStart = rec.dataLo.UnixMicro()
		rec.s.DataEnd = rec.dataHi.UnixMicro()
	}
}

// SetSourceStatus overrides the status of a source.
func (s *CollectionSession) SetSourceStatus(name, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec := s.sources[name]; rec != nil {
		rec.s.Status = status
	}
}

// EndSource closes the request interval of a source.
func (s *CollectionSession) EndSource(name, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.sources[name]
	if rec == nil || rec.finished {
		return
	}
	now := s.clock()
	rec.finished = true
	rec.s.RequestEnd = now.UnixMicro()
	if !rec.reqStart.IsZero() {
		rec.s.RequestDuration = now.Sub(rec.reqStart).Microseconds()
	}
	if len(status) > 0 {
		rec.s.Status = status
	}
}

//------------------------------------------------------------------------------
// domains
//------------------------------------------------------------------------------

// Observe runs fn as an observed (live) domain, recording its request
// interval and deriving the server-clock observation window from the measured
// source clock offset. If the session is canceled the function is not run and
// the domain is recorded as skipped.
func (s *CollectionSession) Observe(source, name string, fn func() error) error {
	return s.observe(source, name, pgmetrics.ClassObserved, "", time.Time{}, fn)
}

// ObserveSnapshot runs fn as a member of a snapshot consistency group. The
// snapshotID and snapshot data time are shared by every domain in the group.
func (s *CollectionSession) ObserveSnapshot(source, name, snapshotID string, dataTime time.Time, fn func() error) error {
	return s.observe(source, name, pgmetrics.ClassSnapshot, snapshotID, dataTime, fn)
}

// RecordDomain records an already-collected domain (used for cloud sources),
// with an explicit representative data time, window and status. The request
// interval is the current instant when fn is absent.
func (s *CollectionSession) RecordDomain(source, name, class, status string, dataTime time.Time, windowLo, windowHi time.Time) {
	s.mu.Lock()
	rec := s.recordLocked(source, name, class, "", dataTime, windowLo, windowHi, status)
	s.mu.Unlock()
	s.after(source, name, class, rec.d.Status)
}

func (s *CollectionSession) observe(source, name, class, snapshotID string, dataTime time.Time, fn func() error) error {
	if s.hooks.BeforeDomain != nil {
		s.hooks.BeforeDomain(source, name, class)
	}

	// Gate: never start a new domain after the parent context ended.
	if s.gateCtx.Err() != nil {
		s.mu.Lock()
		now := s.clock()
		rec := s.recordTimingLocked(source, name, class, snapshotID, time.Time{},
			now, now, pgmetrics.StatusSkipped)
		s.mu.Unlock()
		s.after(source, name, class, rec.d.Status)
		return s.gateCtx.Err()
	}

	start := s.clock()
	err := fn()
	end := s.clock()

	status := pgmetrics.StatusOK
	if err != nil {
		status = pgmetrics.StatusError
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var winLo, winHi, dTime time.Time
	if class == pgmetrics.ClassSnapshot {
		if !dataTime.IsZero() {
			winLo, winHi, dTime = dataTime, dataTime, dataTime
		}
	} else {
		// derive server-clock window from the per-source offset
		offset := time.Duration(0)
		if rec := s.sources[source]; rec != nil && rec.hasOffset {
			offset = rec.offset
		}
		winLo, winHi = start.Add(offset), end.Add(offset)
		dTime = winLo.Add(winHi.Sub(winLo) / 2)
	}

	d := s.recordTimingLocked(source, name, class, snapshotID, dTime, winLo, winHi, status)
	d.d.RequestStart = start.UnixMicro()
	d.d.RequestEnd = end.UnixMicro()
	d.d.RequestDuration = end.Sub(start).Microseconds()
	if err != nil {
		s.after(source, name, class, status)
		return err
	}
	s.after(source, name, class, status)
	return nil
}

// recordLocked creates a domain record with explicit window/status. Caller
// holds mu.
func (s *CollectionSession) recordLocked(source, name, class, snapshotID string,
	dataTime, windowLo, windowHi time.Time, status string) *domainRec {
	now := s.clock()
	d := s.recordTimingLocked(source, name, class, snapshotID, dataTime,
		windowLo, windowHi, status)
	d.d.RequestStart = now.UnixMicro()
	d.d.RequestEnd = now.UnixMicro()
	return d
}

// recordTimingLocked fills the timing/window/skew fields of a new domain
// record. Caller holds mu.
func (s *CollectionSession) recordTimingLocked(source, name, class, snapshotID string,
	dataTime, windowLo, windowHi time.Time, status string) *domainRec {
	rec := &domainRec{d: pgmetrics.DomainTiming{
		Name:       name,
		Source:     source,
		Class:      class,
		Status:     status,
		SnapshotID: snapshotID,
	}}
	if !dataTime.IsZero() {
		rec.hasData = true
		rec.dataTime = dataTime
		rec.d.DataTime = dataTime.UnixMicro()
		freshness := s.anchor.Sub(dataTime)
		rec.d.Freshness = freshness.Microseconds()
		skew := freshness
		if skew < 0 {
			skew = -skew
		}
		rec.skew = skew
		rec.d.WindowStart = windowLo.UnixMicro()
		rec.d.WindowEnd = windowHi.UnixMicro()
		if s.cfg.MaxSkew > 0 && skew > s.cfg.MaxSkew {
			rec.outOfBounds = true
		}
		if s.cfg.MaxStale > 0 && class == pgmetrics.ClassObserved &&
			freshness > s.cfg.MaxStale {
			rec.outOfBounds = true
		}
	}
	s.domains = append(s.domains, rec)
	return rec
}

func (s *CollectionSession) after(source, name, class, status string) {
	if h := s.hooks.AfterDomain; h != nil {
		h(source, name, class, status)
	}
}

// markHoldExceeded notes that a snapshot group reached MaxSnapshotHold; the
// collection cannot be reported as complete.
func (s *CollectionSession) markHoldExceeded() {
	s.mu.Lock()
	s.holdExceeded = true
	s.mu.Unlock()
}

// MarkIncomplete notes that some part of the collection could not finish, so
// Finalize must report status "incomplete".
func (s *CollectionSession) MarkIncomplete() { s.markHoldExceeded() }

// DomainRec returns the most recent timing record for a domain, so a
// snapshot group can track exactly the domains it registered.
func (s *CollectionSession) DomainRec(source, name string) *domainRec {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.domains) - 1; i >= 0; i-- {
		r := s.domains[i]
		if r.d.Source == source && r.d.Name == name {
			return r
		}
	}
	return nil
}

// RetractDomains marks exactly the given snapshot-domain records as errored
// after their transaction was rolled back, and clears their windows so
// rolled-back data cannot enter the overall window or source ranges.
// Membership is passed explicitly: other groups that happen to share a
// snapshot id text (or an empty id on PostgreSQL older than 13) are never
// affected.
func (s *CollectionSession) RetractDomains(recs []*domainRec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range recs {
		if r.d.Class != pgmetrics.ClassSnapshot || r.d.Status != pgmetrics.StatusOK {
			continue
		}
		r.d.Status = pgmetrics.StatusError
		r.d.WindowStart = 0
		r.d.WindowEnd = 0
		r.d.DataTime = 0
		r.d.Freshness = 0
		r.skew = 0
		r.hasData = false
		r.outOfBounds = false
	}
}

// SkipDomain records a domain that was not started (hold cutoff or
// cancellation). It carries no window or data time.
func (s *CollectionSession) SkipDomain(source, name, class, snapshotID string) {
	s.mu.Lock()
	s.recordTimingLocked(source, name, class, snapshotID, time.Time{},
		time.Time{}, time.Time{}, pgmetrics.StatusSkipped)
	s.mu.Unlock()
	s.after(source, name, class, pgmetrics.StatusSkipped)
}

//------------------------------------------------------------------------------
// finalization
//------------------------------------------------------------------------------

// Finalize computes the overall window, skew, out-of-bounds domains and the
// overall status, and returns the integrity contract. It is idempotent:
// repeated calls return the same result.
func (s *CollectionSession) Finalize() *pgmetrics.CollectionIntegrity {
	s.finalOnce.Do(func() { s.final = s.finalizeLocked() })
	return s.final
}

func (s *CollectionSession) finalizeLocked() *pgmetrics.CollectionIntegrity {
	// release both contexts after the ledger is frozen (work context first,
	// so no new SQL can start during finalization)
	defer s.gateCancel()
	defer s.workCancel()

	s.mu.Lock()
	defer s.mu.Unlock()

	finish := s.clock()
	out := &pgmetrics.CollectionIntegrity{
		StartedAt:      s.start.Unix(),
		FinishedAt:     finish.Unix(),
		AnchorTime:     s.anchor.UnixMicro(),
		MaxAllowedSkew: s.cfg.MaxSkew.Microseconds(),
		MaxStale:       s.cfg.MaxStale.Microseconds(),
		Complete:       true,
		Status:         pgmetrics.StatusOK,
	}

	cancelReason := ""
	if err := s.gateCtx.Err(); err != nil {
		switch err {
		case context.DeadlineExceeded:
			cancelReason = "deadline exceeded"
		default:
			cancelReason = "canceled"
		}
	}

	incomplete := cancelReason != "" || s.holdExceeded
	degraded := false

	// domains: overall window, max skew and out-of-bounds list
	var winLo, winHi time.Time
	haveWin := false
	var maxSkew time.Duration
	for _, rec := range s.domains {
		if rec.hasData {
			t := rec.dataTime
			if skew := rec.skew; skew > maxSkew {
				maxSkew = skew
			}
			if !haveWin {
				winLo, winHi, haveWin = t, t, true
			} else {
				if t.Before(winLo) {
					winLo = t
				}
				if t.After(winHi) {
					winHi = t
				}
			}
		}
		if rec.d.WindowStart != 0 && rec.d.WindowEnd != 0 {
			lo := time.UnixMicro(rec.d.WindowStart)
			hi := time.UnixMicro(rec.d.WindowEnd)
			if !haveWin {
				winLo, winHi, haveWin = lo, hi, true
			} else {
				if lo.Before(winLo) {
					winLo = lo
				}
				if hi.After(winHi) {
					winHi = hi
				}
			}
		}
		switch rec.d.Status {
		case pgmetrics.StatusError, pgmetrics.StatusSkipped:
			incomplete = true
		case pgmetrics.StatusStale, pgmetrics.StatusUnavailable:
			degraded = true
		}
		if rec.outOfBounds {
			degraded = true
			label := rec.d.Source + "/" + rec.d.Name
			out.OutOfBounds = append(out.OutOfBounds, label)
		}
		out.Domains = append(out.Domains, rec.d)
	}

	// fold every domain's data window into its source range so that
	// PostgreSQL sources (which set no explicit range) carry their actual
	// earliest/latest data times just like cloud sources do
	for _, rec := range s.domains {
		if rec.d.WindowStart == 0 {
			continue
		}
		sr := s.sources[rec.d.Source]
		if sr == nil {
			continue
		}
		lo, hi := time.UnixMicro(rec.d.WindowStart), time.UnixMicro(rec.d.WindowEnd)
		if !sr.hasData {
			sr.dataLo, sr.dataHi, sr.hasData = lo, hi, true
		} else {
			if lo.Before(sr.dataLo) {
				sr.dataLo = lo
			}
			if hi.After(sr.dataHi) {
				sr.dataHi = hi
			}
		}
	}

	// sources: emit and fold in explicit ranges and statuses
	oob := map[string]bool{}
	for _, l := range out.OutOfBounds {
		oob[l] = true
	}
	addOOB := func(label string) {
		if !oob[label] {
			oob[label] = true
			out.OutOfBounds = append(out.OutOfBounds, label)
		}
	}
	for _, name := range s.sourceOrder {
		rec := s.sources[name]
		skewBad := false
		if rec.hasData {
			rec.s.DataStart = rec.dataLo.UnixMicro()
			rec.s.DataEnd = rec.dataHi.UnixMicro()
			if !haveWin {
				winLo, winHi, haveWin = rec.dataLo, rec.dataHi, true
			} else {
				if rec.dataLo.Before(winLo) {
					winLo = rec.dataLo
				}
				if rec.dataHi.After(winHi) {
					winHi = rec.dataHi
				}
			}
			mid := rec.dataLo.Add(rec.dataHi.Sub(rec.dataLo) / 2)
			d := absDur(mid.Sub(s.anchor))
			if d > maxSkew {
				maxSkew = d
			}
			if s.cfg.MaxSkew > 0 && d > s.cfg.MaxSkew {
				skewBad = true
				degraded = true
			}
		}
		// one explanatory label per source: a staleness/availability label
		// already implies being out of bounds, so a bare "(source)" skew
		// label is only added when nothing more specific applies
		label := ""
		switch rec.s.Status {
		case pgmetrics.StatusError:
			incomplete = true
		case pgmetrics.StatusStale:
			degraded = true
			label = name + " (source:stale)"
		case pgmetrics.StatusUnavailable:
			degraded = true
			if !rec.hasData {
				label = name + " (source:unavailable)"
			}
		}
		if label == "" && skewBad {
			label = name + " (source)"
		}
		if label != "" {
			addOOB(label)
		}
		if !rec.finished {
			incomplete = true
		}
		out.Sources = append(out.Sources, rec.s)
	}

	if haveWin {
		out.WindowStart = winLo.UnixMicro()
		out.WindowEnd = winHi.UnixMicro()
	}
	out.MaxSkew = maxSkew.Microseconds()

	out.CancelReason = cancelReason
	if incomplete {
		out.Complete = false
		out.Status = pgmetrics.StatusIncomplete
	} else if degraded {
		out.Complete = true
		out.Status = pgmetrics.StatusDegraded
	}
	return out
}

func absDur(d time.Duration) time.Duration {
	if d == math.MinInt64 {
		return math.MaxInt64
	}
	if d < 0 {
		return -d
	}
	return d
}

// selectClosest returns the time in candidates closest to anchor, and its
// index. It returns false when candidates is empty.
func selectClosest(anchor time.Time, candidates []time.Time) (time.Time, int, bool) {
	if len(candidates) == 0 {
		return time.Time{}, -1, false
	}
	best := candidates[0]
	bestIdx := 0
	bestDelta := absDur(best.Sub(anchor))
	for i := 1; i < len(candidates); i++ {
		if d := absDur(candidates[i].Sub(anchor)); d < bestDelta {
			best, bestIdx, bestDelta = candidates[i], i, d
		}
	}
	return best, bestIdx, true
}
