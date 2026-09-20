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

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rapidloop/pgmetrics"
)

var testAnchor = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func degradedFixture() *pgmetrics.Model {
	anchor := testAnchor.UnixMicro()
	return &pgmetrics.Model{
		Collection: &pgmetrics.CollectionIntegrity{
			StartedAt:      testAnchor.Unix(),
			FinishedAt:     testAnchor.Add(2 * time.Second).Unix(),
			AnchorTime:     anchor,
			WindowStart:    testAnchor.Add(-10 * time.Second).UnixMicro(),
			WindowEnd:      testAnchor.Add(2 * time.Second).UnixMicro(),
			MaxSkew:        (90 * time.Second).Microseconds(),
			MaxAllowedSkew: (60 * time.Second).Microseconds(),
			MaxStale:       (300 * time.Second).Microseconds(),
			Status:         pgmetrics.StatusDegraded,
			Complete:       true,
			OutOfBounds:    []string{"aws-rds/metrics"},
			Sources: []pgmetrics.SourceTiming{
				{Name: "aws-rds", Kind: "aws", Status: pgmetrics.StatusStale,
					DataStart: testAnchor.Add(-90 * time.Second).UnixMicro(),
					DataEnd:   testAnchor.Add(-90 * time.Second).UnixMicro()},
			},
			Domains: []pgmetrics.DomainTiming{
				{Source: "aws-rds", Name: "metrics", Class: pgmetrics.ClassObserved,
					Status: pgmetrics.StatusStale,
					WindowStart: testAnchor.Add(-90 * time.Second).UnixMicro(),
					WindowEnd:   testAnchor.Add(-90 * time.Second).UnixMicro(),
					DataTime:    testAnchor.Add(-90 * time.Second).UnixMicro(),
					Freshness:   (90 * time.Second).Microseconds()},
			},
		},
	}
}

func incompleteFixture() *pgmetrics.Model {
	anchor := testAnchor.UnixMicro()
	return &pgmetrics.Model{
		Collection: &pgmetrics.CollectionIntegrity{
			AnchorTime:     anchor,
			WindowStart:    testAnchor.Add(-1 * time.Second).UnixMicro(),
			WindowEnd:      testAnchor.Add(1 * time.Second).UnixMicro(),
			MaxAllowedSkew: (60 * time.Second).Microseconds(),
			Status:         pgmetrics.StatusIncomplete,
			Complete:       false,
			CancelReason:   "canceled",
		},
	}
}

// TR-7.1: human report shows the integrity block, window, DEGRADED and OOB
// domains; incomplete fixture shows INCOMPLETE and the cancel reason.
func TestHumanOutputIntegrity(t *testing.T) {
	var buf bytes.Buffer
	postgresWriteHumanTo(&buf, options{}, degradedFixture())
	out := buf.String()
	for _, want := range []string{
		"Collection Integrity:",
		"Overall Window:",
		"DEGRADED",
		"EXCEEDED",
		"aws-rds/metrics",
		"1 Jun 2026 11:59:50.000000 UTC",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("human output missing %q in:\n%s", want, out)
		}
	}

	buf.Reset()
	postgresWriteHumanTo(&buf, options{}, incompleteFixture())
	out = buf.String()
	for _, want := range []string{"Collection Integrity:", "INCOMPLETE", "Cancel Reason:", "canceled"} {
		if !strings.Contains(out, want) {
			t.Fatalf("human output missing %q in:\n%s", want, out)
		}
	}

	// no Collection object -> section omitted (old JSON inputs)
	buf.Reset()
	postgresWriteHumanTo(&buf, options{}, &pgmetrics.Model{})
	if strings.Contains(buf.String(), "Collection Integrity") {
		t.Fatal("integrity section must be absent when model has no Collection")
	}
}

// TR-7.2: CSV carries collection scalars and per-domain records.
func TestCSVOutputIntegrity(t *testing.T) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := model2csv(degradedFixture(), w); err != nil {
		t.Fatal(err)
	}
	w.Flush()

	records := map[string]string{}
	r := csv.NewReader(&buf)
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		records[row[0]] = row[1]
	}
	if records["pgmetrics.collection.status"] != pgmetrics.StatusDegraded {
		t.Fatalf("collection.status = %q", records["pgmetrics.collection.status"])
	}
	if records["pgmetrics.collection.max_skew_us"] != "90000000" {
		t.Fatalf("max_skew_us = %q", records["pgmetrics.collection.max_skew_us"])
	}
	if records["pgmetrics.collection.complete"] != "true" {
		t.Fatalf("complete = %q", records["pgmetrics.collection.complete"])
	}
	if records["pgmetrics.collection.domain.count"] != "1" {
		t.Fatalf("domain.count = %q", records["pgmetrics.collection.domain.count"])
	}
	if records["pgmetrics.collection.domain.0.name"] != "metrics" ||
		records["pgmetrics.collection.domain.0.status"] != pgmetrics.StatusStale {
		t.Fatalf("domain row missing/wrong: %+v", records)
	}
	if records["pgmetrics.collection.source.0.name"] != "aws-rds" {
		t.Fatalf("source row missing: %+v", records)
	}
	if records["pgmetrics.collection.out_of_bounds.0"] != "aws-rds/metrics" {
		t.Fatalf("oob row missing: %+v", records)
	}
}

// JSON round-trips the whole contract.
func TestJSONOutputIntegrity(t *testing.T) {
	var buf bytes.Buffer
	writeJSONTo(&buf, degradedFixture())

	var got map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	c, ok := got["collection"].(map[string]interface{})
	if !ok {
		t.Fatal("json output missing collection object")
	}
	for _, key := range []string{
		"anchor_time_us", "window_start_us", "window_end_us",
		"max_skew_us", "max_allowed_skew_us", "max_stale_us",
		"status", "complete", "out_of_bounds", "sources", "domains",
	} {
		if _, present := c[key]; !present {
			t.Fatalf("json collection missing %s", key)
		}
	}
	if c["status"] != pgmetrics.StatusDegraded {
		t.Fatalf("json status = %v", c["status"])
	}

	// empty fixture must omit the collection key entirely (omitempty)
	buf.Reset()
	writeJSONTo(&buf, &pgmetrics.Model{})
	var bare map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &bare); err != nil {
		t.Fatal(err)
	}
	if _, present := bare["collection"]; present {
		t.Fatal("collection key should be omitted when nil")
	}
}
