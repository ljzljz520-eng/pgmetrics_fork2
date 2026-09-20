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

package pgmetrics_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/rapidloop/pgmetrics"
)

func TestSchemaVersionBumped(t *testing.T) {
	if pgmetrics.ModelSchemaVersion != "1.22" {
		t.Fatalf("schema version = %q, want 1.22", pgmetrics.ModelSchemaVersion)
	}
}

// TR-1.2: a model without the collection contract must not serialize the
// "collection" key (omitempty keeps older consumers working).
func TestCollectionOmitEmpty(t *testing.T) {
	b, err := json.Marshal(pgmetrics.Model{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"collection"`) {
		t.Fatalf("collection key must be omitted when nil: %s", b)
	}
}

// TR-1.1: new fields round-trip while an existing field keeps its JSON name.
func TestCollectionRoundTrip(t *testing.T) {
	m := pgmetrics.Model{
		Collection: &pgmetrics.CollectionIntegrity{
			AnchorTime:     1_780_000_000_000_000,
			WindowStart:    1_779_999_999_000_000,
			WindowEnd:      1_780_000_001_000_000,
			MaxSkew:        2_000_000,
			MaxAllowedSkew: 300_000_000,
			Status:         pgmetrics.StatusDegraded,
			Complete:       true,
			OutOfBounds:    []string{"aws-rds/metrics"},
			Sources: []pgmetrics.SourceTiming{{
				Name:         "aws-rds",
				Kind:         "aws",
				RequestStart: 1_780_000_000_000_100,
				Status:       pgmetrics.StatusStale,
				DataStart:    1_779_999_400_000_000,
				DataEnd:      1_779_999_400_000_000,
			}},
			Domains: []pgmetrics.DomainTiming{{
				Name:        "metrics",
				Source:      "aws-rds",
				Class:       pgmetrics.ClassObserved,
				DataTime:    1_779_999_400_000_000,
				Status:      pgmetrics.StatusStale,
				RequestStart: 1_780_000_000_000_100,
				RequestEnd:   1_780_000_000_100_000,
			}},
		},
		RDS: &pgmetrics.RDS{
			Basic:      map[string]float64{"CPUUtilization": 42.5},
			BasicTimes: map[string]int64{"CPUUtilization": 1_779_999_400_000_000},
			SampleTime: 1_779_999_400_000_000,
			Status:     pgmetrics.StatusStale,
		},
		Azure: &pgmetrics.Azure{
			ResourceName: "pg",
			Metrics:      map[string]float64{"cpu_percent": 7},
			MetricTimes:  map[string]int64{"cpu_percent": 1_779_999_400_000_000},
			Status:       pgmetrics.StatusOK,
		},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		`"collection":{`, `"anchor_time_us":1780000000000000`,
		`"max_skew_us":2000000`, `"out_of_bounds":["aws-rds/metrics"]`,
		`"basic_times":{"CPUUtilization":1779999400000000}`,
		`"sample_time_us":1779999400000000`,
		`"metric_times":{"cpu_percent":1779999400000000}`,
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("JSON missing %s\n%s", want, s)
		}
	}
	var back pgmetrics.Model
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Collection == nil || back.Collection.Status != pgmetrics.StatusDegraded ||
		back.Collection.MaxSkew != 2_000_000 || len(back.Collection.Domains) != 1 {
		t.Fatalf("collection round trip mismatch: %+v", back.Collection)
	}
	if back.RDS == nil || back.RDS.Basic["CPUUtilization"] != 42.5 ||
		back.RDS.BasicTimes["CPUUtilization"] != 1_779_999_400_000_000 {
		t.Fatalf("rds round trip mismatch: %+v", back.RDS)
	}
	if back.Azure == nil || back.Azure.Metrics["cpu_percent"] != 7 ||
		back.Azure.MetricTimes["cpu_percent"] != 1_779_999_400_000_000 {
		t.Fatalf("azure round trip mismatch: %+v", back.Azure)
	}
}
