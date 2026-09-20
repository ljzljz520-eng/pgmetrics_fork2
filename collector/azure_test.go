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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/rapidloop/pgmetrics"
)

const testAzureResourceID = "/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/rg1/providers/Microsoft.DBforPostgreSQL/flexibleServers/srv1"

var testAzureAnchor = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

type fakeAzureLister struct {
	resp  armmonitor.MetricsClientListResponse
	calls int
}

func (f *fakeAzureLister) List(ctx context.Context, resourceURI string,
	options *armmonitor.MetricsClientListOptions) (armmonitor.MetricsClientListResponse, error) {
	f.calls++
	return f.resp, nil
}

func azMetric(idSuffix string, points ...*armmonitor.MetricValue) *armmonitor.Metric {
	id := "/providers/microsoft.insights/metricdefinitions/" + idSuffix
	return &armmonitor.Metric{
		ID: &id,
		Timeseries: []*armmonitor.TimeSeriesElement{{
			Data: points,
		}},
	}
}

func azPoint(t time.Time, average, total, maximum *float64) *armmonitor.MetricValue {
	return &armmonitor.MetricValue{TimeStamp: &t, Average: average, Total: total, Maximum: maximum}
}

func fptr(v float64) *float64 { return &v }

func withAzureFakes(t *testing.T, lister azureMetricsLister) func() {
	t.Helper()
	oldClient := newAzureMetricsClient
	oldCred := newAzureCredential
	newAzureMetricsClient = func(subID string, cred azcore.TokenCredential) (azureMetricsLister, error) {
		return lister, nil
	}
	newAzureCredential = func() (azcore.TokenCredential, error) { return nil, nil }
	return func() {
		newAzureMetricsClient = oldClient
		newAzureCredential = oldCred
	}
}

// TR-6.1: of two points, the one nearest the anchor is selected even when a
// farther point carries the preferred Average value; its timestamp is kept.
func TestAzureClosestSelection(t *testing.T) {
	fl := &fakeAzureLister{}
	region := "eastus"
	fl.resp.Resourceregion = &region
	fl.resp.Value = []*armmonitor.Metric{
		azMetric("cpu_percent",
			azPoint(testAzureAnchor.Add(-180*time.Second), fptr(42), nil, nil),
			azPoint(testAzureAnchor.Add(-5*time.Second), nil, fptr(7), nil),
		),
	}
	restore := withAzureFakes(t, fl)
	defer restore()

	out := &pgmetrics.Azure{}
	if err := collectAzure(context.Background(), testAzureAnchor, 300*time.Second,
		testAzureResourceID, out); err != nil {
		t.Fatal(err)
	}
	if out.Status != pgmetrics.StatusOK {
		t.Fatalf("status = %q, want ok", out.Status)
	}
	if out.Metrics["cpu_percent"] != 7 {
		t.Fatalf("cpu_percent = %v, want the -5s Total value 7", out.Metrics["cpu_percent"])
	}
	wantTS := testAzureAnchor.Add(-5 * time.Second).UnixMicro()
	if out.MetricTimes["cpu_percent"] != wantTS {
		t.Fatalf("metric time = %d, want %d", out.MetricTimes["cpu_percent"], wantTS)
	}
	if out.SampleTime != wantTS || out.WindowStart != wantTS || out.WindowEnd != wantTS {
		t.Fatalf("window/sample wrong: %+v", out)
	}
	if out.ResourceRegion != "eastus" || out.ResourceName != "srv1" {
		t.Fatalf("metadata regression: %+v", out)
	}
}

// TR-6.2: all points older than max-stale -> stale; no points -> unavailable.
func TestAzureStaleAndUnavailable(t *testing.T) {
	fl := &fakeAzureLister{}
	fl.resp.Value = []*armmonitor.Metric{
		azMetric("cpu_percent",
			azPoint(testAzureAnchor.Add(-600*time.Second), fptr(9), nil, nil),
		),
	}
	restore := withAzureFakes(t, fl)
	defer restore()

	out := &pgmetrics.Azure{}
	if err := collectAzure(context.Background(), testAzureAnchor, 300*time.Second,
		testAzureResourceID, out); err != nil {
		t.Fatal(err)
	}
	if out.Status != pgmetrics.StatusStale {
		t.Fatalf("status = %q, want stale", out.Status)
	}
	if out.MetricTimes["cpu_percent"] != testAzureAnchor.Add(-600*time.Second).UnixMicro() {
		t.Fatal("stale sample timestamp must be preserved")
	}

	fl2 := &fakeAzureLister{}
	fl2.resp.Value = []*armmonitor.Metric{azMetric("cpu_percent")}
	restore2 := withAzureFakes(t, fl2)
	defer restore2()
	out2 := &pgmetrics.Azure{}
	if err := collectAzure(context.Background(), testAzureAnchor, 300*time.Second,
		testAzureResourceID, out2); err != nil {
		t.Fatal(err)
	}
	if out2.Status != pgmetrics.StatusUnavailable {
		t.Fatalf("status = %q, want unavailable", out2.Status)
	}
	if len(out2.Metrics) != 0 {
		t.Fatalf("unavailable must not expose values: %+v", out2)
	}
}

// pre-canceled context: no API call is made.
func TestAzureCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fl := &fakeAzureLister{}
	restore := withAzureFakes(t, fl)
	defer restore()
	if err := collectAzure(ctx, testAzureAnchor, 300*time.Second,
		testAzureResourceID, &pgmetrics.Azure{}); err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if fl.calls != 0 {
		t.Fatalf("List called %d times after cancel", fl.calls)
	}
}
