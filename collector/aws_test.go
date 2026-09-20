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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/rapidloop/pgmetrics"
)

type fakeCW struct {
	names   []string
	results []*cloudwatch.MetricDataResult
	calls   int
}

func (f *fakeCW) ListMetricsWithContext(ctx context.Context, in *cloudwatch.ListMetricsInput,
	opts ...request.Option) (*cloudwatch.ListMetricsOutput, error) {
	f.calls++
	out := &cloudwatch.ListMetricsOutput{}
	for _, n := range f.names {
		out.Metrics = append(out.Metrics, &cloudwatch.Metric{MetricName: aws.String(n)})
	}
	return out, nil
}

func (f *fakeCW) GetMetricDataPagesWithContext(ctx context.Context, in *cloudwatch.GetMetricDataInput,
	fn func(*cloudwatch.GetMetricDataOutput, bool) bool, opts ...request.Option) error {
	f.calls++
	fn(&cloudwatch.GetMetricDataOutput{MetricDataResults: f.results}, true)
	return nil
}

type fakeRDSInst struct {
	monitoring int64
	calls      int
}

func (f *fakeRDSInst) DescribeDBInstancesWithContext(ctx context.Context,
	in *rds.DescribeDBInstancesInput, opts ...request.Option) (*rds.DescribeDBInstancesOutput, error) {
	f.calls++
	return &rds.DescribeDBInstancesOutput{DBInstances: []*rds.DBInstance{{
		DbiResourceId:      aws.String("res-1"),
		MonitoringInterval: aws.Int64(f.monitoring),
	}}}, nil
}

func (f *fakeRDSInst) DescribeDBLogFilesPagesWithContext(ctx context.Context,
	in *rds.DescribeDBLogFilesInput,
	fn func(*rds.DescribeDBLogFilesOutput, bool) bool, opts ...request.Option) error {
	f.calls++
	return nil
}

func (f *fakeRDSInst) DownloadDBLogFilePortionWithContext(ctx context.Context,
	in *rds.DownloadDBLogFilePortionInput,
	opts ...request.Option) (*rds.DownloadDBLogFilePortionOutput, error) {
	f.calls++
	return &rds.DownloadDBLogFilePortionOutput{}, nil
}

type fakeCWL struct {
	event *cloudwatchlogs.OutputLogEvent
	calls int
}

func (f *fakeCWL) GetLogEventsWithContext(ctx context.Context,
	in *cloudwatchlogs.GetLogEventsInput,
	opts ...request.Option) (*cloudwatchlogs.GetLogEventsOutput, error) {
	f.calls++
	return &cloudwatchlogs.GetLogEventsOutput{Events: []*cloudwatchlogs.OutputLogEvent{f.event}}, nil
}

func cwResult(id string, pts map[time.Duration]float64) *cloudwatch.MetricDataResult {
	r := &cloudwatch.MetricDataResult{Id: aws.String(id)}
	for d, v := range pts {
		t := testAWSTime.Add(d)
		vv := v
		r.Timestamps = append(r.Timestamps, &t)
		r.Values = append(r.Values, &vv)
	}
	return r
}

var testAWSTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func newAWSCollectorWith(fcw *fakeCW, fcwl *fakeCWL, frds *fakeRDSInst) *awsCollector {
	return &awsCollector{cw: fcw, cwl: fcwl, rdsc: frds}
}

// TR-5.1: per metric the point closest to the anchor is selected and its
// provider timestamp is kept.
func TestAWSClosestSelection(t *testing.T) {
	anchor := testAWSTime
	fcw := &fakeCW{
		names: []string{"CPUUtilization", "FreeableMemory"},
		results: []*cloudwatch.MetricDataResult{
			cwResult("id0", map[time.Duration]float64{-10 * time.Second: 11, -200 * time.Second: 22}),
			cwResult("id1", map[time.Duration]float64{-15 * time.Second: 99}),
		},
	}
	frds := &fakeRDSInst{}
	ac := newAWSCollectorWith(fcw, &fakeCWL{}, frds)

	out := &pgmetrics.RDS{}
	if err := ac.collect(context.Background(), anchor, 300*time.Second, "db1", out); err != nil {
		t.Fatal(err)
	}
	if out.Status != pgmetrics.StatusOK {
		t.Fatalf("status = %q, want ok", out.Status)
	}
	if out.Basic["CPUUtilization"] != 11 {
		t.Fatalf("CPU = %v, want the -10s point value 11", out.Basic["CPUUtilization"])
	}
	wantTS := anchor.Add(-10 * time.Second).UnixMicro()
	if out.BasicTimes["CPUUtilization"] != wantTS {
		t.Fatalf("CPU time = %d, want %d", out.BasicTimes["CPUUtilization"], wantTS)
	}
	if out.SampleTime != wantTS {
		t.Fatalf("sample time = %d, want closest %d", out.SampleTime, wantTS)
	}
	// window spans the SELECTED points only (-10s and -15s), not rejected ones
	if out.WindowStart != anchor.Add(-15*time.Second).UnixMicro() ||
		out.WindowEnd != anchor.Add(-10*time.Second).UnixMicro() {
		t.Fatalf("window = [%d,%d]", out.WindowStart, out.WindowEnd)
	}
}

// TR-5.2: all points too old -> stale; no points -> unavailable.
func TestAWSStaleAndUnavailable(t *testing.T) {
	anchor := testAWSTime
	fcw := &fakeCW{
		names:   []string{"CPUUtilization"},
		results: []*cloudwatch.MetricDataResult{cwResult("id0", map[time.Duration]float64{-600 * time.Second: 5})},
	}
	ac := newAWSCollectorWith(fcw, &fakeCWL{}, &fakeRDSInst{})
	out := &pgmetrics.RDS{}
	if err := ac.collect(context.Background(), anchor, 300*time.Second, "db1", out); err != nil {
		t.Fatal(err)
	}
	if out.Status != pgmetrics.StatusStale {
		t.Fatalf("status = %q, want stale", out.Status)
	}
	// old values keep their timestamps instead of pretending to be current
	if out.BasicTimes["CPUUtilization"] != anchor.Add(-600*time.Second).UnixMicro() {
		t.Fatal("stale sample timestamp must be preserved")
	}

	fcw2 := &fakeCW{
		names:   []string{"CPUUtilization"},
		results: []*cloudwatch.MetricDataResult{{Id: aws.String("id0")}},
	}
	ac2 := newAWSCollectorWith(fcw2, &fakeCWL{}, &fakeRDSInst{})
	out2 := &pgmetrics.RDS{}
	if err := ac2.collect(context.Background(), anchor, 300*time.Second, "db1", out2); err != nil {
		t.Fatal(err)
	}
	if out2.Status != pgmetrics.StatusUnavailable {
		t.Fatalf("status = %q, want unavailable", out2.Status)
	}
	if len(out2.Basic) != 0 || out2.SampleTime != 0 {
		t.Fatalf("unavailable must not present old values as current: %+v", out2)
	}
}

// TR-5.3: enhanced monitoring event timestamp is kept in microseconds.
func TestAWSEnhancedTimestamp(t *testing.T) {
	anchor := testAWSTime
	eventMS := anchor.Add(-20 * time.Second).UnixMilli()
	fcwl := &fakeCWL{event: &cloudwatchlogs.OutputLogEvent{
		Timestamp: aws.Int64(eventMS),
		Message:   aws.String(`{"engine":"postgresql"}`),
	}}
	fcw := &fakeCW{names: nil, results: nil}
	ac := newAWSCollectorWith(fcw, fcwl, &fakeRDSInst{monitoring: 60})
	out := &pgmetrics.RDS{}
	if err := ac.collect(context.Background(), anchor, 300*time.Second, "db1", out); err != nil {
		t.Fatal(err)
	}
	if out.EnhancedAt != eventMS*1000 {
		t.Fatalf("enhanced at = %d, want %d", out.EnhancedAt, eventMS*1000)
	}
	if out.Enhanced == nil {
		t.Fatal("enhanced payload should be decoded")
	}
	if out.WindowStart != eventMS*1000 || out.SampleTime != eventMS*1000 {
		t.Fatalf("enhanced event must drive window/sample time: %+v", out)
	}
	if out.Status != pgmetrics.StatusOK {
		t.Fatalf("status = %q, want ok", out.Status)
	}
}

// TR-5.4: a pre-canceled context makes zero SDK calls.
func TestAWSCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fcw, frds, fcwl := &fakeCW{}, &fakeRDSInst{}, &fakeCWL{}
	ac := newAWSCollectorWith(fcw, fcwl, frds)
	out := &pgmetrics.RDS{}
	err := ac.collect(ctx, testAWSTime, 300*time.Second, "db1", out)
	if err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if fcw.calls+frds.calls+fcwl.calls != 0 {
		t.Fatalf("SDK calls after cancel: cw=%d rds=%d cwl=%d", fcw.calls, frds.calls, fcwl.calls)
	}
}
