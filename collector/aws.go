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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/rapidloop/pgmetrics"
)

// minimal AWS API surfaces, so that tests can inject fakes

type cwAPI interface {
	ListMetricsWithContext(ctx context.Context, in *cloudwatch.ListMetricsInput,
		opts ...request.Option) (*cloudwatch.ListMetricsOutput, error)
	GetMetricDataPagesWithContext(ctx context.Context, in *cloudwatch.GetMetricDataInput,
		fn func(page *cloudwatch.GetMetricDataOutput, lastPage bool) bool,
		opts ...request.Option) error
}

type cwlogsAPI interface {
	GetLogEventsWithContext(ctx context.Context, in *cloudwatchlogs.GetLogEventsInput,
		opts ...request.Option) (*cloudwatchlogs.GetLogEventsOutput, error)
}

type rdsAPI interface {
	DescribeDBInstancesWithContext(ctx context.Context, in *rds.DescribeDBInstancesInput,
		opts ...request.Option) (*rds.DescribeDBInstancesOutput, error)
	DescribeDBLogFilesPagesWithContext(ctx context.Context, in *rds.DescribeDBLogFilesInput,
		fn func(page *rds.DescribeDBLogFilesOutput, lastPage bool) bool,
		opts ...request.Option) error
	DownloadDBLogFilePortionWithContext(ctx context.Context, in *rds.DownloadDBLogFilePortionInput,
		opts ...request.Option) (*rds.DownloadDBLogFilePortionOutput, error)
}

type awsCollector struct {
	sess *session.Session
	cw   cwAPI
	cwl  cwlogsAPI
	rdsc rdsAPI
}

func newAwsCollector() (*awsCollector, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}
	return &awsCollector{
		sess: sess,
		cw:   cloudwatch.New(sess),
		cwl:  cloudwatchlogs.New(sess),
		rdsc: rds.New(sess),
	}, nil
}

// newAwsCollectorFn is the factory used by collection; tests replace it to
// inject fake AWS clients.
var newAwsCollectorFn = newAwsCollector

func (ac *awsCollector) collect(ctx context.Context, anchor time.Time,
	maxStale time.Duration, dbid string, out *pgmetrics.RDS) (err error) {
	if err = ctx.Err(); err != nil {
		return
	}

	out.Basic = map[string]float64{}
	out.BasicTimes = map[string]int64{}

	// describe the db instance
	dbinsts, err := ac.rdsc.DescribeDBInstancesWithContext(ctx, &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(dbid),
	})
	if err != nil {
		return
	}
	if len(dbinsts.DBInstances) != 1 {
		err = fmt.Errorf("failed to locate database instance %q", dbid)
		return
	}

	// get resource ID and see if enhanced monitoring is enabled
	dbinst := dbinsts.DBInstances[0]
	dbirid := *dbinst.DbiResourceId
	emEnabled := *dbinst.MonitoringInterval > 0

	// list available metrics
	avmetrics, err := ac.cw.ListMetricsWithContext(ctx, &cloudwatch.ListMetricsInput{
		Namespace: aws.String("AWS/RDS"),
		Dimensions: []*cloudwatch.DimensionFilter{
			{
				Name:  aws.String("DBInstanceIdentifier"),
				Value: aws.String(dbid),
			},
		},
	})
	if err != nil {
		err = fmt.Errorf("failed to list CloudWatch metrics: %v", err)
		return
	}

	// form query input
	names := make([]string, len(avmetrics.Metrics))
	queries := make([]*cloudwatch.MetricDataQuery, len(avmetrics.Metrics))
	for i, m := range avmetrics.Metrics {
		names[i] = *m.MetricName
		queries[i] = &cloudwatch.MetricDataQuery{
			Id: aws.String(fmt.Sprintf("id%d", i)),
			MetricStat: &cloudwatch.MetricStat{
				Metric: &cloudwatch.Metric{
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("DBInstanceIdentifier"),
							Value: aws.String(dbid),
						},
					},
					MetricName: aws.String(names[i]),
					Namespace:  aws.String("AWS/RDS"),
				},
				Period: aws.Int64(60),
				Stat:   aws.String("Average"),
			},
		}
	}

	// query window ends at the common anchor; provider timestamps are what
	// matter for selection, not our request time
	to := anchor
	from := to.Add(-5 * time.Minute)
	input := &cloudwatch.GetMetricDataInput{
		StartTime:         aws.Time(from),
		EndTime:           aws.Time(to),
		ScanBy:            aws.String("TimestampDescending"),
		MetricDataQueries: queries,
	}

	// per-metric: pick the sample whose PROVIDER timestamp is closest to the
	// anchor, and keep that timestamp
	var selectedTimes []time.Time
	var newestSample time.Time
	if err = ac.cw.GetMetricDataPagesWithContext(ctx, input,
		func(page *cloudwatch.GetMetricDataOutput, lastPage bool) bool {
			for _, r := range page.MetricDataResults {
				n := len(r.Timestamps)
				if len(r.Values) < n {
					n = len(r.Values)
				}
				if n == 0 {
					continue
				}
				id, perr := strconv.Atoi(strings.TrimPrefix(aws.StringValue(r.Id), "id"))
				if perr != nil || id < 0 || id >= len(names) {
					continue
				}
				tsCand := make([]time.Time, 0, n)
				for i := 0; i < n; i++ {
					if r.Timestamps[i] != nil && r.Values[i] != nil {
						tsCand = append(tsCand, *r.Timestamps[i])
					}
				}
				_, idx, ok := selectClosest(anchor, tsCand)
				if !ok {
					continue
				}
				ts := tsCand[idx]
				// recover the value aligned with the chosen timestamp
				var val float64
				for i := 0; i < n; i++ {
					if r.Timestamps[i] != nil && *r.Timestamps[i] == ts {
						val = *r.Values[i]
						break
					}
				}
				name := names[id]
				out.Basic[name] = val
				out.BasicTimes[name] = ts.UnixMicro()
				selectedTimes = append(selectedTimes, ts)
				if ts.After(newestSample) {
					newestSample = ts
				}
			}
			return true
		}); err != nil {
		err = fmt.Errorf("failed to get CloudWatch metric data: %v", err)
		return
	}

	// enhanced monitoring: keep the provider event timestamp as well
	if emEnabled {
		events, gerr := ac.cwl.GetLogEventsWithContext(ctx,
			&cloudwatchlogs.GetLogEventsInput{
				EndTime:       aws.Int64(anchor.Unix() * 1000),
				Limit:         aws.Int64(1),
				LogGroupName:  aws.String("RDSOSMetrics"),
				LogStreamName: aws.String(dbirid),
				StartFromHead: aws.Bool(false),
			})
		if gerr != nil {
			err = fmt.Errorf("failed to get CloudWatchLog events: %v", gerr)
			return
		}
		if len(events.Events) > 0 && events.Events[0].Message != nil &&
			len(*events.Events[0].Message) > 0 {
			if events.Events[0].Timestamp != nil {
				out.EnhancedAt = *events.Events[0].Timestamp * 1000 // ms -> us
				et := time.UnixMicro(out.EnhancedAt)
				selectedTimes = append(selectedTimes, et)
				if et.After(newestSample) {
					newestSample = et
				}
			}
			if uerr := json.Unmarshal([]byte(*events.Events[0].Message),
				&out.Enhanced); uerr != nil {
				err = fmt.Errorf("failed to decode event: %v", uerr)
				return
			}
		}
	}

	// status and window from the data the provider actually returned
	if len(selectedTimes) == 0 {
		out.Status = pgmetrics.StatusUnavailable
		return
	}
	lo, hi := selectedTimes[0], selectedTimes[0]
	for _, t := range selectedTimes[1:] {
		if t.Before(lo) {
			lo = t
		}
		if t.After(hi) {
			hi = t
		}
	}
	best, _, _ := selectClosest(anchor, selectedTimes)
	out.SampleTime = best.UnixMicro()
	out.WindowStart = lo.UnixMicro()
	out.WindowEnd = hi.UnixMicro()
	if maxStale > 0 && anchor.Sub(newestSample) > maxStale {
		out.Status = pgmetrics.StatusStale
	} else {
		out.Status = pgmetrics.StatusOK
	}
	return
}

func (ac *awsCollector) collectLogs(ctx context.Context, dbid string,
	start time.Time, cb func(lines []byte)) (err error) {
	if len(dbid) == 0 || cb == nil {
		return errors.New("internal error, bad input")
	}
	if err = ctx.Err(); err != nil {
		return
	}

	type logFilesType struct {
		name string
		last time.Time
	}

	// describe db log files
	input := &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: aws.String(dbid),
		FileLastWritten:      aws.Int64(start.Unix() * 1000),
	}
	var logFiles []logFilesType
	err = ac.rdsc.DescribeDBLogFilesPagesWithContext(ctx, input,
		func(page *rds.DescribeDBLogFilesOutput, lastPage bool) bool {
			if page == nil {
				return false // should not happen
			}
			for _, d := range page.DescribeDBLogFiles {
				if d != nil && d.LastWritten != nil && d.LogFileName != nil {
					logFiles = append(logFiles, logFilesType{
						name: *d.LogFileName,
						last: time.Unix(*d.LastWritten/1000, *d.LastWritten%1000),
					})
				}
			}
			return true
		})
	if err != nil {
		err = fmt.Errorf("failed to DescribeDBLogFilesPages: %v", err)
		return
	}

	// sort the log files, oldest first
	sort.SliceStable(logFiles, func(i, j int) bool {
		return logFiles[i].last.Before(logFiles[j].last)
	})

	// download db log file portion
	for _, lf := range logFiles {
		if err = ctx.Err(); err != nil {
			return
		}
		marker := "0"
		done := false
		var lines []byte
		for !done {
			output, derr := ac.rdsc.DownloadDBLogFilePortionWithContext(ctx,
				&rds.DownloadDBLogFilePortionInput{
					DBInstanceIdentifier: aws.String(dbid),
					LogFileName:          aws.String(lf.name),
					Marker:               aws.String(marker),
				},
			)
			if derr != nil {
				return fmt.Errorf("failed to DownloadDBLogFilePortionPages: %v", derr)
			}
			if output == nil || output.LogFileData == nil || output.Marker == nil ||
				output.AdditionalDataPending == nil {
				break // should not happen
			}
			lines = append(lines, []byte(*output.LogFileData)...)
			marker = *output.Marker
			done = !*output.AdditionalDataPending
		}
		if len(lines) > 0 {
			cb(lines)
		}
	}
	return nil
}
