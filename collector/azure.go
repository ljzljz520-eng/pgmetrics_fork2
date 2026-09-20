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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/rapidloop/pgmetrics"
)

const (
	flexibleServerMetrics = `backup_storage_used,cpu_percent,memory_percent,iops,disk_queue_depth,read_throughput,write_throughput,read_iops,write_iops,storage_percent,storage_used,storage_free,txlogs_storage_used,active_connections,network_bytes_egress,network_bytes_ingress,connections_failed,connections_succeeded,maximum_used_transactionIDs`
	singleServerMetrics   = `cpu_percent,memory_percent,io_consumption_percent,storage_percent,storage_used,storage_limit,serverlog_storage_percent,serverlog_storage_usage,serverlog_storage_limit,active_connections,connections_failed,backup_storage_used,network_bytes_egress,network_bytes_ingress,pg_replica_log_delay_in_seconds,pg_replica_log_delay_in_bytes`
	citusMetrics          = `cpu_percent,memory_percent,apps_reserved_memory_percent,iops,storage_percent,storage_used,active_connections,network_bytes_egress,network_bytes_ingress`
)

var rxResource = regexp.MustCompile(`(?i)^/subscriptions/([^/]{36})/resourceGroups/([^/]+)/providers/Microsoft.DBforPostgreSQL/(flexibleServers|servers|serverGroupsv2)/([^/]+)$`)

// azureMetricsLister is the Azure Monitor surface used by collection; tests
// replace the factory to inject a fake lister.
type azureMetricsLister interface {
	List(ctx context.Context, resourceURI string,
		options *armmonitor.MetricsClientListOptions) (armmonitor.MetricsClientListResponse, error)
}

// newAzureMetricsClient is overridable by tests.
var newAzureMetricsClient = func(subID string, cred azcore.TokenCredential) (azureMetricsLister, error) {
	return armmonitor.NewMetricsClient(subID, cred, nil)
}

// newAzureCredential is overridable by tests.
var newAzureCredential = func() (azcore.TokenCredential, error) {
	return azidentity.NewDefaultAzureCredential(nil)
}

func collectAzure(ctx context.Context, anchor time.Time, maxStale time.Duration,
	resourceID string, out *pgmetrics.Azure) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// parse resource URI
	m := rxResource.FindStringSubmatch(resourceID)
	if len(m) != 5 {
		return errors.New("invalid resource ID")
	}
	out.ResourceType = "Microsoft.DBforPostgreSQL/" + m[3]
	out.ResourceName = m[4]
	out.Metrics = make(map[string]float64)
	out.MetricTimes = make(map[string]int64)

	// get credentials
	cred, err := newAzureCredential()
	if err != nil {
		return fmt.Errorf("failed to get credentials: %v", err)
	}

	// create a client
	client, err := newAzureMetricsClient(m[1], cred)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}

	// make parameters for query; the timespan ends at the common anchor
	to := anchor.In(time.UTC)
	from := to.Add(-5 * time.Minute)
	timeRange := from.Format(time.RFC3339) + "/" + to.Format(time.RFC3339)
	var interval string
	var top int32 = 1
	var metricNames string
	switch m[3] {
	case "flexibleServers":
		interval = "PT1M"
		metricNames = flexibleServerMetrics
	case "servers":
		interval = "PT15M"
		metricNames = singleServerMetrics
	case "serverGroupsv2":
		interval = "PT1M"
		metricNames = citusMetrics
	}

	// actually query
	resp, err := client.List(
		ctx,
		resourceID,
		&armmonitor.MetricsClientListOptions{
			Interval:    &interval,
			Metricnames: &metricNames,
			Timespan:    &timeRange,
			Top:         &top,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to query Azure API: %v", err)
	}

	// parse response
	if resp.Resourceregion != nil {
		out.ResourceRegion = *resp.Resourceregion
	}

	var selectedTimes []time.Time
	var newestSample time.Time
	for _, metric := range resp.Value {
		if metric.ID == nil || *metric.ID == "" {
			continue
		}
		name := azGetMetricName(*metric.ID)

		// gather every data point carrying a usable value, then select the
		// point whose PROVIDER timestamp is closest to the anchor; value
		// preference is Average -> Total -> Maximum at the chosen point.
		type point struct {
			ts  time.Time
			val float64
		}
		var candidates []point
		var candidateTimes []time.Time
		for _, series := range metric.Timeseries {
			if series == nil {
				continue
			}
			for _, d := range series.Data {
				if d == nil || d.TimeStamp == nil {
					continue
				}
				var v float64
				switch {
				case d.Average != nil:
					v = *d.Average
				case d.Total != nil:
					v = *d.Total
				case d.Maximum != nil:
					v = *d.Maximum
				default:
					continue
				}
				ts := d.TimeStamp.In(time.UTC)
				candidates = append(candidates, point{ts: ts, val: v})
				candidateTimes = append(candidateTimes, ts)
			}
		}
		if len(candidates) == 0 {
			continue // no usable data for this metric, skip quietly
		}
		_, idx, ok := selectClosest(anchor, candidateTimes)
		if !ok {
			continue
		}
		sel := candidates[idx]
		out.Metrics[name] = sel.val
		out.MetricTimes[name] = sel.ts.UnixMicro()
		selectedTimes = append(selectedTimes, sel.ts)
		if sel.ts.After(newestSample) {
			newestSample = sel.ts
		}
	}

	if len(selectedTimes) == 0 {
		out.Status = pgmetrics.StatusUnavailable
		return nil
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
	return nil
}

func azGetMetricName(id string) string {
	i := strings.LastIndexByte(id, '/')
	if i != -1 {
		return id[int(i)+1:]
	}
	return id
}
