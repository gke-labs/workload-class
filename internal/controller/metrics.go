/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// defaultCollectTimeout bounds how long a metrics scrape waits for the managed autoscalers to be listed.
const defaultCollectTimeout = 10 * time.Second

var managedAutoscalersDesc = prometheus.NewDesc(
	"workloadclass_managed_autoscalers",
	"Number of HorizontalPodAutoscalers and VerticalPodAutoscalers that WorkloadClasses manage, by kind.",
	[]string{"kind"},
	nil,
)

// ManagedAutoscalerCollector reports the workloadclass_managed_autoscalers metric: the number of managed
// HorizontalPodAutoscalers and VerticalPodAutoscalers, i.e. those with the workloads.gke.io/managed-by:
// workload-class label. It lists them from Reader, normally the manager's cache, on each scrape, so the metric
// stays right across controller restarts and garbage collection without bookkeeping in the reconcilers.
type ManagedAutoscalerCollector struct {
	// Reader lists the managed autoscalers.
	Reader client.Reader
	// VPAInstalled reports whether VerticalPodAutoscalers can be listed.
	VPAInstalled bool
	// Timeout bounds each scrape. Defaults to 10 seconds.
	Timeout time.Duration
}

// Describe implements prometheus.Collector.
func (c *ManagedAutoscalerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- managedAutoscalersDesc
}

// Collect implements prometheus.Collector.
func (c *ManagedAutoscalerCollector) Collect(ch chan<- prometheus.Metric) {
	timeout := c.Timeout
	if timeout == 0 {
		timeout = defaultCollectTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	c.collect(ctx, ch, "HorizontalPodAutoscaler", &autoscalingv2.HorizontalPodAutoscalerList{}, func(l client.ObjectList) int {
		return len(l.(*autoscalingv2.HorizontalPodAutoscalerList).Items)
	})
	if c.VPAInstalled {
		c.collect(ctx, ch, "VerticalPodAutoscaler", &vpav1.VerticalPodAutoscalerList{}, func(l client.ObjectList) int {
			return len(l.(*vpav1.VerticalPodAutoscalerList).Items)
		})
	}
}

func (c *ManagedAutoscalerCollector) collect(ctx context.Context, ch chan<- prometheus.Metric, kind string, list client.ObjectList, count func(client.ObjectList) int) {
	if err := c.Reader.List(ctx, list, client.MatchingLabels{workloadsv1.ManagedByLabel: workloadsv1.ManagedByLabelValue}); err != nil {
		logf.Log.WithName("metrics").Error(err, "Failed to list managed autoscalers", "kind", kind)
		ch <- prometheus.NewInvalidMetric(managedAutoscalersDesc, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(managedAutoscalersDesc, prometheus.GaugeValue, float64(count(list)), kind)
}

// registerManagedAutoscalerCollector registers the collector with the controller-runtime metrics registry, which
// the manager serves. Registering it again, e.g. from another manager in the same process, is a no-op.
func registerManagedAutoscalerCollector(c *ManagedAutoscalerCollector) error {
	if err := metrics.Registry.Register(c); err != nil {
		if errors.As(err, &prometheus.AlreadyRegisteredError{}) {
			return nil
		}
		return err
	}
	return nil
}
