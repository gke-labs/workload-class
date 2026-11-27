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

package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	hpaKind = "HorizontalPodAutoscaler"
	vpaKind = "VerticalPodAutoscaler"

	// managedAutoscalersListTimeout bounds how long a scrape waits for the autoscalers to be listed.
	managedAutoscalersListTimeout = 10 * time.Second
)

var managedAutoscalersDesc = prometheus.NewDesc(
	"workloadclass_managed_autoscalers",
	"Number of autoscalers that are managed for workloads from the resourceManagement of their WorkloadClass, by kind.",
	[]string{"kind"}, nil,
)

// managedAutoscalersCollector reports the number of managed HorizontalPodAutoscalers and
// VerticalPodAutoscalers. It counts them when the metrics endpoint is scraped, from the reader it is given,
// so the metric cannot drift from the objects that exist.
type managedAutoscalersCollector struct {
	reader     client.Reader
	includeVPA bool
}

// NewManagedAutoscalersCollector returns a collector for the workloadclass_managed_autoscalers metric.
//
// The reader should be the manager's cached client, so that a scrape does not call the API server.
// VerticalPodAutoscalers are only counted if includeVPA is true: listing a kind that the API server does not
// serve fails, so pass false when the VPA CRD is not installed.
func NewManagedAutoscalersCollector(reader client.Reader, includeVPA bool) prometheus.Collector {
	return &managedAutoscalersCollector{reader: reader, includeVPA: includeVPA}
}

// Describe implements prometheus.Collector.
func (c *managedAutoscalersCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- managedAutoscalersDesc
}

// Collect implements prometheus.Collector.
func (c *managedAutoscalersCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), managedAutoscalersListTimeout)
	defer cancel()
	managed := client.MatchingLabels{utils.ManagedByLabel: utils.ManagedByValue}

	hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
	if err := c.reader.List(ctx, hpas, managed); err != nil {
		ch <- prometheus.NewInvalidMetric(managedAutoscalersDesc, err)
	} else {
		ch <- prometheus.MustNewConstMetric(managedAutoscalersDesc, prometheus.GaugeValue, float64(len(hpas.Items)), hpaKind)
	}

	if !c.includeVPA {
		return
	}
	vpas := &unstructured.UnstructuredList{}
	vpas.SetGroupVersionKind(utils.VPAGroupVersionKind.GroupVersion().WithKind(vpaKind + "List"))
	if err := c.reader.List(ctx, vpas, managed); err != nil {
		ch <- prometheus.NewInvalidMetric(managedAutoscalersDesc, err)
	} else {
		ch <- prometheus.MustNewConstMetric(managedAutoscalersDesc, prometheus.GaugeValue, float64(len(vpas.Items)), vpaKind)
	}
}
