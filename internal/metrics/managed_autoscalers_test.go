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
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gke-labs/workload-class/internal/utils"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to build scheme: %v", err)
	}
	scheme.AddKnownTypeWithName(utils.VPAGroupVersionKind, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(utils.VPAGroupVersionKind.GroupVersion().WithKind("VerticalPodAutoscalerList"), &unstructured.UnstructuredList{})
	return scheme
}

func testHPA(name string, managed bool) *autoscalingv2.HorizontalPodAutoscaler {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "shop"}}
	if managed {
		hpa.Labels = map[string]string{utils.ManagedByLabel: utils.ManagedByValue}
	}
	return hpa
}

func testVPA(name string, managed bool) *unstructured.Unstructured {
	vpa := &unstructured.Unstructured{}
	vpa.SetGroupVersionKind(utils.VPAGroupVersionKind)
	vpa.SetName(name)
	vpa.SetNamespace("shop")
	if managed {
		vpa.SetLabels(map[string]string{utils.ManagedByLabel: utils.ManagedByValue})
	}
	return vpa
}

func TestManagedAutoscalersCollector(t *testing.T) {
	objs := []client.Object{
		testHPA("wc-deployment-web", true), testHPA("wc-deployment-api", true), testHPA("user-hpa", false),
		testVPA("wc-deployment-web", true), testVPA("user-vpa", false),
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()

	t.Run("counts_managed_hpas_and_vpas", func(t *testing.T) {
		want := `
# HELP workloadclass_managed_autoscalers Number of autoscalers that are managed for workloads from the resourceManagement of their WorkloadClass, by kind.
# TYPE workloadclass_managed_autoscalers gauge
workloadclass_managed_autoscalers{kind="HorizontalPodAutoscaler"} 2
workloadclass_managed_autoscalers{kind="VerticalPodAutoscaler"} 1
`
		if err := testutil.CollectAndCompare(NewManagedAutoscalersCollector(c, true), strings.NewReader(want)); err != nil {
			t.Error(err)
		}
	})

	t.Run("vpas_are_not_listed_when_the_crd_is_not_installed", func(t *testing.T) {
		want := `
# HELP workloadclass_managed_autoscalers Number of autoscalers that are managed for workloads from the resourceManagement of their WorkloadClass, by kind.
# TYPE workloadclass_managed_autoscalers gauge
workloadclass_managed_autoscalers{kind="HorizontalPodAutoscaler"} 2
`
		if err := testutil.CollectAndCompare(NewManagedAutoscalersCollector(c, false), strings.NewReader(want)); err != nil {
			t.Error(err)
		}
	})

	t.Run("a_list_error_is_reported_to_the_scrape", func(t *testing.T) {
		failing := fake.NewClientBuilder().WithScheme(testScheme(t)).WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("the cache is not synced")
			},
		}).Build()
		registry := prometheus.NewPedanticRegistry()
		registry.MustRegister(NewManagedAutoscalersCollector(failing, false))
		if _, err := registry.Gather(); err == nil || !strings.Contains(err.Error(), "the cache is not synced") {
			t.Errorf("expected the scrape to report the list error, got %v", err)
		}
	})
}
