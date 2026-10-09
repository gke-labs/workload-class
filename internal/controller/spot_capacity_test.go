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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func newSpotCapacityScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))
	return scheme
}

func autoscalerStatusConfigMap(status string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ClusterAutoscalerStatusName, Namespace: ClusterAutoscalerStatusNamespace},
		Data:       map[string]string{"status": status},
	}
}

func spotNode(name string, ready, unschedulable bool) *corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{spotNodeLabelKey: spotNodeLabelValue}},
		Spec:       corev1.NodeSpec{Unschedulable: unschedulable},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}}},
	}
}

func TestCheckSpotCapacity(t *testing.T) {
	testCases := []struct {
		name    string
		objects []client.Object
		want    bool
	}{
		{
			name: "Spot node group in Backoff (ZONE_RESOURCE_POOL_EXHAUSTED) returns false",
			objects: []client.Object{autoscalerStatusConfigMap(`
nodeGroups:
- name: gke-cluster-spot-pool-1234-grp
  health:
    status: Healthy
  scaleUp:
    status: Backoff
    backoffInfo:
      errorCode: ZONE_RESOURCE_POOL_EXHAUSTED
      errorMessage: "Zone resource pool exhausted"
`)},
			want: false,
		},
		{
			name: "Spot node group out of Backoff and Healthy returns true",
			objects: []client.Object{autoscalerStatusConfigMap(`
nodeGroups:
- name: gke-cluster-spot-pool-1234-grp
  health:
    status: Healthy
  scaleUp:
    status: NoActivity
`)},
			want: true,
		},
		{
			name: "Multiple Spot node groups with one in Backoff and one Healthy returns true",
			objects: []client.Object{autoscalerStatusConfigMap(`
nodeGroups:
- name: gke-cluster-spot-pool-zone-a-grp
  health:
    status: Healthy
  scaleUp:
    status: Backoff
    backoffInfo:
      errorCode: ZONE_RESOURCE_POOL_EXHAUSTED
- name: gke-cluster-spot-pool-zone-b-grp
  health:
    status: Healthy
  scaleUp:
    status: NoActivity
`)},
			want: true,
		},
		{
			name: "Spot node group with a backoff error code but no Backoff status returns false",
			objects: []client.Object{autoscalerStatusConfigMap(`
nodeGroups:
- name: gke-cluster-spot-pool-1234-grp
  health:
    status: Healthy
  scaleUp:
    status: NoActivity
    backoffInfo:
      errorCode: QUOTA_EXCEEDED
`)},
			want: false,
		},
		{
			name: "Spot node group with Unhealthy status returns false",
			objects: []client.Object{autoscalerStatusConfigMap(`
nodeGroups:
- name: gke-cluster-spot-pool-1234-grp
  health:
    status: Unhealthy
  scaleUp:
    status: NoActivity
`)},
			want: false,
		},
		{
			name: "Unavailable Spot node group takes precedence over Ready Spot nodes",
			objects: []client.Object{
				autoscalerStatusConfigMap(`
nodeGroups:
- name: gke-cluster-spot-pool-1234-grp
  health:
    status: Unhealthy
`),
				spotNode("spot-node-1", true, false),
			},
			want: false,
		},
		{
			name: "Falls back to Ready Spot node when ConfigMap has no Spot node groups",
			objects: []client.Object{
				autoscalerStatusConfigMap(`
nodeGroups:
- name: gke-cluster-default-pool-1234-grp
  health:
    status: Healthy
  scaleUp:
    status: NoActivity
`),
				spotNode("spot-node-1", true, false),
			},
			want: true,
		},
		{
			name: "Falls back to Ready Spot node when ConfigMap status is unparsable",
			objects: []client.Object{
				autoscalerStatusConfigMap("nodeGroups: [this is: not valid"),
				spotNode("spot-node-1", true, false),
			},
			want: true,
		},
		{
			name: "Falls back to Ready Spot node when ConfigMap status is empty",
			objects: []client.Object{
				autoscalerStatusConfigMap(""),
				spotNode("spot-node-1", true, false),
			},
			want: true,
		},
		{
			name:    "ConfigMap missing and Spot node NotReady returns false",
			objects: []client.Object{spotNode("spot-node-not-ready", false, false)},
			want:    false,
		},
		{
			name:    "ConfigMap missing and Spot node unschedulable returns false",
			objects: []client.Object{spotNode("spot-node-cordoned", true, true)},
			want:    false,
		},
		{
			name: "ConfigMap missing and no Spot nodes returns false",
			want: false,
		},
	}

	scheme := newSpotCapacityScheme()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := &WorkloadClassReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build(),
				Scheme: scheme,
			}

			got, err := r.checkSpotCapacity(context.Background())
			if err != nil {
				t.Fatalf("checkSpotCapacity() unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("checkSpotCapacity() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTargetsSpot(t *testing.T) {
	testCases := []struct {
		name string
		cs   *workloadsv1.CapacityStrategy
		want bool
	}{
		{name: "nil CapacityStrategy", cs: nil, want: false},
		{name: "unset Type defaults to Spot", cs: &workloadsv1.CapacityStrategy{}, want: true},
		{name: "Spot", cs: &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeSpot}, want: true},
		{name: "OnDemand", cs: &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand}, want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := makeWC("wc", metav1.Now())
			wc.Spec.CapacityStrategy = tc.cs
			if got := targetsSpot(wc); got != tc.want {
				t.Errorf("targetsSpot() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFindWorkloadClassesForClusterAutoscalerStatus(t *testing.T) {
	scheme := newSpotCapacityScheme()

	spotWC := makeWC("spot-wc", metav1.Now())
	spotWC.Spec.CapacityStrategy = &workloadsv1.CapacityStrategy{}
	onDemandWC := makeWC("on-demand-wc", metav1.Now())
	onDemandWC.Spec.CapacityStrategy = &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand}
	noStrategyWC := makeWC("no-strategy-wc", metav1.Now())

	r := &WorkloadClassReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(spotWC, onDemandWC, noStrategyWC).Build(),
		Scheme: scheme,
	}

	reqs := r.findWorkloadClassesForClusterAutoscalerStatus(context.Background(), autoscalerStatusConfigMap(""))
	if len(reqs) != 1 || reqs[0].Name != spotWC.Name || reqs[0].Namespace != spotWC.Namespace {
		t.Errorf("expected a single reconcile request for %s/%s, got %v", spotWC.Namespace, spotWC.Name, reqs)
	}

	otherCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "other-cm", Namespace: "default"}}
	if reqs := r.findWorkloadClassesForClusterAutoscalerStatus(context.Background(), otherCM); len(reqs) != 0 {
		t.Errorf("expected 0 reconcile requests for unrelated ConfigMap, got %d", len(reqs))
	}

	sameNameOtherNS := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ClusterAutoscalerStatusName, Namespace: "default"}}
	if reqs := r.findWorkloadClassesForClusterAutoscalerStatus(context.Background(), sameNameOtherNS); len(reqs) != 0 {
		t.Errorf("expected 0 reconcile requests for %s in another namespace, got %d", ClusterAutoscalerStatusName, len(reqs))
	}
}
