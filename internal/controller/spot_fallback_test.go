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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func newFallbackScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))
	return scheme
}

// spotTargetedPod returns a running Pod that carries the Spot toleration (as mutated by the Pod placement
// webhook for Spot), scheduled on nodeName.
func spotTargetedPod(name, nodeName string) *corev1.Pod {
	pod := newReversionPod(name, nodeName, time.Now().Add(-time.Hour))
	pod.Spec.Tolerations = []corev1.Toleration{{
		Key: spotNodeLabelKey, Operator: corev1.TolerationOpEqual, Value: spotNodeLabelValue, Effect: corev1.TaintEffectNoSchedule,
	}}
	return pod
}

// onDemandPod returns a running Pod without Spot targeting (placed on On-Demand on purpose), scheduled on nodeName.
func onDemandPod(name, nodeName string) *corev1.Pod {
	return newReversionPod(name, nodeName, time.Now().Add(-time.Hour))
}

// inFallbackTrue returns a new InFallback=True condition, as set when Pods have fallen back to On-Demand.
func inFallbackTrue() *metav1.Condition {
	return &metav1.Condition{
		Type:   workloadsv1.ConditionTypeInFallback,
		Status: metav1.ConditionTrue,
		Reason: workloadsv1.ReasonFallbackActive,
	}
}

func TestReconcileFallbackState(t *testing.T) {
	scheme := newFallbackScheme()
	spotNodeObj := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "spot-node-1", Labels: map[string]string{spotNodeLabelKey: spotNodeLabelValue},
	}}
	onDemandNodeObj := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ondemand-node-1"}}

	fallbackCS := func(ratio string) *workloadsv1.CapacityStrategy {
		return &workloadsv1.CapacityStrategy{SpotRatio: ratio, FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}
	}
	none := workloadsv1.SpotReversionPolicy{Action: workloadsv1.NoneReversionAction}
	lazy := workloadsv1.SpotReversionPolicy{Action: workloadsv1.LazyReversionAction}

	testCases := []struct {
		name string
		cs   *workloadsv1.CapacityStrategy
		// reversion is the GKESpotPlacementPolicy's reversion; nil means no policy object exists.
		reversion *workloadsv1.SpotReversionPolicy
		noRef     bool
		// existing is the InFallback condition already on the WorkloadClass, if any.
		existing    *metav1.Condition
		notValid    bool
		pods        []client.Object
		wantChanged bool
		// wantStatus is the expected InFallback status; "" means the condition must be absent.
		wantStatus metav1.ConditionStatus
	}{
		{
			name:        "Spot-targeted Pod on an On-Demand node sets InFallback=True",
			cs:          fallbackCS("100%"),
			reversion:   &lazy,
			pods:        []client.Object{spotTargetedPod("spot-1", "spot-node-1"), spotTargetedPod("fallback-1", "ondemand-node-1")},
			wantChanged: true,
			wantStatus:  metav1.ConditionTrue,
		},
		{
			name:        "no fallback Pods sets InFallback=False",
			cs:          fallbackCS("100%"),
			reversion:   &lazy,
			pods:        []client.Object{spotTargetedPod("spot-1", "spot-node-1")},
			wantChanged: true,
			wantStatus:  metav1.ConditionFalse,
		},
		{
			name:        "Pods placed on On-Demand to meet SpotRatio are not fallback",
			cs:          fallbackCS("50%"),
			reversion:   &none,
			pods:        []client.Object{spotTargetedPod("spot-1", "spot-node-1"), onDemandPod("ratio-od-1", "ondemand-node-1")},
			wantChanged: true,
			wantStatus:  metav1.ConditionFalse,
		},
		{
			name:      "scale-up with Pending Pods does not register as fallback",
			cs:        fallbackCS("80%"),
			reversion: &none,
			pods: []client.Object{
				spotTargetedPod("spot-1", "spot-node-1"),
				spotTargetedPod("spot-2", "spot-node-1"),
				spotTargetedPod("spot-3", "spot-node-1"),
				spotTargetedPod("spot-4", "spot-node-1"),
				onDemandPod("ratio-od-1", "ondemand-node-1"),
				spotTargetedPod("pending-1", ""),
				spotTargetedPod("pending-2", ""),
			},
			wantChanged: true,
			wantStatus:  metav1.ConditionFalse,
		},
		{
			name:        "None latches InFallback=True while the workload has active Pods",
			cs:          fallbackCS("100%"),
			reversion:   &none,
			existing:    inFallbackTrue(),
			pods:        []client.Object{onDemandPod("pinned-od-1", "ondemand-node-1")},
			wantChanged: false,
			wantStatus:  metav1.ConditionTrue,
		},
		{
			name:        "unset reversion action defaults to None and latches",
			cs:          fallbackCS("100%"),
			reversion:   &workloadsv1.SpotReversionPolicy{},
			existing:    inFallbackTrue(),
			pods:        []client.Object{onDemandPod("pinned-od-1", "ondemand-node-1")},
			wantChanged: false,
			wantStatus:  metav1.ConditionTrue,
		},
		{
			name:        "None releases the latch when the workload scales to zero",
			cs:          fallbackCS("100%"),
			reversion:   &none,
			existing:    inFallbackTrue(),
			wantChanged: true,
			wantStatus:  metav1.ConditionFalse,
		},
		{
			name:        "Lazy clears InFallback once no fallback Pods remain",
			cs:          fallbackCS("100%"),
			reversion:   &lazy,
			existing:    inFallbackTrue(),
			pods:        []client.Object{spotTargetedPod("spot-1", "spot-node-1")},
			wantChanged: true,
			wantStatus:  metav1.ConditionFalse,
		},
		{
			name:        "without a profile ref reversion is Lazy, so InFallback is not latched",
			cs:          fallbackCS("100%"),
			noRef:       true,
			existing:    inFallbackTrue(),
			pods:        []client.Object{onDemandPod("od-1", "ondemand-node-1")},
			wantChanged: true,
			wantStatus:  metav1.ConditionFalse,
		},
		{
			name:        "missing policy is treated as Lazy",
			cs:          fallbackCS("100%"),
			existing:    inFallbackTrue(),
			pods:        []client.Object{onDemandPod("od-1", "ondemand-node-1")},
			wantChanged: true,
			wantStatus:  metav1.ConditionFalse,
		},
		{
			name:        "already in fallback with fallback Pods is unchanged",
			cs:          fallbackCS("100%"),
			reversion:   &lazy,
			existing:    &metav1.Condition{Type: workloadsv1.ConditionTypeInFallback, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonFallbackActive, Message: fallbackActiveMsg},
			pods:        []client.Object{spotTargetedPod("fallback-1", "ondemand-node-1")},
			wantChanged: false,
			wantStatus:  metav1.ConditionTrue,
		},
		{
			name:        "not validated leaves the condition untouched",
			cs:          fallbackCS("100%"),
			reversion:   &lazy,
			existing:    inFallbackTrue(),
			notValid:    true,
			pods:        []client.Object{spotTargetedPod("spot-1", "spot-node-1")},
			wantChanged: false,
			wantStatus:  metav1.ConditionTrue,
		},
		{
			name:        "FallbackAction Fail removes the condition",
			cs:          &workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFail},
			reversion:   &none,
			existing:    inFallbackTrue(),
			wantChanged: true,
		},
		{
			name:        "Type OnDemand removes the condition",
			cs:          &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand, FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand},
			reversion:   &none,
			existing:    inFallbackTrue(),
			wantChanged: true,
		},
		{
			name:        "no CapacityStrategy and no condition is a no-op",
			wantChanged: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := newReversionWC(time.Now(), tc.cs)
			if tc.noRef {
				wc.Spec.InfrastructureProfileRef = nil
				wc.Status.Conditions = nil
			}
			if tc.existing != nil {
				wc.Status.Conditions = append(wc.Status.Conditions, *tc.existing)
			}

			objects := append([]client.Object{spotNodeObj.DeepCopy(), onDemandNodeObj.DeepCopy()}, tc.pods...)
			if tc.reversion != nil {
				objects = append(objects, newReversionPolicy(*tc.reversion))
			}
			r := &WorkloadClassReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
				Scheme: scheme,
			}

			changed, err := r.reconcileFallbackState(context.Background(), wc, !tc.notValid)
			if err != nil {
				t.Fatalf("reconcileFallbackState() unexpected error: %v", err)
			}
			if changed != tc.wantChanged {
				t.Errorf("reconcileFallbackState() changed = %v, want %v", changed, tc.wantChanged)
			}

			cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeInFallback)
			switch {
			case tc.wantStatus == "" && cond != nil:
				t.Errorf("expected InFallback condition to be absent, got %+v", *cond)
			case tc.wantStatus != "" && cond == nil:
				t.Errorf("expected InFallback=%s, got no condition", tc.wantStatus)
			case tc.wantStatus != "" && cond.Status != tc.wantStatus:
				t.Errorf("InFallback = %s, want %s", cond.Status, tc.wantStatus)
			}
		})
	}
}

func TestIsPodTargetedForSpot(t *testing.T) {
	if !isPodTargetedForSpot(spotTargetedPod("p", "")) {
		t.Errorf("expected Pod with Spot toleration to be Spot-targeted")
	}
	selectorPod := onDemandPod("p", "")
	selectorPod.Spec.NodeSelector = map[string]string{spotNodeLabelKey: spotNodeLabelValue}
	if !isPodTargetedForSpot(selectorPod) {
		t.Errorf("expected Pod with Spot nodeSelector to be Spot-targeted")
	}
	otherToleration := onDemandPod("p", "")
	otherToleration.Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
	if isPodTargetedForSpot(otherToleration) {
		t.Errorf("expected Pod without Spot targeting not to be Spot-targeted")
	}
}

func TestPodPlacementChangePredicate(t *testing.T) {
	p := podPlacementChangePredicate()
	pending := onDemandPod("p", "")
	bound := onDemandPod("p", "node-1")
	terminating := bound.DeepCopy()
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	succeeded := bound.DeepCopy()
	succeeded.Status.Phase = corev1.PodSucceeded
	relabeled := bound.DeepCopy()
	relabeled.Labels["extra"] = "label"

	if p.Create(event.CreateEvent{Object: pending}) {
		t.Errorf("expected create of an unscheduled Pod to be filtered")
	}
	if !p.Create(event.CreateEvent{Object: bound}) {
		t.Errorf("expected create of a scheduled Pod to pass")
	}
	updateCases := []struct {
		name     string
		old, new *corev1.Pod
		want     bool
	}{
		{name: "binding to a node passes", old: pending, new: bound, want: true},
		{name: "starting to terminate passes", old: bound, new: terminating, want: true},
		{name: "reaching a terminal phase passes", old: bound, new: succeeded, want: true},
		{name: "unrelated update is filtered", old: bound, new: relabeled, want: false},
	}
	for _, tc := range updateCases {
		if got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
			t.Errorf("%s: Update() = %v, want %v", tc.name, got, tc.want)
		}
	}
	if !p.Delete(event.DeleteEvent{Object: bound}) {
		t.Errorf("expected delete to pass")
	}
}

func TestFindWorkloadClassForPod(t *testing.T) {
	scheme := newFallbackScheme()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}

	tracked := newReversionWC(time.Now(), &workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand})
	r := &WorkloadClassReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, tracked).Build(),
		Scheme: scheme,
	}
	reqs := r.findWorkloadClassForPod(context.Background(), onDemandPod("p", "node-1"))
	if len(reqs) != 1 || reqs[0].Name != tracked.Name {
		t.Errorf("expected a reconcile request for %s, got %v", tracked.Name, reqs)
	}

	untracked := newReversionWC(time.Now(), &workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFail})
	r.Client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, untracked).Build()
	if reqs := r.findWorkloadClassForPod(context.Background(), onDemandPod("p", "node-1")); len(reqs) != 0 {
		t.Errorf("expected no reconcile requests for a WorkloadClass that doesn't track fallback, got %v", reqs)
	}

	unmatched := onDemandPod("p", "node-1")
	unmatched.Labels = map[string]string{"app": "other"}
	if reqs := r.findWorkloadClassForPod(context.Background(), unmatched); len(reqs) != 0 {
		t.Errorf("expected no reconcile requests for a Pod matching no WorkloadClass, got %v", reqs)
	}
}
