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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const testPolicyName = "spot-policy"

var (
	spotNodeObj     = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "spot-node-1", Labels: map[string]string{utils.SpotLabelKey: utils.SpotLabelValue}}}
	onDemandNodeObj = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ondemand-node-1"}}
)

// newWorkloadPod returns a running Pod selected by makeWC, owned by a ReplicaSet, scheduled on nodeName
// (unscheduled if empty) and started at startTime.
func newWorkloadPod(name, nodeName string, startTime time.Time) *corev1.Pod {
	t := metav1.NewTime(startTime)
	isController := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			Labels:            map[string]string{"app": "test"},
			CreationTimestamp: t,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "test-rs", UID: "rs-uid", Controller: &isController,
			}},
		},
		Spec:   corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, StartTime: &t},
	}
}

// spotTargetedPod returns a Pod carrying the Spot toleration (as mutated by the webhook for Spot), scheduled on nodeName.
func spotTargetedPod(name, nodeName string) *corev1.Pod {
	pod := newWorkloadPod(name, nodeName, time.Now().Add(-time.Hour))
	pod.Spec.Tolerations = []corev1.Toleration{{
		Key: utils.SpotLabelKey, Operator: corev1.TolerationOpEqual, Value: utils.SpotLabelValue, Effect: corev1.TaintEffectNoSchedule,
	}}
	return pod
}

// onDemandPod returns a Pod without Spot targeting (placed on On-Demand on purpose), scheduled on nodeName.
func onDemandPod(name, nodeName string) *corev1.Pod {
	return newWorkloadPod(name, nodeName, time.Now().Add(-time.Hour))
}

// fallbackCS returns a Spot CapacityStrategy with the given ratio that allows falling back to On-Demand.
func fallbackCS(ratio string) *workloadsv1.CapacityStrategy {
	return &workloadsv1.CapacityStrategy{SpotRatio: ratio, FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}
}

// newManagedWC returns a validated WorkloadClass with the given CapacityStrategy that references testPolicyName
// and has the policy attached.
func newManagedWC(cs *workloadsv1.CapacityStrategy) *workloadsv1.WorkloadClass {
	wc := makeWC("spot-wc", metav1.NewTime(time.Now().Add(-2*time.Hour)))
	wc.Spec.CapacityStrategy = cs
	wc.Spec.InfrastructureProfileRef = gkePolicyRef()
	wc.Status.Conditions = []metav1.Condition{
		{Type: workloadsv1.ConditionTypeValidated, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonValidationPassed},
		{Type: workloadsv1.ConditionTypePlacementPluginAttached, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonProfileResolved},
	}
	return wc
}

func newPolicy(reversion workloadsv1.SpotReversionPolicy) *workloadsv1.GKESpotPlacementPolicy {
	return &workloadsv1.GKESpotPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: testPolicyName},
		Spec:       workloadsv1.GKESpotPlacementPolicySpec{Reversion: reversion},
	}
}

func inFallbackTrue() metav1.Condition {
	return metav1.Condition{Type: workloadsv1.ConditionTypeInFallback, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonFallbackActive}
}

func setPluginPending(wc *workloadsv1.WorkloadClass) {
	meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
		Type: workloadsv1.ConditionTypePlacementPluginAttached, Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonPluginPending,
	})
	meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
		Type: workloadsv1.ConditionTypeValidated, Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonValidationFailed,
	})
}

// inFallbackStatus returns the persisted InFallback status for the WorkloadClass, or "" if absent.
func inFallbackStatus(t *testing.T, c client.Client, wc *workloadsv1.WorkloadClass) metav1.ConditionStatus {
	t.Helper()
	latest := &workloadsv1.WorkloadClass{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(wc), latest); err != nil {
		t.Fatalf("failed to get WorkloadClass: %v", err)
	}
	cond := meta.FindStatusCondition(latest.Status.Conditions, workloadsv1.ConditionTypeInFallback)
	if cond == nil {
		return ""
	}
	return cond.Status
}

func TestReconcile_InFallback(t *testing.T) {
	scheme := newGKESpotScheme()
	none := workloadsv1.SpotReversionPolicy{Action: workloadsv1.NoneReversionAction}
	lazy := workloadsv1.SpotReversionPolicy{Action: workloadsv1.LazyReversionAction}

	testCases := []struct {
		name string
		cs   *workloadsv1.CapacityStrategy
		// reversion is the policy's reversion; nil means no policy object exists.
		reversion *workloadsv1.SpotReversionPolicy
		mutateWC  func(wc *workloadsv1.WorkloadClass)
		pods      []client.Object
		// want is the expected persisted InFallback status; "" means absent.
		want metav1.ConditionStatus
	}{
		{
			name: "Spot-targeted Pod on an On-Demand node sets InFallback=True",
			cs:   fallbackCS("100%"), reversion: &lazy,
			pods: []client.Object{spotTargetedPod("spot-1", "spot-node-1"), spotTargetedPod("fallback-1", "ondemand-node-1")},
			want: metav1.ConditionTrue,
		},
		{
			name: "no fallback Pods sets InFallback=False",
			cs:   fallbackCS("100%"), reversion: &lazy,
			pods: []client.Object{spotTargetedPod("spot-1", "spot-node-1")},
			want: metav1.ConditionFalse,
		},
		{
			name: "Pods placed on On-Demand to meet SpotRatio are not fallback",
			cs:   fallbackCS("50%"), reversion: &none,
			pods: []client.Object{spotTargetedPod("spot-1", "spot-node-1"), onDemandPod("ratio-od-1", "ondemand-node-1")},
			want: metav1.ConditionFalse,
		},
		{
			name: "scale-up with Pending Pods does not register as fallback",
			cs:   fallbackCS("80%"), reversion: &none,
			pods: []client.Object{
				spotTargetedPod("spot-1", "spot-node-1"), onDemandPod("ratio-od-1", "ondemand-node-1"),
				spotTargetedPod("pending-1", ""), spotTargetedPod("pending-2", ""),
			},
			want: metav1.ConditionFalse,
		},
		{
			name: "None latches InFallback=True while the workload has active Pods",
			cs:   fallbackCS("100%"), reversion: &none,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			pods: []client.Object{onDemandPod("pinned-od-1", "ondemand-node-1")},
			want: metav1.ConditionTrue,
		},
		{
			name: "unset reversion action defaults to None and latches",
			cs:   fallbackCS("100%"), reversion: &workloadsv1.SpotReversionPolicy{},
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			pods: []client.Object{onDemandPod("pinned-od-1", "ondemand-node-1")},
			want: metav1.ConditionTrue,
		},
		{
			name: "None releases the latch when the workload scales to zero",
			cs:   fallbackCS("100%"), reversion: &none,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			want: metav1.ConditionFalse,
		},
		{
			name: "Lazy clears InFallback once no fallback Pods remain",
			cs:   fallbackCS("100%"), reversion: &lazy,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			pods: []client.Object{spotTargetedPod("spot-1", "spot-node-1")},
			want: metav1.ConditionFalse,
		},
		{
			name: "without a profile ref reversion is Lazy, so InFallback is not latched",
			cs:   fallbackCS("100%"),
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Spec.InfrastructureProfileRef = nil
				wc.Status.Conditions = []metav1.Condition{
					{Type: workloadsv1.ConditionTypeValidated, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonNoGuardrails},
					inFallbackTrue(),
				}
			},
			pods: []client.Object{onDemandPod("od-1", "ondemand-node-1")},
			want: metav1.ConditionFalse,
		},
		{
			name: "missing policy is treated as Lazy",
			cs:   fallbackCS("100%"),
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			pods: []client.Object{onDemandPod("od-1", "ondemand-node-1")},
			want: metav1.ConditionFalse,
		},
		{
			name: "FallbackAction Fail removes the condition",
			cs:   &workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFail}, reversion: &none,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			want: "",
		},
		{
			name: "Type OnDemand removes the condition",
			cs:   &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand, FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand}, reversion: &none,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			want: "",
		},
		{
			name: "leaving the plugin's scope removes the condition",
			cs:   fallbackCS("100%"), reversion: &none,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{Kind: "KarpenterSpotPolicy", Name: "k"}
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			want: "",
		},
		{
			name: "guardrail violation (not validated) removes the condition, releasing a None latch",
			cs:   fallbackCS("100%"), reversion: &none,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
					Type: workloadsv1.ConditionTypeValidated, Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonValidationFailed,
				})
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			pods: []client.Object{onDemandPod("pinned-od-1", "ondemand-node-1")},
			want: "",
		},
		{
			name: "policy not attached (resolution failed) removes the condition",
			cs:   fallbackCS("100%"), reversion: &none,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
					Type: workloadsv1.ConditionTypePlacementPluginAttached, Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonPluginResolutionFailed,
				})
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			want: "",
		},
		{
			name: "plugin pending after a generation bump keeps a None latch",
			cs:   fallbackCS("100%"), reversion: &none,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				setPluginPending(wc)
				wc.Status.Conditions = append(wc.Status.Conditions, inFallbackTrue())
			},
			pods: []client.Object{onDemandPod("pinned-od-1", "ondemand-node-1")},
			want: metav1.ConditionTrue,
		},
		{
			name: "plugin pending does not set the condition either",
			cs:   fallbackCS("100%"), reversion: &lazy,
			mutateWC: setPluginPending,
			pods:     []client.Object{spotTargetedPod("fallback-1", "ondemand-node-1")},
			want:     "",
		},
		{
			name: "overlapping selector ignores Pods owned by a more specific WorkloadClass",
			cs:   fallbackCS("100%"), reversion: &lazy,
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				// Catch-all selector; specific-wc has a more specific selector {"app": "test"}.
				wc.Spec.PodSelector = nil
			},
			pods: []client.Object{
				func() *workloadsv1.WorkloadClass {
					specific := newManagedWC(fallbackCS("100%"))
					specific.Name = "specific-wc"
					return specific
				}(),
				spotTargetedPod("fallback-1", "ondemand-node-1"),
			},
			want: metav1.ConditionFalse,
		},
		{
			name: "namespace-default WorkloadClass counts Pods outside its PodSelector",
			cs:   fallbackCS("100%"), reversion: &lazy,
			pods: []client.Object{
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
					Name: "default", Labels: map[string]string{"workloads.gke.io/default-class": "spot-wc"},
				}},
				func() *corev1.Pod {
					// Not matched by spot-wc's {"app": "test"} selector, but it belongs to the namespace default.
					pod := spotTargetedPod("fallback-1", "ondemand-node-1")
					pod.Labels = map[string]string{"app": "other"}
					return pod
				}(),
			},
			want: metav1.ConditionTrue,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := newManagedWC(tc.cs)
			if tc.mutateWC != nil {
				tc.mutateWC(wc)
			}
			objects := append([]client.Object{spotNodeObj.DeepCopy(), onDemandNodeObj.DeepCopy(), wc}, tc.pods...)
			if tc.reversion != nil {
				objects = append(objects, newPolicy(*tc.reversion))
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(wc).Build()
			r := &GKESpotPlacementReconciler{Client: c, Scheme: scheme}

			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(wc)}); err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}
			if got := inFallbackStatus(t, c, wc); got != tc.want {
				t.Errorf("InFallback = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSetFallbackCondition_OnlyTouchesInFallback(t *testing.T) {
	scheme := newGKESpotScheme()
	wc := newManagedWC(fallbackCS("100%"))
	wc.Status.MaintenanceReadiness = workloadsv1.ReadinessNotReady
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).WithStatusSubresource(wc).Build()
	r := &GKESpotPlacementReconciler{Client: c, Scheme: scheme}

	cond := inFallbackTrue()
	if err := r.setFallbackCondition(context.Background(), wc, &cond); err != nil {
		t.Fatalf("setFallbackCondition() unexpected error: %v", err)
	}

	latest := &workloadsv1.WorkloadClass{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(wc), latest); err != nil {
		t.Fatalf("failed to get WorkloadClass: %v", err)
	}
	if latest.Status.MaintenanceReadiness != workloadsv1.ReadinessNotReady {
		t.Errorf("MaintenanceReadiness changed to %q", latest.Status.MaintenanceReadiness)
	}
	for _, ct := range []string{workloadsv1.ConditionTypeValidated, workloadsv1.ConditionTypePlacementPluginAttached, workloadsv1.ConditionTypeInFallback} {
		if !meta.IsStatusConditionTrue(latest.Status.Conditions, ct) {
			t.Errorf("expected %s=True after update", ct)
		}
	}
	if !meta.IsStatusConditionTrue(wc.Status.Conditions, workloadsv1.ConditionTypeInFallback) {
		t.Errorf("expected the in-memory WorkloadClass to reflect the update")
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
	scheme := newGKESpotScheme()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	pod := onDemandPod("p", "node-1")

	testCases := []struct {
		name string
		wc   *workloadsv1.WorkloadClass
		pod  *corev1.Pod
		want int
	}{
		{name: "tracked WorkloadClass is enqueued", wc: newManagedWC(fallbackCS("100%")), pod: pod, want: 1},
		{name: "FallbackAction Fail is not enqueued", wc: newManagedWC(&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFail}), pod: pod, want: 0},
		{name: "other plugin's WorkloadClass is not enqueued", wc: func() *workloadsv1.WorkloadClass {
			wc := newManagedWC(fallbackCS("100%"))
			wc.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{Kind: "KarpenterSpotPolicy", Name: "k"}
			return wc
		}(), pod: pod, want: 0},
		{name: "Pod matching no WorkloadClass is not enqueued", wc: newManagedWC(fallbackCS("100%")), pod: func() *corev1.Pod {
			p := onDemandPod("p", "node-1")
			p.Labels = map[string]string{"app": "other"}
			return p
		}(), want: 0},
		{name: "namespace without WorkloadClasses is skipped", wc: nil, pod: pod, want: 0},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objs := []client.Object{ns}
			if tc.wc != nil {
				objs = append(objs, tc.wc)
			}
			r := &GKESpotPlacementReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(), Scheme: scheme}
			if got := len(r.findWorkloadClassForPod(context.Background(), tc.pod)); got != tc.want {
				t.Errorf("findWorkloadClassForPod() returned %d requests, want %d", got, tc.want)
			}
		})
	}
}

func TestFindWorkloadClassesForPolicy(t *testing.T) {
	scheme := newGKESpotScheme()

	matchingWC := newManagedWC(fallbackCS("100%"))
	failWC := newManagedWC(&workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFail})
	failWC.Name = "fail-wc"
	otherPolicyWC := newManagedWC(fallbackCS("100%"))
	otherPolicyWC.Name = "other-policy-wc"
	otherPolicyWC.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{Kind: "GKESpotPlacementPolicy", Name: "other-policy"}
	noRefWC := newManagedWC(fallbackCS("100%"))
	noRefWC.Name = "no-ref-wc"
	noRefWC.Spec.InfrastructureProfileRef = nil

	r := &GKESpotPlacementReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(matchingWC, failWC, otherPolicyWC, noRefWC).Build(),
		Scheme: scheme,
	}

	policy := newPolicy(workloadsv1.SpotReversionPolicy{Action: workloadsv1.ActiveReversionAction})
	reqs := r.findWorkloadClassesForPolicy(context.Background(), policy)
	if len(reqs) != 1 || reqs[0].Name != matchingWC.Name || reqs[0].Namespace != matchingWC.Namespace {
		t.Errorf("expected a single reconcile request for %s/%s, got %v", matchingWC.Namespace, matchingWC.Name, reqs)
	}

	if reqs := r.findWorkloadClassesForPolicy(context.Background(), &workloadsv1.GKESpotPlacementPolicy{ObjectMeta: metav1.ObjectMeta{Name: "unused"}}); len(reqs) != 0 {
		t.Errorf("expected 0 reconcile requests for unused policy, got %d", len(reqs))
	}
	if reqs := r.findWorkloadClassesForPolicy(context.Background(), nil); len(reqs) != 0 {
		t.Errorf("expected 0 reconcile requests for nil object, got %d", len(reqs))
	}
}
