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
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const reversionPolicyName = "spot-policy"

// newReversionPod returns a running Pod selected by makeWC, owned by a ReplicaSet, scheduled on nodeName.
func newReversionPod(name, nodeName string, startTime time.Time) *corev1.Pod {
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

func newReversionWC(now time.Time, cs *workloadsv1.CapacityStrategy) *workloadsv1.WorkloadClass {
	wc := makeWC("reversion-wc", metav1.NewTime(now.Add(-2*time.Hour)))
	wc.Spec.CapacityStrategy = cs
	wc.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{
		Group: workloadsv1.GroupVersion.Group,
		Kind:  workloadsv1.GKESpotPlacementPolicyKind,
		Name:  reversionPolicyName,
	}
	wc.Status.Conditions = []metav1.Condition{{
		Type:   workloadsv1.ConditionTypePlacementPluginAttached,
		Status: metav1.ConditionTrue,
		Reason: workloadsv1.ReasonProfileResolved,
	}}
	return wc
}

func newReversionPolicy(reversion workloadsv1.SpotReversionPolicy) *workloadsv1.GKESpotPlacementPolicy {
	return &workloadsv1.GKESpotPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: reversionPolicyName},
		Spec:       workloadsv1.GKESpotPlacementPolicySpec{Reversion: reversion},
	}
}

func TestReconcileSpotReversion(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(policyv1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))

	// Friday, 15:00 UTC.
	now := time.Date(2026, 9, 18, 15, 0, 0, 0, time.UTC)
	hourAgo := now.Add(-time.Hour)
	spotNodeObj := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "spot-node-1", Labels: map[string]string{spotNodeLabelKey: spotNodeLabelValue},
	}}
	onDemandNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ondemand-node-1"}}

	active := workloadsv1.SpotReversionPolicy{Action: workloadsv1.ActiveReversionAction}
	spot100 := &workloadsv1.CapacityStrategy{SpotRatio: "100%"}

	testCases := []struct {
		name string
		cs   *workloadsv1.CapacityStrategy
		// reversion is the GKESpotPlacementPolicy's reversion; nil means no policy object exists.
		reversion       *workloadsv1.SpotReversionPolicy
		mutateWC        func(wc *workloadsv1.WorkloadClass)
		windows         []workloadsv1.DisruptionWindow
		rejectEvictions bool
		pods            []client.Object
		wantRemaining   []string
		wantRequeue     time.Duration
		// wantEvictAttempts is only checked when rejectEvictions is set.
		wantEvictAttempts int
	}{
		{
			name:      "Active with 100% spotRatio evicts On-Demand Pods and keeps Spot Pods",
			cs:        spot100,
			reversion: &active,
			pods: []client.Object{
				newReversionPod("spot-pod-1", "spot-node-1", hourAgo),
				newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo),
			},
			wantRemaining: []string{"spot-pod-1"},
		},
		{
			name: "Active respects MinDurationOnFallback and requeues for the remaining time",
			cs:   spot100,
			reversion: &workloadsv1.SpotReversionPolicy{
				Action:                workloadsv1.ActiveReversionAction,
				MinDurationOnFallback: &metav1.Duration{Duration: 30 * time.Minute},
			},
			pods: []client.Object{
				newReversionPod("young-ondemand-pod", "ondemand-node-1", now.Add(-10*time.Minute)),
				newReversionPod("mature-ondemand-pod", "ondemand-node-1", now.Add(-45*time.Minute)),
			},
			wantRemaining: []string{"young-ondemand-pod"},
			wantRequeue:   20 * time.Minute,
		},
		{
			name:      "Active with 80% spotRatio evicts only enough On-Demand Pods to reach 80%",
			cs:        &workloadsv1.CapacityStrategy{SpotRatio: "80%"},
			reversion: &active,
			pods: []client.Object{
				newReversionPod("spot-pod-1", "spot-node-1", hourAgo),
				newReversionPod("spot-pod-2", "spot-node-1", hourAgo),
				newReversionPod("spot-pod-3", "spot-node-1", hourAgo),
				newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo),
				newReversionPod("ondemand-pod-2", "ondemand-node-1", hourAgo),
			},
			wantRemaining: []string{"ondemand-pod-2", "spot-pod-1", "spot-pod-2", "spot-pod-3"},
		},
		{
			name:      "Active skips Pods without a controller owner",
			cs:        spot100,
			reversion: &active,
			pods: func() []client.Object {
				bare := newReversionPod("bare-ondemand-pod", "ondemand-node-1", hourAgo)
				bare.OwnerReferences = nil
				return []client.Object{bare, newReversionPod("owned-ondemand-pod", "ondemand-node-1", hourAgo)}
			}(),
			wantRemaining: []string{"bare-ondemand-pod"},
		},
		{
			name:      "Unscheduled Pods count towards the total but are not evicted",
			cs:        &workloadsv1.CapacityStrategy{SpotRatio: "50%"},
			reversion: &active,
			pods: []client.Object{
				newReversionPod("pending-pod", "", hourAgo),
				newReversionPod("spot-pod-1", "spot-node-1", hourAgo),
				newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo),
			},
			// 1 Spot of 3 active is below 50%, so the On-Demand Pod is evicted.
			wantRemaining: []string{"pending-pod", "spot-pod-1"},
		},
		{
			name:          "Lazy reversion does not evict",
			cs:            spot100,
			reversion:     &workloadsv1.SpotReversionPolicy{Action: workloadsv1.LazyReversionAction},
			pods:          []client.Object{newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo)},
			wantRemaining: []string{"ondemand-pod-1"},
		},
		{
			name:          "None reversion does not evict",
			cs:            spot100,
			reversion:     &workloadsv1.SpotReversionPolicy{Action: workloadsv1.NoneReversionAction},
			pods:          []client.Object{newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo)},
			wantRemaining: []string{"ondemand-pod-1"},
		},
		{
			name:          "OnDemand CapacityStrategy does not evict",
			cs:            &workloadsv1.CapacityStrategy{Type: workloadsv1.SpotPlacementTypeOnDemand},
			reversion:     &active,
			pods:          []client.Object{newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo)},
			wantRemaining: []string{"ondemand-pod-1"},
		},
		{
			name:          "No InfrastructureProfileRef does not evict (reversion treated as Lazy)",
			cs:            spot100,
			reversion:     &active,
			mutateWC:      func(wc *workloadsv1.WorkloadClass) { wc.Spec.InfrastructureProfileRef = nil },
			pods:          []client.Object{newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo)},
			wantRemaining: []string{"ondemand-pod-1"},
		},
		{
			name:          "Policy not attached does not evict",
			cs:            spot100,
			reversion:     &active,
			mutateWC:      func(wc *workloadsv1.WorkloadClass) { wc.Status.Conditions[0].Status = metav1.ConditionFalse },
			pods:          []client.Object{newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo)},
			wantRemaining: []string{"ondemand-pod-1"},
		},
		{
			name:          "Missing GKESpotPlacementPolicy does not evict",
			cs:            spot100,
			pods:          []client.Object{newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo)},
			wantRemaining: []string{"ondemand-pod-1"},
		},
		{
			name:      "Rejected eviction outside the disruption window stops and requeues for the next window",
			cs:        spot100,
			reversion: &active,
			windows: []workloadsv1.DisruptionWindow{{
				Name: "evening-window", DaysOfWeek: []string{"Friday"}, TimeZone: "Etc/UTC", StartTime: "22:00", EndTime: "23:59",
			}},
			rejectEvictions: true,
			pods: []client.Object{
				newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo),
				newReversionPod("ondemand-pod-2", "ondemand-node-1", hourAgo),
			},
			wantRemaining:     []string{"ondemand-pod-1", "ondemand-pod-2"},
			wantRequeue:       7 * time.Hour,
			wantEvictAttempts: 1,
		},
		{
			name:      "Rejected eviction inside the disruption window retries after evictionRetryDelay",
			cs:        spot100,
			reversion: &active,
			windows: []workloadsv1.DisruptionWindow{{
				Name: "afternoon-window", DaysOfWeek: []string{"Friday"}, TimeZone: "Etc/UTC", StartTime: "14:00", EndTime: "18:00",
			}},
			rejectEvictions: true,
			pods: []client.Object{
				newReversionPod("ondemand-pod-1", "ondemand-node-1", hourAgo),
				newReversionPod("ondemand-pod-2", "ondemand-node-1", hourAgo),
			},
			wantRemaining:     []string{"ondemand-pod-1", "ondemand-pod-2"},
			wantRequeue:       evictionRetryDelay,
			wantEvictAttempts: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := newReversionWC(now, tc.cs)
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = tc.windows
			if tc.mutateWC != nil {
				tc.mutateWC(wc)
			}

			objects := append([]client.Object{spotNodeObj.DeepCopy(), onDemandNode.DeepCopy(), wc}, tc.pods...)
			if tc.reversion != nil {
				objects = append(objects, newReversionPolicy(*tc.reversion))
			}

			evictAttempts := 0
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...)
			if tc.rejectEvictions {
				builder = builder.WithInterceptorFuncs(interceptor.Funcs{
					SubResourceCreate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
						if subResourceName == "eviction" {
							evictAttempts++
							return apierrors.NewTooManyRequestsError("Cannot evict pod as it would violate the pod's disruption budget.")
						}
						return c.SubResource(subResourceName).Create(ctx, obj, subResource, opts...)
					},
				})
			}
			fakeClient := builder.Build()
			r := &WorkloadClassReconciler{Client: fakeClient, Scheme: scheme}

			requeue, err := r.reconcileSpotReversion(context.Background(), wc, now)
			if err != nil {
				t.Fatalf("reconcileSpotReversion() unexpected error: %v", err)
			}
			if requeue != tc.wantRequeue {
				t.Errorf("reconcileSpotReversion() requeue = %v, want %v", requeue, tc.wantRequeue)
			}
			if tc.rejectEvictions && evictAttempts != tc.wantEvictAttempts {
				t.Errorf("eviction attempts = %d, want %d", evictAttempts, tc.wantEvictAttempts)
			}

			podList := &corev1.PodList{}
			if err := fakeClient.List(context.Background(), podList, client.InNamespace("default")); err != nil {
				t.Fatalf("failed to list remaining Pods: %v", err)
			}
			got := make([]string, 0, len(podList.Items))
			for _, p := range podList.Items {
				got = append(got, p.Name)
			}
			slices.Sort(got)
			if !slices.Equal(got, tc.wantRemaining) {
				t.Errorf("remaining Pods = %v, want %v", got, tc.wantRemaining)
			}
		})
	}
}

func TestPodFallbackStartTime(t *testing.T) {
	created := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	scheduled := created.Add(time.Minute)
	started := created.Add(2 * time.Minute)

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(created)}}
	if got := podFallbackStartTime(pod); !got.Equal(created) {
		t.Errorf("with no status, got %v, want creation time %v", got, created)
	}

	pod.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(scheduled),
	}}
	if got := podFallbackStartTime(pod); !got.Equal(scheduled) {
		t.Errorf("with PodScheduled, got %v, want scheduled time %v", got, scheduled)
	}

	startTime := metav1.NewTime(started)
	pod.Status.StartTime = &startTime
	if got := podFallbackStartTime(pod); !got.Equal(started) {
		t.Errorf("with StartTime, got %v, want start time %v", got, started)
	}
}

func TestMinPositiveDuration(t *testing.T) {
	testCases := []struct {
		a, b, want time.Duration
	}{
		{0, 0, 0},
		{0, 5 * time.Second, 5 * time.Second},
		{5 * time.Second, 0, 5 * time.Second},
		{5 * time.Second, 3 * time.Second, 3 * time.Second},
		{-1, 3 * time.Second, 3 * time.Second},
	}
	for _, tc := range testCases {
		if got := minPositiveDuration(tc.a, tc.b); got != tc.want {
			t.Errorf("minPositiveDuration(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
