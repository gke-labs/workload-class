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
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// fallbackPodAt returns a Spot-targeted, controller-owned Pod on the On-Demand node that started at startTime.
func fallbackPodAt(name string, startTime time.Time) *corev1.Pod {
	pod := newWorkloadPod(name, "ondemand-node-1", startTime)
	pod.Spec.Tolerations = []corev1.Toleration{{
		Key: utils.SpotLabelKey, Operator: corev1.TolerationOpEqual, Value: utils.SpotLabelValue, Effect: corev1.TaintEffectNoSchedule,
	}}
	return pod
}

func spotPodAt(name string, startTime time.Time) *corev1.Pod {
	pod := newWorkloadPod(name, "spot-node-1", startTime)
	pod.Spec.Tolerations = fallbackPodAt("", startTime).Spec.Tolerations
	return pod
}

func remainingPodNames(t *testing.T, c client.Client) []string {
	t.Helper()
	podList := &corev1.PodList{}
	if err := c.List(context.Background(), podList, client.InNamespace("default")); err != nil {
		t.Fatalf("failed to list Pods: %v", err)
	}
	names := make([]string, 0, len(podList.Items))
	for _, p := range podList.Items {
		names = append(names, p.Name)
	}
	slices.Sort(names)
	return names
}

func TestReconcileSpotReversion(t *testing.T) {
	scheme := newGKESpotScheme()
	utilruntime.Must(policyv1.AddToScheme(scheme))

	// Friday, 15:00 UTC.
	now := time.Date(2026, 9, 18, 15, 0, 0, 0, time.UTC)
	hourAgo := now.Add(-time.Hour)
	active := workloadsv1.SpotReversionPolicy{Action: workloadsv1.ActiveReversionAction}
	activeNoFloor := workloadsv1.SpotReversionPolicy{Action: workloadsv1.ActiveReversionAction, MinDurationOnFallback: &metav1.Duration{Duration: 0}}

	testCases := []struct {
		name string
		cs   *workloadsv1.CapacityStrategy
		// reversion is the policy's reversion; nil means no policy.
		reversion *workloadsv1.SpotReversionPolicy
		windows   []workloadsv1.DisruptionWindow
		mutateWC  func(wc *workloadsv1.WorkloadClass)
		// rejectWith, if set, makes every eviction fail with this error.
		rejectWith    error
		pods          []client.Object
		wantRemaining []string
		wantRequeue   time.Duration
		// wantEvictAttempts is only checked when rejectWith is set.
		wantEvictAttempts int
	}{
		{
			name: "Active with 100% spotRatio evicts fallback Pods and keeps Spot Pods",
			cs:   fallbackCS("100%"), reversion: &activeNoFloor,
			pods:          []client.Object{spotPodAt("spot-1", hourAgo), fallbackPodAt("fallback-1", hourAgo)},
			wantRemaining: []string{"spot-1"},
		},
		{
			name: "Active does not evict Pods placed on On-Demand on purpose (no Spot targeting)",
			cs:   fallbackCS("100%"), reversion: &activeNoFloor,
			pods:          []client.Object{onDemandPod("intentional-od-1", "ondemand-node-1"), fallbackPodAt("fallback-1", hourAgo)},
			wantRemaining: []string{"intentional-od-1"},
		},
		{
			name: "default 5m floor skips a recently fallen-back Pod and requeues for the remainder",
			cs:   fallbackCS("100%"), reversion: &active,
			pods:          []client.Object{fallbackPodAt("young", now.Add(-2*time.Minute)), fallbackPodAt("mature", now.Add(-10*time.Minute))},
			wantRemaining: []string{"young"},
			wantRequeue:   3 * time.Minute,
		},
		{
			name: "explicit 0s minDurationOnFallback evicts immediately",
			cs:   fallbackCS("100%"), reversion: &activeNoFloor,
			pods:          []client.Object{fallbackPodAt("just-bound", now.Add(-5*time.Second))},
			wantRemaining: []string{},
		},
		{
			name: "explicit minDurationOnFallback is honored",
			cs:   fallbackCS("100%"),
			reversion: &workloadsv1.SpotReversionPolicy{
				Action: workloadsv1.ActiveReversionAction, MinDurationOnFallback: &metav1.Duration{Duration: 30 * time.Minute},
			},
			pods:          []client.Object{fallbackPodAt("young", now.Add(-10*time.Minute)), fallbackPodAt("mature", now.Add(-45*time.Minute))},
			wantRemaining: []string{"young"},
			wantRequeue:   20 * time.Minute,
		},
		{
			name: "Active with 80% spotRatio evicts only enough fallback Pods to reach 80%",
			cs:   fallbackCS("80%"), reversion: &activeNoFloor,
			pods: []client.Object{
				spotPodAt("spot-1", hourAgo), spotPodAt("spot-2", hourAgo), spotPodAt("spot-3", hourAgo),
				fallbackPodAt("fallback-older", now.Add(-2*time.Hour)), fallbackPodAt("fallback-newer", hourAgo),
			},
			// 3/5 < 80% -> evict the oldest fallback Pod; 4/5 >= 80% -> stop.
			wantRemaining: []string{"fallback-newer", "spot-1", "spot-2", "spot-3"},
		},
		{
			name: "Pending Spot-targeted Pods count as Spot, so a scale-up does not trigger evictions",
			cs:   fallbackCS("50%"), reversion: &activeNoFloor,
			pods: []client.Object{
				spotPodAt("spot-1", hourAgo), fallbackPodAt("fallback-1", hourAgo),
				spotTargetedPod("pending-1", ""), spotTargetedPod("pending-2", ""),
			},
			// (1 spot + 2 pending) / 4 = 75% >= 50% -> nothing to do.
			wantRemaining: []string{"fallback-1", "pending-1", "pending-2", "spot-1"},
		},
		{
			name: "Active skips Pods without a controller owner",
			cs:   fallbackCS("100%"), reversion: &activeNoFloor,
			pods: func() []client.Object {
				bare := fallbackPodAt("bare", hourAgo)
				bare.OwnerReferences = nil
				return []client.Object{bare, fallbackPodAt("owned", hourAgo)}
			}(),
			wantRemaining: []string{"bare"},
		},
		{
			name: "Lazy does not evict",
			cs:   fallbackCS("100%"), reversion: &workloadsv1.SpotReversionPolicy{Action: workloadsv1.LazyReversionAction},
			pods:          []client.Object{fallbackPodAt("fallback-1", hourAgo)},
			wantRemaining: []string{"fallback-1"},
		},
		{
			name: "None does not evict",
			cs:   fallbackCS("100%"), reversion: &workloadsv1.SpotReversionPolicy{Action: workloadsv1.NoneReversionAction},
			pods:          []client.Object{fallbackPodAt("fallback-1", hourAgo)},
			wantRemaining: []string{"fallback-1"},
		},
		{
			name:          "no policy (reversion treated as Lazy) does not evict",
			cs:            fallbackCS("100%"),
			pods:          []client.Object{fallbackPodAt("fallback-1", hourAgo)},
			wantRemaining: []string{"fallback-1"},
		},
		{
			name: "FallbackAction Fail does not evict",
			cs:   &workloadsv1.CapacityStrategy{FallbackAction: workloadsv1.FallbackActionFail}, reversion: &activeNoFloor,
			pods:          []client.Object{fallbackPodAt("stray", hourAgo)},
			wantRemaining: []string{"stray"},
		},
		{
			name: "rejected eviction outside the disruption window stops and requeues for the next window",
			cs:   fallbackCS("100%"), reversion: &activeNoFloor,
			windows: []workloadsv1.DisruptionWindow{{
				Name: "evening", DaysOfWeek: []string{"Friday"}, TimeZone: "Etc/UTC", StartTime: "22:00", EndTime: "23:59",
			}},
			rejectWith:        apierrors.NewForbidden(corev1.Resource("pods"), "fallback-1", nil),
			pods:              []client.Object{fallbackPodAt("fallback-1", hourAgo), fallbackPodAt("fallback-2", hourAgo)},
			wantRemaining:     []string{"fallback-1", "fallback-2"},
			wantRequeue:       7 * time.Hour,
			wantEvictAttempts: 1,
		},
		{
			name: "PDB rejection inside the disruption window retries after evictionRetryDelay",
			cs:   fallbackCS("100%"), reversion: &activeNoFloor,
			windows: []workloadsv1.DisruptionWindow{{
				Name: "afternoon", DaysOfWeek: []string{"Friday"}, TimeZone: "Etc/UTC", StartTime: "14:00", EndTime: "18:00",
			}},
			rejectWith:        apierrors.NewTooManyRequestsError("Cannot evict pod as it would violate the pod's disruption budget."),
			pods:              []client.Object{fallbackPodAt("fallback-1", hourAgo), fallbackPodAt("fallback-2", hourAgo)},
			wantRemaining:     []string{"fallback-1", "fallback-2"},
			wantRequeue:       evictionRetryDelay,
			wantEvictAttempts: 2,
		},
		{
			name: "too-new denial inside the disruption window retries after tooNewRetryDelay",
			cs:   fallbackCS("100%"), reversion: &activeNoFloor,
			windows: []workloadsv1.DisruptionWindow{{
				Name: "afternoon", DaysOfWeek: []string{"Friday"}, TimeZone: "Etc/UTC", StartTime: "14:00", EndTime: "18:00",
			}},
			rejectWith: apierrors.NewForbidden(corev1.Resource("pods"), "fallback-1",
				&forbiddenReason{"Eviction blocked: pod is too new (running for 1h0m0s, required 2 days)"}),
			pods:              []client.Object{fallbackPodAt("fallback-1", hourAgo)},
			wantRemaining:     []string{"fallback-1"},
			wantRequeue:       tooNewRetryDelay,
			wantEvictAttempts: 1,
		},
		{
			name: "PDB rejection outside the window when emergencyOverride is true retries after evictionRetryDelay instead of waiting for next window",
			cs:   fallbackCS("100%"), reversion: &activeNoFloor,
			windows: []workloadsv1.DisruptionWindow{{
				Name: "evening", DaysOfWeek: []string{"Friday"}, TimeZone: "Etc/UTC", StartTime: "22:00", EndTime: "23:59",
			}},
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Spec.DisruptionPolicy.EmergencyOverride = true
			},
			rejectWith:        apierrors.NewTooManyRequestsError("Cannot evict pod as it would violate the pod's disruption budget."),
			pods:              []client.Object{fallbackPodAt("fallback-1", hourAgo), fallbackPodAt("fallback-2", hourAgo)},
			wantRemaining:     []string{"fallback-1", "fallback-2"},
			wantRequeue:       evictionRetryDelay,
			wantEvictAttempts: 2,
		},
		{
			name: "PDB rejection outside the window when MaintenanceReadiness is Overdue retries after evictionRetryDelay",
			cs:   fallbackCS("100%"), reversion: &activeNoFloor,
			windows: []workloadsv1.DisruptionWindow{{
				Name: "evening", DaysOfWeek: []string{"Friday"}, TimeZone: "Etc/UTC", StartTime: "22:00", EndTime: "23:59",
			}},
			mutateWC: func(wc *workloadsv1.WorkloadClass) {
				wc.Status.MaintenanceReadiness = workloadsv1.ReadinessOverdue
			},
			rejectWith:        apierrors.NewTooManyRequestsError("Cannot evict pod as it would violate the pod's disruption budget."),
			pods:              []client.Object{fallbackPodAt("fallback-1", hourAgo)},
			wantRemaining:     []string{"fallback-1"},
			wantRequeue:       evictionRetryDelay,
			wantEvictAttempts: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := newManagedWC(tc.cs)
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = tc.windows
			if tc.mutateWC != nil {
				tc.mutateWC(wc)
			}
			var policy *workloadsv1.GKESpotPlacementPolicy
			if tc.reversion != nil {
				policy = newPolicy(*tc.reversion)
			}

			objects := append([]client.Object{spotNodeObj.DeepCopy(), onDemandNodeObj.DeepCopy(), wc}, tc.pods...)
			evictAttempts := 0
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...)
			if tc.rejectWith != nil {
				builder = builder.WithInterceptorFuncs(interceptor.Funcs{
					SubResourceCreate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
						if subResourceName == "eviction" {
							evictAttempts++
							return tc.rejectWith
						}
						return c.SubResource(subResourceName).Create(ctx, obj, subResource, opts...)
					},
				})
			}
			c := builder.Build()
			r := &GKESpotPlacementReconciler{Client: c, Scheme: scheme}

			spotNodes, err := r.listSpotNodeNames(context.Background())
			if err != nil {
				t.Fatalf("listSpotNodeNames() unexpected error: %v", err)
			}
			pods, err := r.classifyWorkloadPods(context.Background(), wc, spotNodes)
			if err != nil {
				t.Fatalf("classifyWorkloadPods() unexpected error: %v", err)
			}

			requeue, err := r.reconcileSpotReversion(context.Background(), wc, policy, pods, now)
			if err != nil {
				t.Fatalf("reconcileSpotReversion() unexpected error: %v", err)
			}
			if requeue != tc.wantRequeue {
				t.Errorf("requeue = %v, want %v", requeue, tc.wantRequeue)
			}
			if tc.rejectWith != nil && evictAttempts != tc.wantEvictAttempts {
				t.Errorf("eviction attempts = %d, want %d", evictAttempts, tc.wantEvictAttempts)
			}
			if got := remainingPodNames(t, c); !slices.Equal(got, tc.wantRemaining) {
				t.Errorf("remaining Pods = %v, want %v", got, tc.wantRemaining)
			}
		})
	}
}

// forbiddenReason is a minimal error carrying the disruption webhook's denial message.
type forbiddenReason struct{ msg string }

func (e *forbiddenReason) Error() string { return e.msg }

func TestReconcile_ReversionGatedOnSpotCapacity(t *testing.T) {
	scheme := newGKESpotScheme()
	utilruntime.Must(policyv1.AddToScheme(scheme))
	hourAgo := time.Now().Add(-time.Hour)
	activeNoFloor := workloadsv1.SpotReversionPolicy{Action: workloadsv1.ActiveReversionAction, MinDurationOnFallback: &metav1.Duration{Duration: 0}}

	testCases := []struct {
		name          string
		spotAvailable bool
		wantRemaining []string
	}{
		{name: "Spot unavailable: no evictions", spotAvailable: false, wantRemaining: []string{"fallback-1"}},
		{name: "Spot available: fallback Pod evicted", spotAvailable: true, wantRemaining: []string{}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := newManagedWC(fallbackCS("100%"))
			objects := []client.Object{spotNodeObj.DeepCopy(), onDemandNodeObj.DeepCopy(), wc, newPolicy(activeNoFloor), fallbackPodAt("fallback-1", hourAgo)}
			if tc.spotAvailable {
				objects = append(objects, autoscalerStatusConfigMap(`
nodeGroups:
- name: gke-cluster-spot-pool-grp
  health:
    status: Healthy
  scaleUp:
    status: NoActivity
`))
			} else {
				objects = append(objects, autoscalerStatusConfigMap(`
nodeGroups:
- name: gke-cluster-spot-pool-grp
  health:
    status: Healthy
  scaleUp:
    status: Backoff
    backoffInfo:
      errorCode: ZONE_RESOURCE_POOL_EXHAUSTED
`))
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(wc).Build()
			r := &GKESpotPlacementReconciler{Client: c, Scheme: scheme}

			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(wc)}); err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}
			if got := remainingPodNames(t, c); !slices.Equal(got, tc.wantRemaining) {
				t.Errorf("remaining Pods = %v, want %v", got, tc.wantRemaining)
			}
			if got := inFallbackStatus(t, c, wc); got != metav1.ConditionTrue {
				t.Errorf("InFallback = %q, want True (set before reversion ran)", got)
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
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(scheduled)}}
	if got := podFallbackStartTime(pod); !got.Equal(scheduled) {
		t.Errorf("with PodScheduled, got %v, want scheduled time %v", got, scheduled)
	}
	startTime := metav1.NewTime(started)
	pod.Status.StartTime = &startTime
	if got := podFallbackStartTime(pod); !got.Equal(started) {
		t.Errorf("with StartTime, got %v, want start time %v", got, started)
	}
}

func TestSpotRatioPercent(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    int
		wantErr bool
	}{
		{in: "", want: 100},
		{in: "80%", want: 80},
		{in: "0%", want: 0},
		{in: "abc%", wantErr: true},
	} {
		got, err := spotRatioPercent(&workloadsv1.CapacityStrategy{SpotRatio: tc.in})
		if (err != nil) != tc.wantErr {
			t.Errorf("spotRatioPercent(%q) error = %v, wantErr %v", tc.in, err, tc.wantErr)
		}
		if err == nil && got != tc.want {
			t.Errorf("spotRatioPercent(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
	if got, _ := spotRatioPercent(nil); got != 100 {
		t.Errorf("spotRatioPercent(nil) = %d, want 100", got)
	}
}

func TestMinPositiveDuration(t *testing.T) {
	for _, tc := range []struct{ a, b, want time.Duration }{
		{0, 0, 0},
		{0, 5 * time.Second, 5 * time.Second},
		{5 * time.Second, 0, 5 * time.Second},
		{5 * time.Second, 3 * time.Second, 3 * time.Second},
		{-1, 3 * time.Second, 3 * time.Second},
	} {
		if got := minPositiveDuration(tc.a, tc.b); got != tc.want {
			t.Errorf("minPositiveDuration(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
