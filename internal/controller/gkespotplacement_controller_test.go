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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func newGKESpotScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))
	return scheme
}

func gkePolicyRef() *workloadsv1.InfrastructureProfileReference {
	return &workloadsv1.InfrastructureProfileReference{
		Group: workloadsv1.GroupVersion.Group,
		Kind:  workloadsv1.GKESpotPlacementPolicyKind,
		Name:  "spot-policy",
	}
}

func trueCondition(condType string) metav1.Condition {
	return metav1.Condition{Type: condType, Status: metav1.ConditionTrue, Reason: "Test"}
}

// gkeSpotWC returns a WorkloadClass with a default (Spot) CapacityStrategy, the given profile ref, and conditions.
func gkeSpotWC(ref *workloadsv1.InfrastructureProfileReference, conds ...metav1.Condition) *workloadsv1.WorkloadClass {
	wc := makeWC("spot-wc", metav1.Now())
	wc.Spec.CapacityStrategy = &workloadsv1.CapacityStrategy{}
	wc.Spec.InfrastructureProfileRef = ref
	wc.Status.Conditions = conds
	return wc
}

func TestGKESpotPlacementReconciler_Reconcile(t *testing.T) {
	scheme := newGKESpotScheme()

	testCases := []struct {
		name string
		wc   *workloadsv1.WorkloadClass
	}{
		{name: "missing WorkloadClass", wc: nil},
		{name: "not GKE Spot managed (other profile kind)", wc: gkeSpotWC(&workloadsv1.InfrastructureProfileReference{Kind: "KarpenterSpotPolicy", Name: "k"})},
		{name: "not validated", wc: gkeSpotWC(gkePolicyRef(), trueCondition(workloadsv1.ConditionTypePlacementPluginAttached))},
		{name: "validated but policy not attached", wc: gkeSpotWC(gkePolicyRef(), trueCondition(workloadsv1.ConditionTypeValidated))},
		{name: "validated and attached", wc: gkeSpotWC(gkePolicyRef(), trueCondition(workloadsv1.ConditionTypeValidated), trueCondition(workloadsv1.ConditionTypePlacementPluginAttached))},
		{name: "validated without a profile ref", wc: gkeSpotWC(nil, trueCondition(workloadsv1.ConditionTypeValidated))},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var objs []client.Object
			if tc.wc != nil {
				objs = append(objs, tc.wc)
			}
			r := &GKESpotPlacementReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
				Scheme: scheme,
			}

			// The scaffold has no behavior yet; every path must complete without error or requeue.
			res, err := r.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: client.ObjectKey{Name: "spot-wc", Namespace: "default"},
			})
			if err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}
			if res.RequeueAfter != 0 {
				t.Errorf("Reconcile() RequeueAfter = %v, want 0", res.RequeueAfter)
			}
		})
	}
}

func TestGKESpotManagedPredicate(t *testing.T) {
	p := gkeSpotManagedPredicate()
	managed := gkeSpotWC(gkePolicyRef())
	noRef := gkeSpotWC(nil)
	other := gkeSpotWC(&workloadsv1.InfrastructureProfileReference{Kind: "KarpenterSpotPolicy", Name: "k"})
	noStrategy := makeWC("plain", metav1.Now())

	if !p.Create(event.CreateEvent{Object: managed}) || !p.Create(event.CreateEvent{Object: noRef}) {
		t.Errorf("expected create of GKE-managed WorkloadClasses to pass")
	}
	if p.Create(event.CreateEvent{Object: other}) || p.Create(event.CreateEvent{Object: noStrategy}) {
		t.Errorf("expected create of unmanaged WorkloadClasses to be filtered")
	}

	updateCases := []struct {
		name     string
		old, new client.Object
		want     bool
	}{
		{name: "managed to managed", old: managed, new: noRef, want: true},
		{name: "managed to unmanaged (becomes other plugin)", old: managed, new: other, want: true},
		{name: "unmanaged to managed", old: other, new: managed, want: true},
		{name: "unmanaged to unmanaged", old: other, new: noStrategy, want: false},
	}
	for _, tc := range updateCases {
		if got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
			t.Errorf("%s: Update() = %v, want %v", tc.name, got, tc.want)
		}
	}

	if p.Delete(event.DeleteEvent{Object: managed}) || p.Delete(event.DeleteEvent{Object: other}) {
		t.Errorf("expected delete events to be filtered")
	}
}
