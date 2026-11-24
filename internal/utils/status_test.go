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

package utils

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestUpdateWorkloadClassStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := workloadsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	t.Run("retries_on_conflict_and_preserves_concurrent_conditions", func(t *testing.T) {
		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
			Status: workloadsv1.WorkloadClassStatus{
				Conditions: []metav1.Condition{{
					Type:   workloadsv1.ConditionTypeInFallback,
					Status: metav1.ConditionTrue,
					Reason: workloadsv1.ReasonFallbackActive,
				}},
			},
		}

		attempts := 0
		fc := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&workloadsv1.WorkloadClass{}).
			WithObjects(wc).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					attempts++
					if attempts == 1 {
						return apierrors.NewConflict(schema.GroupResource{Group: "workloads.gke.io", Resource: "workloadclasses"}, obj.GetName(), errors.New("stale"))
					}
					return c.Status().Update(ctx, obj, opts...)
				},
			}).Build()

		callerWC := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"}}
		readerGets := 0
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				readerGets++
				return fc.Get(ctx, key, obj, opts...)
			},
		}).Build()
		err := UpdateWorkloadClassStatus(t.Context(), fc, reader, callerWC, func(latest *workloadsv1.WorkloadClass) bool {
			latest.Status.MaintenanceReadiness = workloadsv1.ReadinessReady
			return true
		})
		if err != nil {
			t.Fatalf("UpdateWorkloadClassStatus() error = %v", err)
		}
		if attempts != 2 {
			t.Errorf("attempts = %d, want 2", attempts)
		}
		if readerGets != 2 {
			t.Errorf("readerGets = %d, want 2 (expected uncached reader to serve retries)", readerGets)
		}
		if callerWC.Status.MaintenanceReadiness != workloadsv1.ReadinessReady {
			t.Errorf("callerWC readiness = %q, want %q", callerWC.Status.MaintenanceReadiness, workloadsv1.ReadinessReady)
		}
		if !meta.IsStatusConditionTrue(callerWC.Status.Conditions, workloadsv1.ConditionTypeInFallback) {
			t.Error("expected concurrent InFallback condition to be preserved")
		}
	})

	t.Run("skips_update_when_mutate_returns_false", func(t *testing.T) {
		wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"}}
		called := false
		fc := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&workloadsv1.WorkloadClass{}).
			WithObjects(wc).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
					called = true
					return nil
				},
			}).Build()

		if err := UpdateWorkloadClassStatus(t.Context(), fc, nil, wc.DeepCopy(), func(*workloadsv1.WorkloadClass) bool { return false }); err != nil {
			t.Fatalf("UpdateWorkloadClassStatus() error = %v", err)
		}
		if called {
			t.Error("expected Status().Update not to be called when mutate returns false")
		}
	})
}
