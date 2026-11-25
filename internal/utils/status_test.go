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

type countingReader struct {
	client.Reader
	gets int
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.gets++
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestUpdateWorkloadClassStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := workloadsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	wcGR := schema.GroupResource{Group: "workloads.gke.io", Resource: "workloadclasses"}

	t.Run("retries_on_conflict_and_preserves_concurrent_conditions", func(t *testing.T) {
		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
			Status: workloadsv1.WorkloadClassStatus{Conditions: []metav1.Condition{{
				Type: workloadsv1.ConditionTypeInFallback, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonFallbackActive,
			}}},
		}
		attempts := 0
		fc := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workloadsv1.WorkloadClass{}).WithObjects(wc).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, _ string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if attempts++; attempts == 1 {
						return apierrors.NewConflict(wcGR, obj.GetName(), errors.New("stale"))
					}
					return c.Status().Update(ctx, obj, opts...)
				},
			}).Build()
		callerWC := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"}}
		reader := &countingReader{Reader: fc}
		err := UpdateWorkloadClassStatus(t.Context(), fc, reader, callerWC, func(latest *workloadsv1.WorkloadClass) bool {
			latest.Status.MaintenanceReadiness = workloadsv1.ReadinessReady
			return true
		})
		if err != nil {
			t.Fatalf("UpdateWorkloadClassStatus() error = %v", err)
		}
		if attempts != 2 || reader.gets != 1 {
			t.Errorf("got (attempts=%d, readerGets=%d), want (2, 1)", attempts, reader.gets)
		}
		if callerWC.Status.MaintenanceReadiness != workloadsv1.ReadinessReady || !meta.IsStatusConditionTrue(callerWC.Status.Conditions, workloadsv1.ConditionTypeInFallback) {
			t.Errorf("unexpected callerWC status: %+v", callerWC.Status)
		}
	})

	t.Run("skips_update_and_avoids_uncached_reader_in_steady_state", func(t *testing.T) {
		wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"}}
		called := false
		fc := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workloadsv1.WorkloadClass{}).WithObjects(wc).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
					called = true
					return nil
				},
			}).Build()
		reader := &countingReader{Reader: fc}
		if err := UpdateWorkloadClassStatus(t.Context(), fc, reader, wc.DeepCopy(), func(*workloadsv1.WorkloadClass) bool { return false }); err != nil {
			t.Fatalf("UpdateWorkloadClassStatus() error = %v", err)
		}
		if called || reader.gets != 0 {
			t.Errorf("got (updateCalled=%v, readerGets=%d), want (false, 0)", called, reader.gets)
		}
	})

	t.Run("retry_with_nil_reader_falls_back_to_client", func(t *testing.T) {
		wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"}}
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		attempts := 0
		err := RetryOnConflictWithReader(fc, nil, func(rc client.Client) error {
			attempts++
			if getErr := rc.Get(t.Context(), client.ObjectKeyFromObject(wc), &workloadsv1.WorkloadClass{}); getErr != nil {
				return getErr
			}
			if attempts == 1 {
				return apierrors.NewConflict(wcGR, wc.Name, errors.New("stale"))
			}
			return nil
		})
		if err != nil || attempts != 2 {
			t.Fatalf("RetryOnConflictWithReader() = (err=%v, attempts=%d), want (nil, 2)", err, attempts)
		}
	})
}
