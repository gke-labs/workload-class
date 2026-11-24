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
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	// SafeToEvictAnnotation is the Cluster Autoscaler annotation that blocks pod eviction when "false".
	SafeToEvictAnnotation = "cluster-autoscaler.kubernetes.io/safe-to-evict"
	// SafeToEvictSuspendedAnnotation stores the owning WorkloadClass name while safe-to-evict="false" is removed.
	SafeToEvictSuspendedAnnotation = "workloads.gke.io/safe-to-evict-suspended"
	// SafeToEvictFinalizer ensures suspended annotations are restored before WorkloadClass deletion completes.
	SafeToEvictFinalizer = "workloads.gke.io/safe-to-evict-cleanup"
	annotationValueFalse = "false"
)

// reconcileSafeToEvictAnnotations suspends safe-to-evict="false" on governed Pods during open
// windows (Ready/Overdue) for valid WorkloadClasses, recording SafeToEvictSuspendedAnnotation=<wc.Name>.
// It restores safe-to-evict="false" on any Pod in wc.Namespace suspended by wc when the window closes,
// validation fails, the option is disabled, ownership/selector changes, or wc is deleted.
func (r *WorkloadClassReconciler) reconcileSafeToEvictAnnotations(
	ctx context.Context, wc *workloadsv1.WorkloadClass, readiness workloadsv1.MaintenanceReadiness, isValid bool,
) error {
	if !wc.DeletionTimestamp.IsZero() {
		return r.restoreSafeToEvictOnDelete(ctx, wc)
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace)); err != nil {
		return fmt.Errorf("failed to list Pods for safe-to-evict reconciliation: %w", err)
	}
	enforcedWC, err := utils.NamespaceEnforcedWorkloadClass(ctx, r.Client, wc.Namespace)
	if err != nil {
		return err
	}
	wcs := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, wcs, client.InNamespace(wc.Namespace)); err != nil {
		return fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}
	shouldSuspend := wc.Spec.DisruptionPolicy.ManageSafeToEvictAnnotation &&
		isValid && (readiness == workloadsv1.ReadinessReady || readiness == workloadsv1.ReadinessOverdue)

	var errs []error
	hasSuspended := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !utils.IsPodActive(pod) {
			continue
		}
		best := utils.BestMatchWorkloadClassForPod(pod, enforcedWC, wcs.Items)
		isOwner := best != nil && best.Name == wc.Name
		suspendedBySelf := pod.Annotations[SafeToEvictSuspendedAnnotation] == wc.Name
		if isOwner && shouldSuspend {
			if suspendedBySelf {
				hasSuspended = true
			} else if pod.Annotations[SafeToEvictAnnotation] == annotationValueFalse {
				if err := r.setSafeToEvictFinalizer(ctx, wc, true); err != nil {
					errs = append(errs, err)
				} else if err := r.patchSafeToEvict(ctx, client.ObjectKeyFromObject(pod), wc.Name, true); err != nil {
					errs = append(errs, err)
				} else {
					hasSuspended = true
				}
			}
			continue
		}
		if suspendedBySelf {
			if err := r.patchSafeToEvict(ctx, client.ObjectKeyFromObject(pod), wc.Name, false); err != nil {
				errs = append(errs, err)
				hasSuspended = true
			}
		}
	}
	if len(errs) == 0 && !hasSuspended && controllerutil.ContainsFinalizer(wc, SafeToEvictFinalizer) {
		if err := r.setSafeToEvictFinalizer(ctx, wc, false); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *WorkloadClassReconciler) restoreSafeToEvictOnDelete(ctx context.Context, wc *workloadsv1.WorkloadClass) error {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace)); err != nil {
		return fmt.Errorf("failed to list Pods for safe-to-evict cleanup on delete: %w", err)
	}
	var errs []error
	for i := range pods.Items {
		pod := &pods.Items[i]
		if utils.IsPodActive(pod) && pod.Annotations[SafeToEvictSuspendedAnnotation] == wc.Name {
			if err := r.patchSafeToEvict(ctx, client.ObjectKeyFromObject(pod), wc.Name, false); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	return r.setSafeToEvictFinalizer(ctx, wc, false)
}

func (r *WorkloadClassReconciler) setSafeToEvictFinalizer(ctx context.Context, wc *workloadsv1.WorkloadClass, add bool) error {
	before := wc.DeepCopy()
	if add {
		if !controllerutil.AddFinalizer(wc, SafeToEvictFinalizer) {
			return nil
		}
	} else if !controllerutil.RemoveFinalizer(wc, SafeToEvictFinalizer) {
		return nil
	}
	if err := r.Patch(ctx, wc, client.MergeFrom(before)); err != nil {
		if !add && apierrors.IsNotFound(err) {
			return nil
		}
		if add {
			controllerutil.RemoveFinalizer(wc, SafeToEvictFinalizer)
		} else {
			controllerutil.AddFinalizer(wc, SafeToEvictFinalizer)
		}
		return fmt.Errorf("failed to update safe-to-evict finalizer on WorkloadClass %s: %w", wc.Name, err)
	}
	return nil
}

func (r *WorkloadClassReconciler) patchSafeToEvict(ctx context.Context, key client.ObjectKey, wcName string, suspend bool) error {
	log := logf.FromContext(ctx).WithValues("pod", key.Name, "namespace", key.Namespace, "workloadClass", wcName)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1.Pod{}
		if err := r.Get(ctx, key, latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get Pod %s: %w", key.Name, err)
		}
		patch := client.MergeFrom(latest.DeepCopy())
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		if suspend {
			if latest.Annotations[SafeToEvictAnnotation] != annotationValueFalse {
				return nil
			}
			delete(latest.Annotations, SafeToEvictAnnotation)
			latest.Annotations[SafeToEvictSuspendedAnnotation] = wcName
			if err := r.Patch(ctx, latest, patch); err != nil {
				return fmt.Errorf("failed to suspend safe-to-evict on Pod %s: %w", key.Name, err)
			}
			log.Info("Suspended safe-to-evict annotation during open disruption window")
			return nil
		}
		if latest.Annotations[SafeToEvictSuspendedAnnotation] != wcName {
			return nil
		}
		latest.Annotations[SafeToEvictAnnotation] = annotationValueFalse
		delete(latest.Annotations, SafeToEvictSuspendedAnnotation)
		if err := r.Patch(ctx, latest, patch); err != nil {
			return fmt.Errorf("failed to restore safe-to-evict on Pod %s: %w", key.Name, err)
		}
		log.Info("Restored safe-to-evict annotation after disruption window closed")
		return nil
	})
}
