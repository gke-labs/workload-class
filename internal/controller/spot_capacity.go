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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	// ClusterAutoscalerStatusNamespace is the namespace of the GKE cluster autoscaler status ConfigMap.
	ClusterAutoscalerStatusNamespace = "kube-system"
	// ClusterAutoscalerStatusName is the name of the GKE cluster autoscaler status ConfigMap.
	ClusterAutoscalerStatusName = "cluster-autoscaler-status"

	spotNodeLabelKey   = "cloud.google.com/gke-spot"
	spotNodeLabelValue = "true"
)

// clusterAutoscalerStatus is the subset of the cluster autoscaler's YAML status (the "status" key of
// the cluster-autoscaler-status ConfigMap) needed to determine Spot node group availability.
type clusterAutoscalerStatus struct {
	NodeGroups []clusterAutoscalerNodeGroup `json:"nodeGroups"`
}

type clusterAutoscalerNodeGroup struct {
	Name    string                            `json:"name"`
	Health  clusterAutoscalerNodeGroupHealth  `json:"health"`
	ScaleUp clusterAutoscalerNodeGroupScaleUp `json:"scaleUp"`
}

type clusterAutoscalerNodeGroupHealth struct {
	Status string `json:"status"`
}

type clusterAutoscalerNodeGroupScaleUp struct {
	Status      string                        `json:"status"`
	BackoffInfo *clusterAutoscalerBackoffInfo `json:"backoffInfo,omitempty"`
}

type clusterAutoscalerBackoffInfo struct {
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// targetsSpot reports whether the WorkloadClass places any Pods on Spot capacity, i.e. it has a
// CapacityStrategy that is not OnDemand. An unset Type defaults to Spot.
func targetsSpot(wc *workloadsv1.WorkloadClass) bool {
	cs := wc.Spec.CapacityStrategy
	return cs != nil && cs.Type != workloadsv1.SpotPlacementTypeOnDemand
}

// checkSpotCapacity reports whether Spot capacity can currently be provisioned in the cluster.
//
// It inspects the kube-system/cluster-autoscaler-status ConfigMap for Spot node groups: Spot capacity is
// available if any Spot node group is Healthy and not in scale-up Backoff (e.g. a stockout reported as
// ZONE_RESOURCE_POOL_EXHAUSTED). If the ConfigMap is missing, unparsable, or reports no Spot node groups,
// it falls back to checking for Ready, schedulable Spot nodes.
func (r *WorkloadClassReconciler) checkSpotCapacity(ctx context.Context) (bool, error) {
	log := logf.FromContext(ctx)

	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: ClusterAutoscalerStatusNamespace, Name: ClusterAutoscalerStatusName}, cm)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("failed to get %s/%s ConfigMap: %w", ClusterAutoscalerStatusNamespace, ClusterAutoscalerStatusName, err)
	}

	if err == nil {
		available, found, parseErr := spotCapacityFromAutoscalerStatus(cm.Data["status"])
		if parseErr != nil {
			log.Error(parseErr, "Failed to parse cluster autoscaler status; falling back to Spot node readiness")
		} else if found {
			return available, nil
		}
	}

	return r.hasReadySpotNodes(ctx)
}

// spotCapacityFromAutoscalerStatus parses the cluster autoscaler status and reports whether any Spot node
// group is available. found is false when the status is empty or contains no Spot node groups.
//
// Spot node groups are identified by "spot" in their name (e.g. gke-cluster-spot-pool-1234-grp), since the
// autoscaler status does not expose node labels.
func spotCapacityFromAutoscalerStatus(rawStatus string) (available, found bool, err error) {
	if strings.TrimSpace(rawStatus) == "" {
		return false, false, nil
	}

	var status clusterAutoscalerStatus
	if err := yaml.Unmarshal([]byte(rawStatus), &status); err != nil {
		return false, false, err
	}

	for _, ng := range status.NodeGroups {
		if !strings.Contains(strings.ToLower(ng.Name), "spot") {
			continue
		}
		found = true
		if isSpotNodeGroupAvailable(ng) {
			return true, true, nil
		}
	}
	return false, found, nil
}

// isSpotNodeGroupAvailable reports whether the node group is Healthy and able to scale up (not in Backoff
// and not reporting a backoff error).
func isSpotNodeGroupAvailable(ng clusterAutoscalerNodeGroup) bool {
	if !strings.EqualFold(ng.Health.Status, "Healthy") {
		return false
	}
	if strings.EqualFold(ng.ScaleUp.Status, "Backoff") {
		return false
	}
	return ng.ScaleUp.BackoffInfo == nil || ng.ScaleUp.BackoffInfo.ErrorCode == ""
}

// hasReadySpotNodes reports whether the cluster has at least one schedulable Spot node with Ready=True.
func (r *WorkloadClassReconciler) hasReadySpotNodes(ctx context.Context) (bool, error) {
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, client.MatchingLabels{spotNodeLabelKey: spotNodeLabelValue}); err != nil {
		return false, fmt.Errorf("failed to list Spot nodes: %w", err)
	}

	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Spec.Unschedulable {
			continue
		}
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
	}
	return false, nil
}

// isClusterAutoscalerStatus reports whether obj is the kube-system/cluster-autoscaler-status ConfigMap.
func isClusterAutoscalerStatus(obj client.Object) bool {
	return obj.GetNamespace() == ClusterAutoscalerStatusNamespace && obj.GetName() == ClusterAutoscalerStatusName
}

// clusterAutoscalerStatusPredicate filters ConfigMap events down to the cluster autoscaler status ConfigMap.
// The cluster autoscaler rewrites the ConfigMap every few seconds (e.g. with fresh probe timestamps), so
// updates are only passed through when the Spot availability derived from it changes.
func clusterAutoscalerStatusPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return isClusterAutoscalerStatus(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return isClusterAutoscalerStatus(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return isClusterAutoscalerStatus(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !isClusterAutoscalerStatus(e.ObjectNew) {
				return false
			}
			return spotAvailabilityChanged(e.ObjectOld, e.ObjectNew)
		},
	}
}

// spotAvailabilityChanged reports whether the Spot availability parsed from the cluster autoscaler status
// differs between the old and new ConfigMap. A change in whether Spot node groups are reported (which
// switches checkSpotCapacity to or from the node fallback) or in parseability also counts as a change.
func spotAvailabilityChanged(oldObj, newObj client.Object) bool {
	oldCM, okOld := oldObj.(*corev1.ConfigMap)
	newCM, okNew := newObj.(*corev1.ConfigMap)
	if !okOld || !okNew {
		return true
	}

	oldAvailable, oldFound, oldErr := spotCapacityFromAutoscalerStatus(oldCM.Data["status"])
	newAvailable, newFound, newErr := spotCapacityFromAutoscalerStatus(newCM.Data["status"])
	return oldAvailable != newAvailable || oldFound != newFound || (oldErr == nil) != (newErr == nil)
}

// findWorkloadClassesForClusterAutoscalerStatus enqueues all WorkloadClasses that target Spot capacity when
// the cluster autoscaler status ConfigMap changes.
func (r *WorkloadClassReconciler) findWorkloadClassesForClusterAutoscalerStatus(ctx context.Context, obj client.Object) []reconcile.Request {
	if !isClusterAutoscalerStatus(obj) {
		return nil
	}

	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list WorkloadClasses for cluster autoscaler status change")
		return nil
	}

	var requests []reconcile.Request
	for i := range workloadClasses.Items {
		wc := &workloadClasses.Items[i]
		if !targetsSpot(wc) {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(wc)})
	}
	return requests
}
