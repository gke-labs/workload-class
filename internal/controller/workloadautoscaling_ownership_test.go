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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const wasMarkerAnnotation = "test.workloads.gke.io/marker"

// mark annotates an existing autoscaler. The annotation survives an update in place, but not a delete and
// recreate, which the fake client cannot show through UIDs because it does not assign them.
func (e *wasEnv) mark(obj client.Object) {
	e.t.Helper()
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[wasMarkerAnnotation] = "kept"
	obj.SetAnnotations(annotations)
	e.update(obj)
}

func isMarked(obj client.Object) bool {
	return obj.GetAnnotations()[wasMarkerAnnotation] == "kept"
}

// wasFullClass returns a validated WorkloadClass with the given name and selector that sets both autoscaling
// (with the given replicas.max) and rightsizing.
func wasFullClass(name string, selector map[string]string, maxReplicas int32) *workloadsv1.WorkloadClass {
	wc := wasClass()
	wc.Name = name
	wc.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: selector}
	wc.Spec.ResourceManagement.Autoscaling.Replicas.Max = maxReplicas
	// Scale on a Pods metric, so that rightsizing keeps both resources.
	wc.Spec.ResourceManagement.Autoscaling.Metrics[0].Type = "Pods"
	wc.Spec.ResourceManagement.Autoscaling.Metrics[0].Resource = nil
	wc.Spec.ResourceManagement.Rightsizing = &workloadsv1.Rightsizing{}
	return wc
}

// The namespace's enforced WorkloadClass governs every workload in the namespace, whatever its labels.
func TestWorkloadAutoscalingEnforcedClass(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   wasNamespace,
		Labels: map[string]string{workloadsv1.EnforcedClassLabel: "enforced"},
	}}
	enforced := wasFullClass("enforced", map[string]string{"never": "matches"}, 8)
	bySelector := wasFullClass("by-selector", wasWebLabels, 20)
	e := newVPAEnv(t, ns, enforced, bySelector,
		wasDeployment("web", wasWebLabels), wasDeployment("db", map[string]string{"tier": "db"}))

	e.reconcile("web")
	e.reconcile("db")
	for _, name := range []string{"wc-deployment-web", "wc-deployment-db"} {
		hpa := e.hpa(name)
		if hpa == nil || hpa.Spec.MaxReplicas != 8 || hpa.Labels[utils.WorkloadClassLabel] != "enforced" {
			t.Errorf("%s: expected the HPA of the enforced WorkloadClass, got %+v", name, hpa)
		}
		if vpa := e.vpa(name); vpa == nil || vpa.GetLabels()[utils.WorkloadClassLabel] != "enforced" {
			t.Errorf("%s: expected the VPA of the enforced WorkloadClass, got %v", name, vpa)
		}
	}

	// Without the label, WorkloadClasses govern workloads by their PodSelector again.
	e.mark(e.hpa("wc-deployment-web"))
	ns.Labels = nil
	e.update(ns)
	e.reconcile("web")
	e.reconcile("db")

	hpa := e.hpa("wc-deployment-web")
	if hpa == nil || hpa.Spec.MaxReplicas != 20 || hpa.Labels[utils.WorkloadClassLabel] != "by-selector" {
		t.Errorf("expected the HPA to follow the WorkloadClass that selects the workload, got %+v", hpa)
	}
	if hpa != nil && !isMarked(hpa) {
		t.Errorf("expected the HPA to be updated in place, but it was recreated")
	}
	if e.hpa("wc-deployment-db") != nil || e.vpa("wc-deployment-db") != nil {
		t.Errorf("expected the autoscalers of a workload that no WorkloadClass selects to be deleted")
	}
}

// A workload that moves to another WorkloadClass keeps its autoscalers, which are updated in place.
func TestWorkloadAutoscalingMovesBetweenClassesInPlace(t *testing.T) {
	web := wasFullClass("web", wasWebLabels, 20)
	canary := wasFullClass("web-canary", map[string]string{"tier": "web", "track": "canary"}, 5)
	canary.Spec.ResourceManagement.Rightsizing.Mode = workloadsv1.RightsizingModeOnCreate
	deploy := wasDeployment("web", wasWebLabels)
	e := newVPAEnv(t, web, canary, deploy)

	e.reconcile("web")
	hpa, vpa := e.hpa("wc-deployment-web"), e.vpa("wc-deployment-web")
	if hpa == nil || vpa == nil {
		t.Fatal("expected the managed HPA and VPA to be created")
	}
	if hpa.Spec.MaxReplicas != 20 || hpa.Labels[utils.WorkloadClassLabel] != "web" || utils.VPAUpdateModeOf(vpa) != utils.VPAUpdateModeOff {
		t.Fatalf("expected the autoscalers of WorkloadClass web, got HPA %+v and VPA mode %q", hpa.Spec, utils.VPAUpdateModeOf(vpa))
	}

	e.mark(hpa)
	e.mark(vpa)

	// The more specific selector wins once the pod template carries the canary label.
	deploy.Spec.Template.Labels = map[string]string{"tier": "web", "track": "canary"}
	e.update(deploy)
	e.reconcile("web")

	movedHPA, movedVPA := e.hpa("wc-deployment-web"), e.vpa("wc-deployment-web")
	if movedHPA == nil || !isMarked(movedHPA) || movedVPA == nil || !isMarked(movedVPA) {
		t.Fatalf("expected the autoscalers to be updated in place, not recreated")
	}
	if movedHPA.Spec.MaxReplicas != 5 || movedHPA.Labels[utils.WorkloadClassLabel] != "web-canary" {
		t.Errorf("expected the HPA of WorkloadClass web-canary, got maxReplicas %d and labels %v", movedHPA.Spec.MaxReplicas, movedHPA.Labels)
	}
	if utils.VPAUpdateModeOf(movedVPA) != "Initial" || movedVPA.GetLabels()[utils.WorkloadClassLabel] != "web-canary" {
		t.Errorf("expected the VPA of WorkloadClass web-canary, got mode %q and labels %v", utils.VPAUpdateModeOf(movedVPA), movedVPA.GetLabels())
	}
}

// Workloads are matched by their pod template, so a workload without Pods gets its autoscalers too.
func TestWorkloadAutoscalingScaledToZero(t *testing.T) {
	deploy := wasDeployment("web", wasWebLabels)
	deploy.Spec.Replicas = ptr.To[int32](0)
	sts := wasStatefulSet("web", wasWebLabels)
	sts.Spec.Replicas = ptr.To[int32](0)

	e := newVPAEnv(t, deploy, sts, wasFullClass("web", wasWebLabels, 20))
	e.reconcile("web")
	if e.hpa("wc-deployment-web") == nil || e.vpa("wc-deployment-web") == nil {
		t.Errorf("expected the autoscalers of a Deployment that is scaled to zero to be created")
	}

	e.r.Kind = utils.StatefulSetKind
	e.reconcile("web")
	if e.hpa("wc-statefulset-web") == nil || e.vpa("wc-statefulset-web") == nil {
		t.Errorf("expected the autoscalers of a StatefulSet that is scaled to zero to be created")
	}
}

// A workload that is deleted and recreated with the same name can find the autoscalers of its previous
// incarnation, which the garbage collector has not removed yet. They are replaced.
func TestWorkloadAutoscalingReplacesAutoscalersOfAPreviousWorkload(t *testing.T) {
	wc := wasFullClass("web", wasWebLabels, 20)
	previous := wasDeployment("web", wasWebLabels)
	e := newVPAEnv(t, previous, wc)
	e.reconcile("web")
	oldHPA, oldVPA := e.hpa("wc-deployment-web"), e.vpa("wc-deployment-web")
	if oldHPA == nil || oldVPA == nil {
		t.Fatal("expected the managed HPA and VPA to be created")
	}
	e.mark(oldHPA)
	e.mark(oldVPA)

	// Recreate the Deployment: same name, new UID.
	if err := e.client.Delete(context.Background(), previous); err != nil {
		t.Fatalf("failed to delete the Deployment: %v", err)
	}
	current := wasDeployment("web", wasWebLabels)
	current.UID = types.UID("uid-web-recreated")
	if err := e.client.Create(context.Background(), current); err != nil {
		t.Fatalf("failed to recreate the Deployment: %v", err)
	}
	e.eventReasons()

	// The first reconcile removes the stale autoscalers; their deletion re-queues the workload.
	e.reconcile("web")
	if e.hpa("wc-deployment-web") != nil || e.vpa("wc-deployment-web") != nil {
		t.Fatalf("expected the autoscalers of the previous Deployment to be deleted")
	}
	e.reconcile("web")
	hpa, vpa := e.hpa("wc-deployment-web"), e.vpa("wc-deployment-web")
	if hpa == nil || vpa == nil {
		t.Fatal("expected new autoscalers to be created for the current Deployment")
	}
	if !metav1.IsControlledBy(hpa, current) || !metav1.IsControlledBy(vpa, current) {
		t.Errorf("expected the current Deployment to control its autoscalers, got %+v and %+v", hpa.OwnerReferences, vpa.GetOwnerReferences())
	}
	if isMarked(hpa) || isMarked(vpa) {
		t.Errorf("expected the autoscalers to be recreated, not updated in place")
	}
	if got := e.eventReasons(); slices.Contains(got, reasonAutoscalerNameConflict) {
		t.Errorf("a previous incarnation is not a name conflict, got events %v", got)
	}
}

// A managed-looking autoscaler that another object controls is never modified or deleted.
func TestWorkloadAutoscalingLeavesAutoscalersOfAnotherOwnerAlone(t *testing.T) {
	wc := wasFullClass("web", wasWebLabels, 20)
	otherOwner := []metav1.OwnerReference{{
		APIVersion: "example.com/v1", Kind: "App", Name: "shop", UID: "uid-app", Controller: ptr.To(true),
	}}
	foreignHPA := utils.BuildHPA(wc, utils.WorkloadRef{Kind: utils.DeploymentKind, Namespace: wasNamespace, Name: "web"})
	foreignHPA.OwnerReferences = otherOwner
	foreignHPA.Spec.MaxReplicas = 3
	foreignVPA := utils.BuildVPA(wc, utils.WorkloadRef{Kind: utils.DeploymentKind, Namespace: wasNamespace, Name: "web"},
		utils.EffectiveResourceManagement{}, utils.VPACapabilities{})
	foreignVPA.SetOwnerReferences(otherOwner)

	e := newVPAEnv(t, wasDeployment("web", wasWebLabels), wc, foreignHPA, foreignVPA)
	e.reconcile("web")

	hpa, vpa := e.hpa("wc-deployment-web"), e.vpa("wc-deployment-web")
	if hpa == nil || hpa.Spec.MaxReplicas != 3 || metav1.GetControllerOf(hpa).UID != "uid-app" {
		t.Errorf("expected the HPA of another owner to be left alone, got %+v", hpa)
	}
	if vpa == nil || metav1.GetControllerOf(vpa).UID != "uid-app" {
		t.Errorf("expected the VPA of another owner to be left alone, got %v", vpa)
	}
	if got := e.eventReasons(); !slices.Equal(got, []string{reasonAutoscalerNameConflict, reasonAutoscalerNameConflict}) {
		t.Errorf("events = %v, want a name conflict for the HPA and for the VPA", got)
	}

	// They are not deleted either when the WorkloadClass stops applying.
	if err := e.client.Delete(context.Background(), wc); err != nil {
		t.Fatalf("failed to delete WorkloadClass: %v", err)
	}
	e.reconcile("web")
	if e.hpa("wc-deployment-web") == nil || e.vpa("wc-deployment-web") == nil {
		t.Errorf("expected the autoscalers of another owner not to be deleted")
	}
}

// A managed autoscaler without a controller owner (for example after an orphaning delete) is adopted.
func TestWorkloadAutoscalingAdoptsOrphanedAutoscalers(t *testing.T) {
	wc := wasFullClass("web", wasWebLabels, 20)
	deploy := wasDeployment("web", wasWebLabels)
	orphan := utils.BuildHPA(wc, utils.WorkloadRef{Kind: utils.DeploymentKind, Namespace: wasNamespace, Name: "web"})
	orphan.Spec.MaxReplicas = 3

	e := newVPAEnv(t, deploy, wc, orphan)
	e.reconcile("web")

	hpa := e.hpa("wc-deployment-web")
	if hpa == nil || hpa.Spec.MaxReplicas != 20 || !metav1.IsControlledBy(hpa, deploy) {
		t.Errorf("expected the orphaned HPA to be adopted and updated, got %+v", hpa)
	}
}

// Object names can be as long as 253 characters, which does not fit in an autoscaler name with the managed
// prefix, nor in a label value.
func TestWorkloadAutoscalingLongNames(t *testing.T) {
	longWorkload := strings.Repeat("w", 253)
	longClass := strings.Repeat("c", 200)
	wc := wasFullClass(longClass, wasWebLabels, 20)
	e := newVPAEnv(t, wasDeployment(longWorkload, wasWebLabels), wc)

	e.reconcile(longWorkload)

	name := utils.ManagedAutoscalerName(utils.DeploymentKind, longWorkload)
	if len(name) > 253 {
		t.Fatalf("managed autoscaler name has %d characters, more than the 253 that Kubernetes allows", len(name))
	}
	hpa, vpa := e.hpa(name), e.vpa(name)
	if hpa == nil || vpa == nil {
		t.Fatal("expected the managed HPA and VPA to be created under the truncated name")
	}
	for kind, labels := range map[string]map[string]string{"HPA": hpa.Labels, "VPA": vpa.GetLabels()} {
		if value := labels[utils.WorkloadClassLabel]; len(value) > 63 || !strings.HasPrefix(value, "ccc") {
			t.Errorf("%s: label value %q is not a valid truncation of the WorkloadClass name", kind, value)
		}
	}
	if hpa.Spec.ScaleTargetRef.Name != longWorkload {
		t.Errorf("expected the HPA to target the workload by its full name")
	}

	// The autoscalers are found again under the same name.
	e.reconcile(longWorkload)
	if again := e.hpa(name); again.ResourceVersion != hpa.ResourceVersion {
		t.Errorf("expected the second reconcile not to update the HPA")
	}
	if err := e.client.Delete(context.Background(), wc); err != nil {
		t.Fatalf("failed to delete WorkloadClass: %v", err)
	}
	e.reconcile(longWorkload)
	if e.hpa(name) != nil || e.vpa(name) != nil {
		t.Errorf("expected the autoscalers to be deleted with the WorkloadClass")
	}
}

func TestAutoscalerOwnership(t *testing.T) {
	deploy := wasDeployment("web", wasWebLabels)
	ref := utils.WorkloadRef{Kind: utils.DeploymentKind, Namespace: wasNamespace, Name: "web"}
	owner := func(kind, name string, uid types.UID) []metav1.OwnerReference {
		return []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: name, UID: uid, Controller: ptr.To(true)}}
	}
	managed := map[string]string{utils.ManagedByLabel: utils.ManagedByValue}

	testCases := []struct {
		name            string
		resourceVersion string
		labels          map[string]string
		owners          []metav1.OwnerReference
		want            autoscalerOwner
	}{
		{name: "does_not_exist_yet", want: ownedByWorkload},
		{name: "managed_and_controlled_by_the_workload", resourceVersion: "1", labels: managed, owners: owner("Deployment", "web", deploy.UID), want: ownedByWorkload},
		{name: "managed_without_a_controller", resourceVersion: "1", labels: managed, want: ownedByWorkload},
		{name: "managed_by_a_previous_incarnation", resourceVersion: "1", labels: managed, owners: owner("Deployment", "web", "another-uid"), want: ownedByPreviousWorkload},
		{name: "managed_but_controlled_by_another_workload", resourceVersion: "1", labels: managed, owners: owner("Deployment", "api", "uid-api"), want: ownedByOther},
		{name: "managed_but_controlled_by_another_kind", resourceVersion: "1", labels: managed, owners: owner("StatefulSet", "web", "uid-sts"), want: ownedByOther},
		{name: "not_managed", resourceVersion: "1", want: ownedByOther},
		{name: "not_managed_but_controlled_by_the_workload", resourceVersion: "1", owners: owner("Deployment", "web", deploy.UID), want: ownedByOther},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var autoscaler client.Object = wasUserHPA("wc-deployment-web", "Deployment", "web")
			autoscaler.SetResourceVersion(tc.resourceVersion)
			autoscaler.SetLabels(tc.labels)
			autoscaler.SetOwnerReferences(tc.owners)
			if got := autoscalerOwnership(autoscaler, deploy, ref); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}
