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

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	wasNamespace  = "shop"
	wasClassName  = "web-services"
	wasGeneration = 4
)

var wasWebLabels = map[string]string{"tier": "web"}

func wasScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, appsv1.AddToScheme, autoscalingv2.AddToScheme, workloadsv1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("failed to build scheme: %v", err)
		}
	}
	// VerticalPodAutoscalers are handled as unstructured objects.
	scheme.AddKnownTypeWithName(utils.VPAGroupVersionKind, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(utils.VPAGroupVersionKind.GroupVersion().WithKind("VerticalPodAutoscalerList"), &unstructured.UnstructuredList{})
	return scheme
}

func wasDeployment(name string, templateLabels map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: wasNamespace, UID: types.UID("uid-" + name)},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: templateLabels}},
		},
	}
}

func wasStatefulSet(name string, templateLabels map[string]string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: wasNamespace, UID: types.UID("uid-sts-" + name)},
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: templateLabels}},
		},
	}
}

func wasCPUMetric() autoscalingv2.MetricSpec {
	return autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name:   corev1.ResourceCPU,
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](60)},
		},
	}
}

// wasClass returns the WorkloadClass "web-services", which governs the workloads with wasWebLabels, sets
// autoscaling, and is validated for its current generation.
func wasClass() *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: wasClassName, Namespace: wasNamespace, Generation: wasGeneration},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: wasWebLabels},
			ResourceManagement: &workloadsv1.ResourceManagement{
				Autoscaling: &workloadsv1.Autoscaling{
					Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](2), Max: 20},
					Metrics:  []autoscalingv2.MetricSpec{wasCPUMetric()},
				},
			},
		},
		Status: workloadsv1.WorkloadClassStatus{Conditions: []metav1.Condition{{
			Type:               workloadsv1.ConditionTypeResourceManagementValidated,
			Status:             metav1.ConditionTrue,
			Reason:             workloadsv1.ReasonValidationPassed,
			ObservedGeneration: wasGeneration,
		}}},
	}
}

func wasUserHPA(name, targetKind, targetName string) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: wasNamespace},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: targetKind, Name: targetName},
			MaxReplicas:    3,
		},
	}
}

type wasEnv struct {
	t        *testing.T
	client   client.Client
	recorder *events.FakeRecorder
	r        *WorkloadAutoscalingReconciler
}

func newWASEnv(t *testing.T, kind string, objs ...client.Object) *wasEnv {
	t.Helper()
	scheme := wasScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	recorder := events.NewFakeRecorder(20)
	return &wasEnv{
		t: t, client: c, recorder: recorder,
		r: &WorkloadAutoscalingReconciler{Client: c, Scheme: scheme, Recorder: recorder, Kind: kind},
	}
}

func (e *wasEnv) reconcile(name string) {
	e.t.Helper()
	req := ctrl.Request{NamespacedName: client.ObjectKey{Name: name, Namespace: wasNamespace}}
	if _, err := e.r.Reconcile(context.Background(), req); err != nil {
		e.t.Fatalf("Reconcile failed: %v", err)
	}
}

// hpa returns the HPA with the given name, or nil if it does not exist.
func (e *wasEnv) hpa(name string) *autoscalingv2.HorizontalPodAutoscaler {
	e.t.Helper()
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	err := e.client.Get(context.Background(), client.ObjectKey{Name: name, Namespace: wasNamespace}, hpa)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		e.t.Fatalf("failed to get HPA %s: %v", name, err)
	}
	return hpa
}

func (e *wasEnv) update(obj client.Object) {
	e.t.Helper()
	if err := e.client.Update(context.Background(), obj); err != nil {
		e.t.Fatalf("failed to update %T: %v", obj, err)
	}
}

// eventReasons drains the recorded events and returns their reasons.
func (e *wasEnv) eventReasons() []string {
	var reasons []string
	for {
		select {
		case ev := <-e.recorder.Events:
			// FakeRecorder formats events as "<type> <reason> <note>".
			if fields := strings.Fields(ev); len(fields) >= 2 {
				reasons = append(reasons, fields[1])
			}
		default:
			return reasons
		}
	}
}

func TestWorkloadAutoscalingCreatesHPA(t *testing.T) {
	deploy := wasDeployment("web", wasWebLabels)
	e := newWASEnv(t, utils.DeploymentKind, deploy, wasClass())

	e.reconcile("web")

	hpa := e.hpa("wc-deployment-web")
	if hpa == nil {
		t.Fatal("expected the managed HPA to be created")
	}
	if want := (autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"}); hpa.Spec.ScaleTargetRef != want {
		t.Errorf("scaleTargetRef = %+v, want %+v", hpa.Spec.ScaleTargetRef, want)
	}
	if hpa.Spec.MinReplicas == nil || *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 20 {
		t.Errorf("replica bounds = %v..%d, want 2..20", hpa.Spec.MinReplicas, hpa.Spec.MaxReplicas)
	}
	if len(hpa.Spec.Metrics) != 1 || hpa.Spec.Metrics[0].Resource == nil || hpa.Spec.Metrics[0].Resource.Name != corev1.ResourceCPU {
		t.Errorf("unexpected metrics: %+v", hpa.Spec.Metrics)
	}
	if hpa.Spec.Behavior == nil || *hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds != 300 {
		t.Errorf("expected the Balanced behavior, got %+v", hpa.Spec.Behavior)
	}
	if hpa.Labels[utils.ManagedByLabel] != utils.ManagedByValue || hpa.Labels[utils.WorkloadClassLabel] != wasClassName {
		t.Errorf("unexpected labels: %v", hpa.Labels)
	}
	// The workload, not the WorkloadClass, controls the HPA.
	if !metav1.IsControlledBy(hpa, deploy) {
		t.Errorf("expected the Deployment to control the HPA, got owner references %+v", hpa.OwnerReferences)
	}
	if got := e.eventReasons(); !slices.Equal(got, []string{reasonAutoscalerApplied}) {
		t.Errorf("events = %v, want [%s]", got, reasonAutoscalerApplied)
	}

	// Reconciling again is a no-op.
	e.reconcile("web")
	if again := e.hpa("wc-deployment-web"); again.ResourceVersion != hpa.ResourceVersion {
		t.Errorf("expected the second reconcile not to update the HPA")
	}
	if got := e.eventReasons(); len(got) != 0 {
		t.Errorf("expected no events from a no-op reconcile, got %v", got)
	}
}

func TestWorkloadAutoscalingHandlesItsOwnKindOnly(t *testing.T) {
	deploy := wasDeployment("web", wasWebLabels)
	sts := wasStatefulSet("web", wasWebLabels)

	e := newWASEnv(t, utils.StatefulSetKind, deploy, sts, wasClass())
	e.reconcile("web")

	hpa := e.hpa("wc-statefulset-web")
	if hpa == nil {
		t.Fatal("expected the managed HPA of the StatefulSet to be created")
	}
	if hpa.Spec.ScaleTargetRef.Kind != "StatefulSet" || !metav1.IsControlledBy(hpa, sts) {
		t.Errorf("expected the HPA to target and be controlled by the StatefulSet, got %+v / %+v", hpa.Spec.ScaleTargetRef, hpa.OwnerReferences)
	}
	if e.hpa("wc-deployment-web") != nil {
		t.Errorf("the StatefulSet reconciler must not create an HPA for the Deployment")
	}
}

func TestWorkloadAutoscalingUpdatesHPAWhenTheClassChanges(t *testing.T) {
	wc := wasClass()
	e := newWASEnv(t, utils.DeploymentKind, wasDeployment("web", wasWebLabels), wc)
	e.reconcile("web")
	e.eventReasons()

	wc.Spec.ResourceManagement.Autoscaling.Replicas.Max = 50
	wc.Spec.ResourceManagement.Autoscaling.Profile = workloadsv1.AutoscalingProfileConservative
	e.update(wc)
	e.reconcile("web")

	hpa := e.hpa("wc-deployment-web")
	if hpa.Spec.MaxReplicas != 50 || *hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds != 600 {
		t.Errorf("expected the HPA to follow the WorkloadClass, got maxReplicas %d and behavior %+v", hpa.Spec.MaxReplicas, hpa.Spec.Behavior.ScaleDown)
	}
	if got := e.eventReasons(); !slices.Equal(got, []string{reasonAutoscalerApplied}) {
		t.Errorf("events = %v, want [%s]", got, reasonAutoscalerApplied)
	}
}

func TestWorkloadAutoscalingRestoresAManagedHPAThatWasEdited(t *testing.T) {
	e := newWASEnv(t, utils.DeploymentKind, wasDeployment("web", wasWebLabels), wasClass())
	e.reconcile("web")

	hpa := e.hpa("wc-deployment-web")
	hpa.Spec.MaxReplicas = 999
	hpa.Labels["team"] = "shop"
	e.update(hpa)
	e.reconcile("web")

	restored := e.hpa("wc-deployment-web")
	if restored.Spec.MaxReplicas != 20 {
		t.Errorf("maxReplicas = %d, want it restored to 20", restored.Spec.MaxReplicas)
	}
	if restored.Labels["team"] != "shop" {
		t.Errorf("expected labels added by others to be kept, got %v", restored.Labels)
	}
}

// When the WorkloadClass no longer applies, the managed HPA is deleted and the workload keeps its current
// number of replicas.
func TestWorkloadAutoscalingDeletesHPAWhenTheClassNoLongerApplies(t *testing.T) {
	testCases := []struct {
		name   string
		change func(e *wasEnv, wc *workloadsv1.WorkloadClass, deploy *appsv1.Deployment)
	}{
		{
			name: "validation_failed",
			change: func(e *wasEnv, wc *workloadsv1.WorkloadClass, _ *appsv1.Deployment) {
				wc.Status.Conditions[0].Status = metav1.ConditionFalse
				wc.Status.Conditions[0].Reason = workloadsv1.ReasonValidationFailed
				e.update(wc)
			},
		},
		{
			name: "validation_is_stale",
			change: func(e *wasEnv, wc *workloadsv1.WorkloadClass, _ *appsv1.Deployment) {
				wc.Status.Conditions[0].ObservedGeneration = wasGeneration - 1
				e.update(wc)
			},
		},
		{
			name: "condition_removed",
			change: func(e *wasEnv, wc *workloadsv1.WorkloadClass, _ *appsv1.Deployment) {
				wc.Status.Conditions = nil
				e.update(wc)
			},
		},
		{
			name: "autoscaling_removed",
			change: func(e *wasEnv, wc *workloadsv1.WorkloadClass, _ *appsv1.Deployment) {
				wc.Spec.ResourceManagement.Autoscaling = nil
				wc.Spec.ResourceManagement.Rightsizing = &workloadsv1.Rightsizing{}
				e.update(wc)
			},
		},
		{
			name: "resource_management_removed",
			change: func(e *wasEnv, wc *workloadsv1.WorkloadClass, _ *appsv1.Deployment) {
				wc.Spec.ResourceManagement = nil
				e.update(wc)
			},
		},
		{
			name: "class_deleted",
			change: func(e *wasEnv, wc *workloadsv1.WorkloadClass, _ *appsv1.Deployment) {
				if err := e.client.Delete(context.Background(), wc); err != nil {
					e.t.Fatalf("failed to delete WorkloadClass: %v", err)
				}
			},
		},
		{
			name: "workload_no_longer_matches",
			change: func(e *wasEnv, _ *workloadsv1.WorkloadClass, deploy *appsv1.Deployment) {
				deploy.Spec.Template.Labels = map[string]string{"tier": "db"}
				e.update(deploy)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := wasClass()
			deploy := wasDeployment("web", wasWebLabels)
			e := newWASEnv(t, utils.DeploymentKind, deploy, wc)
			e.reconcile("web")
			if e.hpa("wc-deployment-web") == nil {
				t.Fatal("expected the managed HPA to be created")
			}

			tc.change(e, wc, deploy)
			e.reconcile("web")
			if hpa := e.hpa("wc-deployment-web"); hpa != nil {
				t.Errorf("expected the managed HPA to be deleted, got %+v", hpa.Spec)
			}
		})
	}
}

func TestWorkloadAutoscalingSelectorOverlapStillApplies(t *testing.T) {
	wc := wasClass()
	wc.Status.Conditions[0].Status = metav1.ConditionFalse
	wc.Status.Conditions[0].Reason = workloadsv1.ReasonSelectorOverlap
	e := newWASEnv(t, utils.DeploymentKind, wasDeployment("web", wasWebLabels), wc)

	e.reconcile("web")
	if e.hpa("wc-deployment-web") == nil {
		t.Errorf("expected a WorkloadClass that is only flagged for SelectorOverlap to still apply")
	}
}

func TestWorkloadAutoscalingUserHPATakesPrecedence(t *testing.T) {
	deploy := wasDeployment("web", wasWebLabels)

	t.Run("no_managed_hpa_is_created", func(t *testing.T) {
		e := newWASEnv(t, utils.DeploymentKind, deploy, wasClass(), wasUserHPA("my-hpa", "Deployment", "web"))
		e.reconcile("web")
		if e.hpa("wc-deployment-web") != nil {
			t.Errorf("expected no managed HPA when a user HPA targets the workload")
		}
		if got := e.eventReasons(); !slices.Equal(got, []string{reasonUserAutoscalerPresent}) {
			t.Errorf("events = %v, want [%s]", got, reasonUserAutoscalerPresent)
		}
		if user := e.hpa("my-hpa"); user == nil || user.Spec.MaxReplicas != 3 || len(user.OwnerReferences) != 0 {
			t.Errorf("expected the user HPA to be left alone, got %+v", user)
		}
	})

	t.Run("managed_hpa_yields_to_a_new_user_hpa_and_is_reclaimed", func(t *testing.T) {
		e := newWASEnv(t, utils.DeploymentKind, deploy, wasClass())
		e.reconcile("web")
		if e.hpa("wc-deployment-web") == nil {
			t.Fatal("expected the managed HPA to be created")
		}

		user := wasUserHPA("my-hpa", "Deployment", "web")
		if err := e.client.Create(context.Background(), user); err != nil {
			t.Fatalf("failed to create the user HPA: %v", err)
		}
		e.reconcile("web")
		if e.hpa("wc-deployment-web") != nil {
			t.Errorf("expected the managed HPA to be deleted once a user HPA targets the workload")
		}

		if err := e.client.Delete(context.Background(), user); err != nil {
			t.Fatalf("failed to delete the user HPA: %v", err)
		}
		e.reconcile("web")
		if e.hpa("wc-deployment-web") == nil {
			t.Errorf("expected the managed HPA to be recreated once the user HPA is gone")
		}
	})

	t.Run("user_hpas_of_other_workloads_are_ignored", func(t *testing.T) {
		e := newWASEnv(t, utils.DeploymentKind, deploy, wasClass(),
			wasUserHPA("other-deployment", "Deployment", "api"),
			wasUserHPA("statefulset-with-the-same-name", "StatefulSet", "web"))
		e.reconcile("web")
		if e.hpa("wc-deployment-web") == nil {
			t.Errorf("expected the managed HPA to be created")
		}
	})
}

// An HPA that uses the managed name but was not created for a WorkloadClass is never modified or deleted.
func TestWorkloadAutoscalingNameConflict(t *testing.T) {
	conflicting := wasUserHPA("wc-deployment-web", "Deployment", "another-workload")
	e := newWASEnv(t, utils.DeploymentKind, wasDeployment("web", wasWebLabels), wasClass(), conflicting)

	e.reconcile("web")

	hpa := e.hpa("wc-deployment-web")
	if hpa == nil || hpa.Spec.ScaleTargetRef.Name != "another-workload" || hpa.Spec.MaxReplicas != 3 || len(hpa.OwnerReferences) != 0 {
		t.Errorf("expected the conflicting HPA to be left alone, got %+v", hpa)
	}
	if got := e.eventReasons(); !slices.Equal(got, []string{reasonAutoscalerNameConflict}) {
		t.Errorf("events = %v, want [%s]", got, reasonAutoscalerNameConflict)
	}

	// It is not deleted either when the WorkloadClass stops applying.
	wc := wasClass()
	if err := e.client.Delete(context.Background(), wc); err != nil {
		t.Fatalf("failed to delete WorkloadClass: %v", err)
	}
	e.reconcile("web")
	if e.hpa("wc-deployment-web") == nil {
		t.Errorf("expected the conflicting HPA not to be deleted")
	}
}

func TestWorkloadAutoscalingIgnoresMissingAndUngovernedWorkloads(t *testing.T) {
	e := newWASEnv(t, utils.DeploymentKind, wasDeployment("db", map[string]string{"tier": "db"}), wasClass())

	e.reconcile("does-not-exist")
	e.reconcile("db")

	hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
	if err := e.client.List(context.Background(), hpas); err != nil {
		t.Fatalf("failed to list HPAs: %v", err)
	}
	if len(hpas.Items) != 0 {
		t.Errorf("expected no HPAs, got %d", len(hpas.Items))
	}
	if got := e.eventReasons(); len(got) != 0 {
		t.Errorf("expected no events, got %v", got)
	}
}

func TestWorkloadAutoscalingMapFunctions(t *testing.T) {
	ctx := context.Background()
	e := newWASEnv(t, utils.DeploymentKind,
		wasDeployment("web", wasWebLabels), wasDeployment("api", wasWebLabels), wasStatefulSet("db", wasWebLabels))

	names := func(reqs []ctrl.Request) []string {
		out := make([]string, 0, len(reqs))
		for _, r := range reqs {
			out = append(out, r.Namespace+"/"+r.Name)
		}
		slices.Sort(out)
		return out
	}

	// A WorkloadClass or Namespace change re-queues the workloads of the reconciler's kind in the namespace.
	want := []string{"shop/api", "shop/web"}
	if got := names(e.r.findWorkloadsInObjectNamespace(ctx, wasClass())); !slices.Equal(got, want) {
		t.Errorf("findWorkloadsInObjectNamespace = %v, want %v", got, want)
	}
	if got := names(e.r.findWorkloadsInNamespace(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: wasNamespace}})); !slices.Equal(got, want) {
		t.Errorf("findWorkloadsInNamespace = %v, want %v", got, want)
	}
	if got := e.r.findWorkloadsInNamespace(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "empty"}}); len(got) != 0 {
		t.Errorf("expected no requests for a namespace without workloads, got %v", got)
	}

	// A managed HPA maps to its controller owner, and a user HPA to its scale target.
	managed := utils.BuildHPA(wasClass(), utils.WorkloadRef{Kind: utils.DeploymentKind, Namespace: wasNamespace, Name: "web"})
	managed.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "web", Controller: ptr.To(true)}}
	if got := names(e.r.findWorkloadForAutoscaler(ctx, managed)); !slices.Equal(got, []string{"shop/web"}) {
		t.Errorf("managed HPA maps to %v, want [shop/web]", got)
	}
	if got := names(e.r.findWorkloadForAutoscaler(ctx, wasUserHPA("my-hpa", "Deployment", "api"))); !slices.Equal(got, []string{"shop/api"}) {
		t.Errorf("user HPA maps to %v, want [shop/api]", got)
	}
	// HPAs of the other kind of workload belong to the other reconciler.
	managed.OwnerReferences[0].Kind = "StatefulSet"
	if got := e.r.findWorkloadForAutoscaler(ctx, managed); len(got) != 0 {
		t.Errorf("expected a StatefulSet's managed HPA to be ignored, got %v", got)
	}
	if got := e.r.findWorkloadForAutoscaler(ctx, wasUserHPA("sts-hpa", "StatefulSet", "db")); len(got) != 0 {
		t.Errorf("expected a StatefulSet's user HPA to be ignored, got %v", got)
	}
}

func TestWorkloadAutoscalingPredicates(t *testing.T) {
	t.Run("workload", func(t *testing.T) {
		p := workloadTemplateLabelsPredicate()
		base := wasDeployment("web", wasWebLabels)
		scaled := base.DeepCopy()
		scaled.Spec.Replicas = ptr.To[int32](7)
		relabeled := base.DeepCopy()
		relabeled.Spec.Template.Labels = map[string]string{"tier": "db"}

		if !p.Create(event.CreateEvent{Object: base}) {
			t.Errorf("expected creation to pass")
		}
		if p.Delete(event.DeleteEvent{Object: base}) {
			t.Errorf("expected deletion to be ignored")
		}
		if p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: scaled}) {
			t.Errorf("expected a replica change to be ignored")
		}
		if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: relabeled}) {
			t.Errorf("expected a pod template label change to pass")
		}
	})

	t.Run("workload_class", func(t *testing.T) {
		p := workloadClassResourceManagementPredicate()
		base := wasClass()
		readiness := base.DeepCopy()
		readiness.Status.MaintenanceReadiness = workloadsv1.ReadinessReady
		edited := base.DeepCopy()
		edited.Generation++
		failed := base.DeepCopy()
		failed.Status.Conditions[0].Status = metav1.ConditionFalse
		cleared := base.DeepCopy()
		cleared.Status.Conditions = nil

		if !p.Create(event.CreateEvent{Object: base}) || !p.Delete(event.DeleteEvent{Object: base}) {
			t.Errorf("expected creation and deletion to pass")
		}
		if p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: readiness}) {
			t.Errorf("expected an unrelated status change to be ignored")
		}
		for name, changed := range map[string]*workloadsv1.WorkloadClass{"spec": edited, "condition status": failed, "condition removal": cleared} {
			if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: changed}) {
				t.Errorf("expected a %s change to pass", name)
			}
		}
	})

	t.Run("autoscaler", func(t *testing.T) {
		p := autoscalerChangedPredicate()
		base := wasUserHPA("my-hpa", "Deployment", "web")
		status := base.DeepCopy()
		status.Status.CurrentReplicas = 5
		spec := base.DeepCopy()
		spec.Spec.MaxReplicas = 10
		labels := base.DeepCopy()
		labels.Labels = map[string]string{utils.ManagedByLabel: utils.ManagedByValue}

		if !p.Create(event.CreateEvent{Object: base}) || !p.Delete(event.DeleteEvent{Object: base}) {
			t.Errorf("expected creation and deletion to pass")
		}
		if p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: status}) {
			t.Errorf("expected a status update to be ignored")
		}
		if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: spec}) || !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: labels}) {
			t.Errorf("expected spec and label changes to pass")
		}
	})
}
