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
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// wasRightsizingClass returns a validated WorkloadClass that governs the workloads with wasWebLabels and only
// sets rightsizing.
func wasRightsizingClass(rs *workloadsv1.Rightsizing) *workloadsv1.WorkloadClass {
	wc := wasClass()
	wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{Rightsizing: rs}
	return wc
}

func wasAutoRightsizing() *workloadsv1.Rightsizing {
	return &workloadsv1.Rightsizing{
		Mode:              workloadsv1.RightsizingModeAuto,
		ExcludeContainers: []string{"istio-proxy"},
		Bounds:            &workloadsv1.ContainerBounds{Min: rmResources("100m", "128Mi"), Max: rmResources("4", "8Gi")},
	}
}

func wasUserVPA(name, targetKind, targetName string) *unstructured.Unstructured {
	vpa := newVPA()
	vpa.SetName(name)
	vpa.SetNamespace(wasNamespace)
	_ = unstructured.SetNestedMap(vpa.Object, map[string]any{"apiVersion": "apps/v1", "kind": targetKind, "name": targetName}, "spec", "targetRef")
	_ = unstructured.SetNestedField(vpa.Object, "Off", "spec", "updatePolicy", "updateMode")
	return vpa
}

// newVPAEnv returns an environment whose reconciler manages VerticalPodAutoscalers.
func newVPAEnv(t *testing.T, objs ...client.Object) *wasEnv {
	t.Helper()
	e := newWASEnv(t, utils.DeploymentKind, objs...)
	e.r.VPAInstalled = true
	return e
}

// vpa returns the VPA with the given name, or nil if it does not exist.
func (e *wasEnv) vpa(name string) *unstructured.Unstructured {
	e.t.Helper()
	vpa := newVPA()
	err := e.client.Get(context.Background(), client.ObjectKey{Name: name, Namespace: wasNamespace}, vpa)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		e.t.Fatalf("failed to get VPA %s: %v", name, err)
	}
	return vpa
}

func vpaContainerPolicies(t *testing.T, vpa *unstructured.Unstructured) []any {
	t.Helper()
	policies, _, err := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
	if err != nil {
		t.Fatalf("failed to read containerPolicies: %v", err)
	}
	return policies
}

func TestWorkloadAutoscalingCreatesVPA(t *testing.T) {
	deploy := wasDeployment("web", wasWebLabels)
	e := newVPAEnv(t, deploy, wasRightsizingClass(wasAutoRightsizing()))

	e.reconcile("web")

	vpa := e.vpa("wc-deployment-web")
	if vpa == nil {
		t.Fatal("expected the managed VPA to be created")
	}
	if !utils.VPATargetsWorkload(vpa, utils.WorkloadRef{Kind: utils.DeploymentKind, Name: "web"}) {
		t.Errorf("expected the VPA to target the Deployment, got %v", vpa.Object["spec"])
	}
	if mode := utils.VPAUpdateModeOf(vpa); mode != utils.VPAUpdateModeInPlaceOrRecreate {
		t.Errorf("update mode = %q, want %q", mode, utils.VPAUpdateModeInPlaceOrRecreate)
	}
	want := []any{
		map[string]any{
			"containerName":       "*",
			"controlledResources": []any{"cpu", "memory"},
			"minAllowed":          map[string]any{"cpu": "100m", "memory": "128Mi"},
			"maxAllowed":          map[string]any{"cpu": "4", "memory": "8Gi"},
		},
		map[string]any{"containerName": "istio-proxy", "mode": "Off"},
	}
	if got := vpaContainerPolicies(t, vpa); !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("containerPolicies mismatch\n got: %v\nwant: %v", got, want)
	}
	if labels := vpa.GetLabels(); labels[utils.ManagedByLabel] != utils.ManagedByValue || labels[utils.WorkloadClassLabel] != "web-services" {
		t.Errorf("unexpected labels: %v", labels)
	}
	if !metav1.IsControlledBy(vpa, deploy) {
		t.Errorf("expected the Deployment to control the VPA, got owner references %+v", vpa.GetOwnerReferences())
	}
	// This WorkloadClass does not set autoscaling, so no HPA is created.
	if e.hpa("wc-deployment-web") != nil {
		t.Errorf("expected no HPA for a WorkloadClass without autoscaling")
	}
	if got := e.eventReasons(); !slices.Equal(got, []string{reasonAutoscalerApplied}) {
		t.Errorf("events = %v, want [%s]", got, reasonAutoscalerApplied)
	}

	// Reconciling again is a no-op.
	e.reconcile("web")
	if again := e.vpa("wc-deployment-web"); again.GetResourceVersion() != vpa.GetResourceVersion() {
		t.Errorf("expected the second reconcile not to update the VPA")
	}
	if got := e.eventReasons(); len(got) != 0 {
		t.Errorf("expected no events from a no-op reconcile, got %v", got)
	}
}

func TestWorkloadAutoscalingVPANotInstalled(t *testing.T) {
	wc := wasClass()
	wc.Spec.ResourceManagement.Rightsizing = wasAutoRightsizing()
	e := newWASEnv(t, utils.DeploymentKind, wasDeployment("web", wasWebLabels), wc)

	e.reconcile("web")

	// Autoscaling only needs the core HPA API, so it still applies.
	if e.hpa("wc-deployment-web") == nil {
		t.Errorf("expected the managed HPA to be created")
	}
	if e.vpa("wc-deployment-web") != nil {
		t.Errorf("expected no VPA when the VerticalPodAutoscaler CRD is not installed")
	}
}

func TestWorkloadAutoscalingRightsizingModes(t *testing.T) {
	testCases := []struct {
		mode     workloadsv1.RightsizingMode
		strategy workloadsv1.RightsizingUpdateStrategy
		want     string // empty: no VPA
	}{
		{mode: workloadsv1.RightsizingModeOff},
		{mode: "", want: "Off"},
		{mode: workloadsv1.RightsizingModeRecommend, want: "Off"},
		{mode: workloadsv1.RightsizingModeOnCreate, want: "Initial"},
		{mode: workloadsv1.RightsizingModeAuto, want: "InPlaceOrRecreate"},
		{mode: workloadsv1.RightsizingModeAuto, strategy: workloadsv1.RightsizingUpdateStrategyRecreate, want: "Recreate"},
	}
	for _, tc := range testCases {
		t.Run(string(tc.mode)+"_"+string(tc.strategy), func(t *testing.T) {
			wc := wasRightsizingClass(&workloadsv1.Rightsizing{Mode: tc.mode, UpdateStrategy: tc.strategy})
			e := newVPAEnv(t, wasDeployment("web", wasWebLabels), wc)
			e.reconcile("web")

			vpa := e.vpa("wc-deployment-web")
			switch {
			case tc.want == "" && vpa != nil:
				t.Errorf("expected no VPA, got %v", vpa.Object["spec"])
			case tc.want != "" && vpa == nil:
				t.Errorf("expected a VPA with update mode %q", tc.want)
			case tc.want != "" && utils.VPAUpdateModeOf(vpa) != tc.want:
				t.Errorf("update mode = %q, want %q", utils.VPAUpdateModeOf(vpa), tc.want)
			}
		})
	}
}

// Scaling on the utilization of a resource leaves that resource out of rightsizing, unless the platform
// coordinates the two.
func TestWorkloadAutoscalingScalingAndRightsizingTheSameResource(t *testing.T) {
	newClass := func(metrics ...autoscalingv2.MetricSpec) *workloadsv1.WorkloadClass {
		wc := wasClass()
		wc.Spec.ResourceManagement.Autoscaling.Metrics = metrics
		wc.Spec.ResourceManagement.Rightsizing = &workloadsv1.Rightsizing{}
		return wc
	}
	memoryUtilization := wasCPUMetric()
	memoryUtilization.Resource.Name = corev1.ResourceMemory
	controlled := func(t *testing.T, vpa *unstructured.Unstructured) any {
		return vpaContainerPolicies(t, vpa)[0].(map[string]any)["controlledResources"]
	}

	t.Run("cpu_is_left_to_the_hpa", func(t *testing.T) {
		e := newVPAEnv(t, wasDeployment("web", wasWebLabels), newClass(wasCPUMetric()))
		e.reconcile("web")
		if e.hpa("wc-deployment-web") == nil {
			t.Fatal("expected the managed HPA to be created")
		}
		vpa := e.vpa("wc-deployment-web")
		if vpa == nil {
			t.Fatal("expected the managed VPA to be created")
		}
		if got := controlled(t, vpa); !equality.Semantic.DeepEqual(got, []any{"memory"}) {
			t.Errorf("controlledResources = %v, want only memory", got)
		}
	})

	t.Run("coordinated_cpu_is_rightsized_too", func(t *testing.T) {
		e := newVPAEnv(t, wasDeployment("web", wasWebLabels), newClass(wasCPUMetric()))
		e.r.CoordinatedResources = []corev1.ResourceName{corev1.ResourceCPU}
		e.reconcile("web")
		if got := controlled(t, e.vpa("wc-deployment-web")); !equality.Semantic.DeepEqual(got, []any{"cpu", "memory"}) {
			t.Errorf("controlledResources = %v, want cpu and memory", got)
		}
	})

	t.Run("no_vpa_when_every_resource_is_left_to_the_hpa", func(t *testing.T) {
		e := newVPAEnv(t, wasDeployment("web", wasWebLabels), newClass(wasCPUMetric(), memoryUtilization))
		e.reconcile("web")
		if e.hpa("wc-deployment-web") == nil {
			t.Errorf("expected the managed HPA to be created")
		}
		if vpa := e.vpa("wc-deployment-web"); vpa != nil {
			t.Errorf("expected no VPA, got %v", vpa.Object["spec"])
		}
	})
}

func TestWorkloadAutoscalingVPAUsesGuardrailBounds(t *testing.T) {
	loose := rmGuardrails(&workloadsv1.ResourceManagementConstraints{
		ContainerBounds: &workloadsv1.ContainerBounds{Min: rmResources("10m", "32Mi"), Max: rmResources("16", "64Gi")},
	}).Items[0]
	loose.Name = "loose"
	strict := rmGuardrails(&workloadsv1.ResourceManagementConstraints{
		ContainerBounds: &workloadsv1.ContainerBounds{Max: rmResources("8", "128Gi")},
	}).Items[0]
	strict.Name = "strict"

	// The class sets no bounds, so the strictest guardrail bounds apply.
	wc := wasRightsizingClass(&workloadsv1.Rightsizing{})
	e := newVPAEnv(t, wasDeployment("web", wasWebLabels), wc, &loose, &strict)
	e.reconcile("web")

	want := []any{map[string]any{
		"containerName":       "*",
		"controlledResources": []any{"cpu", "memory"},
		"minAllowed":          map[string]any{"cpu": "10m", "memory": "32Mi"},
		"maxAllowed":          map[string]any{"cpu": "8", "memory": "64Gi"},
	}}
	if got := vpaContainerPolicies(t, e.vpa("wc-deployment-web")); !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("containerPolicies mismatch\n got: %v\nwant: %v", got, want)
	}
}

func TestWorkloadAutoscalingDeletesVPAWhenTheClassNoLongerApplies(t *testing.T) {
	testCases := []struct {
		name   string
		change func(wc *workloadsv1.WorkloadClass)
	}{
		{"mode_off", func(wc *workloadsv1.WorkloadClass) {
			wc.Spec.ResourceManagement.Rightsizing.Mode = workloadsv1.RightsizingModeOff
		}},
		{"rightsizing_removed", func(wc *workloadsv1.WorkloadClass) {
			wc.Spec.ResourceManagement.Rightsizing = nil
			wc.Spec.ResourceManagement.InitialResources = rmInitialResources().InitialResources
		}},
		{"validation_failed", func(wc *workloadsv1.WorkloadClass) {
			wc.Status.Conditions[0].Status = metav1.ConditionFalse
			wc.Status.Conditions[0].Reason = workloadsv1.ReasonValidationFailed
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := wasRightsizingClass(wasAutoRightsizing())
			e := newVPAEnv(t, wasDeployment("web", wasWebLabels), wc)
			e.reconcile("web")
			if e.vpa("wc-deployment-web") == nil {
				t.Fatal("expected the managed VPA to be created")
			}

			tc.change(wc)
			e.update(wc)
			e.reconcile("web")
			if vpa := e.vpa("wc-deployment-web"); vpa != nil {
				t.Errorf("expected the managed VPA to be deleted, got %v", vpa.Object["spec"])
			}
		})
	}
}

func TestWorkloadAutoscalingUserVPATakesPrecedence(t *testing.T) {
	deploy := wasDeployment("web", wasWebLabels)
	e := newVPAEnv(t, deploy, wasRightsizingClass(wasAutoRightsizing()),
		wasUserVPA("vpa-of-another-workload", "Deployment", "api"))
	e.reconcile("web")
	if e.vpa("wc-deployment-web") == nil {
		t.Fatal("expected the managed VPA to be created")
	}
	e.eventReasons()

	user := wasUserVPA("my-vpa", "Deployment", "web")
	if err := e.client.Create(context.Background(), user); err != nil {
		t.Fatalf("failed to create the user VPA: %v", err)
	}
	e.reconcile("web")
	if e.vpa("wc-deployment-web") != nil {
		t.Errorf("expected the managed VPA to be deleted once a user VPA targets the workload")
	}
	if got := e.eventReasons(); !slices.Equal(got, []string{reasonUserAutoscalerPresent}) {
		t.Errorf("events = %v, want [%s]", got, reasonUserAutoscalerPresent)
	}
	if kept := e.vpa("my-vpa"); kept == nil || utils.VPAUpdateModeOf(kept) != utils.VPAUpdateModeOff || len(kept.GetOwnerReferences()) != 0 {
		t.Errorf("expected the user VPA to be left alone, got %v", kept)
	}

	if err := e.client.Delete(context.Background(), user); err != nil {
		t.Fatalf("failed to delete the user VPA: %v", err)
	}
	e.reconcile("web")
	if e.vpa("wc-deployment-web") == nil {
		t.Errorf("expected the managed VPA to be recreated once the user VPA is gone")
	}
}

func TestWorkloadAutoscalingVPANameConflict(t *testing.T) {
	conflicting := wasUserVPA("wc-deployment-web", "Deployment", "another-workload")
	e := newVPAEnv(t, wasDeployment("web", wasWebLabels), wasRightsizingClass(wasAutoRightsizing()), conflicting)

	e.reconcile("web")

	vpa := e.vpa("wc-deployment-web")
	if vpa == nil || utils.VPAUpdateModeOf(vpa) != utils.VPAUpdateModeOff || len(vpa.GetOwnerReferences()) != 0 {
		t.Errorf("expected the conflicting VPA to be left alone, got %v", vpa)
	}
	if got := e.eventReasons(); !slices.Equal(got, []string{reasonAutoscalerNameConflict}) {
		t.Errorf("events = %v, want [%s]", got, reasonAutoscalerNameConflict)
	}
}

// rejectingInPlaceClient returns a client that refuses VerticalPodAutoscalers with the InPlaceOrRecreate update
// mode, like a VPA installation that does not support in-place updates, and counts the refusals.
func rejectingInPlaceClient(t *testing.T, rejected *int, objs ...client.Object) client.Client {
	t.Helper()
	reject := func(obj client.Object) error {
		vpa, ok := obj.(*unstructured.Unstructured)
		if !ok || utils.VPAUpdateModeOf(vpa) != utils.VPAUpdateModeInPlaceOrRecreate {
			return nil
		}
		*rejected++
		return apierrors.NewInvalid(utils.VPAGroupVersionKind.GroupKind(), vpa.GetName(), field.ErrorList{
			field.NotSupported(field.NewPath("spec", "updatePolicy", "updateMode"), utils.VPAUpdateModeInPlaceOrRecreate, []string{"Off", "Initial", "Recreate", "Auto"}),
		})
	}
	return fake.NewClientBuilder().WithScheme(wasScheme(t)).WithObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := reject(obj); err != nil {
				return err
			}
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := reject(obj); err != nil {
				return err
			}
			return c.Update(ctx, obj, opts...)
		},
	}).Build()
}

func TestWorkloadAutoscalingFallsBackWhenInPlaceIsUnsupported(t *testing.T) {
	rejected := 0
	c := rejectingInPlaceClient(t, &rejected,
		wasDeployment("web", wasWebLabels), wasDeployment("api", wasWebLabels), wasRightsizingClass(wasAutoRightsizing()))
	recorder := events.NewFakeRecorder(20)
	e := &wasEnv{t: t, client: c, recorder: recorder}
	e.r = &WorkloadAutoscalingReconciler{Client: c, Scheme: wasScheme(t), Recorder: recorder, Kind: utils.DeploymentKind, VPAInstalled: true}

	e.reconcile("web")
	vpa := e.vpa("wc-deployment-web")
	if vpa == nil || utils.VPAUpdateModeOf(vpa) != utils.VPAUpdateModeRecreate {
		t.Fatalf("expected a VPA with the Recreate fallback, got %v", vpa)
	}
	if rejected != 1 {
		t.Errorf("expected 1 rejected attempt, got %d", rejected)
	}
	if got := e.eventReasons(); !slices.Equal(got, []string{reasonAutoscalerApplied, reasonInPlaceUnsupported}) {
		t.Errorf("events = %v, want [%s %s]", got, reasonAutoscalerApplied, reasonInPlaceUnsupported)
	}

	// The outcome is remembered: the next workload gets Recreate without another rejected attempt.
	e.reconcile("api")
	if other := e.vpa("wc-deployment-api"); other == nil || utils.VPAUpdateModeOf(other) != utils.VPAUpdateModeRecreate {
		t.Errorf("expected the next VPA to use Recreate directly, got %v", other)
	}
	if rejected != 1 {
		t.Errorf("expected no further rejected attempts, got %d in total", rejected)
	}
	if got := e.eventReasons(); !slices.Equal(got, []string{reasonAutoscalerApplied, reasonInPlaceUnsupported}) {
		t.Errorf("events = %v, want [%s %s]", got, reasonAutoscalerApplied, reasonInPlaceUnsupported)
	}

	// A no-op reconcile does not repeat the events.
	e.reconcile("web")
	if got := e.eventReasons(); len(got) != 0 {
		t.Errorf("expected no events from a no-op reconcile, got %v", got)
	}
}

// A rejection that is not about the update mode is returned, and is not mistaken for missing in-place support.
func TestWorkloadAutoscalingDoesNotFallBackOnUnrelatedErrors(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "autoscaling.k8s.io", Resource: "verticalpodautoscalers"}, "wc-deployment-web", nil)
	c := fake.NewClientBuilder().WithScheme(wasScheme(t)).
		WithObjects(wasDeployment("web", wasWebLabels), wasRightsizingClass(wasAutoRightsizing())).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*unstructured.Unstructured); ok {
					return forbidden
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()
	r := &WorkloadAutoscalingReconciler{Client: c, Scheme: wasScheme(t), Recorder: events.NewFakeRecorder(20), Kind: utils.DeploymentKind, VPAInstalled: true}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Name: "web", Namespace: wasNamespace}})
	if err == nil || !strings.Contains(err.Error(), "failed to reconcile VerticalPodAutoscaler") {
		t.Fatalf("expected the VPA error to be returned, got %v", err)
	}
	if r.inPlaceUnsupported.Load() {
		t.Errorf("an error that Recreate does not fix must not be remembered as missing in-place support")
	}
}

// A failure to reconcile one autoscaler never blocks the other.
func TestWorkloadAutoscalingReconcilesTheHPAWhenTheVPAFails(t *testing.T) {
	wc := wasClass()
	wc.Spec.ResourceManagement.Rightsizing = &workloadsv1.Rightsizing{Resources: []workloadsv1.RightsizedResource{workloadsv1.RightsizedResourceMemory}}
	c := fake.NewClientBuilder().WithScheme(wasScheme(t)).WithObjects(wasDeployment("web", wasWebLabels), wc).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*unstructured.Unstructured); ok {
					return apierrors.NewServiceUnavailable("the VPA webhook is down")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()
	e := &wasEnv{t: t, client: c, recorder: events.NewFakeRecorder(20)}
	e.r = &WorkloadAutoscalingReconciler{Client: c, Scheme: wasScheme(t), Recorder: e.recorder, Kind: utils.DeploymentKind, VPAInstalled: true}

	_, err := e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Name: "web", Namespace: wasNamespace}})
	if err == nil {
		t.Fatal("expected the VPA error to be returned")
	}
	if e.hpa("wc-deployment-web") == nil {
		t.Errorf("expected the HPA to be created even though the VPA failed")
	}
}

func TestWorkloadAutoscalingVPAMapFunctionAndPredicate(t *testing.T) {
	ctx := context.Background()
	e := newVPAEnv(t)

	managed := utils.BuildVPA(wasRightsizingClass(wasAutoRightsizing()),
		utils.WorkloadRef{Kind: utils.DeploymentKind, Namespace: wasNamespace, Name: "web"}, utils.EffectiveResourceManagement{}, utils.VPACapabilities{})
	managed.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "web", Controller: ptr.To(true)}})
	if got := e.r.findWorkloadForAutoscaler(ctx, managed); len(got) != 1 || got[0].Name != "web" || got[0].Namespace != wasNamespace {
		t.Errorf("managed VPA maps to %v, want shop/web", got)
	}
	if got := e.r.findWorkloadForAutoscaler(ctx, wasUserVPA("my-vpa", "Deployment", "api")); len(got) != 1 || got[0].Name != "api" {
		t.Errorf("user VPA maps to %v, want shop/api", got)
	}
	if got := e.r.findWorkloadForAutoscaler(ctx, wasUserVPA("sts-vpa", "StatefulSet", "db")); len(got) != 0 {
		t.Errorf("expected a StatefulSet's VPA to be ignored by the Deployment reconciler, got %v", got)
	}
	if got := e.r.findWorkloadForAutoscaler(ctx, &appsv1.DaemonSet{}); len(got) != 0 {
		t.Errorf("expected other objects to be ignored, got %v", got)
	}

	// The VPA recommender writes the status continuously; only spec changes re-queue the workload.
	p := autoscalerChangedPredicate()
	base := wasUserVPA("my-vpa", "Deployment", "web")
	status := base.DeepCopy()
	_ = unstructured.SetNestedField(status.Object, "1", "status", "recommendation", "containerRecommendations")
	spec := base.DeepCopy()
	_ = unstructured.SetNestedField(spec.Object, "Recreate", "spec", "updatePolicy", "updateMode")
	if p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: status}) {
		t.Errorf("expected a VPA status update to be ignored")
	}
	if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: spec}) {
		t.Errorf("expected a VPA spec change to pass")
	}
}

func TestWorkloadAutoscalingFindAllWorkloads(t *testing.T) {
	other := wasDeployment("web", wasWebLabels)
	other.Namespace = "other"
	e := newVPAEnv(t, wasDeployment("web", wasWebLabels), other, wasStatefulSet("db", wasWebLabels))

	got := e.r.findAllWorkloads(context.Background(), &workloadsv1.WorkloadClassGuardrail{})
	names := make([]string, 0, len(got))
	for _, req := range got {
		names = append(names, req.Namespace+"/"+req.Name)
	}
	slices.Sort(names)
	if want := []string{"other/web", "shop/web"}; !slices.Equal(names, want) {
		t.Errorf("findAllWorkloads = %v, want %v", names, want)
	}
}

func TestIsVPAInstalled(t *testing.T) {
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, meta.RESTScopeNamespace)
	if installed, err := IsVPAInstalled(mapper); err != nil || installed {
		t.Errorf("without the CRD: got installed=%v err=%v, want false and no error", installed, err)
	}

	mapper.Add(utils.VPAGroupVersionKind, meta.RESTScopeNamespace)
	if installed, err := IsVPAInstalled(mapper); err != nil || !installed {
		t.Errorf("with the CRD: got installed=%v err=%v, want true and no error", installed, err)
	}
}

func TestResourceManagementConditionReportsMissingVPA(t *testing.T) {
	rightsizing := rmWorkloadClass(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{}})
	off := rmWorkloadClass(&workloadsv1.ResourceManagement{
		InitialResources: rmInitialResources().InitialResources,
		Rightsizing:      &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOff},
	})

	// The status is never affected, so initial resources and autoscaling still apply.
	cond := resourceManagementValidatedCondition(rightsizing, rmGuardrails(), nil, false)
	if cond.Status != metav1.ConditionTrue || !strings.Contains(cond.Message, vpaNotInstalledMsg) {
		t.Errorf("without VPA: got %s with message %q, want True with the VPA note", cond.Status, cond.Message)
	}
	if cond := resourceManagementValidatedCondition(rightsizing, rmGuardrails(), nil, true); strings.Contains(cond.Message, vpaNotInstalledMsg) {
		t.Errorf("with VPA: unexpected note in message %q", cond.Message)
	}
	if cond := resourceManagementValidatedCondition(off, rmGuardrails(), nil, false); strings.Contains(cond.Message, vpaNotInstalledMsg) {
		t.Errorf("rightsizing Off: unexpected note in message %q", cond.Message)
	}
}

func TestWarnRightsizingNotApplied(t *testing.T) {
	recorded := func(recorder *events.FakeRecorder) []string {
		var got []string
		for {
			select {
			case ev := <-recorder.Events:
				got = append(got, ev)
			default:
				return got
			}
		}
	}
	scalingOn := func(names ...corev1.ResourceName) *workloadsv1.WorkloadClass {
		wc := rmWorkloadClass(&workloadsv1.ResourceManagement{
			InitialResources: rmInitialResources().InitialResources,
			Autoscaling:      &workloadsv1.Autoscaling{Replicas: workloadsv1.ReplicaBounds{Max: 5}},
			Rightsizing:      &workloadsv1.Rightsizing{},
		})
		for _, name := range names {
			wc.Spec.ResourceManagement.Autoscaling.Metrics = append(wc.Spec.ResourceManagement.Autoscaling.Metrics, autoscalingv2.MetricSpec{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name:   name,
					Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType},
				},
			})
		}
		return wc
	}

	t.Run("vpa_not_installed", func(t *testing.T) {
		recorder := events.NewFakeRecorder(10)
		r := &WorkloadClassReconciler{Recorder: recorder}
		wc := scalingOn()
		r.warnRightsizingNotApplied(wc)
		r.warnRightsizingNotApplied(wc)
		got := recorded(recorder)
		if len(got) != 1 || !strings.HasPrefix(got[0], corev1.EventTypeWarning+" "+reasonVPANotInstalled) {
			t.Errorf("expected one VPANotInstalled warning, got %q", got)
		}
	})

	t.Run("resource_left_to_horizontal_scaling", func(t *testing.T) {
		recorder := events.NewFakeRecorder(10)
		r := &WorkloadClassReconciler{Recorder: recorder, VPAInstalled: true}
		r.warnRightsizingNotApplied(scalingOn(corev1.ResourceCPU))
		got := recorded(recorder)
		if len(got) != 1 || !strings.Contains(got[0], reasonRightsizingSkipped) || !strings.Contains(got[0], "leaves cpu to horizontal scaling") {
			t.Errorf("expected one RightsizingSkipped event for cpu, got %q", got)
		}
		if strings.Contains(got[0], "no VerticalPodAutoscaler is created") {
			t.Errorf("memory is still rightsized, so a VPA is created: %q", got[0])
		}
	})

	t.Run("every_resource_left_to_horizontal_scaling", func(t *testing.T) {
		recorder := events.NewFakeRecorder(10)
		r := &WorkloadClassReconciler{Recorder: recorder, VPAInstalled: true}
		r.warnRightsizingNotApplied(scalingOn(corev1.ResourceCPU, corev1.ResourceMemory))
		got := recorded(recorder)
		if len(got) != 1 || !strings.Contains(got[0], "cpu and memory") || !strings.Contains(got[0], "no VerticalPodAutoscaler is created") {
			t.Errorf("expected one RightsizingSkipped event saying that no VPA is created, got %q", got)
		}
	})

	t.Run("nothing_to_report", func(t *testing.T) {
		recorder := events.NewFakeRecorder(10)
		r := &WorkloadClassReconciler{Recorder: recorder, VPAInstalled: true, CoordinatedResources: []corev1.ResourceName{corev1.ResourceCPU}}
		r.warnRightsizingNotApplied(scalingOn())                           // nothing scales on utilization
		r.warnRightsizingNotApplied(scalingOn(corev1.ResourceCPU))         // cpu is coordinated
		r.warnRightsizingNotApplied(rmWorkloadClass(rmInitialResources())) // no rightsizing
		r.warnRightsizingNotApplied(rmWorkloadClass(nil))
		if got := recorded(recorder); len(got) != 0 {
			t.Errorf("expected no events, got %q", got)
		}
	})
}
