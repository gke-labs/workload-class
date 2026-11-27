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
	"slices"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// newTestDeployment returns a Deployment whose Pod template carries the given labels.
func newTestDeployment(namespace, name string, templateLabels map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: templateLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: templateLabels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}}},
			},
		},
	}
}

// newTestStatefulSet returns a StatefulSet whose Pod template carries the given labels.
func newTestStatefulSet(namespace, name string, templateLabels map[string]string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Selector:    &metav1.LabelSelector{MatchLabels: templateLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: templateLabels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}}},
			},
		},
	}
}

// testAutoscaling returns autoscaling settings that scale between 2 and maxReplicas replicas on CPU utilization.
func testAutoscaling(maxReplicas int32) *workloadsv1.Autoscaling {
	return &workloadsv1.Autoscaling{
		Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](2), Max: maxReplicas},
		Metrics:  []autoscalingv2.MetricSpec{cpuUtilizationMetric()},
		Profile:  workloadsv1.AutoscalingProfileBalanced,
	}
}

var _ = Describe("WorkloadAutoscaling Controller", func() {
	var (
		namespace     string
		recorder      *events.FakeRecorder
		deployments   *WorkloadAutoscalingReconciler
		statefulSets  *WorkloadAutoscalingReconciler
		webLabels     = map[string]string{"app": "web"}
		managedHPAKey func(kind, name string) client.ObjectKey
	)

	BeforeEach(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "workload-autoscaling-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		namespace = ns.Name

		recorder = events.NewFakeRecorder(100)
		deployments = &WorkloadAutoscalingReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder, Workload: DeploymentWorkload}
		statefulSets = &WorkloadAutoscalingReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder, Workload: StatefulSetWorkload}
		managedHPAKey = func(kind, name string) client.ObjectKey {
			return client.ObjectKey{Name: utils.ManagedAutoscalerName(kind, name), Namespace: namespace}
		}
	})

	create := func(obj client.Object) {
		GinkgoHelper()
		Expect(k8sClient.Create(ctx, obj)).To(Succeed())
	}

	// createWorkloadClass creates a WorkloadClass that selects app=web Pods.
	createWorkloadClass := func(name string, rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
		GinkgoHelper()
		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector:        &metav1.LabelSelector{MatchLabels: webLabels},
				ResourceManagement: rm,
			},
		}
		create(wc)
		return wc
	}

	// setResourceManagementValidated writes the ResourceManagementValidated condition for the current generation of
	// the WorkloadClass, standing in for the WorkloadClass controller.
	setResourceManagementValidated := func(wc *workloadsv1.WorkloadClass, status metav1.ConditionStatus, reason string) {
		GinkgoHelper()
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wc), wc)).To(Succeed())
		meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type:               workloadsv1.ConditionTypeResourceManagementValidated,
			Status:             status,
			Reason:             reason,
			Message:            "set by the test",
			ObservedGeneration: wc.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
	}

	reconcileWorkload := func(r *WorkloadAutoscalingReconciler, obj client.Object) error {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
		return err
	}

	getHPA := func(key client.ObjectKey) *autoscalingv2.HorizontalPodAutoscaler {
		GinkgoHelper()
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		Expect(k8sClient.Get(ctx, key, hpa)).To(Succeed())
		return hpa
	}

	expectNoHPA := func(key client.ObjectKey) {
		GinkgoHelper()
		err := k8sClient.Get(ctx, key, &autoscalingv2.HorizontalPodAutoscaler{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected HorizontalPodAutoscaler %s to be absent, got %v", key, err)
	}

	It("creates the HorizontalPodAutoscaler of a valid WorkloadClass and leaves it unchanged on the next reconcile", func() {
		wc := createWorkloadClass("web", &workloadsv1.ResourceManagement{Autoscaling: testAutoscaling(10)})
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonValidationPassed)
		deployment := newTestDeployment(namespace, "web", webLabels)
		create(deployment)

		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())

		hpa := getHPA(managedHPAKey("Deployment", "web"))
		Expect(hpa.Name).To(Equal("wc-deployment-web"))
		Expect(hpa.Labels).To(HaveKeyWithValue(workloadsv1.ManagedByLabel, workloadsv1.ManagedByLabelValue))
		Expect(hpa.Labels).To(HaveKeyWithValue(workloadsv1.WorkloadClassLabel, "web"))
		Expect(metav1.IsControlledBy(hpa, deployment)).To(BeTrue())
		want := utils.BuildHPA(wc, utils.WorkloadRef{Kind: "Deployment", Name: "web", Namespace: namespace})
		Expect(equality.Semantic.DeepEqual(hpa.Spec, want.Spec)).To(BeTrue(), "spec = %+v, want %+v", hpa.Spec, want.Spec)
		Expect(recorder.Events).To(Receive(And(
			ContainSubstring(EventReasonAutoscalerApplied),
			ContainSubstring("Created HorizontalPodAutoscaler wc-deployment-web from WorkloadClass web"),
		)))

		By("reconciling again without changes")
		resourceVersion := hpa.ResourceVersion
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		Expect(getHPA(managedHPAKey("Deployment", "web")).ResourceVersion).To(Equal(resourceVersion))
		Expect(recorder.Events).NotTo(Receive(), "an unchanged HorizontalPodAutoscaler records no event")
	})

	It("creates the HorizontalPodAutoscaler of a StatefulSet", func() {
		wc := createWorkloadClass("web", &workloadsv1.ResourceManagement{Autoscaling: testAutoscaling(5)})
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonNoGuardrails)
		statefulSet := newTestStatefulSet(namespace, "db", webLabels)
		create(statefulSet)

		Expect(reconcileWorkload(statefulSets, statefulSet)).To(Succeed())

		hpa := getHPA(managedHPAKey("StatefulSet", "db"))
		Expect(hpa.Spec.ScaleTargetRef).To(Equal(autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db"}))
		Expect(metav1.IsControlledBy(hpa, statefulSet)).To(BeTrue())

		By("ignoring StatefulSets in the Deployment reconciler")
		Expect(reconcileWorkload(deployments, statefulSet)).To(Succeed())
		expectNoHPA(managedHPAKey("Deployment", "db"))
	})

	It("reverts changes to the managed HorizontalPodAutoscaler and recreates it", func() {
		wc := createWorkloadClass("web", &workloadsv1.ResourceManagement{Autoscaling: testAutoscaling(10)})
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonValidationPassed)
		deployment := newTestDeployment(namespace, "web", webLabels)
		create(deployment)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())

		hpa := getHPA(managedHPAKey("Deployment", "web"))
		hpa.Spec.MaxReplicas = 99
		Expect(k8sClient.Update(ctx, hpa)).To(Succeed())
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		Expect(getHPA(managedHPAKey("Deployment", "web")).Spec.MaxReplicas).To(Equal(int32(10)))
		Expect(recorder.Events).To(Receive(ContainSubstring("Created HorizontalPodAutoscaler")))
		Expect(recorder.Events).To(Receive(ContainSubstring("Updated HorizontalPodAutoscaler")))

		Expect(k8sClient.Delete(ctx, hpa)).To(Succeed())
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		Expect(getHPA(managedHPAKey("Deployment", "web")).Spec.MaxReplicas).To(Equal(int32(10)))
	})

	It("deletes the managed HorizontalPodAutoscaler when the WorkloadClass is invalid or drops autoscaling", func() {
		wc := createWorkloadClass("web", &workloadsv1.ResourceManagement{Autoscaling: testAutoscaling(10)})
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonValidationPassed)
		deployment := newTestDeployment(namespace, "web", webLabels)
		create(deployment)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		getHPA(managedHPAKey("Deployment", "web"))

		By("failing validation")
		setResourceManagementValidated(wc, metav1.ConditionFalse, workloadsv1.ReasonValidationFailed)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		expectNoHPA(managedHPAKey("Deployment", "web"))

		By("passing validation again, only because of a selector overlap")
		setResourceManagementValidated(wc, metav1.ConditionFalse, workloadsv1.ReasonSelectorOverlap)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		getHPA(managedHPAKey("Deployment", "web"))

		By("replacing autoscaling with rightsizing")
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wc), wc)).To(Succeed())
		wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{}}
		Expect(k8sClient.Update(ctx, wc)).To(Succeed())
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonValidationPassed)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		expectNoHPA(managedHPAKey("Deployment", "web"))
	})

	It("leaves the managed HorizontalPodAutoscaler untouched while the condition is stale", func() {
		wc := createWorkloadClass("web", &workloadsv1.ResourceManagement{Autoscaling: testAutoscaling(10)})
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonValidationPassed)
		deployment := newTestDeployment(namespace, "web", webLabels)
		create(deployment)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())

		By("changing the WorkloadClass before the WorkloadClass controller validates it")
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wc), wc)).To(Succeed())
		wc.Spec.ResourceManagement.Autoscaling.Replicas.Max = 15
		Expect(k8sClient.Update(ctx, wc)).To(Succeed())
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		Expect(getHPA(managedHPAKey("Deployment", "web")).Spec.MaxReplicas).To(Equal(int32(10)))

		By("validating the new generation")
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonValidationPassed)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		Expect(getHPA(managedHPAKey("Deployment", "web")).Spec.MaxReplicas).To(Equal(int32(15)))
	})

	It("deletes the managed HorizontalPodAutoscaler when no WorkloadClass governs the workload anymore", func() {
		wc := createWorkloadClass("web", &workloadsv1.ResourceManagement{Autoscaling: testAutoscaling(10)})
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonValidationPassed)
		deployment := newTestDeployment(namespace, "web", webLabels)
		create(deployment)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		getHPA(managedHPAKey("Deployment", "web"))

		Expect(k8sClient.Delete(ctx, wc)).To(Succeed())
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		expectNoHPA(managedHPAKey("Deployment", "web"))
	})

	It("steps back for a user-created HorizontalPodAutoscaler", func() {
		wc := createWorkloadClass("web", &workloadsv1.ResourceManagement{Autoscaling: testAutoscaling(10)})
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonValidationPassed)
		deployment := newTestDeployment(namespace, "web", webLabels)
		create(deployment)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		getHPA(managedHPAKey("Deployment", "web"))

		By("creating a user HorizontalPodAutoscaler that targets the Deployment")
		userHPA := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "my-hpa", Namespace: namespace},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"},
				MaxReplicas:    3,
			},
		}
		create(userHPA)
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		expectNoHPA(managedHPAKey("Deployment", "web"))
		Expect(getHPA(client.ObjectKeyFromObject(userHPA)).Spec.MaxReplicas).To(Equal(int32(3)))
		Eventually(recorder.Events).Should(Receive(ContainSubstring(EventReasonUserAutoscalerPresent)))

		By("deleting the user HorizontalPodAutoscaler")
		Expect(k8sClient.Delete(ctx, userHPA)).To(Succeed())
		Expect(reconcileWorkload(deployments, deployment)).To(Succeed())
		getHPA(managedHPAKey("Deployment", "web"))
	})

	It("refuses to modify an unmanaged object that has the managed name", func() {
		wc := createWorkloadClass("web", &workloadsv1.ResourceManagement{Autoscaling: testAutoscaling(10)})
		setResourceManagementValidated(wc, metav1.ConditionTrue, workloadsv1.ReasonValidationPassed)
		deployment := newTestDeployment(namespace, "web", webLabels)
		create(deployment)
		squatter := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "wc-deployment-web", Namespace: namespace},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "other"},
				MaxReplicas:    3,
			},
		}
		create(squatter)

		err := reconcileWorkload(deployments, deployment)
		Expect(err).To(MatchError(ContainSubstring("already exists and is not managed by a WorkloadClass")))
		hpa := getHPA(client.ObjectKeyFromObject(squatter))
		Expect(hpa.Spec.ScaleTargetRef.Name).To(Equal("other"))
		Expect(hpa.OwnerReferences).To(BeEmpty())
	})
})

func TestAutoscalingGateFor(t *testing.T) {
	newWC := func(generation int64, rm *workloadsv1.ResourceManagement, cond *metav1.Condition) *workloadsv1.WorkloadClass {
		wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "web", Generation: generation}}
		wc.Spec.ResourceManagement = rm
		if cond != nil {
			cond.Type = workloadsv1.ConditionTypeResourceManagementValidated
			wc.Status.Conditions = []metav1.Condition{*cond}
		}
		return wc
	}
	rm := &workloadsv1.ResourceManagement{Autoscaling: testAutoscaling(5)}

	testCases := []struct {
		name string
		wc   *workloadsv1.WorkloadClass
		want autoscalingGate
	}{
		{name: "no_workload_class", want: autoscalingRemove},
		{name: "no_resource_management", wc: newWC(1, nil, nil), want: autoscalingRemove},
		{name: "no_condition_yet", wc: newWC(1, rm, nil), want: autoscalingPending},
		{
			name: "stale_condition",
			wc:   newWC(3, rm, &metav1.Condition{Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonValidationFailed, ObservedGeneration: 2}),
			want: autoscalingPending,
		},
		{
			name: "failing_condition",
			wc:   newWC(3, rm, &metav1.Condition{Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonValidationFailed, ObservedGeneration: 3}),
			want: autoscalingRemove,
		},
		{
			name: "unknown_condition",
			wc:   newWC(3, rm, &metav1.Condition{Status: metav1.ConditionUnknown, Reason: "Unknown", ObservedGeneration: 3}),
			want: autoscalingRemove,
		},
		{
			name: "selector_overlap_only",
			wc:   newWC(3, rm, &metav1.Condition{Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonSelectorOverlap, ObservedGeneration: 3}),
			want: autoscalingApply,
		},
		{
			name: "passing_condition",
			wc:   newWC(3, rm, &metav1.Condition{Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonValidationPassed, ObservedGeneration: 3}),
			want: autoscalingApply,
		},
		{
			name: "no_generation_is_never_stale",
			wc:   newWC(0, rm, &metav1.Condition{Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonNoGuardrails, ObservedGeneration: 7}),
			want: autoscalingApply,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := autoscalingGateFor(tc.wc)
			if got != tc.want {
				t.Errorf("autoscalingGateFor() = %v (%s), want %v", got, reason, tc.want)
			}
			if got != autoscalingApply && reason == "" {
				t.Errorf("autoscalingGateFor() returned no reason for %v", got)
			}
		})
	}
}

func TestWorkloadTemplateLabelsPredicate(t *testing.T) {
	p := workloadTemplateLabelsPredicate()
	web := newTestDeployment("ns", "web", map[string]string{"app": "web"})
	web.ResourceVersion = "1"

	if !p.Create(event.CreateEvent{Object: web}) {
		t.Errorf("Create() = false, want true")
	}
	if p.Delete(event.DeleteEvent{Object: web}) {
		t.Errorf("Delete() = true, want false")
	}

	scaled := web.DeepCopy()
	scaled.ResourceVersion = "2"
	scaled.Spec.Replicas = ptr.To[int32](5)
	if p.Update(event.UpdateEvent{ObjectOld: web, ObjectNew: scaled}) {
		t.Errorf("Update() = true for a replica change, want false")
	}

	relabeled := web.DeepCopy()
	relabeled.ResourceVersion = "3"
	relabeled.Spec.Template.Labels = map[string]string{"app": "web", "tier": "frontend"}
	if !p.Update(event.UpdateEvent{ObjectOld: web, ObjectNew: relabeled}) {
		t.Errorf("Update() = false for a template label change, want true")
	}

	if !p.Update(event.UpdateEvent{ObjectOld: web, ObjectNew: web.DeepCopy()}) {
		t.Errorf("Update() = false for a resync, want true")
	}
}

func TestManagedHPAPredicate(t *testing.T) {
	p := managedHPAPredicate()
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "wc-deployment-web", Labels: utils.ManagedAutoscalerLabels("web")},
		Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 5},
	}

	statusOnly := hpa.DeepCopy()
	statusOnly.Status.CurrentReplicas = 3
	if p.Update(event.UpdateEvent{ObjectOld: hpa, ObjectNew: statusOnly}) {
		t.Errorf("Update() = true for a status update, want false")
	}

	specChange := hpa.DeepCopy()
	specChange.Spec.MaxReplicas = 6
	if !p.Update(event.UpdateEvent{ObjectOld: hpa, ObjectNew: specChange}) {
		t.Errorf("Update() = false for a spec change, want true")
	}

	labelChange := hpa.DeepCopy()
	labelChange.Labels[workloadsv1.WorkloadClassLabel] = "other"
	if !p.Update(event.UpdateEvent{ObjectOld: hpa, ObjectNew: labelChange}) {
		t.Errorf("Update() = false for a label change, want true")
	}

	if !p.Delete(event.DeleteEvent{Object: hpa}) {
		t.Errorf("Delete() = false, want true")
	}
}

func TestWorkloadClassAutoscalingPredicate(t *testing.T) {
	p := workloadClassAutoscalingPredicate()
	wc := &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: "web", Generation: 2}}
	withCondition := func(base *workloadsv1.WorkloadClass, status metav1.ConditionStatus, observedGeneration int64, message string) *workloadsv1.WorkloadClass {
		wc := base.DeepCopy()
		meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type: workloadsv1.ConditionTypeResourceManagementValidated, Status: status, Reason: "Reason",
			ObservedGeneration: observedGeneration, Message: message,
		})
		return wc
	}
	valid := withCondition(wc, metav1.ConditionTrue, 2, "a")

	testCases := []struct {
		name     string
		old, new *workloadsv1.WorkloadClass
		want     bool
	}{
		{name: "condition_added", old: wc, new: valid, want: true},
		{name: "condition_removed", old: valid, new: wc, want: true},
		{name: "condition_status_changed", old: valid, new: withCondition(wc, metav1.ConditionFalse, 2, "a"), want: true},
		{name: "condition_generation_changed", old: valid, new: withCondition(wc, metav1.ConditionTrue, 3, "a"), want: true},
		{name: "condition_message_changed", old: valid, new: withCondition(wc, metav1.ConditionTrue, 2, "b")},
		{
			name: "other_status_changed",
			old:  valid,
			new: func() *workloadsv1.WorkloadClass {
				wc := valid.DeepCopy()
				wc.Status.MaintenanceReadiness = workloadsv1.ReadinessReady
				return wc
			}(),
		},
		{
			name: "spec_changed",
			old:  valid,
			new: func() *workloadsv1.WorkloadClass {
				wc := valid.DeepCopy()
				wc.Generation = 3
				return wc
			}(),
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
				t.Errorf("Update() = %t, want %t", got, tc.want)
			}
		})
	}
	if !p.Create(event.CreateEvent{Object: wc}) || !p.Delete(event.DeleteEvent{Object: wc}) {
		t.Errorf("Create() and Delete() must pass")
	}
}

func TestUserAutoscalerPredicate(t *testing.T) {
	p := userAutoscalerPredicate()
	managed := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Labels: utils.ManagedAutoscalerLabels("web")}}
	user := &autoscalingv2.HorizontalPodAutoscaler{}
	if p.Create(event.CreateEvent{Object: managed}) {
		t.Errorf("Create() = true for a managed autoscaler, want false")
	}
	if !p.Create(event.CreateEvent{Object: user}) {
		t.Errorf("Create() = false for a user autoscaler, want true")
	}
}

func TestHPATarget(t *testing.T) {
	r := &WorkloadAutoscalingReconciler{Workload: DeploymentWorkload}
	newHPA := func(apiVersion, kind, name string) *autoscalingv2.HorizontalPodAutoscaler {
		return &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "hpa", Namespace: "shop"},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: apiVersion, Kind: kind, Name: name},
			},
		}
	}
	testCases := []struct {
		name string
		hpa  *autoscalingv2.HorizontalPodAutoscaler
		want []string
	}{
		{name: "deployment", hpa: newHPA("apps/v1", "Deployment", "web"), want: []string{"shop/web"}},
		{name: "statefulset", hpa: newHPA("apps/v1", "StatefulSet", "web")},
		{name: "other_group", hpa: newHPA("example.com/v1", "Deployment", "web")},
		{name: "invalid_api_version", hpa: newHPA("a/b/c", "Deployment", "web")},
		{name: "no_name", hpa: newHPA("apps/v1", "Deployment", "")},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := requestNames(r.hpaTarget(t.Context(), tc.hpa)); !slices.Equal(got, tc.want) {
				t.Errorf("hpaTarget() = %v, want %v", got, tc.want)
			}
		})
	}
	if got := r.hpaTarget(t.Context(), &corev1.Pod{}); got != nil {
		t.Errorf("hpaTarget(Pod) = %v, want nil", got)
	}
}

func TestWorkloadRequests(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{appsv1.AddToScheme, corev1.AddToScheme, workloadsv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("failed to build scheme: %v", err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		newTestDeployment("shop", "web", nil),
		newTestDeployment("shop", "api", nil),
		newTestDeployment("other", "web", nil),
		newTestStatefulSet("shop", "db", nil),
	).Build()

	for _, tc := range []struct {
		kind WorkloadKind
		want []string
	}{
		{kind: DeploymentWorkload, want: []string{"shop/api", "shop/web"}},
		{kind: StatefulSetWorkload, want: []string{"shop/db"}},
	} {
		t.Run(tc.kind.Kind, func(t *testing.T) {
			r := &WorkloadAutoscalingReconciler{Client: c, Workload: tc.kind}
			if got := requestNames(r.workloadsInObjectNamespace(t.Context(), &workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Namespace: "shop"}})); !slices.Equal(got, tc.want) {
				t.Errorf("workloadsInObjectNamespace() = %v, want %v", got, tc.want)
			}
			if got := requestNames(r.workloadsInNamespace(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shop"}})); !slices.Equal(got, tc.want) {
				t.Errorf("workloadsInNamespace() = %v, want %v", got, tc.want)
			}
		})
	}
}

// requestNames returns the sorted namespace/name of the requests, or nil if there are none.
func requestNames(requests []reconcile.Request) []string {
	if len(requests) == 0 {
		return nil
	}
	names := make([]string, 0, len(requests))
	for _, req := range requests {
		names = append(names, req.String())
	}
	slices.Sort(names)
	return names
}

func TestTargetsWorkload(t *testing.T) {
	workload := utils.WorkloadRef{Kind: "Deployment", Name: "web"}
	for _, tc := range []struct {
		apiVersion, kind, name string
		want                   bool
	}{
		{"apps/v1", "Deployment", "web", true},
		{"apps/v1beta2", "Deployment", "web", true},
		{"extensions/v1beta1", "Deployment", "web", false},
		{"apps/v1", "StatefulSet", "web", false},
		{"apps/v1", "Deployment", "api", false},
	} {
		name := strings.Join([]string{tc.apiVersion, tc.kind, tc.name}, "_")
		if got := targetsWorkload(tc.apiVersion, tc.kind, tc.name, workload); got != tc.want {
			t.Errorf("%s: targetsWorkload() = %t, want %t", name, got, tc.want)
		}
	}
}
