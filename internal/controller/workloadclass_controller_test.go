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
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

type GracePeriodSeconds int64

func (s GracePeriodSeconds) ApplyToDelete(opts *client.DeleteOptions) {
	secs := int64(s)
	opts.GracePeriodSeconds = &secs
}

var _ = Describe("WorkloadClass Controller", func() {
	Context("When reconciling a resource", func() {
		ctx := context.Background()
		const (
			workloadClassName = "test-resource"
			guardrailName     = "test-guardrail"
			podName           = "silly-goose-pod"
			defaultNamespace  = "default"
		)

		podLabels := map[string]string{
			"duck-duck": "goose", // Match the WC PodSelector
		}

		typeNamespacedName := types.NamespacedName{
			Name:      workloadClassName,
			Namespace: defaultNamespace,
		}
		typeNamespacedNamePod := types.NamespacedName{
			Name:      podName,
			Namespace: defaultNamespace,
		}
		typeNamespacedNameGuardrail := types.NamespacedName{
			Name:      guardrailName,
			Namespace: "", // Explicitly empty for cluster-scoped
		}
		var (
			controllerReconciler *WorkloadClassReconciler
			fakeRecorder         *events.FakeRecorder
		)

		BeforeEach(func() {
			fakeRecorder = events.NewFakeRecorder(100)
			controllerReconciler = &WorkloadClassReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: fakeRecorder,
			}

			By("Creating the custom resource for the Kind WorkloadClass")
			workloadclass := &workloadsv1.WorkloadClass{}
			err := k8sClient.Get(ctx, typeNamespacedName, workloadclass)
			if err != nil && errors.IsNotFound(err) {
				resource := &workloadsv1.WorkloadClass{
					ObjectMeta: metav1.ObjectMeta{
						Name:      workloadClassName,
						Namespace: defaultNamespace,
					},
					Spec: workloadsv1.WorkloadClassSpec{
						DisruptionPolicy: workloadsv1.DisruptionPolicy{
							MaxNonDisruptionDurationDays: 1,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}

			By("Creating the custom resource for the Kind WorkloadClassGuardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: guardrailName,
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxAllowedWindows:            int32(2),
							AllowedDisruptionDays:        []string{"Sunday", "Monday", "Tuesday"},
							MaxNonDisruptionDurationDays: 3,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())

			By("Creating a Pod")
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: defaultNamespace,
					Labels:    podLabels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "duckling",
							Image: "nginx:latest",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		})

		AfterEach(func() {
			By("Cleanup the specific resource instance WorkloadClass")
			resource := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			By("Cleanup pod")
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, typeNamespacedNamePod, pod)).To(Succeed())
			Expect(k8sClient.Delete(ctx, pod)).To(Succeed())

			By("Cleanup guardrail to be recreated for the next test")
			gr := &workloadsv1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, typeNamespacedNameGuardrail, gr)).To(Succeed())
			Expect(k8sClient.Delete(ctx, gr)).To(Succeed())

			By("Cleanup PDB")
			// envtest does not run the garbage collector, so owned PDBs must be deleted explicitly.
			pdbKey := types.NamespacedName{
				Name:      utils.PDBName(podName),
				Namespace: defaultNamespace,
			}
			pdb := &policyv1.PodDisruptionBudget{}
			if err := k8sClient.Get(ctx, pdbKey, pdb); err == nil {
				_ = k8sClient.Delete(ctx, pdb)
			}
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		})
		// Validate against guardrails
		It("should fail validation if number of DisruptionWindows less than or equal to MaxAllowedWindows", func() {
			By("updating WorkloadClass with 3 windows")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "M", DaysOfWeek: []string{"Monday"}, StartTime: "10:00", EndTime: "12:00"},
				{Name: "MT", DaysOfWeek: []string{"Monday", "Tuesday"}, StartTime: "10:00", EndTime: "12:00"},
				{Name: "T", DaysOfWeek: []string{"Tuesday"}, StartTime: "10:00", EndTime: "12:00"},
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				for _, cond := range updatedWC.Status.Conditions {
					if cond.Type == workloadsv1.ConditionTypeValidated {
						return cond.Status == metav1.ConditionFalse &&
							cond.Reason == workloadsv1.ReasonValidationFailed &&
							strings.Contains(cond.Message, "exceeds guardrail limit 2")
					}
				}
				return false
			}, "10s", "1s").Should(BeTrue())
		})

		It("should fail if DaysOfWeek is not a subset of AllowedDisruptionDays", func() {
			By("updating WorkloadClass with DisruptionWindow outside of AllowedDisruptionDays")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "M", DaysOfWeek: []string{"Friday"}, StartTime: "10:00", EndTime: "12:00"},
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				for _, cond := range updatedWC.Status.Conditions {
					if cond.Type == workloadsv1.ConditionTypeValidated {
						return cond.Status == metav1.ConditionFalse &&
							cond.Reason == workloadsv1.ReasonValidationFailed &&
							strings.Contains(cond.Message, "contains day(s) of week that are not allowed by guardrail.")
					}
				}
				return false
			}, "10s", "1s").Should(BeTrue())
		})

		It("should fail if timeZone is invalid", func() {
			By("updating WorkloadClass with DisruptionWindow outside of AllowedDisruptionDays")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "MT", DaysOfWeek: []string{"Monday", "Tuesday"}, TimeZone: "invalid/timezone", StartTime: "10:00", EndTime: "12:00"},
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				for _, cond := range updatedWC.Status.Conditions {
					if cond.Type == workloadsv1.ConditionTypeValidated {
						return cond.Status == metav1.ConditionFalse &&
							cond.Reason == workloadsv1.ReasonValidationFailed &&
							strings.Contains(cond.Message, "has invalid time zone")
					}
				}
				return false
			}, "10s", "1s").Should(BeTrue())
		})

		It("should fail if maxNonDisruptionDays exceeds limit", func() {
			By("updating WorkloadClass with MaxNonDisruptionDurationDays exceeding limit")
			guardrail := &workloadsv1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, typeNamespacedNameGuardrail, guardrail)).To(Succeed())

			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())

			wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = guardrail.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays + 1
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				for _, cond := range updatedWC.Status.Conditions {
					if cond.Type == workloadsv1.ConditionTypeValidated {
						return cond.Status == metav1.ConditionFalse &&
							cond.Reason == workloadsv1.ReasonValidationFailed &&
							strings.Contains(cond.Message, "maxNonDisruptionDurationDays 4 exceeds guardrail limit")
					}
				}
				return false
			}, "10s", "1s").Should(BeTrue())
		})
		// Calculate readiness
		It("should be ready if EmergencyOverride is set", func() {
			By("updating WorkloadClass with EmergencyOverride")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "Emergency", DaysOfWeek: []string{"Friday"}, TimeZone: "America/Los_Angeles", StartTime: "10:00", EndTime: "12:00"},
			}
			wc.Spec.DisruptionPolicy.EmergencyOverride = true
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			controllerReconciler := &WorkloadClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status for violations")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				for _, cond := range updatedWC.Status.Conditions {
					if cond.Type == workloadsv1.ConditionTypeValidated {
						return cond.Status == metav1.ConditionFalse &&
							cond.Reason == workloadsv1.ReasonValidationFailed &&
							strings.Contains(cond.Message, "contains day(s) of week that are not allowed by guardrail.")
					}
				}
				return false
			}, "10s", "1s").Should(BeTrue())

			By("Checking Status for readiness")
			updatedWC = &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				return updatedWC.Status.MaintenanceReadiness == workloadsv1.ReadinessReady
			}, "10s", "1s").Should(BeTrue())
		})

		It("should be overdue if time since last disruption exceeds max", func() {
			By("updating WorkloadClass with old last disruption")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 2
			wc.Status.LastDisruptionTime = &metav1.Time{
				Time: time.Now().AddDate(0, 0, -4),
			}
			Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status for readiness")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				return updatedWC.Status.MaintenanceReadiness == workloadsv1.ReadinessOverdue
			}, "10s", "1s").Should(BeTrue())
		})

		It("should not be ready if not within allowed disruption window", func() {
			notToday := time.Now().AddDate(0, 0, 2).Weekday().String()

			By("creating updating the allowed disruption days in the guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, typeNamespacedNameGuardrail, guardrail)).To(Succeed())

			guardrail.Spec.Constraints.Disruption.AllowedDisruptionDays = []string{notToday}
			Expect(k8sClient.Update(ctx, guardrail)).To(Succeed())

			By("updating WorkloadClass with disruption window that is not today")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())

			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "NotToday", DaysOfWeek: []string{notToday}, TimeZone: "America/Los_Angeles", StartTime: "10:00", EndTime: "12:00"},
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status for readiness")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				return updatedWC.Status.MaintenanceReadiness == workloadsv1.ReadinessNotReady
			}, "10s", "1s").Should(BeTrue())
		})

		It("should not be ready if pods haven't run long enough", func() {
			By("updating WorkloadClass with min initial run duration days and pod selector")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			today := time.Now().Weekday().String()
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "Today", DaysOfWeek: []string{today}, TimeZone: "America/Los_Angeles", StartTime: "00:00", EndTime: "23:59"},
			}
			wc.Spec.DisruptionPolicy.MinInitialRunDurationDays = 4
			wc.Spec.PodSelector = &metav1.LabelSelector{
				MatchLabels: podLabels,
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status for readiness")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				return updatedWC.Status.MaintenanceReadiness == workloadsv1.ReadinessNotReady
			}, "10s", "1s").Should(BeTrue())
		})

		It("should not be ready if grace period hasn't been passed for all pods", func() {
			By("updating WorkloadClass with min initial run duration days and pod selector")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			today := time.Now().Weekday().String()
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "Today", DaysOfWeek: []string{today}, TimeZone: "America/Los_Angeles", StartTime: "00:00", EndTime: "23:59"},
			}
			wc.Spec.DisruptionPolicy.MinInitialRunDurationDays = 4
			wc.Spec.DisruptionPolicy.GraceTerminationDuration = 3600
			wc.Spec.PodSelector = &metav1.LabelSelector{
				MatchLabels: podLabels,
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status for readiness")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				return updatedWC.Status.MaintenanceReadiness == workloadsv1.ReadinessNotReady
			}, "10s", "1s").Should(BeTrue())
		})

		It("should be ready with next window", func() {
			By("updating WorkloadClass with disruption window today")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())

			today := time.Now().Weekday().String()
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "Today", DaysOfWeek: []string{today}, TimeZone: "America/Los_Angeles", StartTime: "00:00", EndTime: "23:59"},
			}
			wc.Spec.PodSelector = &metav1.LabelSelector{
				MatchLabels: podLabels,
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking Status for readiness")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				return updatedWC.Status.MaintenanceReadiness == workloadsv1.ReadinessReady
			}, "30s", "1s").Should(BeTrue())
		})

		It("should emit warning when another WorkloadClass has the same selector", func() {
			By("Creating another WorkloadClass with the same selector")
			wcSameName := "wc-same-selector"
			wcSame := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      wcSameName,
					Namespace: defaultNamespace,
				},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"duck-duck": "goose"},
					},
					DisruptionPolicy: workloadsv1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 1,
					},
				},
			}
			Expect(k8sClient.Create(ctx, wcSame)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, wcSame)
			}()

			By("Updating the first WorkloadClass to match the selector")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.PodSelector = &metav1.LabelSelector{
				MatchLabels: map[string]string{"duck-duck": "goose"},
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling the first WorkloadClass")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying a warning event was emitted")
			Eventually(fakeRecorder.Events).Should(Receive(ContainSubstring("ValidationFailed")))
		})

		// PDB Reconciliation Tests
		podPDBKey := types.NamespacedName{Name: utils.PDBName(podName), Namespace: defaultNamespace}

		selectPod := func() {
			GinkgoHelper()
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: podLabels}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())
		}

		reconcileWC := func(key types.NamespacedName) {
			GinkgoHelper()
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
		}

		It("should create a Per-Pod PDB for each governed Pod when successfully reconciled", func() {
			selectPod()

			By("Reconciling")
			reconcileWC(typeNamespacedName)

			By("Verifying the Pod's PDB was created and selects only that Pod")
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, typeNamespacedNamePod, pod)).To(Succeed())
			Expect(pod.Labels).To(HaveKeyWithValue(utils.PodUIDLabel, string(pod.UID)))

			pdb := &policyv1.PodDisruptionBudget{}
			Eventually(func() error {
				return k8sClient.Get(ctx, podPDBKey, pdb)
			}, "10s", "1s").Should(Succeed(), "Failed to find PDB %s", podPDBKey.Name)

			Expect(pdb.Spec.Selector.MatchLabels).To(Equal(map[string]string{utils.PodUIDLabel: string(pod.UID)}))
			Expect(pdb.Spec.UnhealthyPodEvictionPolicy).NotTo(BeNil())
			Expect(*pdb.Spec.UnhealthyPodEvictionPolicy).To(Equal(policyv1.IfHealthyBudget))
			Expect(metav1.GetControllerOf(pdb).Name).To(Equal(workloadClassName))
		})

		It("should open the PDBs instead of deleting them if validation fails", func() {
			selectPod()

			By("First reconciling to ensure the PDB is created initially")
			reconcileWC(typeNamespacedName)
			Eventually(func() error {
				return k8sClient.Get(ctx, podPDBKey, &policyv1.PodDisruptionBudget{})
			}, "10s", "1s").Should(Succeed())

			By("Making the WorkloadClass invalid (exceeding guardrails)")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 100 // Break validation
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling again")
			reconcileWC(typeNamespacedName)

			By("Verifying the PDB was opened because of validation failure")
			Eventually(func() (string, error) {
				pdb := &policyv1.PodDisruptionBudget{}
				if err := k8sClient.Get(ctx, podPDBKey, pdb); err != nil {
					return "", err
				}
				return pdb.Spec.MaxUnavailable.String(), nil
			}, "10s", "1s").Should(Equal("100%"))
		})

		It("should hand the Pod's PDB over to the namespace default WorkloadClass", func() {
			selectPod()

			By("First reconciling to ensure the PDB is created initially")
			reconcileWC(typeNamespacedName)
			Eventually(func() error {
				return k8sClient.Get(ctx, podPDBKey, &policyv1.PodDisruptionBudget{})
			}, "10s", "1s").Should(Succeed())

			By("Creating another WorkloadClass and setting it as the namespace default")
			defaultWCKey := types.NamespacedName{Name: "some-other-class", Namespace: defaultNamespace}
			defaultWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: defaultWCKey.Name, Namespace: defaultNamespace},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 1},
				},
			}
			Expect(k8sClient.Create(ctx, defaultWC)).To(Succeed())

			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultNamespace}, ns)).To(Succeed())
			if ns.Labels == nil {
				ns.Labels = make(map[string]string)
			}
			ns.Labels[workloadsv1.DefaultClassLabel] = defaultWCKey.Name
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, defaultWC)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: defaultNamespace}, ns)
				delete(ns.Labels, workloadsv1.DefaultClassLabel)
				_ = k8sClient.Update(ctx, ns)
			}()

			By("Reconciling the namespace default WorkloadClass, which adopts the PDB")
			reconcileWC(defaultWCKey)
			Eventually(func() (string, error) {
				pdb := &policyv1.PodDisruptionBudget{}
				if err := k8sClient.Get(ctx, podPDBKey, pdb); err != nil {
					return "", err
				}
				return metav1.GetControllerOf(pdb).Name, nil
			}, "10s", "1s").Should(Equal(defaultWCKey.Name))

			By("Reconciling the original WorkloadClass, which no longer touches the PDB")
			reconcileWC(typeNamespacedName)
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, podPDBKey, pdb)).To(Succeed())
			Expect(metav1.GetControllerOf(pdb).Name).To(Equal(defaultWCKey.Name))
		})

		It("should set PlacementPluginAttached=False and Validated=False when InfrastructureProfileRef is set and plugin has not attached", func() {
			By("Updating WorkloadClass with an InfrastructureProfileRef")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{
				Group: "workloads.gke.io",
				Kind:  "GKESpotPlacementPolicy",
				Name:  "gke-t2d-spot-profile",
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying PlacementPluginAttached is False (PluginPending) and Validated is False")
			updatedWC := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updatedWC)).To(Succeed())

			pluginCond := meta.FindStatusCondition(updatedWC.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached)
			Expect(pluginCond).NotTo(BeNil())
			Expect(pluginCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(pluginCond.Reason).To(Equal(workloadsv1.ReasonPluginPending))
			Expect(pluginCond.Message).To(ContainSubstring("Waiting for plugin controller to reconcile GKESpotPlacementPolicy"))
			Expect(pluginCond.ObservedGeneration).To(Equal(updatedWC.Generation))

			validatedCond := meta.FindStatusCondition(updatedWC.Status.Conditions, workloadsv1.ConditionTypeValidated)
			Expect(validatedCond).NotTo(BeNil())
			Expect(validatedCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(validatedCond.Reason).To(Equal(workloadsv1.ReasonValidationFailed))
			Expect(validatedCond.Message).To(ContainSubstring("WorkloadClass references an infrastructure profile, but the plugin has not attached"))

			By("Verifying no PDB is created while plugin has not attached")
			err = k8sClient.Get(ctx, podPDBKey, &policyv1.PodDisruptionBudget{})
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})

		It("should preserve PlacementPluginAttached=True, mark Validated=True, and create PDB when plugin has attached for current generation", func() {
			By("Updating WorkloadClass with an InfrastructureProfileRef and a PodSelector matching the Pod")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{
				Group: "workloads.gke.io",
				Kind:  "GKESpotPlacementPolicy",
				Name:  "gke-t2d-spot-profile",
			}
			wc.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: podLabels}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Simulating the plugin controller attaching for the current generation")
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
				Type:               workloadsv1.ConditionTypePlacementPluginAttached,
				Status:             metav1.ConditionTrue,
				Reason:             workloadsv1.ReasonProfileResolved,
				Message:            "GKESpotPlacementPolicy gke-t2d-spot-profile attached",
				ObservedGeneration: wc.Generation,
				LastTransitionTime: metav1.Now(),
			})
			Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying PlacementPluginAttached remains True and Validated is True")
			updatedWC := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updatedWC)).To(Succeed())

			pluginCond := meta.FindStatusCondition(updatedWC.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached)
			Expect(pluginCond).NotTo(BeNil())
			Expect(pluginCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(pluginCond.Reason).To(Equal(workloadsv1.ReasonProfileResolved))

			validatedCond := meta.FindStatusCondition(updatedWC.Status.Conditions, workloadsv1.ConditionTypeValidated)
			Expect(validatedCond).NotTo(BeNil())
			Expect(validatedCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(validatedCond.Reason).To(Equal(workloadsv1.ReasonValidationPassed))

			By("Verifying the PDB is created")
			Eventually(func() error {
				return k8sClient.Get(ctx, podPDBKey, &policyv1.PodDisruptionBudget{})
			}, "10s", "1s").Should(Succeed())
		})

		It("should transition Validated through PluginPending -> PluginResolutionFailed -> ProfileResolved", func() {
			By("Step 1: Updating WorkloadClass with an InfrastructureProfileRef")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.InfrastructureProfileRef = &workloadsv1.InfrastructureProfileReference{
				Group: "workloads.gke.io",
				Kind:  "GKESpotPlacementPolicy",
				Name:  "gke-t2d-spot-profile",
			}
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Step 2: Reconciling sets initial PlacementPluginAttached=False (PluginPending) and Validated=False")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			validatedCond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeValidated)
			Expect(validatedCond).NotTo(BeNil())
			Expect(validatedCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(validatedCond.Message).To(Equal("WorkloadClass references an infrastructure profile, but the plugin has not attached: Waiting for plugin controller to reconcile GKESpotPlacementPolicy"))

			By("Step 3: Plugin fails to attach and sets PlacementPluginAttached=False (PluginResolutionFailed)")
			meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
				Type:               workloadsv1.ConditionTypePlacementPluginAttached,
				Status:             metav1.ConditionFalse,
				Reason:             workloadsv1.ReasonPluginResolutionFailed,
				Message:            "GKESpotPlacementPolicy 'gke-t2d-spot-profile' not found",
				ObservedGeneration: wc.Generation,
				LastTransitionTime: metav1.Now(),
			})
			Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())

			By("Step 4: Reconciling updates Validated=False with the plugin resolution failure message")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			validatedCond = meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeValidated)
			Expect(validatedCond).NotTo(BeNil())
			Expect(validatedCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(validatedCond.Message).To(Equal("WorkloadClass references an infrastructure profile, but the plugin has not attached: GKESpotPlacementPolicy 'gke-t2d-spot-profile' not found"))

			By("Step 5: Plugin succeeds attaching and sets PlacementPluginAttached=True (ProfileResolved)")
			meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
				Type:               workloadsv1.ConditionTypePlacementPluginAttached,
				Status:             metav1.ConditionTrue,
				Reason:             workloadsv1.ReasonProfileResolved,
				Message:            "GKESpotPlacementPolicy 'gke-t2d-spot-profile' attached",
				ObservedGeneration: wc.Generation,
				LastTransitionTime: metav1.Now(),
			})
			Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())

			By("Step 6: Reconciling clears the placement violation and marks Validated=True")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			validatedCond = meta.FindStatusCondition(wc.Status.Conditions, workloadsv1.ConditionTypeValidated)
			Expect(validatedCond).NotTo(BeNil())
			Expect(validatedCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(validatedCond.Reason).To(Equal(workloadsv1.ReasonValidationPassed))
			Expect(validatedCond.Message).To(Equal("WorkloadClass adheres to all Guardrail constraints"))
		})
	})
})

func TestOverdue(t *testing.T) {
	wc := &workloadsv1.WorkloadClass{
		Spec: workloadsv1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1.DisruptionPolicy{},
		},
		Status: workloadsv1.WorkloadClassStatus{},
	}
	testCases := []struct {
		name                     string
		desc                     string
		wc                       *workloadsv1.WorkloadClass
		setLastDisruption        bool
		setMNDDD                 bool
		maxNonDisruptionDuration int
		lastDisruption           time.Time
		want                     bool
	}{
		{
			name: "wc_nil",
			desc: "WorkloadClass is nil, want true",
			want: true,
		},
		{
			name:              "fields_not_set",
			desc:              "MaxNonDisruptionDurationDays not set and LastDisruptionTime nil, want true",
			setLastDisruption: false,
			setMNDDD:          false,
			want:              true,
		},
		{
			name:              "max_non_disruption_days_not_set",
			desc:              "MaxNonDisruptionDurationDays not set but LastDisrupt ionTime is set, want true",
			setLastDisruption: true,
			setMNDDD:          false,
			want:              true,
		},
		{
			name:              "last_disruption_time_not_set",
			desc:              "MaxNonDisruptionDurationDays is set but LastDisruptionTime is nil, want ",
			setLastDisruption: true,
			setMNDDD:          false,
			want:              true,
		},
		{
			name:              "fields_not_set",
			desc:              "MaxNonDisruptionDurationDays not set and LastDisruptionTime nil, want true",
			setLastDisruption: false,
			setMNDDD:          false,
			want:              true,
		},
		{
			name:                     "diff_greater_than_max_duration",
			desc:                     "Overdue",
			setLastDisruption:        true,
			setMNDDD:                 true,
			maxNonDisruptionDuration: 10,
			lastDisruption:           time.Now().AddDate(0, -1, 0),
			want:                     true,
		},
		{
			name:                     "diff_less_than_max_duration",
			desc:                     "Not overdue",
			setLastDisruption:        true,
			setMNDDD:                 true,
			maxNonDisruptionDuration: 10,
			lastDisruption:           time.Now().AddDate(0, 0, -5),
			want:                     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			if tc.setLastDisruption {
				wc.Status.LastDisruptionTime = &metav1.Time{Time: tc.lastDisruption}
			}
			if tc.setMNDDD {
				wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = int32(tc.maxNonDisruptionDuration)
			}
			defer func() {
				wc.Status.LastDisruptionTime = nil
				wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 0
			}()

			if got := overdue(wc, now); got != tc.want {
				t.Errorf("overdue() returned an unexpected result, got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGracePeriodPassed(t *testing.T) {
	now := time.Now()
	wc := &workloadsv1.WorkloadClass{
		Spec: workloadsv1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1.DisruptionPolicy{},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{},
	}

	testCases := []struct {
		name         string
		desc         string
		grace        int32
		delTimestamp *metav1.Time
		wantPassed   bool
		wantDuration time.Duration
	}{
		{
			name:         "del_timestamp_not_set",
			desc:         "Pod's DeletionTimestamp is nil, expect true and duration = 0",
			grace:        5,
			wantPassed:   true,
			wantDuration: time.Duration(0) * time.Second,
		},
		{
			name:         "grace_period_not_passed",
			desc:         "Grace period has not passed, expect false and positive diff",
			grace:        30,
			delTimestamp: &metav1.Time{Time: now},
			wantPassed:   false,
			wantDuration: time.Duration(30) * time.Second,
		},
		{
			name:         "grace_period_passed",
			desc:         "Grace period has passed, expect true and negative diff",
			grace:        86400, // 1 day, for simplicity of testing
			delTimestamp: &metav1.Time{Time: now.AddDate(0, 0, -2)},
			wantPassed:   true,
			wantDuration: time.Duration(-86400) * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc.Spec.DisruptionPolicy.GraceTerminationDuration = tc.grace
			pod.DeletionTimestamp = tc.delTimestamp
			gotPassed, gotDuration := gracePeriodPassed(wc, pod, now)

			if gotPassed != tc.wantPassed {
				t.Errorf("gracePeriodPassed() returned an unexpected result, got passed: %v, want passed: %v", gotPassed, tc.wantPassed)
			}

			if gotDuration != tc.wantDuration {
				t.Errorf("gracePeriodPassed() returned an unexpected result, got duration: %v, want duration: %v", gotDuration, tc.wantDuration)
			}
		})
	}
}

func TestEvaluatePodGracePeriod(t *testing.T) {
	wc := &workloadsv1.WorkloadClass{
		Spec: workloadsv1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1.DisruptionPolicy{},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			DeletionTimestamp: &metav1.Time{},
		},
	}
	now := time.Now()

	testCases := []struct {
		name                     string
		desc                     string
		initialGracePeriodPassed bool
		initialDuration          time.Duration
		deletionTimestamp        time.Time
		wcGraceDuration          int32
		wantGracePeriodPassed    bool
		wantDuration             time.Duration
	}{
		{
			name:                     "grace_passed_want_true_0",
			desc:                     "Pod grace period passed, expect true, 0",
			initialGracePeriodPassed: true,
			initialDuration:          time.Duration(0),
			deletionTimestamp:        now.AddDate(0, 0, -1),
			wcGraceDuration:          3600,
			wantGracePeriodPassed:    true,
			wantDuration:             time.Duration(0) * time.Second,
		},
		{
			name:                     "grace_not_passed_want_false_N",
			desc:                     "Pod grace period not passed, expect false, N",
			initialGracePeriodPassed: true,
			initialDuration:          time.Duration(0),
			deletionTimestamp:        now.AddDate(0, 0, -1),
			wcGraceDuration:          90000,
			wantGracePeriodPassed:    false,
			wantDuration:             time.Duration(3600) * time.Second,
		},
		{
			name:                     "initial_values_false_N_grace_not_passed",
			desc:                     "Initial gpp is false, Pod grace period passed, expect 0, N",
			initialGracePeriodPassed: false,
			initialDuration:          time.Duration(30) * time.Second,
			deletionTimestamp:        now.AddDate(0, 0, -1),
			wcGraceDuration:          3600,
			wantGracePeriodPassed:    false,
			wantDuration:             time.Duration(30) * time.Second,
		},
		{
			name:                     "grace_not_passed_new_greater_duration",
			desc:                     "Initial gpp is false, initial duration is N, Pod grace period not passed, time until grace period M > N, expect false, M",
			initialGracePeriodPassed: false,
			initialDuration:          time.Duration(30) * time.Second,
			deletionTimestamp:        now.AddDate(0, 0, -1),
			wcGraceDuration:          90000,
			wantGracePeriodPassed:    false,
			wantDuration:             time.Duration(3600) * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod.DeletionTimestamp.Time = tc.deletionTimestamp
			wc.Spec.DisruptionPolicy.GraceTerminationDuration = tc.wcGraceDuration

			gotGracePeriodPassed, gotDuration := evaluatePodGracePeriod(wc, pod, now, tc.initialGracePeriodPassed, tc.initialDuration)

			if gotGracePeriodPassed != tc.wantGracePeriodPassed {
				t.Errorf("evaluatePodGracePeriod() returned an unexpected result, got: %v, want: %v", gotGracePeriodPassed, tc.wantGracePeriodPassed)
			}

			if gotDuration != tc.wantDuration {
				t.Errorf("evaluatePodGracePeriod() returned an unexpected duration, got: %v, want: %v", gotDuration, tc.wantDuration)
			}
		})
	}
}

func TestSameLabelSelectorSemantic(t *testing.T) {
	testCases := []struct {
		name string
		a    *metav1.LabelSelector
		b    *metav1.LabelSelector
		want bool
	}{
		{
			name: "both_nil",
			a:    nil,
			b:    nil,
			want: true,
		},
		{
			name: "one_nil",
			a:    &metav1.LabelSelector{},
			b:    nil,
			want: false,
		},
		{
			name: "same_match_labels",
			a:    &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar", "baz": "qux"}},
			b:    &metav1.LabelSelector{MatchLabels: map[string]string{"baz": "qux", "foo": "bar"}},
			want: true,
		},
		{
			name: "different_match_labels",
			a:    &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
			b:    &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "baz"}},
			want: false,
		},
		{
			name: "same_expressions_different_order",
			a: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "foo", Operator: metav1.LabelSelectorOpIn, Values: []string{"bar", "baz"}},
					{Key: "qux", Operator: metav1.LabelSelectorOpExists},
				},
			},
			b: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "qux", Operator: metav1.LabelSelectorOpExists},
					{Key: "foo", Operator: metav1.LabelSelectorOpIn, Values: []string{"bar", "baz"}},
				},
			},
			want: true,
		},
		{
			name: "same_expressions_different_values_order",
			a: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "foo", Operator: metav1.LabelSelectorOpIn, Values: []string{"bar", "baz"}},
				},
			},
			b: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "foo", Operator: metav1.LabelSelectorOpIn, Values: []string{"baz", "bar"}},
				},
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameLabelSelectorSemantic(tc.a, tc.b); got != tc.want {
				t.Errorf("sameLabelSelectorSemantic() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestValidateSelectors(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(workloadsv1.AddToScheme(scheme))
	ns := "test-namespace"
	wcCurrent := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-current",
			Namespace:         ns,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "nginx"},
			},
		},
	}
	wcOverlapping := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wc-same",
			Namespace: ns,
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "nginx"},
			},
		},
	}
	testCases := []struct {
		name            string
		existing        []client.Object
		wantOverlapping []workloadsv1.WorkloadClass
		wantErrSuffix   string
	}{
		{
			name:          "no_other_workload_classes",
			existing:      []client.Object{wcCurrent},
			wantErrSuffix: "",
		},
		{
			name: "other_different_selector",
			existing: []client.Object{
				wcCurrent,
				&workloadsv1.WorkloadClass{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "wc-different",
						Namespace: ns,
					},
					Spec: workloadsv1.WorkloadClassSpec{
						PodSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "redis"},
						},
					},
				},
			},
			wantErrSuffix: "",
		},
		{
			name:            "other_same_selector",
			existing:        []client.Object{wcCurrent, wcOverlapping},
			wantOverlapping: []workloadsv1.WorkloadClass{*wcOverlapping},
			wantErrSuffix:   "the following WorkloadClasses have the same PodSelector as wc-current: wc-same",
		},
		{
			name: "same_selector_different_namespace",
			existing: []client.Object{
				wcCurrent,
				&workloadsv1.WorkloadClass{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "wc-same-other-ns",
						Namespace: "another-namespace",
					},
					Spec: workloadsv1.WorkloadClassSpec{
						PodSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "nginx"},
						},
					},
				},
			},
			wantErrSuffix: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.existing...).Build()
			r := &WorkloadClassReconciler{
				Client: fakeClient,
				Scheme: scheme,
			}
			overlaps, err := r.validateSelectors(context.Background(), wcCurrent)
			if tc.wantErrSuffix == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.wantErrSuffix) {
					t.Fatalf("expected error containing suffix %q, got: %v", tc.wantErrSuffix, err)
				}
			}
			if (overlaps != nil) != (tc.wantOverlapping != nil) {
				t.Fatalf("validateSelectors returned unexpected workloadClasses, want: %v, got: %v", tc.wantOverlapping, overlaps)
			}

			if overlaps == nil {
				return
			}

			if overlaps[0].Name != tc.wantOverlapping[0].Name {
				t.Fatalf("validateSelectors returned unexpected workloadClasses, want: %v, got: %v", tc.wantOverlapping, overlaps)
			}
		})
	}
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, policyv1.AddToScheme, workloadsv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("failed to build scheme: %v", err)
		}
	}
	return scheme
}

// newTestWC returns a WorkloadClass in the "default" namespace. A nil selector leaves PodSelector unset.
func newTestWC(name string, selector map[string]string, created metav1.Time, readiness workloadsv1.MaintenanceReadiness, valid bool) *workloadsv1.WorkloadClass {
	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			UID:               types.UID("uid-" + name), // Required for owner references
			CreationTimestamp: created,
		},
		Status: workloadsv1.WorkloadClassStatus{MaintenanceReadiness: readiness},
	}
	if selector != nil {
		wc.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: selector}
	}
	reason := workloadsv1.ReasonValidationPassed
	status := metav1.ConditionTrue
	if !valid {
		reason, status = workloadsv1.ReasonValidationFailed, metav1.ConditionFalse
	}
	wc.Status.Conditions = []metav1.Condition{{Type: workloadsv1.ConditionTypeValidated, Status: status, Reason: reason}}
	return wc
}

func newTestPod(name string, podLabels map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: "default",
		UID:       types.UID("uid-" + name),
		Labels:    podLabels,
	}}
}

// newOwnedPDB returns a Per-Pod PDB for the Pod, controlled by the WorkloadClass.
func newOwnedPDB(wc *workloadsv1.WorkloadClass, pod *corev1.Pod, maxUnavailable intstr.IntOrString) *policyv1.PodDisruptionBudget {
	isController := true
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.PDBName(pod.Name),
			Namespace: pod.Namespace,
			Labels:    map[string]string{utils.PodUIDLabel: string(pod.UID)},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: workloadsv1.GroupVersion.String(),
				Kind:       "WorkloadClass",
				Name:       wc.Name,
				UID:        wc.UID,
				Controller: &isController,
			}},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{utils.PodUIDLabel: string(pod.UID)}},
		},
	}
}

func validCondition() metav1.Condition {
	return metav1.Condition{Type: workloadsv1.ConditionTypeValidated, Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonValidationPassed}
}

func invalidCondition() metav1.Condition {
	return metav1.Condition{Type: workloadsv1.ConditionTypeValidated, Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonValidationFailed}
}

func TestReconcilePDBs(t *testing.T) {
	now := metav1.Now()
	older := metav1.NewTime(now.Add(-time.Hour))
	closed := intstr.FromInt32(0)
	open := intstr.FromString("100%")
	webLabels := map[string]string{"app": "web"}
	subject := workloadsv1.Subject{Kind: "ServiceAccount", Name: "cluster-autoscaler", Namespace: "kube-system"}

	ns := func(defaultClass string) *corev1.Namespace {
		n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
		if defaultClass != "" {
			n.Labels = map[string]string{workloadsv1.DefaultClassLabel: defaultClass}
		}
		return n
	}

	type wantPDB struct {
		owner          string // WorkloadClass name of the controller owner reference
		maxUnavailable string
		leaseOwner     string // expected workloads.gke.io/bypass-owner, if any
		// leftForAdoption means the reconciled WorkloadClass still owns the PDB but no longer governs the Pod,
		// and leaves the PDB for the governing WorkloadClass to adopt.
		leftForAdoption bool
	}

	testCases := []struct {
		name        string
		wc          *workloadsv1.WorkloadClass
		condition   metav1.Condition
		objects     []client.Object
		leases      []utils.PendingLease
		wantErr     bool
		wantPDBs    map[string]*wantPDB // PDB name -> expected PDB, or nil if it must not exist
		wantRequeue time.Duration       // approximate expected requeue; 0 means none
	}{
		{
			name:      "creates_per_pod_pdbs_for_governed_pods_only",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: []client.Object{
				ns(""),
				newTestPod("web-1", webLabels),
				newTestPod("web-2", webLabels),
				newTestPod("other", map[string]string{"app": "other"}),
			},
			wantPDBs: map[string]*wantPDB{
				"workload-web-1": {owner: "web", maxUnavailable: "0"},
				"workload-web-2": {owner: "web", maxUnavailable: "0"},
				"workload-other": nil,
			},
		},
		{
			name:      "ready_workload_class_opens_pdbs",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessReady, true),
			condition: validCondition(),
			objects:   []client.Object{ns(""), newTestPod("web-1", webLabels)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": {owner: "web", maxUnavailable: "100%"}},
		},
		{
			name:      "skips_pod_covered_by_user_pdb_and_deletes_its_existing_pdb",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: func() []client.Object {
				wc := newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true)
				covered := newTestPod("web-1", map[string]string{"app": "web", "tier": "frontend"})
				userPDB := &policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{Name: "user-frontend", Namespace: "default"},
					Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "frontend"}}},
				}
				return []client.Object{ns(""), covered, newTestPod("web-2", webLabels), userPDB, newOwnedPDB(wc, covered, closed)}
			}(),
			wantPDBs: map[string]*wantPDB{
				"workload-web-1": nil,
				"workload-web-2": {owner: "web", maxUnavailable: "0"},
			},
		},
		{
			name:      "deletes_pdb_of_deleted_pod",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: []client.Object{
				ns(""),
				newOwnedPDB(newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true), newTestPod("gone", webLabels), closed),
			},
			wantPDBs: map[string]*wantPDB{"workload-gone": nil},
		},
		{
			name:      "invalid_workload_class_opens_owned_pdbs_without_creating_new_ones",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, false),
			condition: invalidCondition(),
			objects: []client.Object{
				ns(""),
				newTestPod("web-1", webLabels),
				newTestPod("web-2", webLabels),
				newOwnedPDB(newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, false), newTestPod("web-1", webLabels), closed),
			},
			wantPDBs: map[string]*wantPDB{
				"workload-web-1": {owner: "web", maxUnavailable: "100%"},
				"workload-web-2": nil,
			},
		},
		{
			name:      "shadowed_by_namespace_default_leaves_pdb_for_default_to_adopt",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: []client.Object{
				ns("ns-default"),
				newTestWC("ns-default", nil, now, workloadsv1.ReadinessReady, true),
				newTestPod("web-1", webLabels),
				newOwnedPDB(newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true), newTestPod("web-1", webLabels), closed),
			},
			wantPDBs: map[string]*wantPDB{"workload-web-1": {owner: "web", maxUnavailable: "0", leftForAdoption: true}},
		},
		{
			name:      "namespace_default_governs_all_pods_and_adopts_existing_pdbs",
			wc:        newTestWC("ns-default", nil, now, workloadsv1.ReadinessReady, true),
			condition: validCondition(),
			objects: []client.Object{
				ns("ns-default"),
				newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
				newTestPod("web-1", webLabels),
				newTestPod("other", map[string]string{"app": "other"}),
				newOwnedPDB(newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true), newTestPod("web-1", webLabels), closed),
			},
			wantPDBs: map[string]*wantPDB{
				"workload-web-1": {owner: "ns-default", maxUnavailable: "100%"},
				"workload-other": {owner: "ns-default", maxUnavailable: "100%"},
			},
		},
		{
			name:      "more_specific_workload_class_adopts_pdb",
			wc:        newTestWC("web-frontend", map[string]string{"app": "web", "tier": "frontend"}, now, workloadsv1.ReadinessReady, true),
			condition: validCondition(),
			objects: []client.Object{
				ns(""),
				newTestWC("web", webLabels, older, workloadsv1.ReadinessNotReady, true),
				newTestPod("web-1", map[string]string{"app": "web", "tier": "frontend"}),
				newOwnedPDB(newTestWC("web", webLabels, older, workloadsv1.ReadinessNotReady, true),
					newTestPod("web-1", map[string]string{"app": "web", "tier": "frontend"}), closed),
			},
			wantPDBs: map[string]*wantPDB{"workload-web-1": {owner: "web-frontend", maxUnavailable: "100%"}},
		},
		{
			name:      "less_specific_workload_class_deletes_nothing_it_does_not_govern",
			wc:        newTestWC("web", webLabels, older, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: []client.Object{
				ns(""),
				newTestWC("web-frontend", map[string]string{"app": "web", "tier": "frontend"}, now, workloadsv1.ReadinessReady, true),
				newTestPod("web-1", map[string]string{"app": "web", "tier": "frontend"}),
				newOwnedPDB(newTestWC("web-frontend", map[string]string{"app": "web", "tier": "frontend"}, now, workloadsv1.ReadinessReady, true),
					newTestPod("web-1", map[string]string{"app": "web", "tier": "frontend"}), open),
			},
			wantPDBs: map[string]*wantPDB{"workload-web-1": {owner: "web-frontend", maxUnavailable: "100%"}},
		},
		{
			name:      "applies_pending_lease_to_requested_pod_only",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects:   []client.Object{ns(""), newTestPod("web-1", webLabels), newTestPod("web-2", webLabels)},
			leases:    []utils.PendingLease{{PodName: "web-1", PodUID: "uid-web-1", Subject: subject}},
			wantPDBs: map[string]*wantPDB{
				"workload-web-1": {owner: "web", maxUnavailable: "100%", leaseOwner: utils.BypassOwnerValue(subject)},
				"workload-web-2": {owner: "web", maxUnavailable: "0"},
			},
			wantRequeue: utils.DefaultLeaseDuration,
		},
		{
			name:      "discards_lease_for_previous_pod_instance",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects:   []client.Object{ns(""), newTestPod("web-1", webLabels)},
			leases:    []utils.PendingLease{{PodName: "web-1", PodUID: "old-uid", Subject: subject}},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": {owner: "web", maxUnavailable: "0"}},
		},
		{
			name:      "preserves_active_lease",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: func() []client.Object {
				wc := newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true)
				pod := newTestPod("web-1", webLabels)
				leased := newOwnedPDB(wc, pod, open)
				leased.Annotations = map[string]string{
					utils.BypassOwner:      "user-a",
					utils.BypassExpiration: time.Now().Add(time.Minute).Format(utils.ExpirationFormat),
				}
				return []client.Object{ns(""), pod, leased}
			}(),
			wantPDBs:    map[string]*wantPDB{"workload-web-1": {owner: "web", maxUnavailable: "100%", leaseOwner: "user-a"}},
			wantRequeue: time.Minute,
		},
		{
			name:      "discards_lease_request_when_another_subject_holds_lease",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: func() []client.Object {
				wc := newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true)
				pod := newTestPod("web-1", webLabels)
				leased := newOwnedPDB(wc, pod, open)
				leased.Annotations = map[string]string{
					utils.BypassOwner:      "user-a",
					utils.BypassExpiration: time.Now().Add(time.Minute).Format(utils.ExpirationFormat),
				}
				return []client.Object{ns(""), pod, leased}
			}(),
			leases:      []utils.PendingLease{{PodName: "web-1", PodUID: "uid-web-1", Subject: subject}},
			wantPDBs:    map[string]*wantPDB{"workload-web-1": {owner: "web", maxUnavailable: "100%", leaseOwner: "user-a"}},
			wantRequeue: time.Minute,
		},
		{
			name:      "expired_lease_is_cleared",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: func() []client.Object {
				wc := newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true)
				pod := newTestPod("web-1", webLabels)
				leased := newOwnedPDB(wc, pod, open)
				leased.Annotations = map[string]string{
					utils.BypassOwner:      "user-a",
					utils.BypassExpiration: time.Now().Add(-time.Minute).Format(utils.ExpirationFormat),
				}
				return []client.Object{ns(""), pod, leased}
			}(),
			wantPDBs: map[string]*wantPDB{"workload-web-1": {owner: "web", maxUnavailable: "0"}},
		},
		{
			name:      "refuses_to_modify_user_pdb_with_the_same_name",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: []client.Object{
				ns(""),
				newTestPod("web-1", webLabels),
				&policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{Name: "workload-web-1", Namespace: "default"},
					Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "unrelated"}}},
				},
			},
			leases:   []utils.PendingLease{{PodName: "web-1", PodUID: "uid-web-1", Subject: subject}},
			wantErr:  true,
			wantPDBs: map[string]*wantPDB{"workload-web-1": {owner: ""}},
		},
		{
			name: "deleting_workload_class_is_skipped",
			wc: func() *workloadsv1.WorkloadClass {
				wc := newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true)
				wc.DeletionTimestamp = &now
				return wc
			}(),
			condition: validCondition(),
			objects:   []client.Object{ns(""), newTestPod("web-1", webLabels)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": nil},
		},
		{
			name:      "skips_terminal_pods_and_deletes_their_existing_pdbs",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: func() []client.Object {
				wc := newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true)
				succeeded := newTestPod("job-succeeded", webLabels)
				succeeded.Status.Phase = corev1.PodSucceeded
				failed := newTestPod("job-failed", webLabels)
				failed.Status.Phase = corev1.PodFailed
				return []client.Object{
					ns(""),
					succeeded,
					failed,
					newTestPod("web-active", webLabels),
					newOwnedPDB(wc, succeeded, closed),
					newOwnedPDB(wc, failed, closed),
				}
			}(),
			wantPDBs: map[string]*wantPDB{
				"workload-job-succeeded": nil,
				"workload-job-failed":    nil,
				"workload-web-active":    {owner: "web", maxUnavailable: "0"},
			},
		},
		{
			name:      "reconciles_and_retains_truncated_pdb_for_long_pod_name",
			wc:        newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true),
			condition: validCondition(),
			objects: func() []client.Object {
				wc := newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true)
				longPod := newTestPod(strings.Repeat("a", 250), webLabels)
				return []client.Object{ns(""), longPod, newOwnedPDB(wc, longPod, open)}
			}(),
			wantPDBs: map[string]*wantPDB{
				utils.PDBName(strings.Repeat("a", 250)): {owner: "web", maxUnavailable: "0"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			wcForClient := tc.wc.DeepCopy()
			wcForClient.DeletionTimestamp = nil // The fake client rejects objects with a DeletionTimestamp and no finalizers.
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(tc.objects, wcForClient)...).Build()
			tracker := utils.NewPendingLeaseTracker(16)
			r := &WorkloadClassReconciler{Client: c, Scheme: scheme, LeaseTracker: tracker}

			requeue, err := r.reconcilePDBs(t.Context(), tc.wc, tc.condition, tc.leases)
			if (err != nil) != tc.wantErr {
				t.Fatalf("reconcilePDBs() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && len(tc.leases) > 0 {
				if drained := tracker.Drain(client.ObjectKeyFromObject(tc.wc)); !reflect.DeepEqual(drained, tc.leases) {
					t.Errorf("failed reconcilePDBs() should re-enqueue unapplied leases: got %v, want %v", drained, tc.leases)
				}
			}

			if tc.wantRequeue == 0 && requeue != 0 {
				t.Errorf("reconcilePDBs() requeue = %v, want 0", requeue)
			}
			if tc.wantRequeue != 0 && (requeue <= 0 || requeue > tc.wantRequeue) {
				t.Errorf("reconcilePDBs() requeue = %v, want in (0, %v]", requeue, tc.wantRequeue)
			}

			for name, want := range tc.wantPDBs {
				pdb := &policyv1.PodDisruptionBudget{}
				err := c.Get(t.Context(), client.ObjectKey{Name: name, Namespace: "default"}, pdb)
				if want == nil {
					if !errors.IsNotFound(err) {
						t.Errorf("PDB %s: expected it to not exist, got err=%v", name, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("PDB %s: expected it to exist, got err=%v", name, err)
				}

				owner := metav1.GetControllerOf(pdb)
				if want.owner == "" {
					if owner != nil {
						t.Errorf("PDB %s: expected no controller, got %s", name, owner.Name)
					}
					continue
				}
				if owner == nil || owner.Name != want.owner {
					t.Errorf("PDB %s: controller = %v, want %s", name, owner, want.owner)
				}
				if got := pdb.Spec.MaxUnavailable.String(); got != want.maxUnavailable {
					t.Errorf("PDB %s: maxUnavailable = %s, want %s", name, got, want.maxUnavailable)
				}
				if got := pdb.Annotations[utils.BypassOwner]; got != want.leaseOwner {
					t.Errorf("PDB %s: %s = %q, want %q", name, utils.BypassOwner, got, want.leaseOwner)
				}

				// The PDB must select exactly its Pod, and the Pod must carry the matching label.
				pod, err := r.getPodForPDB(t.Context(), "default", pdb)
				if err != nil || pod == nil {
					t.Fatalf("getPodForPDB(%s): pod=%v, err=%v", name, pod, err)
				}
				wantSelector := &metav1.LabelSelector{MatchLabels: map[string]string{utils.PodUIDLabel: string(pod.UID)}}
				if !reflect.DeepEqual(pdb.Spec.Selector, wantSelector) {
					t.Errorf("PDB %s: selector = %v, want %v", name, pdb.Spec.Selector, wantSelector)
				}
				// Only a valid WorkloadClass labels the Pods it governs.
				governs := want.owner == tc.wc.Name && !want.leftForAdoption && tc.condition.Reason == workloadsv1.ReasonValidationPassed
				if governs && pod.Labels[utils.PodUIDLabel] != string(pod.UID) {
					t.Errorf("Pod %s: %s label = %q, want %q", pod.Name, utils.PodUIDLabel, pod.Labels[utils.PodUIDLabel], pod.UID)
				}
			}
		})
	}
}

func TestPodPredicate(t *testing.T) {
	pod := newTestPod("web-1", map[string]string{"app": "web"})
	labeled := pod.DeepCopy()
	labeled.Labels[utils.PodUIDLabel] = string(pod.UID)
	relabeled := labeled.DeepCopy()
	relabeled.Labels["app"] = "other"
	terminating := labeled.DeepCopy()
	terminating.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	statusOnly := labeled.DeepCopy()
	statusOnly.Status.Phase = corev1.PodRunning
	succeeded := statusOnly.DeepCopy()
	succeeded.Status.Phase = corev1.PodSucceeded
	failed := statusOnly.DeepCopy()
	failed.Status.Phase = corev1.PodFailed

	testCases := []struct {
		name     string
		old, new *corev1.Pod
		want     bool
	}{
		{name: "pod_uid_label_added_by_controller_is_ignored", old: pod, new: labeled, want: false},
		{name: "status_only_change_is_ignored", old: labeled, new: statusOnly, want: false},
		{name: "label_change_triggers", old: labeled, new: relabeled, want: true},
		{name: "terminating_triggers", old: labeled, new: terminating, want: true},
		{name: "succeeded_phase_triggers", old: statusOnly, new: succeeded, want: true},
		{name: "failed_phase_triggers", old: statusOnly, new: failed, want: true},
		{name: "pod_uid_label_removed_triggers", old: labeled, new: pod, want: true},
	}

	p := podPredicate()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
				t.Errorf("podPredicate().Update() = %v, want %v", got, tc.want)
			}
		})
	}
	if !p.Create(event.CreateEvent{Object: pod}) || !p.Delete(event.DeleteEvent{Object: pod}) {
		t.Error("podPredicate() should pass Create and Delete events")
	}
}

func TestCalculateReadinessBestMatchAndTerminalPods(t *testing.T) {
	now := metav1.Now()
	oldPodTime := metav1.NewTime(now.Add(-10 * 24 * time.Hour))
	youngPodTime := metav1.NewTime(now.Add(-1 * time.Hour))
	today := time.Now().UTC().Weekday().String()

	generalWC := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-general",
			Namespace:         "default",
			CreationTimestamp: oldPodTime,
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			DisruptionPolicy: workloadsv1.DisruptionPolicy{
				MaxNonDisruptionDurationDays: 30,
				MinInitialRunDurationDays:    7,
				AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{
					{Name: "Today", DaysOfWeek: []string{today}, TimeZone: "UTC", StartTime: "00:00", EndTime: "23:59"},
				},
			},
		},
	}
	canaryWC := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-canary",
			Namespace:         "default",
			CreationTimestamp: oldPodTime,
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web", "track": "canary"}},
			DisruptionPolicy: workloadsv1.DisruptionPolicy{
				MaxNonDisruptionDurationDays: 30,
				MinInitialRunDurationDays:    0,
			},
		},
	}

	stablePod := newTestPod("web-stable", map[string]string{"app": "web"})
	stablePod.CreationTimestamp = oldPodTime

	youngCanaryPod := newTestPod("web-canary", map[string]string{"app": "web", "track": "canary"})
	youngCanaryPod.CreationTimestamp = youngPodTime

	youngSucceededPod := newTestPod("web-job-done", map[string]string{"app": "web"})
	youngSucceededPod.CreationTimestamp = youngPodTime
	youngSucceededPod.Status.Phase = corev1.PodSucceeded

	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		generalWC, canaryWC, stablePod, youngCanaryPod, youngSucceededPod,
	).Build()
	r := &WorkloadClassReconciler{Client: c, Scheme: scheme}

	readiness, _, err := r.calculateReadiness(t.Context(), generalWC)
	if err != nil {
		t.Fatalf("calculateReadiness() unexpected error: %v", err)
	}
	if readiness != workloadsv1.ReadinessReady {
		t.Errorf("calculateReadiness() = %v, want %v (young canary and succeeded Pods should not block generalWC)", readiness, workloadsv1.ReadinessReady)
	}
}

func TestNamespaceDefaultClassPredicate(t *testing.T) {
	withDefault := func(value string) *corev1.Namespace {
		n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default", Labels: map[string]string{"other": "label"}}}
		if value != "" {
			n.Labels[workloadsv1.DefaultClassLabel] = value
		}
		return n
	}

	p := namespaceDefaultClassPredicate()
	testCases := []struct {
		name     string
		old, new *corev1.Namespace
		want     bool
	}{
		{name: "default_added", old: withDefault(""), new: withDefault("wc"), want: true},
		{name: "default_changed", old: withDefault("wc"), new: withDefault("other-wc"), want: true},
		{name: "default_removed", old: withDefault("wc"), new: withDefault(""), want: true},
		{name: "unrelated_change", old: withDefault("wc"), new: withDefault("wc"), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
				t.Errorf("namespaceDefaultClassPredicate().Update() = %v, want %v", got, tc.want)
			}
		})
	}
	if !p.Create(event.CreateEvent{Object: withDefault("wc")}) || p.Create(event.CreateEvent{Object: withDefault("")}) {
		t.Error("namespaceDefaultClassPredicate().Create() should pass only Namespaces with a default class")
	}
}

func TestFindWorkloadClassesForPod(t *testing.T) {
	now := metav1.Now()
	webLabels := map[string]string{"app": "web"}
	scheme := newTestScheme(t)
	web := newTestWC("web", webLabels, now, workloadsv1.ReadinessNotReady, true)
	previous := newTestWC("previous", map[string]string{"app": "previous"}, now, workloadsv1.ReadinessNotReady, true)
	pod := newTestPod("web-1", webLabels)

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		web, previous, pod,
		newOwnedPDB(previous, pod, intstr.FromInt32(0)), // The Pod's PDB is still owned by the WorkloadClass it used to match.
	).Build()
	r := &WorkloadClassReconciler{Client: c, Scheme: scheme}

	got := map[string]bool{}
	for _, req := range r.findWorkloadClassesForPod(t.Context(), pod) {
		got[req.Name] = true
	}
	if want := map[string]bool{"web": true, "previous": true}; !reflect.DeepEqual(got, want) {
		t.Errorf("findWorkloadClassesForPod() = %v, want %v", got, want)
	}
}

func makeWC(name string, creationTime metav1.Time) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			UID:               types.UID("uid-" + name), // Required for SetControllerReference
			CreationTimestamp: creationTime,
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
		},
		Status: workloadsv1.WorkloadClassStatus{
			MaintenanceReadiness: workloadsv1.ReadinessReady,
		},
	}
}

func TestPlacementPluginCondition(t *testing.T) {
	testCases := []struct {
		name           string
		wc             *workloadsv1.WorkloadClass
		wantNil        bool
		wantStatus     metav1.ConditionStatus
		wantReason     string
		wantMessageSub string
		wantGen        int64
	}{
		{
			name: "no_infrastructure_profile_ref_returns_nil",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "wc-no-ref",
					Namespace:  "default",
					Generation: 1,
				},
			},
			wantNil: true,
		},
		{
			name: "infrastructure_profile_ref_with_no_existing_condition_returns_plugin_pending",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "wc-pending",
					Namespace:  "default",
					Generation: 2,
				},
				Spec: workloadsv1.WorkloadClassSpec{
					InfrastructureProfileRef: &workloadsv1.InfrastructureProfileReference{
						Group: "workloads.gke.io",
						Kind:  "GKESpotPlacementPolicy",
						Name:  "gke-t2d-spot",
					},
				},
			},
			wantNil:        false,
			wantStatus:     metav1.ConditionFalse,
			wantReason:     workloadsv1.ReasonPluginPending,
			wantMessageSub: "Waiting for plugin controller to reconcile GKESpotPlacementPolicy",
			wantGen:        2,
		},
		{
			name: "existing_condition_matching_generation_is_preserved",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "wc-attached",
					Namespace:  "default",
					Generation: 3,
				},
				Spec: workloadsv1.WorkloadClassSpec{
					InfrastructureProfileRef: &workloadsv1.InfrastructureProfileReference{
						Group: "workloads.gke.io",
						Kind:  "GKESpotPlacementPolicy",
						Name:  "gke-t2d-spot",
					},
				},
				Status: workloadsv1.WorkloadClassStatus{
					Conditions: []metav1.Condition{
						{
							Type:               workloadsv1.ConditionTypePlacementPluginAttached,
							Status:             metav1.ConditionTrue,
							Reason:             workloadsv1.ReasonProfileResolved,
							Message:            "GKESpotPlacementPolicy gke-t2d-spot attached",
							ObservedGeneration: 3,
						},
					},
				},
			},
			wantNil:        false,
			wantStatus:     metav1.ConditionTrue,
			wantReason:     workloadsv1.ReasonProfileResolved,
			wantMessageSub: "GKESpotPlacementPolicy gke-t2d-spot attached",
			wantGen:        3,
		},
		{
			name: "existing_condition_with_stale_generation_resets_to_plugin_pending",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "wc-stale-gen",
					Namespace:  "default",
					Generation: 4,
				},
				Spec: workloadsv1.WorkloadClassSpec{
					InfrastructureProfileRef: &workloadsv1.InfrastructureProfileReference{
						Group: "workloads.gke.io",
						Kind:  "GKESpotPlacementPolicy",
						Name:  "gke-t2d-spot",
					},
				},
				Status: workloadsv1.WorkloadClassStatus{
					Conditions: []metav1.Condition{
						{
							Type:               workloadsv1.ConditionTypePlacementPluginAttached,
							Status:             metav1.ConditionTrue,
							Reason:             workloadsv1.ReasonProfileResolved,
							Message:            "GKESpotPlacementPolicy gke-t2d-spot attached",
							ObservedGeneration: 3,
						},
					},
				},
			},
			wantNil:        false,
			wantStatus:     metav1.ConditionFalse,
			wantReason:     workloadsv1.ReasonPluginPending,
			wantMessageSub: "Waiting for plugin controller to reconcile GKESpotPlacementPolicy",
			wantGen:        4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := placementPluginCondition(context.Background(), tc.wc)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("placementPluginCondition() = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("placementPluginCondition() = nil, want non-nil condition")
			}
			if got.Type != workloadsv1.ConditionTypePlacementPluginAttached {
				t.Errorf("got Type = %q, want %q", got.Type, workloadsv1.ConditionTypePlacementPluginAttached)
			}
			if got.Status != tc.wantStatus {
				t.Errorf("got Status = %q, want %q", got.Status, tc.wantStatus)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("got Reason = %q, want %q", got.Reason, tc.wantReason)
			}
			if !strings.Contains(got.Message, tc.wantMessageSub) {
				t.Errorf("got Message = %q, want substring %q", got.Message, tc.wantMessageSub)
			}
			if got.ObservedGeneration != tc.wantGen {
				t.Errorf("got ObservedGeneration = %d, want %d", got.ObservedGeneration, tc.wantGen)
			}
		})
	}
}

func TestReconcileConditions(t *testing.T) {
	testCases := []struct {
		name        string
		validation  metav1.Condition
		placement   *metav1.Condition
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string
	}{
		{
			name: "nil_placement_leaves_validation_unchanged",
			validation: metav1.Condition{
				Type:    workloadsv1.ConditionTypeValidated,
				Status:  metav1.ConditionTrue,
				Reason:  workloadsv1.ReasonValidationPassed,
				Message: "WorkloadClass adheres to all Guardrail constraints",
			},
			placement:   nil,
			wantStatus:  metav1.ConditionTrue,
			wantReason:  workloadsv1.ReasonValidationPassed,
			wantMessage: "WorkloadClass adheres to all Guardrail constraints",
		},
		{
			name: "attached_placement_leaves_passing_validation_unchanged",
			validation: metav1.Condition{
				Type:    workloadsv1.ConditionTypeValidated,
				Status:  metav1.ConditionTrue,
				Reason:  workloadsv1.ReasonValidationPassed,
				Message: "WorkloadClass adheres to all Guardrail constraints",
			},
			placement: &metav1.Condition{
				Type:   workloadsv1.ConditionTypePlacementPluginAttached,
				Status: metav1.ConditionTrue,
				Reason: workloadsv1.ReasonProfileResolved,
			},
			wantStatus:  metav1.ConditionTrue,
			wantReason:  workloadsv1.ReasonValidationPassed,
			wantMessage: "WorkloadClass adheres to all Guardrail constraints",
		},
		{
			name: "attached_placement_leaves_failing_validation_unchanged",
			validation: metav1.Condition{
				Type:    workloadsv1.ConditionTypeValidated,
				Status:  metav1.ConditionFalse,
				Reason:  workloadsv1.ReasonValidationFailed,
				Message: "number of windows exceeds guardrail limit 2",
			},
			placement: &metav1.Condition{
				Type:   workloadsv1.ConditionTypePlacementPluginAttached,
				Status: metav1.ConditionTrue,
				Reason: workloadsv1.ReasonProfileResolved,
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonValidationFailed,
			wantMessage: "number of windows exceeds guardrail limit 2",
		},
		{
			name: "unattached_placement_marks_passing_validation_as_failed_with_clean_message",
			validation: metav1.Condition{
				Type:    workloadsv1.ConditionTypeValidated,
				Status:  metav1.ConditionTrue,
				Reason:  workloadsv1.ReasonValidationPassed,
				Message: "WorkloadClass adheres to all Guardrail constraints",
			},
			placement: &metav1.Condition{
				Type:   workloadsv1.ConditionTypePlacementPluginAttached,
				Status: metav1.ConditionFalse,
				Reason: workloadsv1.ReasonPluginPending,
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonValidationFailed,
			wantMessage: "WorkloadClass references an infrastructure profile, but the plugin has not attached",
		},
		{
			name: "unattached_placement_with_message_surfaces_details_in_validation",
			validation: metav1.Condition{
				Type:    workloadsv1.ConditionTypeValidated,
				Status:  metav1.ConditionTrue,
				Reason:  workloadsv1.ReasonValidationPassed,
				Message: "WorkloadClass adheres to all Guardrail constraints",
			},
			placement: &metav1.Condition{
				Type:    workloadsv1.ConditionTypePlacementPluginAttached,
				Status:  metav1.ConditionFalse,
				Reason:  workloadsv1.ReasonPluginResolutionFailed,
				Message: "GKESpotPlacementPolicy 'gke-t2d-spot' not found",
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonValidationFailed,
			wantMessage: "WorkloadClass references an infrastructure profile, but the plugin has not attached: GKESpotPlacementPolicy 'gke-t2d-spot' not found",
		},
		{
			name: "unattached_placement_appends_to_existing_validation_failures",
			validation: metav1.Condition{
				Type:    workloadsv1.ConditionTypeValidated,
				Status:  metav1.ConditionFalse,
				Reason:  workloadsv1.ReasonValidationFailed,
				Message: "number of windows exceeds guardrail limit 2",
			},
			placement: &metav1.Condition{
				Type:   workloadsv1.ConditionTypePlacementPluginAttached,
				Status: metav1.ConditionFalse,
				Reason: workloadsv1.ReasonPluginPending,
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1.ReasonValidationFailed,
			wantMessage: "number of windows exceeds guardrail limit 2; WorkloadClass references an infrastructure profile, but the plugin has not attached",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.validation
			reconcileConditions(&v, tc.placement)
			if v.Status != tc.wantStatus {
				t.Errorf("got Status = %q, want %q", v.Status, tc.wantStatus)
			}
			if v.Reason != tc.wantReason {
				t.Errorf("got Reason = %q, want %q", v.Reason, tc.wantReason)
			}
			if v.Message != tc.wantMessage {
				t.Errorf("got Message = %q, want %q", v.Message, tc.wantMessage)
			}
		})
	}
}

func TestGuardrailCapacityConstraints(t *testing.T) {
	testCases := []struct {
		name            string
		guardrailJSONs  []string
		wantEnforcement workloadsv1.EnforcementMode
		wantMinRatio    int
		wantFallback    bool
		wantErr         bool
	}{
		{
			name:            "empty_guardrails_returns_defaults",
			guardrailJSONs:  nil,
			wantEnforcement: workloadsv1.AllowedEnforcementMode,
			wantMinRatio:    0,
			wantFallback:    true,
		},
		{
			name: "most_restrictive_across_multiple_guardrails",
			guardrailJSONs: []string{
				`{"enforcementMode": "Allowed", "minSpotRatio": "20%", "fallback": {"allowFallbackToOnDemand": true}}`,
				`{"enforcementMode": "Required", "minSpotRatio": "60%"}`,
			},
			wantEnforcement: workloadsv1.RequiredEnforcementMode,
			wantMinRatio:    60,
			wantFallback:    true,
		},
		{
			name: "forbidden_overrides_required_and_resets_min_spot_ratio_to_zero",
			guardrailJSONs: []string{
				`{"enforcementMode": "Required", "minSpotRatio": "50%"}`,
				`{"enforcementMode": "Forbidden", "fallback": {"allowFallbackToOnDemand": false}}`,
			},
			wantEnforcement: workloadsv1.ForbiddenEnforcementMode,
			wantMinRatio:    0,
			wantFallback:    false,
		},
		{
			name: "forbidden_preceding_required_is_not_overwritten",
			guardrailJSONs: []string{
				`{"enforcementMode": "Forbidden"}`,
				`{"enforcementMode": "Required", "minSpotRatio": "50%"}`,
			},
			wantEnforcement: workloadsv1.ForbiddenEnforcementMode,
			wantMinRatio:    0,
			wantFallback:    true,
		},
		{
			name: "fallback_disallowed_by_zero_max_fallback_ratio",
			guardrailJSONs: []string{
				`{"fallback": {"maxFallbackRatio": "0%"}}`,
			},
			wantEnforcement: workloadsv1.AllowedEnforcementMode,
			wantMinRatio:    0,
			wantFallback:    false,
		},
		{
			name: "malformed_guardrail_json_returns_error_and_still_processes_valid_guardrails",
			guardrailJSONs: []string{
				`{"unknownField": true}`,
				`{"enforcementMode": "Required", "minSpotRatio": "50%"}`,
			},
			wantEnforcement: workloadsv1.RequiredEnforcementMode,
			wantMinRatio:    50,
			wantFallback:    true,
			wantErr:         true,
		},
		{
			name: "invalid_guardrail_min_spot_ratio_returns_error",
			guardrailJSONs: []string{
				`{"enforcementMode": "Required", "minSpotRatio": "invalid%"}`,
			},
			wantEnforcement: workloadsv1.RequiredEnforcementMode,
			wantMinRatio:    0,
			wantFallback:    true,
			wantErr:         true,
		},
		{
			name: "out_of_range_guardrail_min_spot_ratio_returns_error",
			guardrailJSONs: []string{
				`{"enforcementMode": "Required", "minSpotRatio": "150%"}`,
			},
			wantEnforcement: workloadsv1.RequiredEnforcementMode,
			wantMinRatio:    0,
			wantFallback:    true,
			wantErr:         true,
		},
		{
			name: "negative_guardrail_min_spot_ratio_returns_error",
			guardrailJSONs: []string{
				`{"minSpotRatio": "-10%"}`,
			},
			wantEnforcement: workloadsv1.AllowedEnforcementMode,
			wantMinRatio:    0,
			wantFallback:    true,
			wantErr:         true,
		},
		{
			name: "unsuffixed_guardrail_min_spot_ratio_returns_error",
			guardrailJSONs: []string{
				`{"minSpotRatio": "60"}`,
			},
			wantEnforcement: workloadsv1.AllowedEnforcementMode,
			wantMinRatio:    0,
			wantFallback:    true,
			wantErr:         true,
		},
		{
			name: "invalid_guardrail_enforcement_mode_returns_error",
			guardrailJSONs: []string{
				`{"enforcementMode": "InvalidMode"}`,
			},
			wantEnforcement: workloadsv1.AllowedEnforcementMode,
			wantMinRatio:    0,
			wantFallback:    true,
			wantErr:         true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			guardrails := make([]workloadsv1.WorkloadClassGuardrail, 0, len(tc.guardrailJSONs))
			for i, raw := range tc.guardrailJSONs {
				guardrails = append(guardrails, workloadsv1.WorkloadClassGuardrail{
					ObjectMeta: metav1.ObjectMeta{Name: "g-" + string(rune('a'+i))},
					Spec: workloadsv1.WorkloadClassGuardrailSpec{
						PluginConstraints: &workloadsv1.PluginConstraints{
							Placement: []workloadsv1.PluginConstraint{
								{
									Name:       workloadsv1.PluginNameGKESpotPlacement,
									Parameters: &apiextensionsv1.JSON{Raw: []byte(raw)},
								},
							},
						},
					},
				})
			}

			gotEnforcement, gotMinRatio, gotFallback, err := guardrailCapacityConstraints(context.Background(), guardrails)
			if (err != nil) != tc.wantErr {
				t.Errorf("got err = %v, wantErr %v", err, tc.wantErr)
			}
			if gotEnforcement != tc.wantEnforcement {
				t.Errorf("got enforcementMode = %q, want %q", gotEnforcement, tc.wantEnforcement)
			}
			if gotMinRatio != tc.wantMinRatio {
				t.Errorf("got minSpotRatio = %d, want %d", gotMinRatio, tc.wantMinRatio)
			}
			if gotFallback != tc.wantFallback {
				t.Errorf("got allowFallback = %v, want %v", gotFallback, tc.wantFallback)
			}
		})
	}
}

func TestValidateCapacityStrategy(t *testing.T) {
	testCases := []struct {
		name             string
		capacityStrategy *workloadsv1.CapacityStrategy
		guardrailJSONs   []string
		wantViolations   []string
	}{
		{
			name: "no_guardrails_is_valid",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type:           workloadsv1.SpotPlacementTypeSpot,
				SpotRatio:      "50%",
				FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand,
			},
			guardrailJSONs: nil,
			wantViolations: nil,
		},
		{
			name:             "nil_capacity_strategy_allowed_when_enforcement_is_allowed",
			capacityStrategy: nil,
			guardrailJSONs:   []string{`{"enforcementMode": "Allowed", "minSpotRatio": "50%"}`},
			wantViolations:   nil,
		},
		{
			name: "on_demand_allowed_when_enforcement_is_allowed_with_min_spot_ratio",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type: workloadsv1.SpotPlacementTypeOnDemand,
			},
			guardrailJSONs: []string{`{"enforcementMode": "Allowed", "minSpotRatio": "50%"}`},
			wantViolations: nil,
		},
		{
			name:             "nil_capacity_strategy_fails_when_enforcement_is_required",
			capacityStrategy: nil,
			guardrailJSONs:   []string{`{"enforcementMode": "Required"}`},
			wantViolations:   []string{"capacityStrategy is required when guardrail enforcementMode is Required"},
		},
		{
			name: "on_demand_fails_when_enforcement_is_required",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type: workloadsv1.SpotPlacementTypeOnDemand,
			},
			guardrailJSONs: []string{`{"enforcementMode": "Required"}`},
			wantViolations: []string{"capacityStrategy type OnDemand is not allowed when guardrail enforcementMode is Required"},
		},
		{
			name: "zero_spot_ratio_fails_when_enforcement_is_required",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "0%",
			},
			guardrailJSONs: []string{`{"enforcementMode": "Required"}`},
			wantViolations: []string{"spotRatio cannot be 0% when guardrail enforcementMode is Required"},
		},
		{
			name: "spot_fails_when_enforcement_is_forbidden",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type: workloadsv1.SpotPlacementTypeSpot,
			},
			guardrailJSONs: []string{`{"enforcementMode": "Forbidden"}`},
			wantViolations: []string{"capacityStrategy type Spot is not allowed when guardrail enforcementMode is Forbidden"},
		},
		{
			name: "spot_ratio_below_guardrail_min_spot_ratio_fails",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "40%",
			},
			guardrailJSONs: []string{
				`{"minSpotRatio": "30%"}`,
				`{"minSpotRatio": "60%"}`,
			},
			wantViolations: []string{"spotRatio 40% is less than guardrail minSpotRatio 60%"},
		},
		{
			name: "fallback_to_on_demand_fails_when_guardrail_disallows_fallback",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type:           workloadsv1.SpotPlacementTypeSpot,
				SpotRatio:      "80%",
				FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand,
			},
			guardrailJSONs: []string{
				`{"fallback": {"allowFallbackToOnDemand": false}}`,
			},
			wantViolations: []string{"fallback action FallbackToOnDemand is not allowed when guardrail disallows fallback to OnDemand"},
		},
		{
			name: "multiple_simultaneous_violations_are_all_returned",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type:           workloadsv1.SpotPlacementTypeSpot,
				SpotRatio:      "40%",
				FallbackAction: workloadsv1.FallbackActionFallbackToOnDemand,
			},
			guardrailJSONs: []string{
				`{"minSpotRatio": "60%", "fallback": {"allowFallbackToOnDemand": false}}`,
			},
			wantViolations: []string{
				"spotRatio 40% is less than guardrail minSpotRatio 60%",
				"fallback action FallbackToOnDemand is not allowed when guardrail disallows fallback to OnDemand",
			},
		},
		{
			name:             "empty_capacity_strategy_uses_defaults_and_passes_required_guardrail",
			capacityStrategy: &workloadsv1.CapacityStrategy{},
			guardrailJSONs: []string{
				`{"enforcementMode": "Required", "minSpotRatio": "80%", "fallback": {"allowFallbackToOnDemand": false}}`,
			},
			wantViolations: nil,
		},
		{
			name: "valid_spot_capacity_strategy_passes_all_constraints",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type:           workloadsv1.SpotPlacementTypeSpot,
				SpotRatio:      "80%",
				FallbackAction: workloadsv1.FallbackActionFail,
			},
			guardrailJSONs: []string{
				`{"enforcementMode": "Required", "minSpotRatio": "60%", "fallback": {"allowFallbackToOnDemand": false}}`,
			},
			wantViolations: nil,
		},
		{
			name:             "malformed_guardrail_parameters_fails_closed",
			capacityStrategy: nil,
			guardrailJSONs: []string{
				`{"unknownField": true}`,
			},
			wantViolations: []string{
				`failed to parse guardrail parameters: guardrail g-a: json: unknown field "unknownField"`,
			},
		},
		{
			name: "invalid_spot_ratio_fails_closed",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "invalid%",
			},
			guardrailJSONs: []string{
				`{"enforcementMode": "Required", "minSpotRatio": "60%"}`,
			},
			wantViolations: []string{
				`invalid spotRatio "invalid%": strconv.Atoi: parsing "invalid": invalid syntax`,
			},
		},
		{
			name: "out_of_range_guardrail_min_spot_ratio_fails_closed",
			capacityStrategy: &workloadsv1.CapacityStrategy{
				Type:      workloadsv1.SpotPlacementTypeSpot,
				SpotRatio: "80%",
			},
			guardrailJSONs: []string{
				`{"enforcementMode": "Required", "minSpotRatio": "150%"}`,
			},
			wantViolations: []string{
				`failed to parse guardrail parameters: guardrail g-a has invalid minSpotRatio "150%": must be between 0% and 100%`,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			guardrails := make([]workloadsv1.WorkloadClassGuardrail, 0, len(tc.guardrailJSONs))
			for i, raw := range tc.guardrailJSONs {
				guardrails = append(guardrails, workloadsv1.WorkloadClassGuardrail{
					ObjectMeta: metav1.ObjectMeta{Name: "g-" + string(rune('a'+i))},
					Spec: workloadsv1.WorkloadClassGuardrailSpec{
						PluginConstraints: &workloadsv1.PluginConstraints{
							Placement: []workloadsv1.PluginConstraint{
								{
									Name:       workloadsv1.PluginNameGKESpotPlacement,
									Parameters: &apiextensionsv1.JSON{Raw: []byte(raw)},
								},
							},
						},
					},
				})
			}

			wc := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wc"},
				Spec: workloadsv1.WorkloadClassSpec{
					CapacityStrategy: tc.capacityStrategy,
				},
			}
			guardrailList := &workloadsv1.WorkloadClassGuardrailList{Items: guardrails}

			got := validateCapacityStrategy(context.Background(), wc, guardrailList)
			if len(got) != len(tc.wantViolations) {
				t.Fatalf("got violations %v, want %v", got, tc.wantViolations)
			}
			for i := range got {
				if got[i] != tc.wantViolations[i] {
					t.Errorf("violation[%d] = %q, want %q", i, got[i], tc.wantViolations[i])
				}
			}
		})
	}
}
