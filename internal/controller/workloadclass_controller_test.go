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
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
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
			pdbKey := types.NamespacedName{
				Name:      "workload-" + workloadClassName,
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
		It("should create a PDB when successfully reconciled", func() {
			By("Reconciling")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the PDB was created in the API server")
			// Get the WC and ask the helper for the exact PDB name
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			expectedPDBName := "workload-" + wc.Name
			pdbKey := types.NamespacedName{Name: expectedPDBName, Namespace: wc.Namespace}

			pdb := &policyv1.PodDisruptionBudget{}
			// Use Eventually to wait for the cache to sync!
			Eventually(func() error {
				return k8sClient.Get(ctx, pdbKey, pdb)
			}, "10s", "1s").Should(Succeed(), "Failed to find PDB with name %s", expectedPDBName)

			Expect(pdb.Spec.UnhealthyPodEvictionPolicy).NotTo(BeNil())
			Expect(*pdb.Spec.UnhealthyPodEvictionPolicy).To(Equal(policyv1.IfHealthyBudget))
		})

		It("should delete the PDB if validation fails", func() {
			By("First reconciling to ensure PDB is created initially")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Wait for it to exist in the cache first!
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			expectedPDBName := "workload-" + wc.Name
			pdbKey := types.NamespacedName{Name: expectedPDBName, Namespace: wc.Namespace}

			Eventually(func() error {
				return k8sClient.Get(ctx, pdbKey, &policyv1.PodDisruptionBudget{})
			}, "10s", "1s").Should(Succeed())

			By("Making the WorkloadClass invalid (exceeding guardrails)")
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 100 // Break validation
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())

			By("Reconciling again")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the PDB was deleted because of validation failure")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, pdbKey, &policyv1.PodDisruptionBudget{})
				return errors.IsNotFound(err)
			}, "10s", "1s").Should(BeTrue(), "Expected PDB %s to be deleted", expectedPDBName)
		})

		It("should delete the PDB if another WorkloadClass is the namespace default", func() {
			By("First reconciling to ensure PDB is created initially")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			expectedPDBName := "workload-" + wc.Name
			pdbKey := types.NamespacedName{Name: expectedPDBName, Namespace: wc.Namespace}

			Eventually(func() error {
				return k8sClient.Get(ctx, pdbKey, &policyv1.PodDisruptionBudget{})
			}, "10s", "1s").Should(Succeed())

			By("Setting another WorkloadClass as the namespace default")
			otherWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "some-other-class",
					Namespace: defaultNamespace,
				},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 1,
					},
				},
			}
			Expect(k8sClient.Create(ctx, otherWC)).To(Succeed())

			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultNamespace}, ns)).To(Succeed())

			if ns.Labels == nil {
				ns.Labels = make(map[string]string)
			}
			ns.Labels[workloadsv1.DefaultClassLabel] = "some-other-class"
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, otherWC)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: defaultNamespace}, ns)
				delete(ns.Labels, workloadsv1.DefaultClassLabel)
				_ = k8sClient.Update(ctx, ns)
			}()

			By("Reconciling again")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the PDB was deleted because this class is not the default")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, pdbKey, &policyv1.PodDisruptionBudget{})
				return errors.IsNotFound(err)
			}, "10s", "1s").Should(BeTrue(), "Expected PDB %s to be deleted", expectedPDBName)
		})

		It("should trigger reconciliation for other classes when the default PDB is deleted", func() {
			By("Creating a default WorkloadClass")
			defaultWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "default-wc", Namespace: defaultNamespace},
				Spec:       workloadsv1.WorkloadClassSpec{DisruptionPolicy: workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 1}},
			}
			Expect(k8sClient.Create(ctx, defaultWC)).To(Succeed())

			By("Setting the namespace default label")
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultNamespace}, ns)).To(Succeed())
			if ns.Labels == nil {
				ns.Labels = make(map[string]string)
			}
			ns.Labels[workloadsv1.DefaultClassLabel] = "default-wc"
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			By("Creating a secondary WorkloadClass")
			secondaryWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "secondary-wc", Namespace: defaultNamespace},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 1},
					PodSelector:      &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
				},
			}
			Expect(k8sClient.Create(ctx, secondaryWC)).To(Succeed())

			By("Reconciling the default WorkloadClass to create its PDB")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "default-wc", Namespace: defaultNamespace}})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the default PDB exists")
			defaultPDBKey := types.NamespacedName{Name: "workload-default-wc", Namespace: defaultNamespace}
			Eventually(func() error {
				return k8sClient.Get(ctx, defaultPDBKey, &policyv1.PodDisruptionBudget{})
			}, "10s", "1s").Should(Succeed())

			By("Deleting the default WorkloadClass and the default PDB")
			// We MUST delete the defaultWC first. If we delete the label, the PDB watcher
			// won't realize this PDB belonged to the default class and will skip queueing secondary-wc.
			Expect(k8sClient.Delete(ctx, defaultWC)).To(Succeed())

			// Deleting the PDB triggers the Watches DeleteFunc and queues secondary-wc
			// (Since envtest doesn't have GC, we simulate the GC deleting the PDB)
			defaultPDB := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, defaultPDBKey, defaultPDB)).To(Succeed())
			Expect(k8sClient.Delete(ctx, defaultPDB)).To(Succeed())

			By("Verifying secondary-wc was automatically queued and created its own PDB")
			// We must call Reconcile manually because the test suite does not run the Manager / Watch loops
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "secondary-wc", Namespace: defaultNamespace}})
			Expect(err).NotTo(HaveOccurred())

			secondaryPDBKey := types.NamespacedName{Name: "workload-secondary-wc", Namespace: defaultNamespace}
			Eventually(func() error {
				return k8sClient.Get(ctx, secondaryPDBKey, &policyv1.PodDisruptionBudget{})
			}, "15s", "1s").Should(Succeed(), "Secondary PDB was never created, meaning it was not queued for reconciliation")

			// Cleanup
			_ = k8sClient.Delete(ctx, defaultWC)
			_ = k8sClient.Delete(ctx, secondaryWC)
			_ = k8sClient.Delete(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "workload-secondary-wc", Namespace: defaultNamespace}})
		})

		It("should trigger reconciliation for other classes when the default label is removed from the namespace", func() {
			By("Creating a default WorkloadClass")
			defaultWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "default-wc-test-2", Namespace: defaultNamespace},
				Spec:       workloadsv1.WorkloadClassSpec{DisruptionPolicy: workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 1}},
			}
			Expect(k8sClient.Create(ctx, defaultWC)).To(Succeed())

			By("Setting the namespace default label")
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultNamespace}, ns)).To(Succeed())
			if ns.Labels == nil {
				ns.Labels = make(map[string]string)
			}
			ns.Labels[workloadsv1.DefaultClassLabel] = "default-wc-test-2"
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			By("Creating a secondary WorkloadClass")
			secondaryWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "secondary-wc-test-2", Namespace: defaultNamespace},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 1},
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"unique-to": "secondary-wc-2"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, secondaryWC)).To(Succeed())

			By("Reconciling secondary-wc-test-2 and verifying it DOES NOT create a PDB")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "secondary-wc-test-2", Namespace: defaultNamespace}})
			Expect(err).NotTo(HaveOccurred())

			secondaryPDBKey := types.NamespacedName{Name: "workload-secondary-wc-test-2", Namespace: defaultNamespace}
			err = k8sClient.Get(ctx, secondaryPDBKey, &policyv1.PodDisruptionBudget{})
			Expect(errors.IsNotFound(err)).To(BeTrue(), "Expected PDB to not be created yet")

			By("Removing the default label from the namespace")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultNamespace}, ns)).To(Succeed())
			delete(ns.Labels, workloadsv1.DefaultClassLabel)
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			By("Reconciling secondary-wc-test-2 and verifying it DOES create a PDB")
			// We must call Reconcile manually because the test suite does not run the Manager / Watch loops
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "secondary-wc-test-2", Namespace: defaultNamespace}})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				return k8sClient.Get(ctx, secondaryPDBKey, &policyv1.PodDisruptionBudget{})
			}, "10s", "1s").Should(Succeed())

			// Cleanup
			_ = k8sClient.Delete(ctx, defaultWC)
			_ = k8sClient.Delete(ctx, secondaryWC)
			_ = k8sClient.Delete(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "workload-secondary-wc-test-2", Namespace: defaultNamespace}})
		})

		It("should trigger reconciliation for other classes when the default label is modified on the namespace", func() {
			By("Creating a default WorkloadClass")
			defaultWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "default-wc-test-3", Namespace: defaultNamespace},
				Spec:       workloadsv1.WorkloadClassSpec{DisruptionPolicy: workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 1}},
			}
			Expect(k8sClient.Create(ctx, defaultWC)).To(Succeed())

			By("Setting the namespace default label")
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultNamespace}, ns)).To(Succeed())
			if ns.Labels == nil {
				ns.Labels = make(map[string]string)
			}
			ns.Labels[workloadsv1.DefaultClassLabel] = "default-wc-test-3"
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			By("Creating a secondary WorkloadClass")
			secondaryWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "secondary-wc-test-3", Namespace: defaultNamespace},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 1},
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"unique-to": "secondary-wc-3"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, secondaryWC)).To(Succeed())

			By("Reconciling secondary-wc-test-3 and verifying it DOES NOT create a PDB")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "secondary-wc-test-3", Namespace: defaultNamespace}})
			Expect(err).NotTo(HaveOccurred())

			secondaryPDBKey := types.NamespacedName{Name: "workload-secondary-wc-test-3", Namespace: defaultNamespace}
			err = k8sClient.Get(ctx, secondaryPDBKey, &policyv1.PodDisruptionBudget{})
			Expect(errors.IsNotFound(err)).To(BeTrue(), "Expected PDB to not be created yet")

			By("Modifying the default label on the namespace to point to a non-existent class")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultNamespace}, ns)).To(Succeed())
			ns.Labels[workloadsv1.DefaultClassLabel] = "some-other-nonexistent-class"
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			By("Reconciling secondary-wc-test-3 and verifying it DOES create a PDB")
			// We must call Reconcile manually because the test suite does not run the Manager / Watch loops
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "secondary-wc-test-3", Namespace: defaultNamespace}})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				return k8sClient.Get(ctx, secondaryPDBKey, &policyv1.PodDisruptionBudget{})
			}, "10s", "1s").Should(Succeed())

			// Cleanup
			_ = k8sClient.Delete(ctx, defaultWC)
			_ = k8sClient.Delete(ctx, secondaryWC)
			_ = k8sClient.Delete(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "workload-secondary-wc-test-3", Namespace: defaultNamespace}})
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

func TestNamespaceDefault(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = workloadsv1.AddToScheme(scheme)

	criticalBatch := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "critical-batch",
			Namespace: "test-ns",
		},
	}

	testCases := []struct {
		name      string
		namespace *corev1.Namespace
		wc        *workloadsv1.WorkloadClass
		want      string
		wantErr   bool
	}{
		{
			name: "namespace_has_the_default_class_label",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-ns",
					Labels: map[string]string{
						workloadsv1.DefaultClassLabel: "critical-batch",
					},
				},
			},
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "some-class",
					Namespace: "test-ns",
				},
			},
			want:    "critical-batch",
			wantErr: false,
		},
		{
			name: "namespace_does_not_have_the_default_class_label",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-ns",
					Labels: map[string]string{
						"unrelated-label": "true",
					},
				},
			},
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "some-class",
					Namespace: "test-ns",
				},
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "namespace_is_missing_entirely",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "some-class",
					Namespace: "missing-ns",
				},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clientBuilder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.namespace != nil {
				clientBuilder = clientBuilder.WithObjects(tc.namespace, criticalBatch)
			}
			fakeClient := clientBuilder.Build()

			reconciler := &WorkloadClassReconciler{
				Client: fakeClient,
			}

			got, err := reconciler.namespaceDefault(context.Background(), tc.wc)
			if (err != nil) != tc.wantErr {
				t.Errorf("namespaceDefault() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if got != nil && got.Name != tc.want {
				t.Errorf("namespaceDefault() got = %v, want %q", got, tc.want)
			} else if got == nil && tc.want != "" {
				t.Errorf("namespaceDefault() got nil, want %q", tc.want)

			}
		})
	}
}

func TestOldestWorkloadClass(t *testing.T) {
	now := metav1.Now()
	oneHourAgo := metav1.NewTime(now.Add(-1 * time.Hour))
	twoHoursAgo := metav1.NewTime(now.Add(-2 * time.Hour))

	testCases := []struct {
		name               string
		wc                 *workloadsv1.WorkloadClass
		overlappingClasses []workloadsv1.WorkloadClass
		wantName           string
	}{
		{
			name: "target_wc_is_oldest_empty_overlapping_list",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "target-wc", CreationTimestamp: oneHourAgo},
			},
			overlappingClasses: []workloadsv1.WorkloadClass{},
			wantName:           "target-wc",
		},
		{
			name: "target_wc_is_older_than_all_overlapping_classes",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "target-wc", CreationTimestamp: twoHoursAgo},
			},
			overlappingClasses: []workloadsv1.WorkloadClass{
				{ObjectMeta: metav1.ObjectMeta{Name: "overlap-1", CreationTimestamp: oneHourAgo}},
				{ObjectMeta: metav1.ObjectMeta{Name: "overlap-2", CreationTimestamp: now}},
			},
			wantName: "target-wc",
		},
		{
			name: "an_overlapping_class_is_oldest",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "target-wc", CreationTimestamp: oneHourAgo},
			},
			overlappingClasses: []workloadsv1.WorkloadClass{
				{ObjectMeta: metav1.ObjectMeta{Name: "overlap-1", CreationTimestamp: now}},
				{ObjectMeta: metav1.ObjectMeta{Name: "overlap-oldest", CreationTimestamp: twoHoursAgo}},
			},
			wantName: "overlap-oldest",
		},
		{
			name: "ties_in_timestamp_return_first_checked",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "target-wc", CreationTimestamp: oneHourAgo},
			},
			overlappingClasses: []workloadsv1.WorkloadClass{
				{ObjectMeta: metav1.ObjectMeta{Name: "overlap-tie", CreationTimestamp: oneHourAgo}},
			},
			wantName: "target-wc",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := oldestWorkloadClass(tc.wc, tc.overlappingClasses)

			if got.Name != tc.wantName {
				t.Errorf("oldestWorkloadClass() returned %q, want %q", got.Name, tc.wantName)
			}
		})
	}
}

func TestDeletePDB(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = policyv1.AddToScheme(scheme)
	testCases := []struct {
		name      string
		pdbExists bool
		wc        *workloadsv1.WorkloadClass
		wantErr   bool
	}{
		{
			name:      "successful_deletion",
			pdbExists: true,
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-wc",
					Namespace: "default",
				},
			},
			wantErr: false,
		},
		{
			name:      "pdb_already_gone",
			pdbExists: false,
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "missing-wc",
					Namespace: "default",
				},
			},
			wantErr: false, // The IsNotFound error should be swallowed
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clientBuilder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.pdbExists {
				existingPDB := &policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{
						Name:      tc.wc.Name,
						Namespace: tc.wc.Namespace,
					},
				}
				clientBuilder = clientBuilder.WithObjects(existingPDB)
			}
			fakeClient := clientBuilder.Build()
			reconciler := &WorkloadClassReconciler{
				Client: fakeClient,
			}

			err := reconciler.deletePDB(context.Background(), tc.wc)
			if (err != nil) != tc.wantErr {
				t.Errorf("deletePDB() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			// Verify that the PDB was actually deleted from the cluster
			if err == nil {
				checkPDB := &policyv1.PodDisruptionBudget{}
				errCheck := fakeClient.Get(context.Background(), types.NamespacedName{
					Name:      "workload-" + tc.wc.Name,
					Namespace: tc.wc.Namespace,
				}, checkPDB)

				if !errors.IsNotFound(errCheck) {
					t.Errorf("Expected PDB to be deleted (IsNotFound error), but got: %v", errCheck)
				}
			}
		})
	}
}

func TestCreateOrUpdatePDB(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = policyv1.AddToScheme(scheme)
	_ = workloadsv1.AddToScheme(scheme)

	testCases := []struct {
		name         string
		pdbExists    bool
		nsDefault    bool
		wantSelector *metav1.LabelSelector
		wc           *workloadsv1.WorkloadClass
		wantErr      bool
	}{
		{
			name:      "creates_pdb_when_missing",
			pdbExists: false,
			nsDefault: false,
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-wc",
					Namespace: "default",
					UID:       "fake-uid-123",
				},
			},
			wantErr: false,
		},
		{
			name:      "updates_existing_pdb",
			pdbExists: true,
			nsDefault: false,
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-wc",
					Namespace: "default",
					UID:       "fake-uid-123",
				},
			},
			wantErr: false,
		},
		{
			name:      "updates_existing_namespace_default_pdb",
			pdbExists: true,
			nsDefault: true,
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-wc",
					Namespace: "default",
					UID:       "fake-uid-123",
				},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"labelA": "valueA"}},
				},
			},
			wantSelector: &metav1.LabelSelector{},
			wantErr:      false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clientBuilder := fake.NewClientBuilder().WithScheme(scheme)
			expectedPDB := utils.PDBBase(tc.wc)
			if tc.pdbExists {
				// Inject an existing, outdated, PDB into the fake cluster
				existingPDB := &policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{
						Name:      expectedPDB.Name,
						Namespace: expectedPDB.Namespace,
					},
				}
				clientBuilder = clientBuilder.WithObjects(existingPDB)
			}
			fakeClient := clientBuilder.Build()
			reconciler := &WorkloadClassReconciler{
				Client: fakeClient,
				Scheme: scheme,
			}

			err := reconciler.createOrUpdatePDB(context.Background(), tc.wc, tc.nsDefault)
			if (err != nil) != tc.wantErr {
				t.Errorf("createOrUpdatePDB() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if err == nil {
				savedPDB := &policyv1.PodDisruptionBudget{}
				errGet := fakeClient.Get(context.Background(), types.NamespacedName{
					Name:      expectedPDB.Name,
					Namespace: expectedPDB.Namespace,
				}, savedPDB)

				if errGet != nil {
					t.Fatalf("Expected PDB to exist in the API server, but got error: %v", errGet)
				}

				if len(savedPDB.OwnerReferences) == 0 {
					t.Errorf("Expected PDB to have an OwnerReference to the WorkloadClass, but found none")
				} else {
					if savedPDB.OwnerReferences[0].UID != tc.wc.UID {
						t.Errorf("Expected OwnerReference UID %q, got %q", tc.wc.UID, savedPDB.OwnerReferences[0].UID)
					}
				}

				if tc.nsDefault {
					if !reflect.DeepEqual(savedPDB.Spec.Selector, tc.wantSelector) {
						t.Errorf("PDBWithLease() Selector = %v, want %v", savedPDB.Spec.Selector, tc.wantSelector)
					}
				}
			}
		})
	}
}

func TestReconcilePDB(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = workloadsv1.AddToScheme(scheme)

	now := metav1.Now()
	oneHourAgo := metav1.NewTime(now.Add(-1 * time.Hour))

	testWC := makeWC("test-wc", now)
	otherWC := makeWC("some-other-wc", now)
	oldestWC := makeWC("oldest-wc", oneHourAgo)
	newerWC := makeWC("newer-wc", now)
	deletingWC := makeWC("deleting-wc", oneHourAgo)
	deletingWC.DeletionTimestamp = &now
	controllerutil.AddFinalizer(deletingWC, "stop")

	testCases := []struct {
		name               string
		namespace          *corev1.Namespace
		wc                 *workloadsv1.WorkloadClass
		wcsForClient       []*workloadsv1.WorkloadClass
		condition          metav1.Condition
		overlappingClasses []workloadsv1.WorkloadClass
		wantErr            bool
		wantPDBExists      bool // true if we expect createOrUpdatePDB to run, false for deletePDB
	}{
		{
			name:          "deletes_pdb_if_validation_fails",
			namespace:     &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
			wc:            testWC,
			wcsForClient:  []*workloadsv1.WorkloadClass{testWC},
			condition:     metav1.Condition{Reason: "ValidationError"}, // Anything other than ReasonValidationPassed
			wantPDBExists: false,                                       // Calls deletePDB
		},
		{
			name:         "errors_if_namespace_missing",
			namespace:    nil, // Namespace Get will fail
			wc:           testWC,
			wcsForClient: []*workloadsv1.WorkloadClass{testWC},
			condition:    metav1.Condition{Reason: workloadsv1.ReasonValidationPassed},
			wantErr:      true,
		},
		{
			name: "creates_pdb_if_this_wc_is_namespace_default",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "default",
					Labels: map[string]string{workloadsv1.DefaultClassLabel: "test-wc"},
				},
			},
			wc:            testWC,
			wcsForClient:  []*workloadsv1.WorkloadClass{testWC},
			condition:     metav1.Condition{Reason: workloadsv1.ReasonValidationPassed},
			wantPDBExists: true,
		},
		{
			name: "creates_pdb_if_the_namespace_default_does_not_exist",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "default",
					Labels: map[string]string{workloadsv1.DefaultClassLabel: "non-existent-wc"},
				},
			},
			wc:            testWC,
			wcsForClient:  []*workloadsv1.WorkloadClass{testWC},
			condition:     metav1.Condition{Reason: workloadsv1.ReasonValidationPassed},
			wantPDBExists: true,
		},
		{
			name: "creates_pdb_if_the_namespace_default_has_deletion_timestamp",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "default",
					Labels: map[string]string{workloadsv1.DefaultClassLabel: "deleting-wc"},
				},
			},
			wc:            testWC,
			wcsForClient:  []*workloadsv1.WorkloadClass{testWC, deletingWC},
			condition:     metav1.Condition{Reason: workloadsv1.ReasonValidationPassed},
			wantPDBExists: true,
		},
		{
			name: "deletes_pdb_if_another_wc_is_namespace_default",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "default",
					Labels: map[string]string{workloadsv1.DefaultClassLabel: "some-other-wc"},
				},
			},
			wc:            testWC,
			wcsForClient:  []*workloadsv1.WorkloadClass{testWC, otherWC},
			condition:     metav1.Condition{Reason: workloadsv1.ReasonValidationPassed},
			wantPDBExists: false,
		},
		{
			name:               "creates_pdb_if_no_default_and_no_overlaps",
			namespace:          &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
			wc:                 testWC,
			wcsForClient:       []*workloadsv1.WorkloadClass{testWC},
			condition:          metav1.Condition{Reason: workloadsv1.ReasonValidationPassed},
			overlappingClasses: nil,
			wantPDBExists:      true,
		},
		{
			name:         "creates_pdb_if_no_default_and_this_is_oldest_overlap",
			namespace:    &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
			wc:           oldestWC,
			wcsForClient: []*workloadsv1.WorkloadClass{oldestWC, newerWC},
			condition:    metav1.Condition{Reason: workloadsv1.ReasonValidationPassed},
			overlappingClasses: []workloadsv1.WorkloadClass{
				*newerWC,
			},
			wantPDBExists: true,
		},
		{
			name:         "deletes_pdb_if_no_default_and_this_is_not_oldest_overlap",
			namespace:    &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
			wc:           newerWC,
			wcsForClient: []*workloadsv1.WorkloadClass{oldestWC, newerWC},
			condition:    metav1.Condition{Reason: workloadsv1.ReasonValidationPassed},
			overlappingClasses: []workloadsv1.WorkloadClass{
				*oldestWC,
			},
			wantPDBExists: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clientBuilder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.namespace != nil {
				clientBuilder = clientBuilder.WithObjects(tc.namespace)
			}

			// Purposefully inject a dummy PDB beforehand.
			// If deletePDB is called, it will vanish. If createOrUpdatePDB is called, it will remain/be updated.
			expectedPDB := utils.PDBBase(tc.wc)
			dummyPDB := &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: expectedPDB.Name, Namespace: expectedPDB.Namespace},
			}
			clientBuilder = clientBuilder.WithObjects(dummyPDB)
			for _, o := range tc.wcsForClient {
				clientBuilder = clientBuilder.WithObjects(o)
			}
			fakeClient := clientBuilder.Build()
			reconciler := &WorkloadClassReconciler{
				Client: fakeClient,
				Scheme: scheme,
			}

			err := reconciler.reconcilePDB(context.Background(), tc.wc, tc.condition, tc.overlappingClasses)
			if (err != nil) != tc.wantErr {
				t.Errorf("reconcilePDB() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr {
				return
			}

			checkPDB := &policyv1.PodDisruptionBudget{}
			errGet := fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      expectedPDB.Name,
				Namespace: expectedPDB.Namespace,
			}, checkPDB)
			pdbExists := errGet == nil
			if !pdbExists && !errors.IsNotFound(errGet) {
				t.Fatalf("Unexpected error getting PDB: %v", errGet)
			}
			if pdbExists != tc.wantPDBExists {
				t.Errorf("Expected PDB existence: %v, but was: %v", tc.wantPDBExists, pdbExists)
			}
		})
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

func TestFindNonDefaultWorkloadClasses(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(policyv1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))

	testCases := []struct {
		name         string
		pdb          *policyv1.PodDisruptionBudget
		namespace    *corev1.Namespace
		existingWCs  []*workloadsv1.WorkloadClass
		wantRequests []reconcile.Request
	}{
		{
			name: "pdb_not_default_should_return_nil",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload-my-wc",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{Kind: "WorkloadClass", Name: "my-wc"},
					},
				},
			},
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "default",
					Labels: map[string]string{
						workloadsv1.DefaultClassLabel: "other-wc",
					},
				},
			},
			existingWCs: []*workloadsv1.WorkloadClass{
				{ObjectMeta: metav1.ObjectMeta{Name: "my-wc", Namespace: "default"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "other-wc", Namespace: "default"}},
			},
			wantRequests: nil,
		},
		{
			name: "pdb_is_default_should_return_other_classes",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload-default-wc",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{Kind: "WorkloadClass", Name: "default-wc"},
					},
				},
			},
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "default",
					Labels: map[string]string{
						workloadsv1.DefaultClassLabel: "default-wc",
					},
				},
			},
			existingWCs: []*workloadsv1.WorkloadClass{
				{ObjectMeta: metav1.ObjectMeta{Name: "default-wc", Namespace: "default"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "other-wc", Namespace: "default"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "another-wc", Namespace: "default"}},
			},
			wantRequests: []reconcile.Request{
				{NamespacedName: types.NamespacedName{Name: "other-wc", Namespace: "default"}},
				{NamespacedName: types.NamespacedName{Name: "another-wc", Namespace: "default"}},
			},
		},
		{
			name: "pdb_without_owner_reference_uses_fallback_name",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload-fallback-wc",
					Namespace: "default",
				},
			},
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "default",
					Labels: map[string]string{
						workloadsv1.DefaultClassLabel: "fallback-wc",
					},
				},
			},
			existingWCs: []*workloadsv1.WorkloadClass{
				{ObjectMeta: metav1.ObjectMeta{Name: "fallback-wc", Namespace: "default"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "other-wc", Namespace: "default"}},
			},
			wantRequests: []reconcile.Request{
				{NamespacedName: types.NamespacedName{Name: "other-wc", Namespace: "default"}},
			},
		},
		{
			name: "namespace_missing_returns_nil",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload-my-wc",
					Namespace: "missing-ns",
				},
			},
			namespace:    nil,
			wantRequests: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clientBuilder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.namespace != nil {
				clientBuilder = clientBuilder.WithObjects(tc.namespace)
			}
			for _, wc := range tc.existingWCs {
				clientBuilder = clientBuilder.WithObjects(wc)
			}

			fakeClient := clientBuilder.Build()
			reconciler := &WorkloadClassReconciler{
				Client: fakeClient,
				Scheme: scheme,
			}

			requests := reconciler.findNonDefaultWorkloadClasses(context.Background(), tc.pdb)

			if len(requests) != len(tc.wantRequests) {
				t.Errorf("expected %d requests, got %d", len(tc.wantRequests), len(requests))
			}

			// verify contents of requests
			for _, wantReq := range tc.wantRequests {
				if !slices.Contains(requests, wantReq) {
					t.Errorf("expected request %v not found in result %v", wantReq, requests)
				}
			}
		})
	}
}

func TestFindWorkloadClassesByNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(workloadsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	testCases := []struct {
		name         string
		namespace    *corev1.Namespace
		existingWCs  []*workloadsv1.WorkloadClass
		wantRequests []reconcile.Request
	}{
		{
			name: "namespace_with_workload_classes",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "default",
				},
			},
			existingWCs: []*workloadsv1.WorkloadClass{
				{ObjectMeta: metav1.ObjectMeta{Name: "my-wc-1", Namespace: "default"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "my-wc-2", Namespace: "default"}},
			},
			wantRequests: []reconcile.Request{
				{NamespacedName: types.NamespacedName{Name: "my-wc-1", Namespace: "default"}},
				{NamespacedName: types.NamespacedName{Name: "my-wc-2", Namespace: "default"}},
			},
		},
		{
			name: "namespace_with_no_workload_classes",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "empty-ns",
				},
			},
			existingWCs:  []*workloadsv1.WorkloadClass{},
			wantRequests: []reconcile.Request{},
		},
		{
			name: "skips_workload_classes_in_other_namespaces",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "target-ns",
				},
			},
			existingWCs: []*workloadsv1.WorkloadClass{
				{ObjectMeta: metav1.ObjectMeta{Name: "wc-1", Namespace: "target-ns"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "wc-2", Namespace: "other-ns"}},
			},
			wantRequests: []reconcile.Request{
				{NamespacedName: types.NamespacedName{Name: "wc-1", Namespace: "target-ns"}},
			},
		},
		{
			name:      "invalid_object_type_returns_nil",
			namespace: nil, // Simulates passing an object that isn't a Namespace
			existingWCs: []*workloadsv1.WorkloadClass{
				{ObjectMeta: metav1.ObjectMeta{Name: "wc-1", Namespace: "default"}},
			},
			wantRequests: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clientBuilder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.namespace != nil {
				clientBuilder = clientBuilder.WithObjects(tc.namespace)
			}
			for _, wc := range tc.existingWCs {
				clientBuilder = clientBuilder.WithObjects(wc)
			}

			fakeClient := clientBuilder.Build()
			reconciler := &WorkloadClassReconciler{
				Client: fakeClient,
				Scheme: scheme,
			}

			var obj client.Object
			if tc.namespace != nil {
				obj = tc.namespace
			} else {
				// Pass an invalid object to test the type assertion failure
				obj = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "not-a-namespace"}}
			}

			requests := reconciler.findWorkloadClassesByNamespace(context.Background(), obj)

			if len(requests) != len(tc.wantRequests) {
				t.Errorf("expected %d requests, got %d", len(tc.wantRequests), len(requests))
			}

			for _, wantReq := range tc.wantRequests {
				if !slices.Contains(requests, wantReq) {
					t.Errorf("expected request %v not found in result %v", wantReq, requests)
				}
			}
		})
	}
}
