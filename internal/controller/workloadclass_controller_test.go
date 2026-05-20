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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

var _ = Describe("WorkloadClass Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		workloadclass := &workloadsv1.WorkloadClass{}
		typeNamespacedNamePod := types.NamespacedName{
			Name:      "silly-goose-pod",
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource for the Kind WorkloadClass")
			err := k8sClient.Get(ctx, typeNamespacedName, workloadclass)
			if err != nil && errors.IsNotFound(err) {
				resource := &workloadsv1.WorkloadClass{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: workloadsv1.WorkloadClassSpec{
						DisruptionPolicy: workloadsv1.DisruptionPolicy{
							MaxNonDisruptionDurationDays: 1,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &workloadsv1.WorkloadClass{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance WorkloadClass")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			pod := &corev1.Pod{}
			err = k8sClient.Get(ctx, typeNamespacedNamePod, pod)
			if err != nil && strings.Contains(err.Error(), "not found") {
				// Not all test cases make a pod
				return
			}
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup pod")
			Expect(k8sClient.Delete(ctx, pod)).To(Succeed())

		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &WorkloadClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		})
		// Validate against guardrails
		It("should fail validation if number of DisruptionWindows less than or equal to MaxAllowedWindows", func() {
			By("creating a guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-guardrail",
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxAllowedWindows:            int32(2),
							AllowedDisruptionDays:        []string{"Monday", "Tuesday"},
							MaxNonDisruptionDurationDays: 1,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, guardrail)
			}()

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
			controllerReconciler := &WorkloadClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
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
			By("creating a guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-guardrail",
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxAllowedWindows:            int32(2),
							AllowedDisruptionDays:        []string{"Monday", "Tuesday"},
							MaxNonDisruptionDurationDays: 1,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, guardrail)
			}()
			By("updating WorkloadClass with DisruptionWindow outside of AllowedDisruptionDays")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "M", DaysOfWeek: []string{"Friday"}, StartTime: "10:00", EndTime: "12:00"},
			}
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
			By("creating a guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-guardrail",
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxAllowedWindows:            int32(2),
							AllowedDisruptionDays:        []string{"Monday", "Tuesday"},
							MaxNonDisruptionDurationDays: 1,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, guardrail)
			}()
			By("updating WorkloadClass with DisruptionWindow outside of AllowedDisruptionDays")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "MT", DaysOfWeek: []string{"Monday", "Tuesday"}, TimeZone: "invalid/timezone", StartTime: "10:00", EndTime: "12:00"},
			}
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
			By("creating a guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-guardrail",
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxAllowedWindows:            int32(2),
							AllowedDisruptionDays:        []string{"Monday", "Tuesday"},
							MaxNonDisruptionDurationDays: 3,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, guardrail)
			}()
			By("updating WorkloadClass with MaxNonDisruptionDurationDays exceeding limit")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "MT", DaysOfWeek: []string{"Monday", "Tuesday"}, TimeZone: "America/Los_Angeles", StartTime: "10:00", EndTime: "12:00"},
			}
			wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = guardrail.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays + 1
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
			By("creating a guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-guardrail",
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							AllowedDisruptionDays:            []string{"Sunday", "Monday", "Tuesday"},
							MaxAllowedWindows:                int32(2),
							MaxNonDisruptionDurationDays:     3,
							EnforcedDisruptionTimeoutSeconds: 20,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, guardrail)
			}()
			By("updating WorkloadClass with EmergencyOverride")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "Emergency", DaysOfWeek: []string{"Wednesday"}, TimeZone: "America/Los_Angeles", StartTime: "10:00", EndTime: "12:00"},
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
			By("creating a guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-guardrail",
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxAllowedWindows:                int32(2),
							MaxNonDisruptionDurationDays:     3,
							EnforcedDisruptionTimeoutSeconds: 20,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, guardrail)
			}()
			By("updating WorkloadClass with old last disruption")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 2
			wc.Status.LastDisruptionTime = &metav1.Time{
				Time: time.Now().AddDate(0, 0, -4),
			}
			Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())

			By("Reconciling")
			controllerReconciler := &WorkloadClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
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
			By("creating a guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-guardrail",
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxAllowedWindows:                int32(2),
							MaxNonDisruptionDurationDays:     3,
							EnforcedDisruptionTimeoutSeconds: 20,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, guardrail)
			}()
			By("updating WorkloadClass with disruption window that is not today")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			notToday := time.Now().AddDate(0, 0, 2).Weekday().String()
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "NotToday", DaysOfWeek: []string{notToday}, TimeZone: "America/Los_Angeles", StartTime: "10:00", EndTime: "12:00"},
			}
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
			By("creating a guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-guardrail",
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxAllowedWindows:                int32(2),
							MaxNonDisruptionDurationDays:     3,
							EnforcedDisruptionTimeoutSeconds: 20,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, guardrail)
			}()
			By("creating a pod")
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "silly-goose-pod",
					Namespace: "default",
					Labels: map[string]string{ // Match the WC PodSelector
						"duck-duck": "goose",
					},
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
			By("updating WorkloadClass with min initial run duration days and pod selector")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			today := time.Now().Weekday().String()
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "Today", DaysOfWeek: []string{today}, TimeZone: "America/Los_Angeles", StartTime: "00:00", EndTime: "23:59"},
			}
			wc.Spec.DisruptionPolicy.MinInitialRunDurationDays = 4
			wc.Spec.PodSelector = &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"duck-duck": "goose",
				},
			}
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
			By("creating a guardrail")
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-guardrail",
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxAllowedWindows:                int32(2),
							MaxNonDisruptionDurationDays:     3,
							EnforcedDisruptionTimeoutSeconds: 20,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, guardrail)
			}()
			By("creating a pod")
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "silly-goose-pod",
					Namespace: "default",
					Labels: map[string]string{ // Match the WC PodSelector
						"duck-duck": "goose",
					},
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
			By("updating WorkloadClass with disruption window today")
			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			today := time.Now().Weekday().String()
			wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1.DisruptionWindow{
				{Name: "Today", DaysOfWeek: []string{today}, TimeZone: "America/Los_Angeles", StartTime: "00:00", EndTime: "23:59"},
			}
			wc.Spec.PodSelector = &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"duck-duck": "goose",
				},
			}
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

			By("Checking Status for readiness")
			updatedWC := &workloadsv1.WorkloadClass{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, updatedWC)
				if err != nil {
					return false
				}
				return updatedWC.Status.MaintenanceReadiness == workloadsv1.ReadinessReady
			}, "10s", "1s").Should(BeTrue())
		})
	})
})
