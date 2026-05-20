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

		BeforeEach(func() {
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
			By("updating WorkloadClass with MaxNonDisruptionDurationDays exceeding limit")
			guardrail := &workloadsv1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, typeNamespacedNameGuardrail, guardrail)).To(Succeed())

			wc := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())

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
