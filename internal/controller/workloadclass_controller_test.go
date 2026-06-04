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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
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
			}, "30s", "1s").Should(BeTrue())
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
