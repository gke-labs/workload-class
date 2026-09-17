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

package v1

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

var _ = Describe("WorkloadClass Webhook", func() {
	var (
		obj       *workloadsv1.WorkloadClass
		oldObj    *workloadsv1.WorkloadClass
		validator WorkloadClassCustomValidator
	)

	BeforeEach(func() {
		obj = &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-wc",
				Namespace: "default",
			},
		}
		oldObj = &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-wc",
				Namespace: "default",
			},
		}
		validator = WorkloadClassCustomValidator{
			Client: k8sClient,
		}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	Context("When creating or updating WorkloadClass under Validating Webhook", func() {
		It("Should admit WorkloadClass with default/empty PlacementPolicy", func() {
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should admit WorkloadClass with full valid Spot placement policy", func() {
			obj.Spec.PlacementPolicy = workloadsv1.PlacementPolicy{
				SpotPlacement: workloadsv1.SpotPlacementPolicy{
					Type:      workloadsv1.SpotPlacementTypeSpot,
					SpotRatio: "80%",
					Fallback: workloadsv1.SpotFallbackPolicy{
						Action:                 workloadsv1.FallbackActionFallbackToOnDemand,
						AllowedMachineFamilies: []string{"n2d", "t2d"},
					},
					Reversion: workloadsv1.SpotReversionPolicy{
						Action:                workloadsv1.ReversionActionActive,
						MinDurationOnFallback: &metav1.Duration{Duration: 4 * time.Hour},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should admit WorkloadClass with OnDemand placement type", func() {
			obj.Spec.PlacementPolicy = workloadsv1.PlacementPolicy{
				SpotPlacement: workloadsv1.SpotPlacementPolicy{
					Type: workloadsv1.SpotPlacementTypeOnDemand,
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should deny invalid spotPlacement type", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Type = workloadsv1.SpotPlacementType("Hybrid")
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid spotPlacement type"))
		})

		It("Should deny invalid spotRatio format", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.SpotRatio = "120%"
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid spotRatio"))
		})

		It("Should deny spotRatio of 0% when type is Spot", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Type = workloadsv1.SpotPlacementTypeSpot
			obj.Spec.PlacementPolicy.SpotPlacement.SpotRatio = "0%"
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spotRatio cannot be 0% when spotPlacement type is Spot"))
		})

		It("Should deny non-zero/non-default spotRatio when type is OnDemand", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Type = workloadsv1.SpotPlacementTypeOnDemand
			obj.Spec.PlacementPolicy.SpotPlacement.SpotRatio = "50%"
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spotRatio cannot be set to \"50%\" when spotPlacement type is OnDemand"))
		})

		It("Should deny invalid fallback action", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Fallback.Action = workloadsv1.FallbackAction("Retry")
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid fallback action"))
		})

		It("Should deny fallback configuration when type is OnDemand", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Type = workloadsv1.SpotPlacementTypeOnDemand
			obj.Spec.PlacementPolicy.SpotPlacement.Fallback.Action = workloadsv1.FallbackActionFallbackToOnDemand
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("fallback cannot be configured when spotPlacement type is OnDemand"))
		})

		It("Should deny allowedMachineFamilies when fallback action is Fail", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Fallback = workloadsv1.SpotFallbackPolicy{
				Action:                 workloadsv1.FallbackActionFail,
				AllowedMachineFamilies: []string{"n2d"},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("allowedMachineFamilies cannot be set when fallback action is Fail"))
		})

		It("Should deny empty strings in allowedMachineFamilies", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Fallback = workloadsv1.SpotFallbackPolicy{
				Action:                 workloadsv1.FallbackActionFallbackToOnDemand,
				AllowedMachineFamilies: []string{"n2d", "  "},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("allowedMachineFamilies cannot contain empty machine family names"))
		})

		It("Should deny invalid reversion action", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Fallback.Action = workloadsv1.FallbackActionFallbackToOnDemand
			obj.Spec.PlacementPolicy.SpotPlacement.Reversion.Action = workloadsv1.ReversionAction("Immediate")
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid reversion action"))
		})

		It("Should deny negative minDurationOnFallback", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Fallback.Action = workloadsv1.FallbackActionFallbackToOnDemand
			obj.Spec.PlacementPolicy.SpotPlacement.Reversion = workloadsv1.SpotReversionPolicy{
				Action:                workloadsv1.ReversionActionActive,
				MinDurationOnFallback: &metav1.Duration{Duration: -1 * time.Hour},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("minDurationOnFallback must be non-negative"))
		})

		It("Should deny reversion configuration when type is OnDemand", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Type = workloadsv1.SpotPlacementTypeOnDemand
			obj.Spec.PlacementPolicy.SpotPlacement.Reversion.Action = workloadsv1.ReversionActionLazy
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reversion cannot be configured when spotPlacement type is OnDemand"))
		})

		It("Should deny reversion configuration when fallback action is Fail", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Fallback.Action = workloadsv1.FallbackActionFail
			obj.Spec.PlacementPolicy.SpotPlacement.Reversion.Action = workloadsv1.ReversionActionActive
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reversion cannot be configured when fallback action is Fail"))
		})

		It("Should deny minDurationOnFallback when reversion action is not Active", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Fallback.Action = workloadsv1.FallbackActionFallbackToOnDemand
			obj.Spec.PlacementPolicy.SpotPlacement.Reversion = workloadsv1.SpotReversionPolicy{
				Action:                workloadsv1.ReversionActionLazy,
				MinDurationOnFallback: &metav1.Duration{Duration: 2 * time.Hour},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("minDurationOnFallback can only be set when reversion action is Active"))
		})

		It("Should validate updates via ValidateUpdate and allow deletes via ValidateDelete", func() {
			obj.Spec.PlacementPolicy.SpotPlacement.Type = workloadsv1.SpotPlacementTypeSpot
			obj.Spec.PlacementPolicy.SpotPlacement.SpotRatio = "80%"
			warnings, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())

			obj.Spec.PlacementPolicy.SpotPlacement.SpotRatio = "0%"
			_, err = validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spotRatio cannot be 0% when spotPlacement type is Spot"))

			warnings, err = validator.ValidateDelete(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})
	})
})
