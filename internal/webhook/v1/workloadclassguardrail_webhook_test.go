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
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func boolPtr(b bool) *bool {
	return &b
}

var _ = Describe("WorkloadClassGuardrail Webhook", func() {
	var (
		obj       *workloadsv1.WorkloadClassGuardrail
		validator WorkloadClassGuardrailCustomValidator
	)

	BeforeEach(func() {
		obj = &workloadsv1.WorkloadClassGuardrail{}
		validator = WorkloadClassGuardrailCustomValidator{
			Client: k8sClient,
		}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	Context("When creating or updating WorkloadClassGuardrail under Validating Webhook", func() {
		It("Should admit valid guardrail when no WorkloadClasses exist", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "test-guardrail"},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should admit valid guardrail when compliant WorkloadClass exists", func() {
			wc := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "wc-1",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, wc) }()

			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "test-guardrail"},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should deny creation if a WorkloadClass violates the guardrail", func() {
			wc := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lax-workloadclass",
					Namespace: "default",
				},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 30, // Violates the guardrail limit of 10
					},
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, wc) }()

			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "strict-guardrail"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxNonDisruptionDurationDays: 10,
						},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("guardrail is too restrictive"))
			Expect(warnings).NotTo(BeEmpty())
		})

		It("Should admit valid SpotPlacement configuration", func() {
			ratio := intstr.FromString("30%")
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "spot-guardrail"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								EnforcementMode: workloadsv1.EnforcementRequired,
								MinSpotRatio:    "50%",
								Fallback: workloadsv1.Fallback{
									AllowFallbackToOnDemand: boolPtr(true),
									MaxFallbackRatio:        &ratio,
								},
								Reversion: &workloadsv1.Reversion{
									RequiredReversionAction: workloadsv1.ReversionActionActive,
									MaxFallbackDuration:     &metav1.Duration{Duration: 4 * time.Hour},
								},
							},
						},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should deny SpotPlacement when EnforcementMode is Forbidden and MinSpotRatio is set", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "forbidden-spot-ratio"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								EnforcementMode: workloadsv1.EnforcementForbidden,
								MinSpotRatio:    "50%",
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("minSpotRatio cannot be set when enforcementMode is Forbidden"))
		})

		It("Should deny SpotPlacement when AllowFallbackToOnDemand is false and Reversion is configured", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "no-fallback-with-reversion"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								Fallback: workloadsv1.Fallback{
									AllowFallbackToOnDemand: boolPtr(false),
								},
								Reversion: &workloadsv1.Reversion{
									RequiredReversionAction: workloadsv1.ReversionActionActive,
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reversion cannot be configured when allowFallbackToOnDemand is false"))
		})

		It("Should deny Reversion when RequiredReversionAction is None and MaxFallbackDuration is set", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "none-reversion-with-duration"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								Reversion: &workloadsv1.Reversion{
									RequiredReversionAction: workloadsv1.ReversionActionNone,
									MaxFallbackDuration:     &metav1.Duration{Duration: 2 * time.Hour},
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxFallbackDuration cannot be set when requiredReversionAction is None"))
		})

		It("Should admit valid SpotPlacement with integer MaxFallbackRatio and Lazy reversion", func() {
			ratio := intstr.FromInt32(5)
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "spot-int-fallback"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								EnforcementMode: workloadsv1.EnforcementAllowed,
								MinSpotRatio:    "0%",
								Fallback: workloadsv1.Fallback{
									AllowFallbackToOnDemand: boolPtr(true),
									MaxFallbackRatio:        &ratio,
								},
								Reversion: &workloadsv1.Reversion{
									RequiredReversionAction: workloadsv1.ReversionActionLazy,
								},
							},
						},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should deny invalid EnforcementMode", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid-enforcement"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								EnforcementMode: workloadsv1.Enforcement("Strict"),
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid enforcementMode"))
		})

		It("Should deny invalid MinSpotRatio format", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid-min-spot-ratio"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								MinSpotRatio: "150%",
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid minSpotRatio"))
		})

		It("Should deny MinSpotRatio of 0% when EnforcementMode is Required", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "required-zero-spot"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								EnforcementMode: workloadsv1.EnforcementRequired,
								MinSpotRatio:    "0%",
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("minSpotRatio cannot be 0% when enforcementMode is Required"))
		})

		It("Should deny when EnforcementMode is Forbidden and AllowFallbackToOnDemand is false", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "forbidden-no-fallback"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								EnforcementMode: workloadsv1.EnforcementForbidden,
								Fallback: workloadsv1.Fallback{
									AllowFallbackToOnDemand: boolPtr(false),
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("allowFallbackToOnDemand cannot be false when enforcementMode is Forbidden"))
		})

		It("Should deny when EnforcementMode is Forbidden and MaxFallbackRatio is set", func() {
			ratio := intstr.FromString("20%")
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "forbidden-max-fallback"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								EnforcementMode: workloadsv1.EnforcementForbidden,
								Fallback: workloadsv1.Fallback{
									MaxFallbackRatio: &ratio,
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxFallbackRatio cannot be set when enforcementMode is Forbidden"))
		})

		It("Should deny when EnforcementMode is Forbidden and Reversion is configured", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "forbidden-with-reversion"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								EnforcementMode: workloadsv1.EnforcementForbidden,
								Reversion: &workloadsv1.Reversion{
									RequiredReversionAction: workloadsv1.ReversionActionActive,
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reversion cannot be configured when enforcementMode is Forbidden"))
		})

		It("Should deny when AllowFallbackToOnDemand is false and MaxFallbackRatio is set", func() {
			ratio := intstr.FromInt32(3)
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "no-fallback-with-max-ratio"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								Fallback: workloadsv1.Fallback{
									AllowFallbackToOnDemand: boolPtr(false),
									MaxFallbackRatio:        &ratio,
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxFallbackRatio cannot be set when allowFallbackToOnDemand is false"))
		})

		It("Should deny negative integer MaxFallbackRatio", func() {
			ratio := intstr.FromInt32(-5)
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "neg-int-fallback"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								Fallback: workloadsv1.Fallback{
									MaxFallbackRatio: &ratio,
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxFallbackRatio integer value must be non-negative"))
		})

		It("Should deny invalid string MaxFallbackRatio", func() {
			ratio := intstr.FromString("120%")
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-str-fallback"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								Fallback: workloadsv1.Fallback{
									MaxFallbackRatio: &ratio,
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must be a percentage between 0% and 100%"))
		})

		It("Should deny invalid RequiredReversionAction", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid-reversion-action"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								Reversion: &workloadsv1.Reversion{
									RequiredReversionAction: workloadsv1.ReversionAction("Immediate"),
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid requiredReversionAction"))
		})

		It("Should deny non-positive MaxFallbackDuration", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "zero-fallback-duration"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Placement: workloadsv1.Placement{
							SpotPlacement: workloadsv1.SpotPlacement{
								Reversion: &workloadsv1.Reversion{
									MaxFallbackDuration: &metav1.Duration{Duration: 0},
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxFallbackDuration must be greater than 0"))
		})
	})
})
