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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

var _ = Describe("GKESpotPlacementPolicy Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			policyName    = "test-spot-policy"
			wcName        = "test-wc-spot-ref"
			wcNamespace   = "default"
			guardrailName = "test-spot-guardrail"
		)

		ctx := context.Background()
		policyKey := types.NamespacedName{Name: policyName}
		wcKey := types.NamespacedName{Name: wcName, Namespace: wcNamespace}
		guardrailKey := types.NamespacedName{Name: guardrailName}

		var reconciler *GKESpotPlacementPolicyReconciler

		BeforeEach(func() {
			reconciler = &GKESpotPlacementPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			wc := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      wcName,
					Namespace: wcNamespace,
				},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "spot-test"},
					},
					DisruptionPolicy: workloadsv1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 5,
					},
					InfrastructureProfileRef: &workloadsv1.InfrastructureProfileReference{
						Group: workloadsv1.GroupVersion.Group,
						Kind:  workloadsv1.GKESpotPlacementPolicyKind,
						Name:  policyName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
		})

		AfterEach(func() {
			wc := &workloadsv1.WorkloadClass{}
			if err := k8sClient.Get(ctx, wcKey, wc); err == nil {
				Expect(k8sClient.Delete(ctx, wc)).To(Succeed())
			}

			policy := &workloadsv1.GKESpotPlacementPolicy{}
			if err := k8sClient.Get(ctx, policyKey, policy); err == nil {
				Expect(k8sClient.Delete(ctx, policy)).To(Succeed())
			}

			guardrail := &workloadsv1.WorkloadClassGuardrail{}
			if err := k8sClient.Get(ctx, guardrailKey, guardrail); err == nil {
				Expect(k8sClient.Delete(ctx, guardrail)).To(Succeed())
			}
		})

		It("should set PlacementPluginAttached=False on referencing WorkloadClasses when policy is not found", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyKey})
			Expect(err).NotTo(HaveOccurred())

			updatedWC := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, wcKey, updatedWC)).To(Succeed())

			cond := meta.FindStatusCondition(updatedWC.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1.ReasonPluginResolutionFailed))
			Expect(cond.Message).To(Equal("GKESpotPlacementPolicy 'test-spot-policy' not found"))
			Expect(cond.ObservedGeneration).To(Equal(updatedWC.Generation))
		})

		It("should validate policy with NoGuardrails and attach to referencing WorkloadClasses when no guardrails exist", func() {
			policy := &workloadsv1.GKESpotPlacementPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name: policyName,
				},
				Spec: workloadsv1.GKESpotPlacementPolicySpec{
					Reversion: workloadsv1.SpotReversionPolicy{
						Action: workloadsv1.ActiveReversionAction,
					},
				},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyKey})
			Expect(err).NotTo(HaveOccurred())

			updatedPolicy := &workloadsv1.GKESpotPlacementPolicy{}
			Expect(k8sClient.Get(ctx, policyKey, updatedPolicy)).To(Succeed())
			policyCond := meta.FindStatusCondition(updatedPolicy.Status.Conditions, workloadsv1.ConditionTypeValidated)
			Expect(policyCond).NotTo(BeNil())
			Expect(policyCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(policyCond.Reason).To(Equal(workloadsv1.ReasonNoGuardrails))

			updatedWC := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, wcKey, updatedWC)).To(Succeed())
			wcCond := meta.FindStatusCondition(updatedWC.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached)
			Expect(wcCond).NotTo(BeNil())
			Expect(wcCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(wcCond.Reason).To(Equal(workloadsv1.ReasonProfileResolved))
			Expect(wcCond.Message).To(Equal("GKESpotPlacementPolicy 'test-spot-policy' attached"))
		})

		It("should pass validation and attach when policy satisfies guardrail pluginConstraints", func() {
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: guardrailName,
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxNonDisruptionDurationDays: 7,
						},
					},
					PluginConstraints: &workloadsv1.PluginConstraints{
						Placement: []workloadsv1.PluginConstraint{
							{
								PluginName: workloadsv1.PluginNameGKESpotPlacement,
								Parameters: &apiextensionsv1.JSON{
									Raw: []byte(`{"enforcementMode":"Required","reversion":{"requiredReversionAction":"Active","maxFallbackDuration":"2h"}}`),
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())

			policy := &workloadsv1.GKESpotPlacementPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name: policyName,
				},
				Spec: workloadsv1.GKESpotPlacementPolicySpec{
					Reversion: workloadsv1.SpotReversionPolicy{
						Action:                workloadsv1.ActiveReversionAction,
						MinDurationOnFallback: &metav1.Duration{Duration: 1 * time.Hour},
					},
				},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyKey})
			Expect(err).NotTo(HaveOccurred())

			updatedPolicy := &workloadsv1.GKESpotPlacementPolicy{}
			Expect(k8sClient.Get(ctx, policyKey, updatedPolicy)).To(Succeed())
			policyCond := meta.FindStatusCondition(updatedPolicy.Status.Conditions, workloadsv1.ConditionTypeValidated)
			Expect(policyCond).NotTo(BeNil())
			Expect(policyCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(policyCond.Reason).To(Equal(workloadsv1.ReasonValidationPassed))

			updatedWC := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, wcKey, updatedWC)).To(Succeed())
			wcCond := meta.FindStatusCondition(updatedWC.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached)
			Expect(wcCond).NotTo(BeNil())
			Expect(wcCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(wcCond.Reason).To(Equal(workloadsv1.ReasonProfileResolved))
		})

		It("should fail validation and mark referencing WorkloadClasses as PluginResolutionFailed when policy violates guardrail pluginConstraints", func() {
			guardrail := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{
					Name: guardrailName,
				},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxNonDisruptionDurationDays: 7,
						},
					},
					PluginConstraints: &workloadsv1.PluginConstraints{
						Placement: []workloadsv1.PluginConstraint{
							{
								PluginName: workloadsv1.PluginNameGKESpotPlacement,
								Parameters: &apiextensionsv1.JSON{
									Raw: []byte(`{"reversion":{"requiredReversionAction":"Active","maxFallbackDuration":"1h"}}`),
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())

			policy := &workloadsv1.GKESpotPlacementPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name: policyName,
				},
				Spec: workloadsv1.GKESpotPlacementPolicySpec{
					Reversion: workloadsv1.SpotReversionPolicy{
						Action: workloadsv1.LazyReversionAction,
					},
				},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyKey})
			Expect(err).NotTo(HaveOccurred())

			updatedPolicy := &workloadsv1.GKESpotPlacementPolicy{}
			Expect(k8sClient.Get(ctx, policyKey, updatedPolicy)).To(Succeed())
			policyCond := meta.FindStatusCondition(updatedPolicy.Status.Conditions, workloadsv1.ConditionTypeValidated)
			Expect(policyCond).NotTo(BeNil())
			Expect(policyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(policyCond.Reason).To(Equal(workloadsv1.ReasonValidationFailed))
			Expect(policyCond.Message).To(ContainSubstring("reversion action Lazy does not match guardrail requiredReversionAction Active"))

			updatedWC := &workloadsv1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, wcKey, updatedWC)).To(Succeed())
			wcCond := meta.FindStatusCondition(updatedWC.Status.Conditions, workloadsv1.ConditionTypePlacementPluginAttached)
			Expect(wcCond).NotTo(BeNil())
			Expect(wcCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(wcCond.Reason).To(Equal(workloadsv1.ReasonPluginResolutionFailed))
			Expect(wcCond.Message).To(ContainSubstring("GKESpotPlacementPolicy 'test-spot-policy' is invalid"))
		})
	})
})

func TestValidateSpotPolicyAgainstGuardrails(t *testing.T) {
	dur1h := metav1.Duration{Duration: 1 * time.Hour}
	dur2h := metav1.Duration{Duration: 2 * time.Hour}

	tests := []struct {
		name           string
		policySpec     workloadsv1.GKESpotPlacementPolicySpec
		guardrailJSONs []string
		wantViolations []string
	}{
		{
			name:           "empty_policy_against_guardrail_with_no_pluginConstraints_passes",
			policySpec:     workloadsv1.GKESpotPlacementPolicySpec{},
			guardrailJSONs: nil,
			wantViolations: nil,
		},
		{
			name:       "Forbidden_enforcementMode_fails",
			policySpec: workloadsv1.GKESpotPlacementPolicySpec{},
			guardrailJSONs: []string{
				`{"enforcementMode":"Forbidden"}`,
			},
			wantViolations: []string{
				"GKESpotPlacementPolicy is not allowed when guardrail enforcementMode is Forbidden",
			},
		},
		{
			name: "RequiredReversionAction_passes_when_matching",
			policySpec: workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{
					Action: workloadsv1.ActiveReversionAction,
				},
			},
			guardrailJSONs: []string{
				`{"reversion":{"requiredReversionAction":"Active"}}`,
			},
			wantViolations: nil,
		},
		{
			name: "RequiredReversionAction_fails_when_mismatched",
			policySpec: workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{
					Action: workloadsv1.LazyReversionAction,
				},
			},
			guardrailJSONs: []string{
				`{"reversion":{"requiredReversionAction":"Active"}}`,
			},
			wantViolations: []string{
				"reversion action Lazy does not match guardrail requiredReversionAction Active",
			},
		},
		{
			name: "MaxFallbackDuration_fails_when_reversion_action_is_None_or_default_empty",
			policySpec: workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{
					Action: workloadsv1.NoneReversionAction,
				},
			},
			guardrailJSONs: []string{
				`{"reversion":{"maxFallbackDuration":"2h"}}`,
			},
			wantViolations: []string{
				"reversion action cannot be None when guardrail specifies maxFallbackDuration",
			},
		},
		{
			name: "MaxFallbackDuration_fails_when_minDurationOnFallback_exceeds_it",
			policySpec: workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{
					Action:                workloadsv1.ActiveReversionAction,
					MinDurationOnFallback: &dur2h,
				},
			},
			guardrailJSONs: []string{
				`{"reversion":{"maxFallbackDuration":"1h"}}`,
			},
			wantViolations: []string{
				"minDurationOnFallback 2h0m0s exceeds guardrail maxFallbackDuration 1h0m0s",
			},
		},
		{
			name: "MaxFallbackDuration_passes_when_minDurationOnFallback_is_within_limit",
			policySpec: workloadsv1.GKESpotPlacementPolicySpec{
				Reversion: workloadsv1.SpotReversionPolicy{
					Action:                workloadsv1.ActiveReversionAction,
					MinDurationOnFallback: &dur1h,
				},
			},
			guardrailJSONs: []string{
				`{"reversion":{"maxFallbackDuration":"2h"}}`,
			},
			wantViolations: nil,
		},
		{
			name:       "multiple_guardrails_deduplicate_identical_violations_and_combine_distinct_ones",
			policySpec: workloadsv1.GKESpotPlacementPolicySpec{},
			guardrailJSONs: []string{
				`{"enforcementMode":"Forbidden"}`,
				`{"enforcementMode":"Forbidden","reversion":{"requiredReversionAction":"Active"}}`,
			},
			wantViolations: []string{
				"GKESpotPlacementPolicy is not allowed when guardrail enforcementMode is Forbidden",
				"reversion action None does not match guardrail requiredReversionAction Active",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policy := &workloadsv1.GKESpotPlacementPolicy{
				Spec: tc.policySpec,
			}
			guardrails := make([]workloadsv1.WorkloadClassGuardrail, 0, len(tc.guardrailJSONs))
			for _, raw := range tc.guardrailJSONs {
				guardrails = append(guardrails, workloadsv1.WorkloadClassGuardrail{
					Spec: workloadsv1.WorkloadClassGuardrailSpec{
						PluginConstraints: &workloadsv1.PluginConstraints{
							Placement: []workloadsv1.PluginConstraint{
								{
									PluginName: workloadsv1.PluginNameGKESpotPlacement,
									Parameters: &apiextensionsv1.JSON{Raw: []byte(raw)},
								},
							},
						},
					},
				})
			}

			got := validateSpotPolicyAgainstGuardrails(policy, guardrails)
			if len(got) != len(tc.wantViolations) {
				t.Fatalf("got %d violations (%v), want %d (%v)", len(got), got, len(tc.wantViolations), tc.wantViolations)
			}
			for i := range got {
				if got[i] != tc.wantViolations[i] {
					t.Errorf("violation[%d] = %q, want %q", i, got[i], tc.wantViolations[i])
				}
			}
		})
	}
}
