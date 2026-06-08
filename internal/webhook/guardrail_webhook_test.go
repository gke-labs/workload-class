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
package webhook

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestGuardrailWebhook_Handle(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = workloadsv1.AddToScheme(scheme)
	decoder := admission.NewDecoder(scheme)
	testCases := []struct {
		name         string
		reqObj       []byte
		listWCErr    error
		listWCResult *workloadsv1.WorkloadClassList
		wantAllowed  bool
		wantCode     int32
	}{
		{
			name:        "decode_error",
			reqObj:      []byte(`invalid-json`),
			wantAllowed: false,
			wantCode:    http.StatusBadRequest,
		},
		{
			name:         "list_error",
			reqObj:       []byte(`{"apiVersion":"workloads.gke.io/v1","kind":"WorkloadClassGuardrail","metadata":{"name":"test-guardrail"}}`),
			listWCResult: &workloadsv1.WorkloadClassList{},
			listWCErr:    fmt.Errorf("failed to list WCs"),
			wantAllowed:  false,
			wantCode:     http.StatusInternalServerError,
		},
		{
			name:         "valid_guardrail_no_wcs",
			reqObj:       []byte(`{"apiVersion":"workloads.gke.io/v1","kind":"WorkloadClassGuardrail","metadata":{"name":"test-guardrail"}}`),
			listWCResult: &workloadsv1.WorkloadClassList{},
			wantAllowed:  true,
			wantCode:     http.StatusOK,
		},
		{
			name:   "valid_guardrail_with_wc",
			reqObj: []byte(`{"apiVersion":"workloads.gke.io/v1","kind":"WorkloadClassGuardrail","metadata":{"name":"test-guardrail"}}`),
			listWCResult: &workloadsv1.WorkloadClassList{
				Items: []workloadsv1.WorkloadClass{
					{ObjectMeta: metav1.ObjectMeta{Name: "wc-1"}},
				},
			},
			wantAllowed: true,
			wantCode:    http.StatusOK,
		},
		{
			name:   "violating_guardrail_rejected",
			reqObj: []byte(`{"apiVersion":"workloads.gke.io/v1","kind":"WorkloadClassGuardrail","metadata":{"name":"strict-guardrail"},"spec":{"constraints":{"disruption":{"maxNonDisruptionDurationDays":10}}}}`),
			listWCResult: &workloadsv1.WorkloadClassList{
				Items: []workloadsv1.WorkloadClass{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "lax-workloadclass"},
						Spec: workloadsv1.WorkloadClassSpec{
							DisruptionPolicy: workloadsv1.DisruptionPolicy{
								MaxNonDisruptionDurationDays: 30, // Violates the guardrail limit of 10
							},
						},
					},
				},
			},
			wantAllowed: false,
			wantCode:    http.StatusForbidden,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := createClient(nil, nil, tc.listWCErr, nil, nil, nil, tc.listWCResult, nil)
			v := &GuardrailWebhook{
				Client: client,
			}
			_ = v.InjectDecoder(&decoder)
			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Object: runtime.RawExtension{
						Raw: tc.reqObj,
					},
				},
			}
			resp := v.Handle(context.Background(), req)
			if resp.Allowed != tc.wantAllowed {
				t.Errorf("expected allowed %v, got %v (result: %v)", tc.wantAllowed, resp.Allowed, resp.Result)
			}
			if resp.Result != nil && resp.Result.Code != tc.wantCode {
				t.Errorf("expected code %d, got %d", tc.wantCode, resp.Result.Code)
			}
		})
	}
}
