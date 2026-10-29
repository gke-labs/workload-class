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

package utils

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSelectorsOverlap(t *testing.T) {
	labels := func(kv ...string) *metav1.LabelSelector {
		m := map[string]string{}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return &metav1.LabelSelector{MatchLabels: m}
	}
	exprs := func(reqs ...metav1.LabelSelectorRequirement) *metav1.LabelSelector {
		return &metav1.LabelSelector{MatchExpressions: reqs}
	}
	req := func(key string, op metav1.LabelSelectorOperator, values ...string) metav1.LabelSelectorRequirement {
		return metav1.LabelSelectorRequirement{Key: key, Operator: op, Values: values}
	}

	tests := []struct {
		name    string
		a, b    *metav1.LabelSelector
		want    bool
		wantErr bool
	}{
		{
			name: "nil selectors match nothing",
			a:    nil,
			b:    labels("app", "a"),
			want: false,
		},
		{
			name: "both nil",
			want: false,
		},
		{
			name: "empty selector matches everything",
			a:    &metav1.LabelSelector{},
			b:    labels("app", "a"),
			want: true,
		},
		{
			name: "identical match labels",
			a:    labels("app", "a"),
			b:    labels("app", "a"),
			want: true,
		},
		{
			name: "superset of match labels overlaps",
			a:    labels("app", "a"),
			b:    labels("app", "a", "tier", "web"),
			want: true,
		},
		{
			name: "disjoint keys overlap",
			a:    labels("app", "a"),
			b:    labels("tier", "web"),
			want: true,
		},
		{
			name: "conflicting match labels",
			a:    labels("app", "a"),
			b:    labels("app", "b"),
			want: false,
		},
		{
			name: "match label within In set",
			a:    labels("app", "a"),
			b:    exprs(req("app", metav1.LabelSelectorOpIn, "a", "b")),
			want: true,
		},
		{
			name: "In sets intersect",
			a:    exprs(req("env", metav1.LabelSelectorOpIn, "dev", "staging")),
			b:    exprs(req("env", metav1.LabelSelectorOpIn, "staging", "prod")),
			want: true,
		},
		{
			name: "In sets disjoint",
			a:    exprs(req("env", metav1.LabelSelectorOpIn, "dev")),
			b:    exprs(req("env", metav1.LabelSelectorOpIn, "prod")),
			want: false,
		},
		{
			name: "In fully excluded by NotIn",
			a:    exprs(req("env", metav1.LabelSelectorOpIn, "dev", "prod")),
			b:    exprs(req("env", metav1.LabelSelectorOpNotIn, "dev", "prod")),
			want: false,
		},
		{
			name: "In partially excluded by NotIn",
			a:    exprs(req("env", metav1.LabelSelectorOpIn, "dev", "prod")),
			b:    exprs(req("env", metav1.LabelSelectorOpNotIn, "dev")),
			want: true,
		},
		{
			name: "Exists and DoesNotExist conflict",
			a:    exprs(req("canary", metav1.LabelSelectorOpExists)),
			b:    exprs(req("canary", metav1.LabelSelectorOpDoesNotExist)),
			want: false,
		},
		{
			name: "match label and DoesNotExist conflict",
			a:    labels("canary", "true"),
			b:    exprs(req("canary", metav1.LabelSelectorOpDoesNotExist)),
			want: false,
		},
		{
			name: "NotIn and DoesNotExist overlap on an absent label",
			a:    exprs(req("env", metav1.LabelSelectorOpNotIn, "prod")),
			b:    exprs(req("env", metav1.LabelSelectorOpDoesNotExist)),
			want: true,
		},
		{
			name: "Exists and NotIn overlap on another value",
			a:    exprs(req("env", metav1.LabelSelectorOpExists)),
			b:    exprs(req("env", metav1.LabelSelectorOpNotIn, "dev", "prod")),
			want: true,
		},
		{
			name: "one conflicting key prevents overlap",
			a:    labels("app", "a", "tier", "web"),
			b:    labels("app", "a", "tier", "db"),
			want: false,
		},
		{
			name:    "invalid selector",
			a:       exprs(req("env", metav1.LabelSelectorOpIn)),
			b:       labels("app", "a"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SelectorsOverlap(tt.a, tt.b)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SelectorsOverlap() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("SelectorsOverlap() = %v, want %v", got, tt.want)
			}
			// Overlap is symmetric.
			if gotReversed, _ := SelectorsOverlap(tt.b, tt.a); gotReversed != got {
				t.Errorf("SelectorsOverlap() is not symmetric: (a, b) = %v, (b, a) = %v", got, gotReversed)
			}
		})
	}
}
