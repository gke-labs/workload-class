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
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestWorkloadRefFor(t *testing.T) {
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}}}
	meta := metav1.ObjectMeta{Name: "web", Namespace: "shop", UID: "uid-web", Labels: map[string]string{"team": "a"}}

	testCases := []struct {
		name   string
		obj    client.Object
		want   WorkloadRef
		wantOK bool
	}{
		{
			name:   "deployment",
			obj:    &appsv1.Deployment{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{Template: template}},
			want:   WorkloadRef{Kind: "Deployment", Name: "web", Namespace: "shop", UID: "uid-web", TemplateLabels: map[string]string{"app": "web"}},
			wantOK: true,
		},
		{
			name:   "statefulset",
			obj:    &appsv1.StatefulSet{ObjectMeta: meta, Spec: appsv1.StatefulSetSpec{Template: template}},
			want:   WorkloadRef{Kind: "StatefulSet", Name: "web", Namespace: "shop", UID: "uid-web", TemplateLabels: map[string]string{"app": "web"}},
			wantOK: true,
		},
		{
			name: "other_kind",
			obj:  &appsv1.DaemonSet{ObjectMeta: meta, Spec: appsv1.DaemonSetSpec{Template: template}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := WorkloadRefFor(tc.obj)
			if ok != tc.wantOK || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("WorkloadRefFor() = (%+v, %t), want (%+v, %t)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestBestMatchWorkloadClassForTemplate(t *testing.T) {
	now := metav1.Now()
	older := metav1.NewTime(now.Add(-time.Hour))
	newWC := func(name, namespace string, selector map[string]string, created metav1.Time) workloadsv1.WorkloadClass {
		wc := workloadsv1.WorkloadClass{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, CreationTimestamp: created}}
		if selector != nil {
			wc.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: selector}
		}
		return wc
	}
	general := newWC("general", "shop", map[string]string{"app": "web"}, now)
	specific := newWC("specific", "shop", map[string]string{"app": "web", "tier": "frontend"}, now)
	oldTwin := newWC("old-twin", "shop", map[string]string{"app": "web", "tier": "frontend"}, older)
	otherNamespace := newWC("other-namespace", "elsewhere", map[string]string{"app": "web"}, older)
	enforced := newWC("enforced", "shop", nil, now)

	workload := WorkloadRef{Kind: WorkloadKindDeployment, Name: "web", Namespace: "shop", TemplateLabels: map[string]string{"app": "web", "tier": "frontend"}}
	testCases := []struct {
		name     string
		workload WorkloadRef
		enforced *workloadsv1.WorkloadClass
		wcs      []workloadsv1.WorkloadClass
		want     string
	}{
		{name: "no_workload_classes", workload: workload},
		{name: "most_specific_wins", workload: workload, wcs: []workloadsv1.WorkloadClass{general, specific}, want: "specific"},
		{name: "oldest_wins_ties", workload: workload, wcs: []workloadsv1.WorkloadClass{general, specific, oldTwin}, want: "old-twin"},
		{name: "other_namespaces_are_ignored", workload: workload, wcs: []workloadsv1.WorkloadClass{otherNamespace}},
		{name: "enforced_class_wins", workload: workload, enforced: &enforced, wcs: []workloadsv1.WorkloadClass{general, specific, enforced}, want: "enforced"},
		{
			name:     "template_without_labels_only_matches_the_enforced_class",
			workload: WorkloadRef{Kind: WorkloadKindStatefulSet, Name: "db", Namespace: "shop"},
			enforced: &enforced,
			wcs:      []workloadsv1.WorkloadClass{general, enforced},
			want:     "enforced",
		},
		{
			name:     "non_matching_template",
			workload: WorkloadRef{Kind: WorkloadKindDeployment, Name: "api", Namespace: "shop", TemplateLabels: map[string]string{"app": "api"}},
			wcs:      []workloadsv1.WorkloadClass{general, specific},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := BestMatchWorkloadClassForTemplate(tc.workload, tc.enforced, tc.wcs)
			gotName := ""
			if got != nil {
				gotName = got.Name
			}
			if gotName != tc.want {
				t.Errorf("BestMatchWorkloadClassForTemplate() = %q, want %q", gotName, tc.want)
			}
		})
	}
}

func TestManagedAutoscalerName(t *testing.T) {
	if got := ManagedAutoscalerName("Deployment", "web"); got != "wc-deployment-web" {
		t.Errorf("ManagedAutoscalerName(Deployment, web) = %q, want wc-deployment-web", got)
	}
	if got := ManagedAutoscalerName("StatefulSet", "db"); got != "wc-statefulset-db" {
		t.Errorf("ManagedAutoscalerName(StatefulSet, db) = %q, want wc-statefulset-db", got)
	}

	// The longest name that fits is kept as is.
	fits := strings.Repeat("a", MaxManagedAutoscalerNameLength-len("wc-deployment-"))
	if got := ManagedAutoscalerName("Deployment", fits); got != "wc-deployment-"+fits {
		t.Errorf("ManagedAutoscalerName() truncated a name that fits: %q", got)
	}

	// Longer names are truncated to a valid, deterministic, distinct name.
	long := strings.Repeat("a", 250) + ".b"
	longer := strings.Repeat("a", 250) + ".c"
	got := ManagedAutoscalerName("StatefulSet", long)
	if len(got) > MaxManagedAutoscalerNameLength {
		t.Errorf("len(ManagedAutoscalerName()) = %d, want at most %d", len(got), MaxManagedAutoscalerNameLength)
	}
	if errs := validation.IsDNS1123Subdomain(got); len(errs) > 0 {
		t.Errorf("ManagedAutoscalerName() = %q is not a valid name: %v", got, errs)
	}
	if !strings.HasPrefix(got, "wc-statefulset-aaaa") {
		t.Errorf("ManagedAutoscalerName() = %q, want the wc-statefulset- prefix and the start of the workload name", got)
	}
	if again := ManagedAutoscalerName("StatefulSet", long); again != got {
		t.Errorf("ManagedAutoscalerName() is not deterministic: %q != %q", again, got)
	}
	if other := ManagedAutoscalerName("StatefulSet", longer); other == got {
		t.Errorf("ManagedAutoscalerName() returned %q for two different workloads", got)
	}
}

func TestManagedAutoscalerLabels(t *testing.T) {
	want := map[string]string{"workloads.gke.io/managed-by": "workload-class", "workloads.gke.io/workload-class": "web-services"}
	if got := ManagedAutoscalerLabels("web-services"); !reflect.DeepEqual(got, want) {
		t.Errorf("ManagedAutoscalerLabels() = %v, want %v", got, want)
	}

	long := strings.Repeat("x", 100)
	got := ManagedAutoscalerLabels(long)[workloadsv1.WorkloadClassLabel]
	if errs := validation.IsValidLabelValue(got); len(errs) > 0 {
		t.Errorf("label value %q for a long WorkloadClass name is invalid: %v", got, errs)
	}
	if got != WorkloadClassLabelValue(long) {
		t.Errorf("label value = %q, want WorkloadClassLabelValue() = %q", got, WorkloadClassLabelValue(long))
	}
}

func TestIsWorkloadClassManagedAutoscaler(t *testing.T) {
	testCases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{name: "no_labels"},
		{name: "other_manager", labels: map[string]string{workloadsv1.ManagedByLabel: "helm"}},
		{name: "workload_class_label_only", labels: map[string]string{workloadsv1.WorkloadClassLabel: "web"}},
		{name: "managed", labels: ManagedAutoscalerLabels("web"), want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obj := &metav1.ObjectMeta{Labels: tc.labels}
			if got := IsWorkloadClassManagedAutoscaler(obj); got != tc.want {
				t.Errorf("IsWorkloadClassManagedAutoscaler() = %t, want %t", got, tc.want)
			}
		})
	}
	if IsWorkloadClassManagedAutoscaler(nil) {
		t.Errorf("IsWorkloadClassManagedAutoscaler(nil) = true, want false")
	}
}
