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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func spotToleration() corev1.Toleration {
	return corev1.Toleration{Key: SpotLabelKey, Operator: corev1.TolerationOpEqual, Value: SpotLabelValue, Effect: corev1.TaintEffectNoSchedule}
}

func TestIsPodTargetedForSpot(t *testing.T) {
	testCases := []struct {
		name string
		pod  corev1.Pod
		want bool
	}{
		{name: "no Spot targeting", pod: corev1.Pod{}, want: false},
		{name: "Spot nodeSelector", pod: corev1.Pod{Spec: corev1.PodSpec{NodeSelector: map[string]string{SpotLabelKey: SpotLabelValue}}}, want: true},
		{name: "Spot toleration", pod: corev1.Pod{Spec: corev1.PodSpec{Tolerations: []corev1.Toleration{spotToleration()}}}, want: true},
		{name: "Spot toleration with empty operator", pod: corev1.Pod{Spec: corev1.PodSpec{Tolerations: []corev1.Toleration{{Key: SpotLabelKey, Value: SpotLabelValue, Effect: corev1.TaintEffectNoSchedule}}}}, want: true},
		{name: "other toleration", pod: corev1.Pod{Spec: corev1.PodSpec{Tolerations: []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}}}, want: false},
		{name: "Spot key with NoExecute effect", pod: corev1.Pod{Spec: corev1.PodSpec{Tolerations: []corev1.Toleration{{Key: SpotLabelKey, Operator: corev1.TolerationOpEqual, Value: SpotLabelValue, Effect: corev1.TaintEffectNoExecute}}}}, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPodTargetedForSpot(&tc.pod); got != tc.want {
				t.Errorf("IsPodTargetedForSpot() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsPodActive(t *testing.T) {
	now := metav1.Now()
	testCases := []struct {
		name string
		pod  corev1.Pod
		want bool
	}{
		{name: "running", pod: corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}, want: true},
		{name: "pending", pod: corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}}, want: true},
		{name: "terminating", pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}, want: false},
		{name: "succeeded", pod: corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}, want: false},
		{name: "failed", pod: corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}}, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPodActive(&tc.pod); got != tc.want {
				t.Errorf("IsPodActive() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsSpotNode(t *testing.T) {
	spot := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{SpotLabelKey: SpotLabelValue}}}
	if !IsSpotNode(spot) {
		t.Errorf("expected labeled node to be Spot")
	}
	if IsSpotNode(&corev1.Node{}) {
		t.Errorf("expected unlabeled node not to be Spot")
	}
}
