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
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// DefaultPendingLeaseBufferSize is the default capacity of the PendingLeaseTracker event channel.
const DefaultPendingLeaseBufferSize = 1024

// PendingLease is a request from the Disruption Webhook for the WorkloadClass controller to lease
// the Per-Pod PDB of a specific Pod instance on behalf of an authorized Subject.
type PendingLease struct {
	PodName string
	PodUID  types.UID
	Subject workloadsv1.Subject
}

// PendingLeaseTracker records pending Per-Pod PDB lease requests from the Disruption Webhook and signals
// the WorkloadClass controller to process them.
//
// The Disruption Webhook and the WorkloadClass controller run in the same controller-manager process, so the
// webhook can hand lease requests to the controller in memory. The webhook calls Enqueue, which records the
// request and sends a non-blocking event.GenericEvent for the governing WorkloadClass on the channel returned by
// Events. The controller watches that channel (via source.Channel), and calls Drain when reconciling the
// WorkloadClass to retrieve and apply the pending leases. All PDB lease writes are therefore serialized
// through the controller's workqueue.
//
// The pending map is the source of truth; events are only wake-up signals. If the event channel is full, the
// request is still recorded and will be drained by the next reconcile of the WorkloadClass.
type PendingLeaseTracker struct {
	mu      sync.Mutex
	pending map[types.NamespacedName]map[string]PendingLease // WorkloadClass key -> Pod name -> lease
	events  chan event.GenericEvent
}

// NewPendingLeaseTracker returns a PendingLeaseTracker whose event channel has the given buffer size.
// A non-positive bufferSize uses DefaultPendingLeaseBufferSize.
func NewPendingLeaseTracker(bufferSize int) *PendingLeaseTracker {
	if bufferSize <= 0 {
		bufferSize = DefaultPendingLeaseBufferSize
	}
	return &PendingLeaseTracker{
		pending: map[types.NamespacedName]map[string]PendingLease{},
		events:  make(chan event.GenericEvent, bufferSize),
	}
}

// Enqueue records a pending lease request for the Pod under the given WorkloadClass and signals the controller.
//
// If a pending lease already exists for the same Pod instance (same name and UID), the existing request is kept
// so that the first subject to request a lease for a Pod wins. A pending lease for a previous Pod instance with
// the same name (different UID) is replaced.
//
// Enqueue returns true if the controller was signaled, or false if the tracker, WorkloadClass, or Pod is nil, or
// if the event channel is full (in which case the request is still recorded).
func (t *PendingLeaseTracker) Enqueue(wc *workloadsv1.WorkloadClass, pod *corev1.Pod, subject workloadsv1.Subject) bool {
	if t == nil || wc == nil || pod == nil {
		return false
	}

	wlcKey := types.NamespacedName{Namespace: wc.Namespace, Name: wc.Name}

	t.mu.Lock()
	leases, ok := t.pending[wlcKey]
	if !ok {
		leases = map[string]PendingLease{}
		t.pending[wlcKey] = leases
	}
	if existingLease, ok := leases[pod.Name]; !ok || existingLease.PodUID != pod.UID {
		leases[pod.Name] = PendingLease{
			PodName: pod.Name,
			PodUID:  pod.UID,
			Subject: subject,
		}
	}
	t.mu.Unlock()

	return t.signal(wlcKey)
}

// Drain atomically retrieves and removes all pending leases for the given WorkloadClass, sorted by Pod name.
func (t *PendingLeaseTracker) Drain(wcKey types.NamespacedName) []PendingLease {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	leases := t.pending[wcKey]
	delete(t.pending, wcKey)
	t.mu.Unlock()

	if len(leases) == 0 {
		return nil
	}

	drained := make([]PendingLease, 0, len(leases))
	for _, lease := range leases {
		drained = append(drained, lease)
	}
	sort.Slice(drained, func(i, j int) bool { return drained[i].PodName < drained[j].PodName })
	return drained
}

// Events returns the channel on which the tracker signals WorkloadClasses with pending leases,
// for use with source.Channel.
func (t *PendingLeaseTracker) Events() <-chan event.GenericEvent {
	return t.events
}

// signal performs a non-blocking send of a GenericEvent identifying the WorkloadClass.
func (t *PendingLeaseTracker) signal(wcKey types.NamespacedName) bool {
	// Only the name and namespace are needed to enqueue a reconcile.Request for the WorkloadClass.
	obj := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:      wcKey.Name,
			Namespace: wcKey.Namespace,
		},
	}

	select {
	case t.events <- event.GenericEvent{Object: obj}:
		return true
	default:
		return false
	}
}
