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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// SelectorsOverlap reports whether there exists a set of labels that is matched by both label selectors,
// i.e. whether a Pod could be selected by both. It does not inspect existing Pods.
//
// It follows the semantics of metav1.LabelSelectorAsSelector: a nil selector matches nothing, and an empty
// selector matches everything. An error is returned if either selector is invalid.
//
// Each selector is a conjunction of requirements, so the two selectors overlap if and only if the combined
// requirements are satisfiable. Requirements on different keys are independent, so this reduces to checking
// that the requirements on each key can be satisfied together. The check runs in time linear in the size of
// the two selectors.
func SelectorsOverlap(a, b *metav1.LabelSelector) (bool, error) {
	if a == nil || b == nil {
		return false, nil
	}

	constraints := map[string]*keyConstraint{}
	for _, sel := range []*metav1.LabelSelector{a, b} {
		if _, err := metav1.LabelSelectorAsSelector(sel); err != nil {
			return false, fmt.Errorf("invalid label selector: %w", err)
		}
		for k, v := range sel.MatchLabels {
			constraintFor(constraints, k).add(metav1.LabelSelectorOpIn, []string{v})
		}
		for _, expr := range sel.MatchExpressions {
			constraintFor(constraints, expr.Key).add(expr.Operator, expr.Values)
		}
	}

	for _, c := range constraints {
		if !c.satisfiable() {
			return false, nil
		}
	}
	return true, nil
}

// keyConstraint accumulates all the requirements placed on a single label key.
type keyConstraint struct {
	// in is the set of values the label may take, or nil if no In requirement has been seen.
	in sets.Set[string]
	// notIn is the set of values the label must not take.
	notIn sets.Set[string]
	// exists is true if the label must be present.
	exists bool
	// notExists is true if the label must be absent.
	notExists bool
}

func constraintFor(constraints map[string]*keyConstraint, key string) *keyConstraint {
	c, ok := constraints[key]
	if !ok {
		c = &keyConstraint{notIn: sets.New[string]()}
		constraints[key] = c
	}
	return c
}

func (c *keyConstraint) add(op metav1.LabelSelectorOperator, values []string) {
	switch op {
	case metav1.LabelSelectorOpIn:
		c.exists = true
		if c.in == nil {
			c.in = sets.New(values...)
		} else {
			c.in = c.in.Intersection(sets.New(values...))
		}
	case metav1.LabelSelectorOpNotIn:
		c.notIn.Insert(values...)
	case metav1.LabelSelectorOpExists:
		c.exists = true
	case metav1.LabelSelectorOpDoesNotExist:
		c.notExists = true
	}
}

func (c *keyConstraint) satisfiable() bool {
	if c.notExists {
		// An absent label satisfies NotIn and DoesNotExist, but not In or Exists.
		return !c.exists
	}
	if c.in != nil {
		// The label must take one of the In values that is not excluded by NotIn.
		return c.in.Difference(c.notIn).Len() > 0
	}
	// Only Exists and/or NotIn: there is always a value outside the finite NotIn set, or the label can be absent.
	return true
}
