// Package classification provides various types and functions to facilitate
// data classification. The type Classifier provides an interface which takes
// arbitrary data as input and returns a classified version of that data as
// output. The package contains at least one implementation which uses OPA and
// Rego to perform the actual classification logic (see LabelClassifier),
// however other implementations may be added in the future.
package classification

import (
	"context"
	"encoding/json"

	"golang.org/x/exp/maps"
)

// Classifier is an interface that represents a data classifier. A classifier
// takes a set of data attributes and classifies them into a set of labels.
type Classifier interface {
	// Classify takes the given input, which amounts to essentially a "row of
	// data", and returns the data classifications for that input. The input is
	// a map of attribute names (i.e. columns) to their values. The returned
	// Result is a map of attribute names to the set of labels that attributes
	// were classified as.
	Classify(ctx context.Context, input map[string]any) (Result, error)
}

// Result represents the classifications for a set of data attributes. The key
// is the attribute (i.e. column) name and the value is the set of labels
// that attribute was classified as.
type Result map[string]LabelSet

// LabelSet is a set of unique labels.
type LabelSet map[string]struct{}

func (l LabelSet) MarshalJSON() ([]byte, error) {
	return json.Marshal(maps.Keys(l))
}

// Classification represents the classification of a data repository attribute.
type Classification struct {
	// AttributePath is the full path of the data repository attribute
	// (e.g. the column). Each element corresponds to a component, in increasing
	// order of granularity (e.g. [database, schema, table, column]).
	AttributePath []string `json:"attributePath"`
	// Labels is the set of labels that the attribute was classified as.
	Labels LabelSet `json:"labels"`
}
