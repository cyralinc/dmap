package classification

import (
	"context"
	"fmt"

	"github.com/open-policy-agent/opa/rego"
	log "github.com/sirupsen/logrus"
)

// LabelClassifier is a Classifier implementation that uses a set of labels and
// their classification rules to classify data.
type LabelClassifier struct {
	labels LabelSet
}

// LabelClassifier implements Classifier
var _ Classifier = (*LabelClassifier)(nil)

// NewLabelClassifier creates a new LabelClassifier with the provided labels and
// classification rules.
func NewLabelClassifier(labels ...Label) (*LabelClassifier, error) {
	if len(labels) == 0 {
		return nil, fmt.Errorf("labels cannot be empty")
	}
	l := make(LabelSet, len(labels))
	for _, lbl := range labels {
		l[lbl.Name] = lbl
	}
	return &LabelClassifier{labels: l}, nil
}

// Classify performs the classification of the provided attributes using the
// classifier's labels and classification rules. It returns a Result, which is
// a map of attribute names to the set of labels that the attribute was
// classified as.
func (c *LabelClassifier) Classify(ctx context.Context, input map[string]any) (Result, error) {
	result := make(Result, len(c.labels))
	for _, lbl := range c.labels {
		output, err := evalQuery(ctx, lbl.ClassificationRule, input)
		if err != nil {
			return nil, fmt.Errorf("error evaluating query for label %s: %w", lbl.Name, err)
		}
		log.Debugf("classification results for label %s: %v", lbl.Name, output)
		for attrName, classified := range output {
			if classified {
				attrLabels, ok := result[attrName]
				if !ok {
					attrLabels = make(LabelSet)
					result[attrName] = attrLabels
				}
				attrLabels[lbl.Name] = lbl
			}
		}
	}
	return result, nil
}

// evalQuery evaluates the provided prepared Rego query with the given
// attributes as input, and returns the classification results. The output is a
// map of attribute names to boolean values, where the boolean indicates whether
// the attribute is classified as belonging to the label.
func evalQuery(ctx context.Context, rule *rego.Rego, input map[string]any) (map[string]bool, error) {
	q, err := rule.PrepareForEval(ctx)
	if err != nil {
		return nil, fmt.Errorf("error preparing rule for evaluation: %w", err)
	}
	// Evaluate the prepared Rego query. This performs the actual classification
	// logic.
	res, err := q.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return nil, fmt.Errorf(
			"error evaluating query for input %s; %w", input, err,
		)
	}
	// Ensure the result is well-formed.
	if len(res) != 1 {
		return nil, fmt.Errorf("expected 1 result but found: %d", len(res))
	}
	if len(res[0].Expressions) != 1 {
		return nil, fmt.Errorf("expected 1 expression but found: %d", len(res[0].Expressions))
	}
	if res[0].Expressions[0] == nil {
		return nil, fmt.Errorf("expression is nil")
	}
	if res[0].Expressions[0].Value == nil {
		return nil, fmt.Errorf("expression value is nil")
	}
	// Unpack the results. The output is expected to be a map[string]bool, where
	// the key is the attribute name and the value is a boolean indicating
	// whether the attribute is classified as belonging to the label.
	val, ok := res[0].Expressions[0].Value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf(
			"expected output type to be map[string]any, but found: %T",
			res[0].Expressions[0].Value,
		)
	}
	output := make(map[string]bool, len(val))
	for k, v := range val {
		if b, ok := v.(bool); ok {
			output[k] = b
		} else {
			return nil, fmt.Errorf("expected value to be bool but found: %T", v)
		}
	}
	return output, nil
}
