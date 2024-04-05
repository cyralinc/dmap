package classification

import (
	"context"
	"errors"
	"fmt"

	"github.com/open-policy-agent/opa/rego"
	log "github.com/sirupsen/logrus"
)

// LabelClassifier is a Classifier implementation that uses a set of labels and
// their classification rules to classify data.
type LabelClassifier struct {
	queries map[string]rego.PreparedEvalQuery
}

// LabelClassifier implements Classifier
var _ Classifier = (*LabelClassifier)(nil)

// NewLabelClassifier creates a new LabelClassifier with the provided labels.
func NewLabelClassifier(ctx context.Context, labels ...Label) (*LabelClassifier, error) {
	queries := make(map[string]rego.PreparedEvalQuery, len(labels))
	for _, lbl := range labels {
		query, err := rego.New(
			// We only care about the 'output' variable.
			rego.Query(lbl.ClassificationRule.Package.Path.String()+".output"),
			rego.ParsedModule(lbl.ClassificationRule),
		).PrepareForEval(ctx)
		if err != nil {
			log.WithError(err).Errorf(
				"error preparing query for label %s; label will not be evaluated for classification",
				lbl.Name,
			)
		} else {
			queries[lbl.Name] = query
		}
	}
	return &LabelClassifier{queries: queries}, nil
}

// Classify performs the classification of the provided input using the
// classifier's labels and their corresponding classification rules. The input
// parameter is a map of attribute names to their values, e.g. a single
// database row. The classifier returns a Result, which is a map of attribute
// names to the set of labels that the attribute was classified as.
func (c *LabelClassifier) Classify(ctx context.Context, input map[string]any) (Result, error) {
	result := make(Result, len(c.queries))
	var errs error
	for lbl, query := range c.queries {
		output, err := evalQuery(ctx, query, input)
		if err != nil {
			// A single error should not prevent the classification of other
			// labels. Aggregate the error and continue.
			errs = errors.Join(errs, fmt.Errorf("error evaluating query for label %s: %w", lbl, err))
			continue
		}
		log.Debugf("classification results for label %s: %v", lbl, output)
		for attrName, classified := range output {
			if classified {
				attrLabels, ok := result[attrName]
				if !ok {
					attrLabels = make(LabelSet)

					result[attrName] = attrLabels
				}
				// Add the label to the set of labels for the attribute.
				attrLabels[lbl] = struct{}{}
			}
		}
	}
	return result, errs
}

// evalQuery evaluates the provided Rego query with the given attributes as input, and returns the classification results. The output is a
// map of attribute names to boolean values, where the boolean indicates whether
// the attribute is classified as belonging to the label.
func evalQuery(ctx context.Context, query rego.PreparedEvalQuery, input map[string]any) (map[string]bool, error) {
	// Evaluate the prepared Rego query. This performs the actual classification
	// logic.
	res, err := query.Eval(ctx, rego.EvalInput(input))
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
