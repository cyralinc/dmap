package classification

import (
	"context"
	"fmt"

	"github.com/open-policy-agent/opa/ast"
	"github.com/open-policy-agent/opa/rego"

	log "github.com/sirupsen/logrus"
)

const (
	unlabeledLabel = "UNLABELED"
)

// TODO: godoc -ccampo 2024-03-26
type LabelClassifier struct {
	lbl           *Label
	preparedQuery rego.PreparedEvalQuery
}

// *LabelClassifier implements Classifier
var _ Classifier = (*LabelClassifier)(nil)

// TODO: godoc -ccampo 2024-03-26
func NewLabelClassifier(lbl *Label) (*LabelClassifier, error) {
	if lbl == nil {
		return nil, fmt.Errorf("label cannot be nil")
	}
	q, err := prepareClassifierCode(lbl.ClassificationRule)
	if err != nil {
		return nil, err
	}
	return &LabelClassifier{lbl: lbl, preparedQuery: q}, nil
}

// TODO: godoc -ccampo 2024-03-26
func (c *LabelClassifier) Classify(
	_ context.Context,
	table *ClassifiedTable,
	attrs map[string]any,
) ([]Result, error) {
	if c == nil || len(attrs) == 0 {
		return nil, fmt.Errorf("invalid arguments")
	}

	results, err := c.preparedQuery.Eval(context.Background(), rego.EvalInput(attrs))
	if err != nil {
		return nil, fmt.Errorf(
			"[classifier %s] error evaluating query for inputs %s; %w", c.lbl.Name, attrs, err,
		)
	}

	log.Debugf("[classifier %s] results: '%s'", c.lbl.Name, results)
	if len(results) != 1 || len(results[0].Expressions) != 1 {
		return nil, fmt.Errorf(
			"[classifier %s] received malformed result in classification eval - expected 1 result with 1 expression result, but found: '%s'",
			c.lbl.Name,
			results,
		)
	}

	exprValue := results[0].Expressions[0]
	if exprValue == nil {
		return nil, fmt.Errorf("[classifier %s] did not expect nil value in expression value", c.lbl.Name)
	}

	mapVal := results[0].Expressions[0].Value
	resolvedMap, ok := mapVal.(map[string]any)
	if !ok {
		return nil, fmt.Errorf(
			"[classifier %s] expected type to be map[string]any, but found: %T", c.lbl.Name, mapVal,
		)
	}

	// Count the number of classifications, ignoring unlabeled classifications.
	resultLength := 0
	for _, v := range resolvedMap {
		if v != unlabeledLabel {
			resultLength++
		}
	}

	// If there are no classifications, return nil.
	if resultLength == 0 {
		return nil, nil
	}

	classifications := make([]Result, resultLength)
	resultIndex := 0
	for attrName, v := range resolvedMap {
		valueAsString, ok := v.(string)
		if !ok {
			log.Errorf("value type expected to be string, was: %T", v)
			continue
		}
		if valueAsString != unlabeledLabel {
			classifications[resultIndex] = Result{
				Table:           table,
				AttributeName:   attrName,
				Classifications: []*Label{c.lbl},
			}
			resultIndex++
		}
	}
	return classifications, nil
}

func prepareClassifierCode(classifierCode string) (rego.PreparedEvalQuery, error) {
	log.Tracef("classifier module code: '%s'", classifierCode)
	moduleName := "classifier"
	compiledRego, err := ast.CompileModules(map[string]string{moduleName: classifierCode})
	if err != nil {
		return rego.PreparedEvalQuery{}, fmt.Errorf("error compiling rego code: %w", err)
	}
	regoQuery := compiledRego.Modules[moduleName].Package.Path.String() + ".output"
	retVal, err := rego.New(
		rego.Query(regoQuery),
		rego.Compiler(compiledRego),
	).PrepareForEval(context.Background())
	if err != nil {
		return rego.PreparedEvalQuery{}, fmt.Errorf("error preparing rego code for evaluation: %w", err)
	}
	return retVal, nil
}
