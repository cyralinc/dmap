package classification

import (
	"embed"
	"fmt"
	"strings"

	"context"

	"github.com/open-policy-agent/opa/ast"
	"github.com/open-policy-agent/opa/rego"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// Label represents a data classification label.
type Label struct {
	Name               string   `json:"name"`
	Description        string   `json:"description"`
	Tags               []string `json:"tags"`
	ClassificationRule string   `json:"-"`
}

//go:embed rego/*.rego
var regoFs embed.FS

//go:embed rego/labels.yaml
var labelsYaml string

// TODO: godoc -ccampo 2024-03-27
func GetEmbeddedLabelClassifiers() ([]*LabelClassifier, error) {
	lbls := struct {
		Labels []*Label `yaml:"labels"`
	}{}
	if err := yaml.Unmarshal([]byte(labelsYaml), &lbls); err != nil {
		return nil, fmt.Errorf("error unmarshalling labels.yaml: %w", err)
	}
	classifiers := make([]*LabelClassifier, len(lbls.Labels))
	for i, lbl := range lbls.Labels {
		fname := "rego/" + strings.ReplaceAll(strings.ToLower(lbl.Name), " ", "_") + ".rego"
		b, err := regoFs.ReadFile(fname)
		if err != nil {
			return nil, fmt.Errorf("error reading rego file %s: %w", fname, err)
		}
		lbl.ClassificationRule = string(b)
		classifier, err := NewLabelClassifier(lbl)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize classifier for label %s: %w", lbl.Name, err)
		}
		classifiers[i] = classifier
	}
	return classifiers, nil
}

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
		return nil, fmt.Errorf("invalid arguments; classifier or attributes are nil/empty")
	}

	res, err := c.preparedQuery.Eval(context.Background(), rego.EvalInput(attrs))
	if err != nil {
		return nil, fmt.Errorf(
			"[classifier %s] error evaluating query for inputs %s; %w", c.lbl.Name, attrs, err,
		)
	}

	if len(res) != 1 || len(res[0].Expressions) != 1 {
		return nil, fmt.Errorf(
			"[classifier %s] received malformed result in classification eval - expected 1 result with 1 expression result, but found: '%s'",
			c.lbl.Name,
			res,
		)
	}
	log.Debugf("[classifier %s] results: '%s'", c.lbl.Name, res)

	exprValue := res[0].Expressions[0]
	if exprValue == nil {
		return nil, fmt.Errorf("[classifier %s] expression value is nil", c.lbl.Name)
	}

	output, ok := res[0].Expressions[0].Value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf(
			"[classifier %s] expected output type to be map[string]any, but found: %T",
			c.lbl.Name,
			res[0].Expressions[0].Value,
		)
	}

	// TODO: comment explaining this -ccampo 2024-03-27
	classifications := make([]Result, 0, len(output))
	i := 0
	for attrName, v := range output {
		if v, ok := v.(bool); v && ok {
			classifications[i] = Result{
				Table:           table,
				AttributeName:   attrName,
				Classifications: []*Label{c.lbl},
			}
			i++
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
