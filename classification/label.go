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
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

// TODO: godoc -ccampo 2024-03-27
type LabelAndRule struct {
	Label
	ClassificationRule rego.PreparedEvalQuery
}

//go:embed rego/*.rego
var regoFs embed.FS

//go:embed rego/labels.yaml
var labelsYaml string

// TODO: godoc -ccampo 2024-03-27
func GetEmbeddedLabels() ([]LabelAndRule, error) {
	lbls := struct {
		Labels []Label `yaml:"labels"`
	}{}
	if err := yaml.Unmarshal([]byte(labelsYaml), &lbls); err != nil {
		return nil, fmt.Errorf("error unmarshalling labels.yaml: %w", err)
	}
	lblAndRules := make([]LabelAndRule, len(lbls.Labels))
	for i, lbl := range lbls.Labels {
		fname := "rego/" + strings.ReplaceAll(strings.ToLower(lbl.Name), " ", "_") + ".rego"
		b, err := regoFs.ReadFile(fname)
		if err != nil {
			return nil, fmt.Errorf("error reading rego file %s: %w", fname, err)
		}
		rule, err := prepareClassificationRule(string(b))
		if err != nil {
			return nil, fmt.Errorf("error preparing classification rule for label %s: %w", lbl.Name, err)
		}
		lblAndRules[i] = LabelAndRule{Label: lbl, ClassificationRule: rule}
	}
	return lblAndRules, nil
}

// TODO: godoc -ccampo 2024-03-26
type LabelClassifier struct {
	lbls []LabelAndRule
}

// *LabelClassifier implements Classifier
var _ Classifier = (*LabelClassifier)(nil)

// TODO: godoc -ccampo 2024-03-26
func NewLabelClassifier(lbls ...LabelAndRule) (*LabelClassifier, error) {
	if len(lbls) == 0 {
		return nil, fmt.Errorf("labels cannot be empty")
	}
	return &LabelClassifier{lbls: lbls}, nil
}

// TODO: godoc -ccampo 2024-03-26
func (c *LabelClassifier) Classify(ctx context.Context, attrs map[string]any) (map[string][]Label, error) {
	if c == nil || len(attrs) == 0 {
		return nil, fmt.Errorf("invalid arguments; classifier or attributes are nil/empty")
	}
	classifications := make(map[string][]Label, len(c.lbls))
	for _, lbl := range c.lbls {
		output, err := c.evalQuery(ctx, lbl.ClassificationRule, attrs)
		if err != nil {
			return nil, fmt.Errorf("error evaluating query for label %s: %w", lbl.Name, err)
		}
		log.Debugf("classification results for label %s: %v", lbl.Name, output)
		for attrName, v := range output {
			if v {
				classifications[attrName] = append(classifications[attrName], lbl.Label)
			}
		}
	}
	return classifications, nil
}

func (c *LabelClassifier) evalQuery(
	ctx context.Context,
	q rego.PreparedEvalQuery,
	attrs map[string]any,
) (map[string]bool, error) {
	// Evaluate the prepared Rego query. This performs the actual classification
	// logic.
	res, err := q.Eval(ctx, rego.EvalInput(attrs))
	if err != nil {
		return nil, fmt.Errorf(
			"error evaluating query for attrs %s; %w", attrs, err,
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

func prepareClassificationRule(classifierCode string) (rego.PreparedEvalQuery, error) {
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
