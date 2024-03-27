package classification

import (
	"embed"
	"fmt"
	"strings"

	"github.com/open-policy-agent/opa/ast"
	"github.com/open-policy-agent/opa/rego"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var (
	//go:embed rego/*.rego
	regoFs embed.FS
	//go:embed rego/labels.yaml
	labelsYaml string
)

// Label represents a data classification label.
type Label struct {
	Name               string     `yaml:"name" json:"name"`
	Description        string     `yaml:"description" json:"description"`
	Tags               []string   `yaml:"tags" json:"tags"`
	ClassificationRule *rego.Rego `yaml:"-" json:"-"`
}

// NewLabel creates a new Label with the given name, description, classification
// rule, and tags. The classification rule is expected to be the raw Rego code
// that will be used to classify data. If the classification rule is invalid, an
// error is returned.
func NewLabel(name, description, classificationRule string, tags []string) (Label, error) {
	rule, err := ruleRego(classificationRule)
	if err != nil {
		return Label{}, fmt.Errorf("error preparing classification rule for label %s: %w", name, err)
	}
	return Label{
		Name:               name,
		Description:        description,
		Tags:               tags,
		ClassificationRule: rule,
	}, nil
}

// LabelSet is a set of unique labels. The key is the label name and the value
// is the label itself.
type LabelSet map[string]Label

// ToSlice returns the labels in the set as a slice.
func (s LabelSet) ToSlice() []Label {
	var labels []Label
	for _, label := range s {
		labels = append(labels, label)
	}
	return labels
}

// GetEmbeddedLabels returns the predefined embedded labels and their
// classification rules. The labels are read from the embedded labels.yaml file
// and the classification rules are read from the embedded Rego files.
func GetEmbeddedLabels() (LabelSet, error) {
	labels := struct {
		Labels LabelSet `yaml:"labels"`
	}{}
	if err := yaml.Unmarshal([]byte(labelsYaml), &labels); err != nil {
		return nil, fmt.Errorf("error unmarshalling labels.yaml: %w", err)
	}
	for name, lbl := range labels.Labels {
		fname := "rego/" + strings.ReplaceAll(strings.ToLower(name), " ", "_") + ".rego"
		b, err := regoFs.ReadFile(fname)
		if err != nil {
			return nil, fmt.Errorf("error reading rego file %s: %w", fname, err)
		}
		rule, err := ruleRego(string(b))
		if err != nil {
			return nil, fmt.Errorf("error preparing classification rule for label %s: %w", lbl.Name, err)
		}
		lbl.Name = name
		lbl.ClassificationRule = rule
		labels.Labels[name] = lbl
	}
	return labels.Labels, nil
}

func ruleRego(code string) (*rego.Rego, error) {
	log.Tracef("classifier module code: '%s'", code)
	moduleName := "classifier"
	compiledRego, err := ast.CompileModules(map[string]string{moduleName: code})
	if err != nil {
		return nil, fmt.Errorf("error compiling rego code: %w", err)
	}
	regoQuery := compiledRego.Modules[moduleName].Package.Path.String() + ".output"
	return rego.New(rego.Query(regoQuery), rego.Compiler(compiledRego)), nil
}
