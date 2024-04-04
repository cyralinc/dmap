package classification

import (
	"embed"
	"fmt"
	"strings"

	"github.com/open-policy-agent/opa/ast"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v3"
)

var (
	//go:embed labels/*.rego
	regoFs embed.FS
	//go:embed labels/labels.yaml
	labelsYaml string
)

// Label represents a data classification label.
type Label struct {
	// Name is the name of the label.
	Name string `yaml:"name" json:"name"`
	// Description is a brief description of the label.
	Description string `yaml:"description" json:"description"`
	// Tags are a list of arbitrary tags associated with the label.
	Tags []string `yaml:"tags" json:"tags"`
	// ClassificationRule is the compiled Rego classification rule used to
	// classify data.
	ClassificationRule *ast.Module `yaml:"-" json:"-"`
}

// NewLabel creates a new Label with the given name, description, classification
// rule, and tags. The classification rule is expected to be the raw Rego code
// that will be used to classify data. If the classification rule is invalid, an
// error is returned.
func NewLabel(name, description, classificationRule string, tags ...string) (Label, error) {
	rule, err := parseRego(classificationRule)
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

// GetEmbeddedLabels returns the predefined embedded labels and their
// classification rules. The labels are read from the embedded labels.yaml file
// and the classification rules are read from the embedded Rego files. If there
// is an error unmarshalling the labels file, it is returned. If there is an
// error reading or parsing a classification rule for a label, a warning is
// logged and that label is skipped.
func GetEmbeddedLabels() ([]Label, error) {
	labels := struct {
		Labels map[string]Label `yaml:"labels"`
	}{}
	if err := yaml.Unmarshal([]byte(labelsYaml), &labels); err != nil {
		return nil, fmt.Errorf("error unmarshalling labels.yaml: %w", err)
	}
	for name, lbl := range labels.Labels {
		fname := "labels/" + strings.ReplaceAll(strings.ToLower(name), " ", "_") + ".rego"
		b, err := regoFs.ReadFile(fname)
		if err != nil {
			log.WithError(err).Warnf("error reading rego file %s", fname)
			continue
		}
		rule, err := parseRego(string(b))
		if err != nil {
			log.WithError(err).Warnf("error parsing classification rule for label %s", lbl.Name)
			continue
		}
		lbl.Name = name
		lbl.ClassificationRule = rule
		labels.Labels[name] = lbl
	}
	return maps.Values(labels.Labels), nil
}

func parseRego(code string) (*ast.Module, error) {
	log.Tracef("classifier module code: '%s'", code)
	module, err := ast.ParseModule("classifier", code)
	if err != nil {
		return nil, fmt.Errorf("error parsing rego code: %w", err)
	}
	return module, nil
}
