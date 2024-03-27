package classification

import (
	"embed"
	"fmt"
	"strings"

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

// GetLabels returns a slice of all known labels defined in the labels.yaml
// file.
func GetLabels() ([]*Label, error) {
	lbls := struct {
		Labels []*Label `yaml:"labels"`
	}{}
	if err := yaml.Unmarshal([]byte(labelsYaml), &lbls); err != nil {
		return nil, fmt.Errorf("error unmarshalling labels.yaml: %w", err)
	}
	for _, lbl := range lbls.Labels {
		fname := "rego/" + strings.ReplaceAll(strings.ToLower(lbl.Name), " ", "_") + ".rego"
		b, err := regoFs.ReadFile(fname)
		if err != nil {
			return nil, fmt.Errorf("error reading rego file %s: %w", fname, err)
		}
		lbl.ClassificationRule = string(b)
	}
	return lbls.Labels, nil
}
