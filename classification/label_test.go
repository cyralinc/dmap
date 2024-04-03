package classification

import (
	"context"
	"fmt"
	"testing"

	"github.com/open-policy-agent/opa/rego"
	"github.com/stretchr/testify/require"
)

func TestGetEmbeddedLabels(t *testing.T) {
	got, err := GetEmbeddedLabels()
	require.NoError(t, err)
	require.NotEmpty(t, got)
}

func TestRego(t *testing.T) {

	module := `
package example.authz

import rego.v1

default allow := false

allow if {
   input.method == "GET"
   input.path == ["salary", input.subject.user]
}

allow if is_admin

is_admin if "admin" in input.subject.groups
`

	mod, err := parseRego(module)
	require.NoError(t, err)
	require.NotNil(t, mod)
	path := mod.Package.Path.String()
	fmt.Println(path)

	ctx := context.TODO()

	query, err := rego.New(
		rego.Query("data.example.authz.allow"),
		rego.Module("example.rego", module),
	).PrepareForEval(ctx)
	require.NoError(t, err)
	require.NotNil(t, query)

	input := map[string]interface{}{
		"method": "GET",
		"path": []interface{}{"salary", "bob"},
		"subject": map[string]interface{}{
			"user": "bob",
			"groups": []interface{}{"sales", "marketing"},
		},
	}

	results, err := query.Eval(ctx, rego.EvalInput(input))
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.True(t, results.Allowed())
}
