package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUtil(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
	){
		"build connection string succeeds":                            testBuildConnOptionsSucc,
		"build connection string fails":                               testBuildConnOptionsFail,
		"build map from correct connection options succeeds":          testMapConnOptionsSucc,
		"build map from missing connection options does not err":      testMapConnOptionsMissing,
		"build map from non-list connection options fails":            testMapConnOptionsMalformedMap,
		"build map from malformed (non-'=') connection options fails": testMapConnOptionsMalformedColon,
		"fetch correct advanced config succeeds":                      testAdvancedConfigSucc,
		"fetch advanced config with missing parameter errs":           testAdvancedConfigMissing,
		"fetch advanced config with malformed parameters errs":        testAdvancedConfigMalformed,
	} {
		t.Run(
			scenario, func(t *testing.T) {
				teardown := setupTest(t)
				defer teardown()
				fn(t)
			},
		)
	}
}

func setupTest(t *testing.T) func() {
	return func() {}
}

// Returns a correct repo config
func getSampleRepoConfig() RepoConfig {
	return RepoConfig{
		Advanced: map[string]any{
			configConnOpts: []any{"sslmode=disable"},
		},
	}
}

func testBuildConnOptionsSucc(t *testing.T) {
	sampleRepoCfg := getSampleRepoConfig()
	connOptsStr, err := BuildConnOptsStr(sampleRepoCfg)
	require.NoError(t, err)
	require.Equal(t, connOptsStr, "?sslmode=disable")
}

func testBuildConnOptionsFail(t *testing.T) {
	invalidRepoCfg := RepoConfig{
		Advanced: map[string]any{
			// Invalid: map instead of string
			configConnOpts: []any{
				map[string]string{"sslmode": "disable"},
			},
		},
	}
	connOptsStr, err := BuildConnOptsStr(invalidRepoCfg)
	require.Error(t, err)
	require.Empty(t, connOptsStr)
}

func testMapConnOptionsSucc(t *testing.T) {
	sampleRepoCfg := getSampleRepoConfig()
	connOptsMap, err := MapFromConnOpts(sampleRepoCfg)
	require.NoError(t, err)
	require.EqualValues(
		t, connOptsMap, map[string]string{
			"sslmode": "disable",
		},
	)
}

// The mapping should only fail if the config is malformed, not if it is missing
func testMapConnOptionsMissing(t *testing.T) {
	sampleCfg := RepoConfig{}
	optsMap, err := MapFromConnOpts(sampleCfg)
	require.NoError(t, err)
	require.Empty(t, optsMap)
}

func testMapConnOptionsMalformedMap(t *testing.T) {
	sampleCfg := RepoConfig{
		Advanced: map[string]any{
			// Let's put a map instead of the required list
			configConnOpts: map[string]any{
				"testKey": "testValue",
			},
		},
	}
	_, err := MapFromConnOpts(sampleCfg)
	require.Error(t, err)
}

func testMapConnOptionsMalformedColon(t *testing.T) {
	sampleCfg := RepoConfig{
		Advanced: map[string]any{
			// Let's use a colon instead of '=' to divide options
			configConnOpts: []string{"sslmode:disable"},
		},
	}
	_, err := MapFromConnOpts(sampleCfg)
	require.Error(t, err)
}

func testAdvancedConfigSucc(t *testing.T) {
	sampleCfg := RepoConfig{
		Advanced: map[string]any{
			"snowflake": map[string]any{
				"account":   "exampleAccount",
				"role":      "exampleRole",
				"warehouse": "exampleWarehouse",
			},
		},
	}
	repoSpecificMap, err := FetchAdvancedConfigString(
		sampleCfg,
		"snowflake", []string{"account", "role", "warehouse"},
	)
	require.NoError(t, err)
	require.EqualValues(
		t, repoSpecificMap, map[string]string{
			"account":   "exampleAccount",
			"role":      "exampleRole",
			"warehouse": "exampleWarehouse",
		},
	)
}

func testAdvancedConfigMissing(t *testing.T) {
	// Without the snowflake config at all
	sampleCfg := RepoConfig{
		Advanced: map[string]any{},
	}
	_, err := FetchAdvancedConfigString(
		sampleCfg,
		"snowflake", []string{"account", "role", "warehouse"},
	)
	require.Error(t, err)

	sampleCfg = RepoConfig{
		Advanced: map[string]any{
			"snowflake": map[string]any{
				// Missing account

				"role":      "exampleRole",
				"warehouse": "exampleWarehouse",
			},
		},
	}
	_, err = FetchAdvancedConfigString(
		sampleCfg,
		"snowflake", []string{"account", "role", "warehouse"},
	)
	require.Error(t, err)
}

func testAdvancedConfigMalformed(t *testing.T) {
	sampleCfg := RepoConfig{
		Advanced: map[string]any{
			"snowflake": map[string]any{
				// Let's give a _list_ of things
				"account":   []string{"account1", "account2"},
				"role":      []string{"role1", "role2"},
				"warehouse": []string{"warehouse1", "warehouse2"},
			},
		},
	}
	_, err := FetchAdvancedConfigString(
		sampleCfg,
		"snowflake", []string{"account", "role", "warehouse"},
	)
	require.Error(t, err)
}
