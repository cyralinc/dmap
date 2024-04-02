package discovery

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Returns a correct repo config
func getSampleRepoConfig() RepoConfig {
	return RepoConfig{
		Advanced: map[string]any{
			configConnOpts: []any{"sslmode=disable"},
		},
	}
}

func TestBuildConnOptionsSucc(t *testing.T) {
	sampleRepoCfg := getSampleRepoConfig()
	connOptsStr, err := BuildConnOptsStr(sampleRepoCfg)
	require.NoError(t, err)
	require.Equal(t, connOptsStr, "?sslmode=disable")
}

func TestBuildConnOptionsFail(t *testing.T) {
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

func TestMapConnOptionsSucc(t *testing.T) {
	sampleRepoCfg := getSampleRepoConfig()
	connOptsMap, err := mapFromConnOpts(sampleRepoCfg)
	require.NoError(t, err)
	require.EqualValues(
		t, connOptsMap, map[string]string{
			"sslmode": "disable",
		},
	)
}

// The mapping should only fail if the config is malformed, not if it is missing
func TestMapConnOptionsMissing(t *testing.T) {
	sampleCfg := RepoConfig{}
	optsMap, err := mapFromConnOpts(sampleCfg)
	require.NoError(t, err)
	require.Empty(t, optsMap)
}

func TestMapConnOptionsMalformedMap(t *testing.T) {
	sampleCfg := RepoConfig{
		Advanced: map[string]any{
			// Let's put a map instead of the required list
			configConnOpts: map[string]any{
				"testKey": "testValue",
			},
		},
	}
	_, err := mapFromConnOpts(sampleCfg)
	require.Error(t, err)
}

func TestMapConnOptionsMalformedColon(t *testing.T) {
	sampleCfg := RepoConfig{
		Advanced: map[string]any{
			// Let's use a colon instead of '=' to divide options
			configConnOpts: []string{"sslmode:disable"},
		},
	}
	_, err := mapFromConnOpts(sampleCfg)
	require.Error(t, err)
}

func TestAdvancedConfigSucc(t *testing.T) {
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

func TestAdvancedConfigMissing(t *testing.T) {
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

func TestAdvancedConfigMalformed(t *testing.T) {
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
