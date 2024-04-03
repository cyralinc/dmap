package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
