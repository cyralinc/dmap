package classification

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetEmbeddedLabels(t *testing.T) {
	got, err := GetEmbeddedLabels()
	require.NoError(t, err)
	require.NotEmpty(t, got)
}
