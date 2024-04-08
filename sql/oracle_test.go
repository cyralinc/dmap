package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOracleConfigFromMap(t *testing.T) {
	tests := []struct {
		name    string
		cfg     map[string]any
		want    OracleConfig
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "Returns config when ServiceName key is present",
			cfg: map[string]any{
				configServiceName: "testServiceName",
			},
			want: OracleConfig{
				ServiceName: "testServiceName",
			},
		},
		{
			name:    "Returns error when ServiceName key is missing",
			cfg:     map[string]any{},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := NewOracleConfigFromMap(tt.cfg)
				if tt.wantErr == nil {
					tt.wantErr = require.NoError
				}
				tt.wantErr(t, err)
				require.Equal(t, tt.want, got)
			},
		)
	}
}
