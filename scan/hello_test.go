package scan

import (
	"testing"
)

func Test_hello(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{
			name: "success",
			want: "Hello, World!",
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				if got := hello(); got != tt.want {
					t.Errorf("hello() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}
