package aws

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ScannerConfigTestSuite struct {
	suite.Suite
}

func TestScannerConfig(t *testing.T) {
	s := new(ScannerConfigTestSuite)
	suite.Run(t, s)
}

func (s *ScannerConfigTestSuite) TestValidate() {
	type TestCase struct {
		description      string
		config           ScannerConfig
		expectedErrorMsg string
	}
	tests := []TestCase{
		{
			description:      "No regions should return error",
			config:           ScannerConfig{},
			expectedErrorMsg: "AWS regions are required",
		},
		{
			description: "Empty region should return error",
			config: ScannerConfig{
				Regions: []string{""},
			},
			expectedErrorMsg: "AWS region can't be empty",
		},
		{
			description: "Invalid IAM Role format should return error",
			config: ScannerConfig{
				Regions: []string{"us-east-1"},
				AssumeRole: &AssumeRoleConfig{
					IAMRoleARN: "invalid-iam-role-format",
				},
			},
			expectedErrorMsg: "invalid IAM Role: must match format " +
				"'^arn:aws:iam::\\d{12}:role/.*$'",
		},
		{
			description: "Valid config should return nil error",
			config: ScannerConfig{
				Regions: []string{"us-east-1"},
				AssumeRole: &AssumeRoleConfig{
					IAMRoleARN: "arn:aws:iam::123456789012:role/SomeIAMRole",
				},
			},
		},
	}

	for _, test := range tests {
		s.T().Run(test.description, func(t *testing.T) {
			err := test.config.Validate()
			if test.expectedErrorMsg == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.expectedErrorMsg)
			}
		})
	}
}
