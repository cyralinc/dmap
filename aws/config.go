package aws

import (
	"fmt"
	"regexp"
)

// ScannerConfig represents an AWSScanner configuration. It allows defining the
// AWS regions that should be scanned and an optional AssumeRoleConfig that
// contains the configuration for assuming an IAM Role during the scan. If
// AssumeRoleConfig is nil, the AWS default external configuration will be used
// instead.
type ScannerConfig struct {
	Regions    []string
	AssumeRole *AssumeRoleConfig
}

// AssumeRoleConfig represents the information of an IAM Role to be assumed by
// the AWSScanner when performing request to the AWS services during the data
// repositories scan.
type AssumeRoleConfig struct {
	// The ARN of the IAM Role to be assumed.
	IAMRoleARN string
	// Optional External ID to be used as part of the assume role process.
	ExternalID string
}

// Validate validates the ScannerConfig configuration.
func (config *ScannerConfig) Validate() error {
	if len(config.Regions) == 0 {
		return fmt.Errorf("AWS regions are required")
	}
	for _, region := range config.Regions {
		if region == "" {
			return fmt.Errorf("AWS region can't be empty")
		}
	}
	if config.AssumeRole != nil {
		iamRolePatern := "^arn:aws:iam::\\d{12}:role/.*$"
		match, err := regexp.MatchString(
			iamRolePatern,
			config.AssumeRole.IAMRoleARN,
		)
		if err != nil {
			return fmt.Errorf("error verifying IAM Role format: %w", err)
		}
		if !match {
			return fmt.Errorf(
				"invalid IAM Role: must match format '%s'",
				iamRolePatern,
			)
		}
	}
	return nil
}
