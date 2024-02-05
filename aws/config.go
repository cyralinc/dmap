package aws

import (
	"fmt"
)

type ScannerConfig struct {
	Regions    []string
	AssumeRole *AssumeRoleConfig
}

type AssumeRoleConfig struct {
	IAMRoleARN string
	ExternalID string
}

func (config *ScannerConfig) Validate() error {
	if len(config.Regions) == 0 {
		return fmt.Errorf("AWS regions are required")
	}
	for _, region := range config.Regions {
		if region == "" {
			return fmt.Errorf("AWS region can't be empty")
		}
	}
	return nil
}
