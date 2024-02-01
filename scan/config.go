package scan

import "fmt"

type Config struct {
	AWS *AWSConfig
}

type AWSConfig struct {
	Regions    []string
	AssumeRole *AWSAssumeRoleConfig
}

type AWSAssumeRoleConfig struct {
	IAMRoleARN string
	ExternalID string
}

func (c *Config) validateConfig() error {
	if c.isAWSConfigured() {
		if len(c.AWS.Regions) == 0 {
			return fmt.Errorf("AWS regions are required")
		}
		for _, region := range c.AWS.Regions {
			if region == "" {
				return fmt.Errorf("AWS region can't be empty")
			}
		}
	} else {
		return fmt.Errorf("AWS configuration must be specified")
	}
	return nil
}

func (c *Config) isAWSConfigured() bool {
	return c.AWS != nil
}
