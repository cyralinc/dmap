package scan

type Config struct {
	AWS *AWSConfig
}

type AWSConfig struct {
	Regions []string
}

// TODO: validate config
