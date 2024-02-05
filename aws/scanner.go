package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/cyralinc/dmap/scan"
)

type AWSScanner struct {
	scannerConfig        ScannerConfig
	awsConfig            aws.Config
	awsClientConstructor awsClientConstructor
}

// AWSScanner implements scan.Scanner
var _ scan.Scanner = (*AWSScanner)(nil)

func NewAWSScanner(
	ctx context.Context,
	scannerConfig ScannerConfig,
) (*AWSScanner, error) {
	if err := scannerConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid scanner config: %w", err)
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	s := &AWSScanner{
		scannerConfig: scannerConfig,
		awsConfig:     awsConfig,
	}
	if s.scannerConfig.AssumeRole != nil {
		if err := s.assumeRole(ctx); err != nil {
			return nil, fmt.Errorf("error assuming IAM role: %w", err)
		}
	}
	s.awsClientConstructor = newAWSClient
	return s, nil
}

func (s *AWSScanner) Scan(ctx context.Context) (*scan.ScanResults, error) {
	repositories := []scan.Repository{}
	var errs []error

	numRoutines := len(s.scannerConfig.Regions)
	responseChan := make(chan scanResponse, numRoutines)
	var wg sync.WaitGroup
	wg.Add(numRoutines)

	for i := range s.scannerConfig.Regions {
		go func(region string) {
			defer wg.Done()
			response := scanRegion(ctx, s.awsConfig, region, s.awsClientConstructor)
			responseChan <- response
		}(s.scannerConfig.Regions[i])
	}

	wg.Wait()
	close(responseChan)

	for response := range responseChan {
		repositories = append(repositories, response.repositories...)
		errs = append(errs, response.scanErrors)
	}

	scanResults := &scan.ScanResults{
		Repositories: repositories,
	}
	scanErrors := errors.Join(errs...)

	return scanResults, scanErrors
}

func scanRegion(
	ctx context.Context,
	awsConfig aws.Config,
	region string,
	newAWSClient awsClientConstructor,
) scanResponse {
	repositories := []scan.Repository{}
	var errs []error

	awsConfig.Region = region
	awsClient := newAWSClient(awsConfig)

	scanFunctions := []scanFunction{
		scanRDSClusterRepositories,
		scanRDSInstanceRepositories,
		scanRedshiftRepositories,
		scanDynamoDBRepositories,
	}

	numRoutines := len(scanFunctions)
	responseChan := make(chan scanResponse, numRoutines)
	var wg sync.WaitGroup
	wg.Add(numRoutines)

	for i := range scanFunctions {
		go func(scanFunc scanFunction) {
			defer wg.Done()
			response := scanFunc(ctx, awsClient)
			responseChan <- response
		}(scanFunctions[i])
	}

	wg.Wait()
	close(responseChan)

	for response := range responseChan {
		repositories = append(repositories, response.repositories...)
		errs = append(errs, response.scanErrors)
	}

	return scanResponse{
		repositories: repositories,
		scanErrors:   errors.Join(errs...),
	}
}

func (s *AWSScanner) assumeRole(
	ctx context.Context,
) error {
	stsClient := sts.NewFromConfig(s.awsConfig)
	credsProvider := stscreds.NewAssumeRoleProvider(
		stsClient,
		s.scannerConfig.AssumeRole.IAMRoleARN,
		func(o *stscreds.AssumeRoleOptions) {
			o.ExternalID = &s.scannerConfig.AssumeRole.ExternalID
		},
	)
	s.awsConfig.Credentials = aws.NewCredentialsCache(credsProvider)
	// Validate AWS credentials provider.
	if _, err := s.awsConfig.Credentials.Retrieve(ctx); err != nil {
		return fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}
	return nil
}
