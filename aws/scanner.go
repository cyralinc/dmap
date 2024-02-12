package aws

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/cyralinc/dmap/scan"
)

// AWSScanner is an implementation of the Scanner interface for the AWS cloud
// provider. It supports scanning data repositories from multiple AWS regions,
// including RDS clusters and instances, Redshift clusters and DynamoDB tables.
type AWSScanner struct {
	scannerConfig        ScannerConfig
	awsConfig            aws.Config
	awsClientConstructor awsClientConstructor
}

// AWSScanner implements scan.Scanner
var _ scan.Scanner = (*AWSScanner)(nil)

// NewAWSScanner creates a new instance of AWSScanner based on the ScannerConfig.
// If AssumeRoleConfig is specified, the AWSScanner will assume this IAM Role
// and use it during service requests. If AssumeRoleConfig is nil, the AWSScanner
// will use the AWS default external configuration.
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

// Scan performs a scan across all the AWS regions configured and return a scan
// results, containing a list of data repositories that includes: RDS clusters
// and instances, Redshift clusters and DynamoDB tables.
func (s *AWSScanner) Scan(ctx context.Context) (*scan.ScanResults, error) {
	repositories := []scan.Repository{}
	var scanErrors []error

	responseChan := make(chan scanResponse)
	var wg sync.WaitGroup
	wg.Add(len(s.scannerConfig.Regions))

	for i := range s.scannerConfig.Regions {
		go func(region string) {
			defer wg.Done()
			response := scanRegion(ctx, s.awsConfig, region, s.awsClientConstructor)

			select {
			case responseChan <- response:
			// NOOP

			case <-ctx.Done():
				return
			}
		}(s.scannerConfig.Regions[i])
	}

	go func() {
		wg.Wait()
		close(responseChan)
	}()

	for {
		select {
		case <-ctx.Done():
			scanErrors = append(scanErrors, ctx.Err())
			return &scan.ScanResults{
				Repositories: repositories,
			}, &scan.ScanError{Errs: scanErrors}

		case response, ok := <-responseChan:
			if !ok {
				// Channel closed, all scans finished.
				var scanErr error
				if len(scanErrors) > 0 {
					scanErr = &scan.ScanError{Errs: scanErrors}
				}
				return &scan.ScanResults{
					Repositories: repositories,
				}, scanErr

			}
			repositories = append(repositories, response.repositories...)
			scanErrors = append(scanErrors, response.scanErrors...)
		}
	}
}

func scanRegion(
	ctx context.Context,
	awsConfig aws.Config,
	region string,
	newAWSClient awsClientConstructor,
) scanResponse {
	repositories := []scan.Repository{}
	var scanErrors []error

	awsConfig.Region = region
	awsClient := newAWSClient(awsConfig)

	scanFunctions := []scanFunction{
		scanRDSClusterRepositories,
		scanRDSInstanceRepositories,
		scanRedshiftRepositories,
		scanDynamoDBRepositories,
		scanDocumentDBRepositories,
	}

	responseChan := make(chan scanResponse)
	var wg sync.WaitGroup
	wg.Add(len(scanFunctions))

	for i := range scanFunctions {
		go func(scanFunc scanFunction) {
			defer wg.Done()
			response := scanFunc(ctx, awsClient)

			select {
			case responseChan <- response:
			// NOOP

			case <-ctx.Done():
				return
			}
		}(scanFunctions[i])
	}

	go func() {
		wg.Wait()
		close(responseChan)
	}()

	for {
		select {
		case <-ctx.Done():
			scanErrors = append(scanErrors, ctx.Err())
			return scanResponse{
				repositories: repositories,
				scanErrors:   scanErrors,
			}

		case response, ok := <-responseChan:
			if !ok {
				// Channel closed
				return scanResponse{
					repositories: repositories,
					scanErrors:   scanErrors,
				}
			}

			repositories = append(repositories, response.repositories...)
			scanErrors = append(scanErrors, response.scanErrors...)
		}
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
