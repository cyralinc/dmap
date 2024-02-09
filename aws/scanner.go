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
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithClientLogMode(aws.LogRetries | aws.LogRequest | aws.LogResponse))
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
			responseChan <- response
		}(s.scannerConfig.Regions[i])
	}

	go func() {
		wg.Wait()
		close(responseChan)
	}()

	for response := range responseChan {
		repositories = append(repositories, response.repositories...)
		scanErrors = append(scanErrors, response.scanErrors...)
	}

	scanResults := &scan.ScanResults{
		Repositories: repositories,
	}

	return scanResults, errors.Join(scanErrors...)
}

func scanRegion(
	ctx context.Context,
	awsConfig aws.Config,
	region string,
	newAWSClient awsClientConstructor,
) scanResponse {
	fmt.Println("Scanning region", region)
	repositories := []scan.Repository{}
	var scanErrors []error

	awsConfig.Region = region
	fmt.Printf("Creating new AWS client with config %v\n", awsConfig)
	awsClient := newAWSClient(awsConfig)
	identity, err := awsClient.sts.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		fmt.Println("Error getting caller identity")
	} else {
		fmt.Printf("Caller identity account: %v\n", *identity.Account)
		fmt.Printf("Caller identity arn: %v\n", *identity.Arn)
		fmt.Printf("Caller identity user id: %v\n", *identity.UserId)
	}

	scanFunctions := []scanFunction{
		scanRDSClusterRepositories,
		scanRDSInstanceRepositories,
		scanRedshiftRepositories,
		scanDynamoDBRepositories,
	}

	responseChan := make(chan scanResponse)
	var wg sync.WaitGroup
	wg.Add(len(scanFunctions))

	for i := range scanFunctions {
		go func(scanFunc scanFunction) {
			defer wg.Done()
			response := scanFunc(ctx, awsClient)
			responseChan <- response
		}(scanFunctions[i])
	}

	go func() {
		wg.Wait()
		close(responseChan)
	}()

	for response := range responseChan {
		repositories = append(repositories, response.repositories...)
		scanErrors = append(scanErrors, response.scanErrors...)
	}

	return scanResponse{
		repositories: repositories,
		scanErrors:   scanErrors,
	}
}

func (s *AWSScanner) assumeRole(
	ctx context.Context,
) error {
	fmt.Printf("Assuming IAM role %s\n", s.scannerConfig.AssumeRole.IAMRoleARN)
	fmt.Printf("Using external ID %s\n", s.scannerConfig.AssumeRole.ExternalID)
	fmt.Printf("AWS config: %v\n", s.awsConfig)
	sts.NewFromConfig(s.awsConfig)
	stsClient := sts.NewFromConfig(s.awsConfig)
	credsProvider := stscreds.NewAssumeRoleProvider(
		stsClient,
		s.scannerConfig.AssumeRole.IAMRoleARN,
		func(o *stscreds.AssumeRoleOptions) {
			o.ExternalID = &s.scannerConfig.AssumeRole.ExternalID
			fmt.Printf("Set role provider external ID %s\n", *o.ExternalID)
		},
	)
	s.awsConfig.Credentials = aws.NewCredentialsCache(credsProvider)
	// Validate AWS credentials provider.
	creds, err := s.awsConfig.Credentials.Retrieve(ctx)
	if err != nil {
		fmt.Printf("Failed to retrieve AWS credentials: %v\n", err)
		return fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}
	fmt.Println("Assume role successful, got creds:")
	fmt.Printf("Creds access key ID: %s\n", creds.AccessKeyID)
	fmt.Printf("Creds secret access key: %s\n", creds.SecretAccessKey)
	fmt.Printf("Creds session token: %s\n", creds.SessionToken)
	fmt.Printf("Creds source: %s\n", creds.Source)
	fmt.Printf("Creds can expire %t\n", creds.CanExpire)
	fmt.Printf("Creds expires at %s\n", creds.Expires.String())
	return nil
}
