# Dmap

<div align="center">
  <img src="https://dmap-prod-public-dmap.s3.amazonaws.com/dmap-logo.svg">
</div>

Dmap is a free and open-source tool to assess your data security posture in AWS. 
It allows you to quickly find information about your data repositories from 
different AWS account environments, across multiple regions, as well as scan 
those repositories for sensitive data patterns.

It currently supports different data repository types from across AWS services, 
including:
- Amazon RDS (MySQL, PostgreSQL, SQL Server, etc)
- RDS Clusters (Aurora, Multi-AZ Clusters)
- Redshift
- DynamoDB
- DocumentDB

## Command Line Interface (CLI)

### Installation

#### Verification

```bash
sha256sum -c dmap_<version>_sha256sums.txt
gpg --verify dmap_<version>_sha256sums.txt.sig dmap_<version>_sha256sums.txt
```

## Go Library

### Usage

#### Requirements

The Dmap library requires a set of read-only AWS service permissions, so that 
it's able to find existing data repositories from these services. IAM credentials 
with permissions for the following actions are required: 

- `rds:DescribeDBClusters`
- `rds:DescribeDBInstances`
- `rds:ListTagsForResource`
- `redshift:DescribeClusters`
- `dynamodb:DescribeTable`
- `dynamodb:ListTables`
- `dynamodb:ListTagsOfResource`

Make sure to use proper AWS credentials that contain the permissions above.

#### Import

To import the Dmap library into your project, use the `go get` command below:
```go
go get github.com/cyralinc/dmap
```

#### Scan

To use the Dmap library to find information about your existing data repositories, 
follow the steps below:

1. Define the AWS credentials for the account to be scanned. This can be done 
through one of the following options:
    * Using credentials defined through AWS environment variables.
    * Using the `default` profile from the AWS credentials file.
    * Assuming an AWS IAM Role. 
    
    If you want to use an IAM Role, follow the instructions below on how to 
    configure the `ScannerConfig` to assume an IAM role. For more details, see the 
    AWS official guide on [Specifying Credentials](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials).
2. Define the `ScannerConfig`.
    1. Define the AWS regions to be scanned.
    2. (Optional) Define the `AssumeRoleConfig` parameters for the IAM Role to 
       be assumed. The AWS default external configurations will be used instead 
       if this is not defined.
3. Instantiate a new `AWSScanner` using the `NewAWSScanner` function with the 
`ScannerConfig` defined.
4. Use the `AWSScanner` to call the `Scan` method for scanning all the existing 
data repositories for the configuration provided.

Here's a code example of how to do that:
```Go
func main() {
	ctx := context.Background()
	// Define the AWS regions to be scanned.
	regions := []string{
		"us-east-1",
		"us-east-2",
		"us-west-1",
		"us-west-2",
	}
	// Define the scanner configuration.
	scannerConfig := aws.ScannerConfig{
		Regions: regions,
		// Optionally define an AssumeRoleConfig if you want to use an IAM Role.
		// Otherwise, the AWS default external configurations will be used instead.
		// AssumeRole: &aws.AssumeRoleConfig{
		// 	IAMRoleARN: "",
		// 	ExternalID: "",
		// },
	}
	// Create the AWS scanner.
	scanner, err := aws.NewAWSScanner(ctx, scannerConfig)
	if err != nil {
		fmt.Printf("Error creating AWS scanner: %v\n", err)
	}
	// Run data repositories scan.
	results, err := scanner.Scan(ctx)
	if err != nil {
		fmt.Printf("Scan errors: %v\n", err)
	}
	// Print number of repositories scanned.
	fmt.Printf(
		"Scanned %d repositories:\n",
		len(results.Repositories),
	)
	// Print each repository found.
	for repoId, repo := range results.Repositories {
		fmt.Printf(
			"Id: %s | Repo: %v\n\n",
			repoId,
			repo,
		)
	}
}
```

## Resources

Learn more about Cyral by visiting [Cyral.com](https://cyral.com/) and also the 
links below:

- [Cyral Dmap](https://dmap.cyral.io/)
- [Cyral Public Docs](https://cyral.com/docs/)

## Contribution guidelines

We use [GitHub issues](https://github.com/cyralinc/dmap/issues) for tracking 
requests and bugs, please feel free to use that for reporting any requests or 
issues.
