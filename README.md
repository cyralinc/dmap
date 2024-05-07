# Dmap

<div align="center">
  <img src="https://dmap-prod-public-dmap.s3.amazonaws.com/dmap-logo.svg">
</div>

Dmap is a free and open-source toolkit to assess your data security posture in
the cloud. It allows you to quickly find information about your data 
repositories from different cloud environments, and then perform data discovery
and classification by scanning those repositories for sensitive data patterns.

## Overview

Dmap provides a [hosted web service](https://dmap.cyral.io), command line 
interface (CLI), and Go library API to scan cloud environments to discover data
repositories, and scan data repositories to discover and classify sensitive
data.

We define a data repository as a collection of data that is stored in a specific
location in the cloud. For example, an Amazon RDS database, a Redshift cluster,
or a DynamoDB table are all examples of Dmap data repositories. Think of it as a
more generic term for a data store or database.

We also define a cloud environment as a collection of cloud resources that are
managed by a specific cloud provider. For example, an AWS account is a cloud
environment.

Scanning a cloud environment with Dmap will provide you with a list of data
repositories that exist in that environment. Scanning a data repository with
Dmap will provide you with all the fields in the data repository that contain
sensitive data.

We use Open Policy Agent's (OPA) [Rego API](https://pkg.go.dev/github.com/open-policy-agent/opa/rego) 
to define rules that can be used to classify sensitive data in data repositories
and assign them appropriate labels (such as CCN, SSN etc). Dmap provides a set 
of [predefined data labels](classification/labels) that can be used to classify 
sensitive data in data repositories. You can also define your own data labels if
desired. Each data label has a name, description, and a set of tags that can be
used to group labels, e.g. "PII", "PCI", "HIPAA", etc.

## Command Line Interface (CLI)

The Dmap CLI can be used to scan data repositories to perform data discovery and
classification. It will produce JSON-formatted output that lists all the data
labels used for classification, as well as the fields in the data repository
that were classified as containing sensitive data. For example:

```bash
$ dmap repo-scan \
  --type postgres \
  --host ... \
  --port ... \
  --user ... \
  --password ...

{
    "labels": [
        {
            "name": "ADDRESS",
            "description": "Address",
            "tags": [
                "PII"
            ]
        },
        ...
    ],
    "classifications": [
        {
            "attributePath": [
                "postgres",
                "public",
                "patients",
                "address"
            ],
            "labels": [
                "ADDRESS"
            ]
        },
        ...
    ]
}
```

Optionally, by providing the `--repo-id`, `--client-id`, and `--client-secret`
flags, the results can be sent to the Dmap web service for further analysis and
reporting.

Use the `--help` flag to see all available commands and options, e.g.:

```bash
$ dmap --help
$ dmap repo-scan --help
```

### Installation

The Dmap CLI can be installed as a native binary, a Docker image, or directly 
from source. Each approach is described below.

#### Release Binaries

Binary executables of the CLI are available for Linux, MacOS, and Windows
platforms. The appropriate binary for your platform can be downloaded from the 
[releases page](https://github.com/cyralinc/dmap/releases), e.g.:

```bash
# Replace with the desired version, e.g. v0.1.0
VERSION="v0.1.0"
curl -OL "https://github.com/cyralinc/dmap/releases/download/$VERSION/dmap_$VERSION_darwin_amd64.zip"
unzip dmap_$VERSION_darwin_amd64.zip
```

Optionally, put the binary in a location in your `PATH` for easy use.

The SHA256 checksums for each release are provided in a file named
`dmap_<version>_sha256sums.txt`. You can verify the integrity of the downloaded
binary by comparing its checksum to the one in the file. The checksums are also
signed with Cyral's GPG key (fingerprint 
`E8DBE6574C87BF0FED7FFC464D91812ADF732B74`), and you can verify the checksums 
file, e.g.:

```bash
# Replace with the desired version, e.g. v0.1.0.
# Assuming the binary/binaries and checksums are in the same directory.
sha256sum -c dmap_<version>_sha256sums.txt
gpg --verify dmap_<version>_sha256sums.txt.sig dmap_<version>_sha256sums.txt
```

#### Docker

Docker images for the Dmap CLI are available on the public Cyral ECR. Tags for
each version of Dmap are released, as well as a `latest` tag. The image can be 
run as a container, e.g.:

```bash
# Optionally replace `latest` with the desired version, e.g. v0.1.0.
docker run --rm public.ecr.aws/cyral/dmap:latest repo-scan \
  --type ... \
  --database ... \
  --host ... \
  --port ... \
  --user ... \
  --password ...
```

#### From Source

Requires Go 1.21 or later.

```bash
# Replace <version> with the desired version, e.g. v0.1.0, or the branch, e.g. main.
go install github.com/cyralinc/dmap/cmd/dmap@<version>
```

## Go Library

The Dmap Go library provides APIs to scan cloud environments to discover data
repositories in those environments, as well as scan individual data repositories
for sensitive data.

### Import

To import the Dmap library into your project, use the `go get` command below:

```bash
go get github.com/cyralinc/dmap
```

### Cloud Environment Scanning

The Cloud environment scanning API currently supports scanning AWS environments,
and the following data repository types from across AWS services including:
- Amazon RDS (MySQL, PostgreSQL, SQL Server, etc)
- RDS Clusters (Aurora, Multi-AZ Clusters)
- Redshift
- DynamoDB
- DocumentDB

#### Requirements

The Dmap library requires a set of read-only AWS service permissions to perform
and environment scan, so that it's able to find existing data repositories from
these services. IAM credentials with permissions for the following actions are
required: 

- `rds:DescribeDBClusters`
- `rds:DescribeDBInstances`
- `rds:ListTagsForResource`
- `redshift:DescribeClusters`
- `dynamodb:DescribeTable`
- `dynamodb:ListTables`
- `dynamodb:ListTagsOfResource`

Make sure to use proper AWS credentials that contain the permissions above.

#### Scan an Environment

To use Dmap to find information about your existing data repositories, follow
the steps below:

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
package main

import (
	"context"
    "fmt"

    "github.com/cyralinc/dmap/aws"
)

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

### Repository Scanning

The main API used for scanning repositories is [`sql.Scanner`](sql/scanner.go).
It is an implementation of the [`RepoScanner`](scan/scanner.go) interface.

The repository scanning API currently supports scanning the following SQL data
repositories out of the box:
- MySQL
- PostgreSQL
- SQL Server
- Redshift
- Snowflake
- Oracle
- Denodo

Example usage:

```Go
package main

import (
	"context"
	"encoding/json"
    "fmt"
	"log"
	
	"github.com/cyralinc/dmap/sql"
)

func main() {
	ctx := context.Background()
	// Configure and instantiate the scanner.
	cfg := sql.ScannerConfig{
		RepoType: "postgres",
		RepoConfig: sql.RepoConfig{
			Host:         "example.com",
			Port:         "5431",
			User:         "user",
			Password:     "password",
		},
	}
	scanner, err := sql.NewScanner(ctx, cfg)
	if err != nil {
		log.Fatalf("error creating new scanner: %v", err)
	}
	// Scan the repository.
	results, err := scanner.Scan(ctx)
	if err != nil {
		log.Fatalf("error scanning repository: %v", err)
	}
    // Print the results to stdout as JSON.
    jsonResults, err := json.MarshalIndent(results, "", "    ")
    if err != nil {
        log.Fatalf("error marshalling results: %v", err)
    }
    fmt.Println(string(jsonResults))
}
```

Additional repository types can be added by implementing the [`sql.Repository`](sql/repository.go)
interface and registering it in a [`sql.Registry`](sql/registry.go). See the
[`sql`](sql) package for more details.

See the [`classification`](classification) package for more details on how to
define and use data labels for classifying sensitive data.

## Resources

Learn more about Cyral by visiting [Cyral.com](https://cyral.com/) and also the 
links below:

- [Cyral Dmap](https://dmap.cyral.io/)
- [Cyral Public Docs](https://cyral.com/docs/)

## Contribution guidelines

We use [GitHub issues](https://github.com/cyralinc/dmap/issues) for tracking 
requests and bugs, please feel free to use that for reporting any requests or 
issues.
