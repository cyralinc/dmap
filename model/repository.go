package model

import (
	"time"
)

type RepoType string

const (
	// Repo types
	RepoTypeRDS      RepoType = "TYPE_RDS"
	RepoTypeRedshift RepoType = "TYPE_REDSHIFT"
	RepoTypeDynamoDB RepoType = "TYPE_DYNAMODB"
)

type Repository struct {
	Id         string
	Name       string
	Type       RepoType
	CreatedAt  time.Time
	Tags       []string
	Properties any
}
