package sql

import (
	"fmt"
	"time"
)

// RepoConfig is the necessary configuration to connect to a data sql.
type RepoConfig struct {
	// Host is the hostname of the database.
	Host string
	// Port is the port of the database.
	Port uint16
	// User is the username to connect to the database.
	User string
	// Password is the password to connect to the database.
	Password string
	// Database is the name of the database to connect to.
	Database string
	// MaxOpenConns is the maximum number of open connections to the database.
	MaxOpenConns uint
	// MaxParallelDbs is the maximum number of parallel databases scanned at
	// once.
	MaxParallelDbs uint
	// MaxConcurrency is the maximum number of concurrent query goroutines.
	MaxConcurrency uint
	// QueryTimeout is the maximum time a query can run before being cancelled.
	QueryTimeout time.Duration
	// Advanced is a map of advanced configuration options.
	Advanced map[string]any
}

// keyAsString returns the value of the given key as a string from the given
// configuration map. It returns an error if the key does not exist or if the
// value is not a string.
func keyAsString(cfg map[string]any, key string) (string, error) {
	val, ok := cfg[key]
	if !ok {
		return "", fmt.Errorf("%s key does not exist", key)
	}
	valStr, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("%s key must be a string", key)
	}
	return valStr, nil
}
