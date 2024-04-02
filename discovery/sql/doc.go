// Package sql provides an API for performing database introspection and data
// discovery on SQL databases. The Repository type encapsulates the concept of a
// Dmap data SQL repository. The package provides a Registry for all supported
// repository implementations and a factory function to create new instances of
// a repository from the registry. All out-of-the-box Repository implementations
// are included in their own files named after the repository type, e.g.
// mysql.go, postgres.go, etc.
package sql
