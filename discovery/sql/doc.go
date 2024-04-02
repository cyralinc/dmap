// Package sql provides an API for performing data discovery on SQL databases.
// The Repository type encapsulates the concept of a Dmap data SQL repository.
// The package provides a registry for all supported repository implementations
// and a factory function to create new instances of a repository
// from the registry. All supported repositories are represented as sub-packages
// of the repository name, e.g. mysql, postgresql, etc.
//
// Repository implementations should reside in their own sub-package of the
// repository package. Each implementation register itself with the repository
// registry by calling the Register function with a RepoConstructor function
// that returns a new instance of the repository implementation. This will make
// the repository implementation available to the NewRepository factory
// function. Registration is typically done in the sub-package's init function.
// TODO: fix this doc -ccampo 2024-04-02
package sql
