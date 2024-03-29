// Package repository encapsulates the concept of a Dmap data repository, and
// provides functionality to register multiple implementations of such
// repositories. All supported repositories are represented as sub-packages of
// the repository name, e.g. mysql, postgresql, etc.
//
// The key interface is Repository, which provides encapsulates functionality to
// introspect and sample the underlying database it represents.
//
// Repository implementations should reside in their own sub-package of the
// repository package. Each implementation register itself with the repository
// registry by calling the Register function with a RepoConstructor function
// that returns a new instance of the repository implementation. This will make
// the repository implementation available to the NewRepository factory
// function. Registration is typically done in the sub-package's init function.
package repository
