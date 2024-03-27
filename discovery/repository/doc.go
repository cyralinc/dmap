// Package repository encapsulates the concept of a Dmap data repository, and
// provides functionality to register multiple implementations of such
// repositories. All supported repositories are represented as sub-packages of
// the repository name, e.g. mysql, postgresql, etc.
//
// The key interface is Repository, which provides encapsulates functionality to
// introspect and sample the underlying database it represents.
//
// Repository implementations should reside in their own sub-package of the
// repository package. Each implementation should provide a constructor that
// matches the signature of the RepoConstructor type, and pass it to Register
// function as part of the repository package's init function. This will make
// the repository implementation available to the NewRepository factory
// function.
package repository
