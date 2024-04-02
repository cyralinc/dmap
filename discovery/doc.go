// Package discovery provides mechanisms to perform database introspection and
// data discovery on various data repositories. It provides a RepoScanner type that
// can be used to scan a data repository for sensitive data, classify the data,
// and publish the results to external sources.
//
// Additionally, the SQLRepository interface provides an API for performing
// database introspection and data discovery on SQL databases. It encapsulates
// the concept of a Dmap data SQL repository. All out-of-the-box SQLRepository
// implementations are included in their own files named after the repository
// type, e.g. mysql.go, postgres.go, etc.
//
// Registry provides an API for registering and constructing SQLRepository
// implementations within an application. There is a global DefaultRegistry
// which has all-out-of-the-box SQLRepository implementations registered to it
// by default.
package discovery
