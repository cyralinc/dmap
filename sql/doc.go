// Package sql provides mechanisms to perform database introspection, sampling,
// and classification on various SQL data repositories. The Repository interface
// provides the API for performing database introspection and sampling. It
// encapsulates the concept of a Dmap data SQL repository. All out-of-the-box
// Repository implementations are included in their own files named after the
// repository type, e.g. mysql.go, postgres.go, etc.
//
// Registry provides an API for registering and constructing Repository
// implementations within an application. There is a global DefaultRegistry
// which has all-out-of-the-box Repository implementations registered to it
// by default.
//
// Scanner is a scan.RepoScanner implementation that can be used to perform
// data discovery and classification on SQL repositories.
package sql
