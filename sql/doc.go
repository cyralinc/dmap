// Package sql provides mechanisms to perform database introspection and
// data discovery on various SQL data repositories.
//
// Additionally, the Repository interface provides an API for performing
// database introspection and data discovery on SQL databases. It encapsulates
// the concept of a Dmap data SQL repository. All out-of-the-box Repository
// implementations are included in their own files named after the repository
// type, e.g. mysql.go, postgres.go, etc.
//
// Registry provides an API for registering and constructing Repository
// implementations within an application. There is a global DefaultRegistry
// which has all-out-of-the-box Repository implementations registered to it
// by default.
package sql
