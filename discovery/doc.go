// Package discovery provides mechanisms to perform database introspection and
// data discovery on various data repositories. It provides a Scanner type that
// can be used to scan a data repository for sensitive data, classify the data,
// and publish the results to external sources. Additionally, the sql subpackage
// provides various SQL repository implementations that can be used to
// introspect and sample SQL-based data repositories. Support for additional
// data repository types, such as NoSQL-based repos, is intended to be added in
// the future.
package discovery
