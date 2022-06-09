# Client-Side Graph Refactorings for Neo4j

[![Go](https://github.com/fbiville/graph-refactorings/actions/workflows/go.yml/badge.svg)](https://github.com/fbiville/graph-refactorings/actions/workflows/go.yml)

This is inspired from the APOC refactorings, but implemented completely on
the driver side, with the [official Go driver for Neo4j](https://github.com/neo4j/neo4j-go-driver).

This does not necessarily match 1:1 with APOC.

Run it at your own risk!

## Merge Nodes

### API

Import `"github.com/fbiville/node-clone/pkg/refactoring"` and either call:

 - `MergeNodes(neo4j.Transaction, refactoring.Pattern, []refactoring.PropertyMergePolicy)`
 - `MergeNodesFn(refactoring.Pattern, policies []refactoring.PropertyMergePolicy) neo4j.TransactionWork`

The first is better suited for explicit driver transactions.
The other is available as a transaction function.

### Example

Adapt `cmd/merge-nodes-example/main.go` connection settings and run:

```shell
go run ./cmd/merge-nodes-example
```

This assumes the movie graph has been imported.
