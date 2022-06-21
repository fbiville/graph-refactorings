package main

import (
	"github.com/graph-refactoring/graph-refactoring-go/pkg/refactoring"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"io"
)

func main() {
	driver, err := neo4j.NewDriver("neo4j://localhost", neo4j.BasicAuth("neo4j", "toto", ""))
	panicIfErr(err)
	defer closeThingy(driver)
	session := driver.NewSession(neo4j.SessionConfig{})
	defer closeThingy(session)
	tx, err := session.BeginTransaction()
	panicIfErr(err)
	defer closeThingy(tx)

	err = refactoring.MergeNodes(tx, refactoring.Pattern{
		CypherFragment: "(p:Person) WITH p ORDER BY p.name ASC",
		OutputVariable: "p",
	}, []refactoring.PropertyMergePolicy{
		refactoring.NewPropertyMergePolicy(".*", refactoring.KeepAll),
	})

	panicIfErr(tx.Commit())
	panicIfErr(err)
}

func closeThingy(thingy io.Closer) {
	panicIfErr(thingy.Close())
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
