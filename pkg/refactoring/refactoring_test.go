package refactoring_test

import (
	"context"
	"fmt"
	"github.com/fbiville/graph-refactorings/pkg/refactoring"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"reflect"
	"testing"
)

type base struct {
	name        string
	initQueries []string
	pattern     refactoring.Pattern
	policies    []refactoring.PropertyMergePolicy
}

func TestMergeNodes(outer *testing.T) {
	ctx := context.Background()
	container, driver, err := startNeo4jContainer(ctx)
	if err != nil {
		outer.Fatal(err)
	}
	defer func() {
		assertCloses(outer, driver)
		assertNilError(outer, container.Terminate(ctx))
	}()

	outer.Run("no data", func(t *testing.T) {
		session := driver.NewSession(neo4j.SessionConfig{})
		defer assertCloses(t, session)
		initGraph(t, session, []string{"MATCH (n) DETACH DELETE n"})
		tx, err := session.BeginTransaction()
		assertNilError(t, err)

		err = refactoring.MergeNodes(tx, refactoring.Pattern{
			CypherFragment: "(n)",
			OutputVariable: "n",
		}, nil)

		assertNilError(t, err)
		assertNilError(t, tx.Commit())
		assertNilError(t, tx.Close())
		result, err := session.Run("MATCH (n) RETURN count(n) AS count", nil)
		assertNilError(t, err)
		record, err := result.Single()
		assertNilError(t, err)
		actual, _ := record.Get("count")
		if actual.(int64) != 0 {
			t.Fatalf("Expected 0 got: %v", actual)
		}
	})

	outer.Run("single node", func(t *testing.T) {
		session := driver.NewSession(neo4j.SessionConfig{})
		defer assertCloses(t, session)
		initGraph(t, session, []string{"MATCH (n) DETACH DELETE n", "CREATE (:Person {name: 'Florent'})"})
		tx, err := session.BeginTransaction()
		assertNilError(t, err)

		err = refactoring.MergeNodes(tx, refactoring.Pattern{
			CypherFragment: "(n)",
			OutputVariable: "n",
		}, nil)

		assertNilError(t, err)
		assertNilError(t, tx.Commit())
		assertNilError(t, tx.Close())
		result, err := session.Run("MATCH (n) RETURN n", nil)
		assertNilError(t, err)
		record, err := result.Single()
		assertNilError(t, err)
		rawNode, _ := record.Get("n")
		node := rawNode.(neo4j.Node)
		expectedLabels := []string{"Person"}
		actualLabels := node.Labels
		if !reflect.DeepEqual(actualLabels, expectedLabels) {
			t.Errorf("Expected labels %v, got %v", expectedLabels, actualLabels)
		}
		expectedProps := map[string]any{"name": "Florent"}
		actualProps := node.Props
		if !reflect.DeepEqual(actualProps, expectedProps) {
			t.Errorf("Expected props %v, got %v", expectedProps, actualProps)
		}
	})

	outer.Run("single node matching pattern", func(t *testing.T) {
		session := driver.NewSession(neo4j.SessionConfig{})
		defer assertCloses(t, session)
		initGraph(t, session, []string{"MATCH (n) DETACH DELETE n", "CREATE (:Person {name: 'Florent'}), (:Robot {name: 'Flower'})"})
		tx, err := session.BeginTransaction()
		assertNilError(t, err)

		err = refactoring.MergeNodes(tx, refactoring.Pattern{
			CypherFragment: "(n:Person)",
			OutputVariable: "n",
		}, nil)

		assertNilError(t, err)
		assertNilError(t, tx.Commit())
		assertNilError(t, tx.Close())
		result, err := session.Run("MATCH (n:Person) RETURN n", nil)
		assertNilError(t, err)
		record, err := result.Single()
		assertNilError(t, err)
		rawNode, _ := record.Get("n")
		node := rawNode.(neo4j.Node)
		expectedLabels := []string{"Person"}
		actualLabels := node.Labels
		if !reflect.DeepEqual(actualLabels, expectedLabels) {
			t.Errorf("Expected labels %v, got %v", expectedLabels, actualLabels)
		}
		expectedProps := map[string]any{"name": "Florent"}
		actualProps := node.Props
		if !reflect.DeepEqual(actualProps, expectedProps) {
			t.Errorf("Expected props %v, got %v", expectedProps, actualProps)
		}
	})

	outer.Run("two nodes with same label, no prop", func(t *testing.T) {
		session := driver.NewSession(neo4j.SessionConfig{})
		defer assertCloses(t, session)
		initGraph(t, session, []string{"MATCH (n) DETACH DELETE n", "CREATE (:`A Person`), (:`A Person`)"})
		tx, err := session.BeginTransaction()
		assertNilError(t, err)

		err = refactoring.MergeNodes(tx, refactoring.Pattern{
			CypherFragment: "(n)",
			OutputVariable: "n",
		}, nil)

		assertNilError(t, err)
		assertNilError(t, tx.Commit())
		assertNilError(t, tx.Close())
		result, err := session.Run("MATCH (n) RETURN n", nil)
		assertNilError(t, err)
		record, err := result.Single()
		assertNilError(t, err)
		rawNode, _ := record.Get("n")
		node := rawNode.(neo4j.Node)
		expectedLabels := []string{"A Person"}
		actualLabels := node.Labels
		if !reflect.DeepEqual(actualLabels, expectedLabels) {
			t.Errorf("Expected labels %v, got %v", expectedLabels, actualLabels)
		}
		actualProps := node.Props
		if len(actualProps) != 0 {
			t.Errorf("Expected no props, got %v", actualProps)
		}
	})

	outer.Run("two matching nodes with varyings labels", func(t *testing.T) {
		session := driver.NewSession(neo4j.SessionConfig{})
		defer assertCloses(t, session)
		initGraph(t, session, []string{"MATCH (n) DETACH DELETE n", "CREATE (:Person:Robot), (:Person:Human:Being)"})
		tx, err := session.BeginTransaction()
		assertNilError(t, err)

		err = refactoring.MergeNodes(tx, refactoring.Pattern{
			CypherFragment: "(n)",
			OutputVariable: "n",
		}, nil)

		assertNilError(t, err)
		assertNilError(t, tx.Commit())
		assertNilError(t, tx.Close())
		result, err := session.Run(`
	MATCH (n) UNWIND labels(n) AS label
	WITH label ORDER BY label ASC
	RETURN collect(label) AS labels`, nil)
		assertNilError(t, err)
		record, err := result.Single()
		assertNilError(t, err)
		rawLabels, _ := record.Get("labels")
		actualLabels := rawLabels.([]any)
		expectedLabels := []any{"Being", "Human", "Person", "Robot"}
		if !reflect.DeepEqual(actualLabels, expectedLabels) {
			t.Errorf("Expected labels %v, got %v", expectedLabels, actualLabels)
		}
	})

	outer.Run("two matching nodes with varyings labels and unmatched others", func(t *testing.T) {
		session := driver.NewSession(neo4j.SessionConfig{})
		defer assertCloses(t, session)
		initGraph(t, session, []string{"MATCH (n) DETACH DELETE n", "CREATE (:Person:Robot), (:Person:Human:Being), (:Zombie)"})
		tx, err := session.BeginTransaction()
		assertNilError(t, err)

		err = refactoring.MergeNodes(tx, refactoring.Pattern{
			CypherFragment: "(n:Person)",
			OutputVariable: "n",
		}, nil)

		assertNilError(t, err)
		assertNilError(t, tx.Commit())
		assertNilError(t, tx.Close())
		result, err := session.Run(`
	MATCH (n:Person) UNWIND labels(n) AS label
	WITH label ORDER BY label ASC
	RETURN collect(label) AS labels`, nil)
		assertNilError(t, err)
		record, err := result.Single()
		assertNilError(t, err)
		rawLabels, _ := record.Get("labels")
		actualLabels := rawLabels.([]any)
		expectedLabels := []any{"Being", "Human", "Person", "Robot"}
		if !reflect.DeepEqual(actualLabels, expectedLabels) {
			t.Errorf("Expected labels %v, got %v", expectedLabels, actualLabels)
		}
	})

	outer.Run("two matching nodes plus other unmatched", func(t *testing.T) {
		session := driver.NewSession(neo4j.SessionConfig{})
		defer assertCloses(t, session)
		initGraph(t, session, []string{"MATCH (n) DETACH DELETE n", "CREATE (:Person), (:Person), (:Robot {name: 'mystery'})"})
		tx, err := session.BeginTransaction()
		assertNilError(t, err)

		err = refactoring.MergeNodes(tx, refactoring.Pattern{
			CypherFragment: "(n:Person)",
			OutputVariable: "n",
		}, nil)

		assertNilError(t, err)
		assertNilError(t, tx.Commit())
		assertNilError(t, tx.Close())
		result, err := session.Run("MATCH (n:Person) RETURN n", nil)
		assertNilError(t, err)
		record, err := result.Single()
		assertNilError(t, err)
		rawNode, _ := record.Get("n")
		node := rawNode.(neo4j.Node)
		expectedLabels := []string{"Person"}
		actualLabels := node.Labels
		if !reflect.DeepEqual(actualLabels, expectedLabels) {
			t.Errorf("Expected labels %v, got %v", expectedLabels, actualLabels)
		}
		actualProps := node.Props
		if len(actualProps) != 0 {
			t.Errorf("Expected no props, got %v", actualProps)
		}
	})

	outer.Run("disconnected nodes", func(inner *testing.T) {
		type testCase struct {
			base
			expectedNodeName any
		}
		testCases := []testCase{
			{
				base: base{
					name: "keeps all properties",
					initQueries: []string{
						"CREATE (:Person {name: 'Florent'}), (:Person {name: 'Latifa', `the name`: 'ignored by test'})",
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name ASC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy(".*name", refactoring.KeepAll),
					},
				},
				expectedNodeName: []any{"Florent", "Latifa"},
			},
			{
				base: base{
					name: "keeps first property",
					initQueries: []string{
						"CREATE (:Person {name: 'Florent'}), (:Person {name: 'Latifa'})",
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name ASC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy("name", refactoring.KeepFirst),
					},
				},
				expectedNodeName: "Florent",
			},
			{
				base: base{
					name: "keeps first property matching regex",
					initQueries: []string{
						"CREATE (:Person {name: 'Florent'}), (:Person {name: 'Latifa'})",
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name ASC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy("na.*", refactoring.KeepFirst),
					},
				},
				expectedNodeName: "Florent",
			},
			{
				base: base{
					name: "keeps first set property",
					initQueries: []string{
						"CREATE (:Person), (:Person {name: 'Florent'}), (:Person {name: 'Latifa'})",
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name ASC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy("name", refactoring.KeepFirst),
					},
				},
				expectedNodeName: "Florent",
			},
			{
				base: base{
					name: "keeps last property",
					initQueries: []string{
						"CREATE (:Person {name: 'Florent'}), (:Person {name: 'Latifa'})",
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name ASC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy("name", refactoring.KeepLast),
					},
				},
				expectedNodeName: "Latifa",
			},
			{
				base: base{
					name: "keeps last property matching regex",
					initQueries: []string{
						"CREATE (:Person {name: 'Florent'}), (:Person {name: 'Latifa'})",
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name ASC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy(".*e", refactoring.KeepLast),
					},
				},
				expectedNodeName: "Latifa",
			},
			{
				base: base{
					name: "keeps last set property",
					initQueries: []string{
						"CREATE (:Person {name: 'Florent'}), (:Person {name: 'Latifa'}), (:Person)",
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name ASC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy("name", refactoring.KeepLast),
					},
				},
				expectedNodeName: "Latifa",
			},
		}

		for i, testCase := range testCases {
			inner.Run(fmt.Sprintf("[%d] %s", i, testCase.name), func(t *testing.T) {
				session := driver.NewSession(neo4j.SessionConfig{})
				defer assertCloses(t, session)
				initGraph(t, session, append([]string{"MATCH (n) DETACH DELETE n"}, testCase.initQueries...))
				tx, err := session.BeginTransaction()
				assertNilError(t, err)

				err = refactoring.MergeNodes(tx, testCase.pattern, testCase.policies)

				assertNilError(t, err)
				assertNilError(t, tx.Commit())
				assertNilError(t, tx.Close())
				result, err := session.Run("MATCH (p:Person) WHERE p.name IS NOT NULL RETURN p.name AS name", nil)
				assertNilError(t, err)
				record, err := result.Single()
				assertNilError(t, err)
				actual, _ := record.Get("name")
				expected := testCase.expectedNodeName
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("Expected %v got: %v", expected, actual)
				}
			})
		}
	})

	outer.Run("connected nodes", func(inner *testing.T) {
		type testCase struct {
			base
			expectedNodeName any
			expectedIncoming []any
			expectedOutgoing []any
		}

		testCases := []testCase{
			{
				base: base{
					name: "attaches outgoing relationships of merged nodes",
					initQueries: []string{`
		CREATE (florent:Person {name: "Florent"}), (nathan:Person {name: "Nathan"}), (liquibase:Project {name: "Liquibase"})
		CREATE (florent)-[:WORKS_ON {module: "neo4j"}]->(liquibase)
		CREATE (nathan)-[:WORKS_ON {module: "core"}]->(liquibase)
		CREATE (liquibase)-[:FOUNDED_BY]->(nathan)
		`,
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name DESC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy("name", refactoring.KeepFirst),
					},
				},
				expectedNodeName: "Nathan",
				expectedIncoming: []any{
					neo4j.Relationship{
						Type:  "FOUNDED_BY",
						Props: map[string]interface{}{},
					},
				},
				expectedOutgoing: []any{
					neo4j.Relationship{
						Type:  "WORKS_ON",
						Props: map[string]interface{}{"module": "neo4j"},
					},
					neo4j.Relationship{
						Type:  "WORKS_ON",
						Props: map[string]interface{}{"module": "core"},
					},
				},
			},
			{
				base: base{
					name: "attaches incoming relationships of merged nodes",
					initQueries: []string{`
CREATE (florent:Person {name: "Florent"}), (nathan:Person {name: "Nathan"}), (liquigraph:Project {name: "Liquigraph"})
CREATE (florent)<-[:FOUNDED_BY {since: "some date"}]-(liquigraph)
`,
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name DESC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy("name", refactoring.KeepAll),
					},
				},
				expectedNodeName: []any{"Nathan", "Florent"},
				expectedIncoming: []any{
					neo4j.Relationship{
						Type: "FOUNDED_BY",
						Props: map[string]interface{}{
							"since": "some date",
						},
					},
				},
				expectedOutgoing: []any{},
			},
		}

		for i, testCase := range testCases {
			inner.Run(fmt.Sprintf("[%d] %s", i, testCase.name), func(t *testing.T) {
				session := driver.NewSession(neo4j.SessionConfig{})
				defer assertCloses(t, session)
				initGraph(t, session, append([]string{"MATCH (n) DETACH DELETE n"}, testCase.initQueries...))
				tx, err := session.BeginTransaction()
				assertNilError(t, err)

				err = refactoring.MergeNodes(tx, testCase.pattern, testCase.policies)

				assertNilError(t, err)
				assertNilError(t, tx.Commit())
				assertNilError(t, tx.Close())
				result, err := session.Run(`
MATCH (p:Person)
RETURN p.name AS name, [ (p)-[outgoing]->() | outgoing ] AS allOutgoing, [ (p)<-[incoming]-() | incoming ] AS allIncoming
`, nil)
				assertNilError(t, err)
				record, err := result.Single()
				assertNilError(t, err)
				actual, _ := record.Get("name")
				expected := testCase.expectedNodeName
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("Expected %v got: %v", expected, actual)
				}
				rawIncoming, _ := record.Get("allIncoming")
				incoming := rawIncoming.([]any)
				actual = ignoringIds(incoming)
				expected = testCase.expectedIncoming
				if !reflect.DeepEqual(expected, actual) {
					t.Fatalf("Expected %v got: %v", expected, actual)
				}
				rawOutgoing, _ := record.Get("allOutgoing")
				outgoing := rawOutgoing.([]any)
				actual = ignoringIds(outgoing)
				expected = testCase.expectedOutgoing
				if !reflect.DeepEqual(expected, actual) {
					t.Fatalf("Expected %v got: %v", expected, actual)
				}
			})
		}
	})

	outer.Run("nodes with current or post-merge self-refs", func(inner *testing.T) {
		type testCase struct {
			base
			expectedNodeName any
			expectedSelfRels []any
		}

		testCases := []testCase{
			{
				base: base{
					name: "attaches self relationships of merged nodes",
					initQueries: []string{`
CREATE (florent:Person {name: "Florent"}), (nathan:Person {name: "Nathan"})
CREATE (florent)<-[:BLAMES {frequency: "sometimes"}]-(florent)
`,
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name DESC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy("name", refactoring.KeepLast),
					},
				},
				expectedNodeName: "Florent",
				expectedSelfRels: []any{
					neo4j.Relationship{
						Type: "BLAMES",
						Props: map[string]interface{}{
							"frequency": "sometimes",
						},
					},
				},
			},
			{
				base: base{
					name: "yields self relationships when merging nodes linked between themselves",
					initQueries: []string{`
CREATE (:Person {name: "Florent"})-[:` + "`" + `FOLLOWS 1` + "`" + ` {name: 'f to m'}]->(:Person {name: "Marouane"})-[:FOLLOWS_2 {name: 'm to n'}]->(nathan:Person {name: "Nathan"})-[:FOLLOWS_3 {name: 'n to n'}]->(nathan)
`,
					},
					pattern: refactoring.Pattern{
						CypherFragment: "(p:Person) WITH p ORDER BY p.name DESC",
						OutputVariable: "p",
					},
					policies: []refactoring.PropertyMergePolicy{
						refactoring.NewPropertyMergePolicy("name", refactoring.KeepFirst),
					},
				},
				expectedNodeName: "Nathan",
				expectedSelfRels: []any{
					neo4j.Relationship{
						Type: "FOLLOWS 1",
						Props: map[string]interface{}{
							"name": "f to m",
						},
					},
					neo4j.Relationship{
						Type: "FOLLOWS_2",
						Props: map[string]interface{}{
							"name": "m to n",
						},
					},
					neo4j.Relationship{
						Type: "FOLLOWS_3",
						Props: map[string]interface{}{
							"name": "n to n",
						},
					},
				},
			},
		}

		for i, testCase := range testCases {
			inner.Run(fmt.Sprintf("[%d] %s", i, testCase.name), func(t *testing.T) {
				session := driver.NewSession(neo4j.SessionConfig{})
				defer assertCloses(t, session)
				initGraph(t, session, append([]string{"MATCH (n) DETACH DELETE n"}, testCase.initQueries...))
				tx, err := session.BeginTransaction()
				assertNilError(t, err)

				err = refactoring.MergeNodes(tx, testCase.pattern, testCase.policies)

				assertNilError(t, err)
				assertNilError(t, tx.Commit())
				assertNilError(t, tx.Close())
				result, err := session.Run(`
MATCH (p:Person)-[rel]->(p)
WITH p.name AS name, rel
ORDER BY type(rel) ASC
RETURN name, collect(rel) AS rels
`, nil)
				assertNilError(t, err)
				record, err := result.Single()
				assertNilError(t, err)
				actual, _ := record.Get("name")
				expected := testCase.expectedNodeName
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("Expected %v got: %v", expected, actual)
				}
				rawRels, _ := record.Get("rels")
				rel := rawRels.([]any)
				actual = ignoringIds(rel)
				expected = testCase.expectedSelfRels
				if !reflect.DeepEqual(expected, actual) {
					t.Fatalf("Expected %v got: %v", expected, actual)
				}
			})
		}
	})
}

func initGraph(t *testing.T, session neo4j.Session, queries []string) {
	for i, query := range queries {
		result, err := session.Run(query, nil)
		if err != nil {
			t.Fatalf("query execution %d %q failed: %v", i, query, err)
		}
		_, err = result.Consume()
		if err != nil {
			t.Fatalf("result consumption for query %d %q failed: %v", i, query, err)
		}
	}
}

const username = "neo4j"

const password = "s3cr3t"

func startNeo4jContainer(ctx context.Context) (testcontainers.Container, neo4j.Driver, error) {
	request := testcontainers.ContainerRequest{
		Image:        "neo4j:4.4",
		ExposedPorts: []string{"7687/tcp"},
		Env: map[string]string{"NEO4J_AUTH": fmt.Sprintf("%s/%s",
			username, password)},
		WaitingFor: wait.ForLog("Bolt enabled"),
	}
	container, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: request,
			Started:          true,
		})
	if err != nil {
		return nil, nil, err
	}
	driver, err := newNeo4jDriver(ctx, container)
	if err != nil {
		return container, nil, err
	}
	return container, driver, driver.VerifyConnectivity()
}

func newNeo4jDriver(ctx context.Context, container testcontainers.Container) (
	neo4j.Driver, error) {
	port, err := container.MappedPort(ctx, "7687")
	if err != nil {
		return nil, err
	}
	uri := fmt.Sprintf("neo4j://localhost:%d", port.Int())
	return neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""))
}

func assertCloses(t *testing.T, closer io.Closer) {
	assertNilError(t, closer.Close())
}

func assertNilError(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func ignoringIds(rels []any) []any {
	result := make([]any, len(rels))
	for i, rel := range rels {
		relationship := rel.(dbtype.Relationship)
		result[i] = neo4j.Relationship{
			Type:  relationship.Type,
			Props: relationship.Props,
		}
	}
	return result
}
