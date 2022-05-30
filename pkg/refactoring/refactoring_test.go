package refactoring_test

import (
	"context"
	"fmt"
	"github.com/fbiville/node-clone/pkg/refactoring"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"reflect"
	"testing"
)

const username = "neo4j"

const password = "s3cr3t"

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

	refactorer := refactoring.NewGraphRefactorer(driver)

	type testCase struct {
		initQueries    []string
		pattern        refactoring.Pattern
		policies       map[string]refactoring.MergePolicy
		expectedResult []string
	}
	pattern := refactoring.Pattern{
		CypherFragment: "(p:Person) WITH p ORDER BY p.name ASC",
		OutputVariable: "p",
	}
	testCases := []testCase{
		{
			initQueries: []string{
				"CREATE (:Person {name: 'Florent'}), (:Person {name: 'Latifa'})",
			},
			pattern: pattern,
			policies: map[string]refactoring.MergePolicy{
				"name": refactoring.KeepAll,
			},
			expectedResult: []string{
				"Florent", "Latifa",
			},
		},
		{
			initQueries: []string{
				"CREATE (:Person {name: 'Florent'}), (:Person {name: 'Latifa'})",
			},
			pattern: pattern,
			policies: map[string]refactoring.MergePolicy{
				"name": refactoring.KeepFirst,
			},
			expectedResult: []string{
				"Florent",
			},
		},
		{
			initQueries: []string{
				"CREATE (:Person {name: 'Florent'}), (:Person {name: 'Latifa'})",
			},
			pattern: pattern,
			policies: map[string]refactoring.MergePolicy{
				"name": refactoring.KeepLast,
			},
			expectedResult: []string{
				"Latifa",
			},
		},
	}

	for i, testCase := range testCases {
		outer.Run(fmt.Sprintf("[%d] merge node properties %v", i, testCase.policies), func(t *testing.T) {
			session := driver.NewSession(neo4j.SessionConfig{})
			initGraph(t, session, append([]string{"MATCH (n) DETACH DELETE n"}, testCase.initQueries...))

			err := refactorer.MergeNodes(testCase.pattern, testCase.policies)

			assertNilError(t, err)
			defer assertCloses(t, session)
			result, err := session.Run("MATCH (p:Person) WHERE p.name IS NOT NULL RETURN p.name AS name", nil)
			assertNilError(t, err)
			record, err := result.Single()
			assertNilError(t, err)
			actual, _ := record.Get("name")
			expected := testCase.expectedResult
			if !reflect.DeepEqual(asStringSlice(actual.([]any)), expected) {
				t.Fatalf("Expected %v got: %v", expected, actual)
			}
		})
	}
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

func runQuery(t *testing.T, driver neo4j.Driver, query string) {
	session := driver.NewSession(neo4j.SessionConfig{})
	defer assertCloses(t, session)
	result, err := session.Run(query, nil)
	assertNilError(t, err)
	_, err = result.Consume()
	assertNilError(t, err)
}

func asStringSlice(slice []any) []string {
	result := make([]string, len(slice))
	for i, element := range slice {
		result[i] = element.(string)
	}
	return result
}
