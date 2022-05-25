package refactoring

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"io"
	"strings"
)

type MergePolicy int

func (m MergePolicy) Combine(values []any) []any {
	size := len(values)
	switch m {
	case KeepAll:
		return values
	case KeepFirst:
		if size > 0 {
			return values[0:1]
		}
		return nil
	case KeepLast:
		if size > 0 {
			return values[size-1 : size]
		}
		return nil
	default:
		panic(fmt.Errorf("unknown merge policy: %v", m))
	}
}

const (
	KeepAll MergePolicy = iota
	KeepFirst
	KeepLast
)

type Pattern struct {
	CypherFragment string
	OutputVariable string
}

type GraphRefactorings interface {
	MergeNodes(pattern Pattern, policies map[string]MergePolicy) error
}

type graphRefactorer struct {
	driver neo4j.Driver
}

func NewGraphRefactorer(driver neo4j.Driver) GraphRefactorings {
	return &graphRefactorer{driver: driver}
}

func (g *graphRefactorer) MergeNodes(pattern Pattern, policies map[string]MergePolicy) (err error) {
	session := g.driver.NewSession(neo4j.SessionConfig{})
	defer func() {
		err = terminateCloser(session, err)
	}()
	transaction, err := session.BeginTransaction()
	if err != nil {
		return err
	}
	defer func() {
		err = terminateCloser(transaction, err)
	}()
	result, err := transaction.Run(fmt.Sprintf(`MATCH %s
UNWIND keys(%[2]s) AS key
WITH {key: key, values: collect(%[2]s[key])} AS property, tail(collect(%[2]s)) AS rest
UNWIND rest AS otherNode
DELETE otherNode
RETURN property
`, pattern.CypherFragment, pattern.OutputVariable), nil)
	if err != nil {
		return err
	}
	records, err := result.Collect()
	if err != nil {
		return err
	}
	properties := make([]property, len(records))
	for i, record := range records {
		rawProperty, _ := record.Get("property")
		prop := rawProperty.(map[string]any)
		propertyName := prop["key"].(string)
		policy, found := policies[propertyName]
		if !found {
			return fmt.Errorf("could not find merge policy for property %s", propertyName)
		}
		properties[i] = property{
			name:  propertyName,
			value: policy.Combine(prop["values"].([]any)),
		}
	}

	cypherQuery, parameters := updateProperties(pattern, properties)
	result, err = transaction.Run(cypherQuery, parameters)
	if err != nil {
		return err
	}
	if _, err = result.Consume(); err != nil {
		return nil
	}

	if err = transaction.Commit(); err != nil {
		return err
	}
	return nil
}

func updateProperties(pattern Pattern, properties []property) (string, map[string]any) {
	parameters := make(map[string]any, len(properties))
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("MATCH %s SET ", pattern.CypherFragment))
	for i, prop := range properties {
		parameter := fmt.Sprintf("prop_%d", i)
		builder.WriteString(fmt.Sprintf("%s.`%s` = $%s", pattern.OutputVariable, prop.name, parameter))
		parameters[parameter] = prop.value
		if i < len(properties)-1 {
			builder.WriteString(", ")
		}
	}
	return builder.String(), parameters
}

type property struct {
	name  string
	value any
}

func terminateCloser(closer io.Closer, previousErr error) error {
	err := closer.Close()
	if err == nil {
		return previousErr
	}
	if previousErr == nil {
		return err
	}
	return fmt.Errorf("error %v occurred after %w", err, previousErr)
}
