package refactoring

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"regexp"
	"strings"
)

type PropertyMergePolicy struct {
	regex    *regexp.Regexp
	strategy PropertyMergeStrategy
}

func NewPropertyMergePolicy(nameRegex string, strategy PropertyMergeStrategy) PropertyMergePolicy {
	return PropertyMergePolicy{
		regex:    regexp.MustCompile(nameRegex),
		strategy: strategy,
	}
}

type PropertyMergeStrategy int

const (
	KeepAll PropertyMergeStrategy = iota
	KeepFirst
	KeepLast
)

func (m PropertyMergeStrategy) Combine(values []any) any {
	size := len(values)
	switch m {
	case KeepAll:
		return values
	case KeepFirst:
		if size > 0 {
			return values[0]
		}
		return nil
	case KeepLast:
		if size > 0 {
			return values[size-1]
		}
		return nil
	default:
		panic(fmt.Errorf("unknown merge policy: %v", m))
	}
}

type Pattern struct {
	CypherFragment string
	OutputVariable string
}

func MergeNodes(transaction neo4j.Transaction, pattern Pattern, policies []PropertyMergePolicy) error {
	_, err := MergeNodesFn(pattern, policies)(transaction)
	return err
}

func MergeNodesFn(pattern Pattern, policies []PropertyMergePolicy) neo4j.TransactionWork {
	return func(transaction neo4j.Transaction) (interface{}, error) {
		ids, err := getNodeIds(transaction, pattern)
		if err != nil {
			return nil, err
		}
		if len(ids) < 2 {
			return nil, nil
		}
		if err := copyLabels(transaction, ids); err != nil {
			return nil, err
		}
		if err := copyRelationships(transaction, ids); err != nil {
			return nil, err
		}
		if err := copyProperties(transaction, ids, policies); err != nil {
			return nil, err
		}
		return nil, detachDeleteOtherNodes(transaction, ids)
	}
}

func getNodeIds(transaction neo4j.Transaction, pattern Pattern) ([]int64, error) {
	query := fmt.Sprintf("MATCH %s RETURN id(%s) AS id", pattern.CypherFragment, pattern.OutputVariable)
	result, err := transaction.Run(query, nil)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for result.Next() {
		record := result.Record()
		rawId, _ := record.Get("id")
		ids = append(ids, rawId.(int64))
	}
	return ids, result.Err()
}

func copyLabels(transaction neo4j.Transaction, ids []int64) error {
	result, err := transaction.Run(`
MATCH (n) WHERE ID(n) IN $ids
UNWIND labels(n) AS label
WITH DISTINCT label
ORDER BY label ASC
RETURN collect(label) AS labels
`, map[string]interface{}{"ids": ids[1:]})
	if err != nil {
		return err
	}
	record, err := result.Single()
	if err != nil {
		return err
	}

	labels, _ := record.Get("labels")
	var query strings.Builder
	query.WriteString("MATCH (n) WHERE ID(n) = $id SET n")
	for _, label := range labels.([]any) {
		query.WriteString(":")
		query.WriteString(label.(string))
	}
	result, err = transaction.Run(query.String(), map[string]interface{}{"id": ids[0]})
	if err != nil {
		return err
	}
	_, err = result.Consume()
	return err
}

func copyRelationships(transaction neo4j.Transaction, ids []int64) error {
	if err := copyIncomingRelationships(transaction, ids); err != nil {
		return err
	}
	return copyOutgoingRelationships(transaction, ids)
}

func copyIncomingRelationships(transaction neo4j.Transaction, ids []int64) error {
	result, err := transaction.Run(`MATCH (n) WHERE id(n) IN $ids
WITH [ (n)<-[incoming]-() | incoming ] AS incomingRels
UNWIND incomingRels AS incoming
RETURN incoming
`, map[string]interface{}{"ids": ids[1:]})
	if err != nil {
		return err
	}
	records, err := result.Collect()
	if err != nil {
		return err
	}
	run := false
	parameters := make(map[string]any, 1+len(records))
	parameters["id_end"] = ids[0]
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("MATCH (end) WHERE id(end) = $id_end\n")
	for i, record := range records {
		rawIncoming, _ := record.Get("incoming")
		if rawIncoming == nil {
			continue
		}
		run = true
		incoming := rawIncoming.(neo4j.Relationship)
		parameters[fmt.Sprintf("id_%d", i)] = incoming.StartId
		parameters[fmt.Sprintf("props_%d", i)] = incoming.Props
		queryBuilder.WriteString(fmt.Sprintf("WITH end MATCH (n_%[1]d) WHERE id(n_%[1]d) = $id_%[1]d\n", i))
		queryBuilder.WriteString(fmt.Sprintf("CREATE (n_%[1]d)-[rel_%[1]d:`%[2]s`]->(end) SET rel_%[1]d = $props_%[1]d\n", i, incoming.Type))
	}
	if !run {
		return nil
	}
	query := queryBuilder.String()
	result, err = transaction.Run(query, parameters)
	if err != nil {
		return err
	}
	_, err = result.Consume()
	return err
}

func copyOutgoingRelationships(transaction neo4j.Transaction, ids []int64) error {
	result, err := transaction.Run(`MATCH (n) WHERE id(n) IN $ids
WITH [ (n)-[outgoing]->() | outgoing ] AS outgoingRels
UNWIND outgoingRels AS outgoing
RETURN outgoing
`, map[string]interface{}{"ids": ids[1:]})
	if err != nil {
		return err
	}
	records, err := result.Collect()
	if err != nil {
		return err
	}
	run := false
	parameters := make(map[string]any, 1+len(records))
	parameters["id_start"] = ids[0]
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("MATCH (start) WHERE id(start) = $id_start\n")
	for i, record := range records {
		rawOutgoing, _ := record.Get("outgoing")
		if rawOutgoing == nil {
			continue
		}
		run = true
		outgoing := rawOutgoing.(neo4j.Relationship)
		parameters[fmt.Sprintf("id_%d", i)] = outgoing.EndId
		parameters[fmt.Sprintf("props_%d", i)] = outgoing.Props
		queryBuilder.WriteString(fmt.Sprintf("WITH start MATCH (n_%[1]d) WHERE id(n_%[1]d) = $id_%[1]d\n", i))
		queryBuilder.WriteString(fmt.Sprintf("CREATE (n_%[1]d)<-[rel_%[1]d:`%[2]s`]-(start) SET rel_%[1]d = $props_%[1]d\n", i, outgoing.Type))
	}
	if !run {
		return nil
	}
	query := queryBuilder.String()
	result, err = transaction.Run(query, parameters)
	if err != nil {
		return err
	}
	_, err = result.Consume()
	return err
}

type property struct {
	name  string
	value any
}

func copyProperties(transaction neo4j.Transaction, ids []int64, policies []PropertyMergePolicy) error {
	properties, err := aggregateProperties(transaction, ids, policies)
	if err != nil {
		return err
	}
	return updateProperties(transaction, ids, properties)
}

func aggregateProperties(transaction neo4j.Transaction, ids []int64, policies []PropertyMergePolicy) ([]property, error) {
	result, err := transaction.Run(`UNWIND $ids AS id
MATCH (n) WHERE id(n) = id
UNWIND keys(n) AS key
WITH {key: key, values: collect(n[key])} AS property
RETURN property
`, map[string]interface{}{"ids": ids})
	if err != nil {
		return nil, err
	}
	records, err := result.Collect()
	if err != nil {
		return nil, err
	}
	properties := make([]property, len(records))
	for i, record := range records {
		rawProperty, _ := record.Get("property")
		prop := rawProperty.(map[string]any)
		propertyName := prop["key"].(string)
		policy, found := findPolicy(propertyName, policies)
		if !found {
			return nil, fmt.Errorf("could not find merge policy for property %s", propertyName)
		}
		properties[i] = property{
			name:  propertyName,
			value: policy.strategy.Combine(prop["values"].([]any)),
		}
	}
	return properties, nil
}

func findPolicy(name string, policies []PropertyMergePolicy) (PropertyMergePolicy, bool) {
	for _, policy := range policies {
		if policy.regex.MatchString(name) {
			return policy, true
		}
	}
	return PropertyMergePolicy{}, false
}

func updateProperties(transaction neo4j.Transaction, ids []int64, properties []property) error {
	if len(properties) == 0 {
		return nil
	}
	parameters := make(map[string]any, 1+len(properties))
	parameters["id"] = ids[0]
	var builder strings.Builder
	builder.WriteString("MATCH (n) WHERE id(n) = $id SET ")
	for i, prop := range properties {
		parameter := fmt.Sprintf("prop_%d", i)
		builder.WriteString(fmt.Sprintf("n.`%s` = $%s", prop.name, parameter))
		parameters[parameter] = prop.value
		if i < len(properties)-1 {
			builder.WriteString(", ")
		}
	}
	query := builder.String()
	result, err := transaction.Run(query, parameters)
	if err != nil {
		return err
	}
	if _, err = result.Consume(); err != nil {
		return err
	}
	return nil
}

func detachDeleteOtherNodes(transaction neo4j.Transaction, ids []int64) error {
	result, err := transaction.Run(
		`MATCH (n) WHERE id(n) IN $ids DETACH DELETE n`,
		map[string]interface{}{"ids": ids[1:]},
	)
	if err != nil {
		return err
	}
	_, err = result.Consume()
	return err
}
