package postgres

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/valueencoding"
)

func (p *plan) canonicalizePredicateValues(predicate *storage.Predicate) error {
	if predicate == nil {
		return nil
	}
	if comparison := predicate.Comparison; comparison != nil {
		if comparison.Value.Type == "" {
			return fmt.Errorf("comparison for %s requires a typed literal", comparison.Field.Display())
		}
		if comparison.Value.Type == storage.TypeList || comparison.Value.Type == storage.TypeObject {
			return fmt.Errorf("comparison type %q is not supported", comparison.Value.Type)
		}
		value := comparison.Value.Value()
		if comparison.Value.Type == storage.TypeDateTime {
			value.DateTime = storage.NormalizeDateTime(value.DateTime)
		} else {
			canonicalBytes, err := storage.Canonicalize(comparison.Value.Type, comparison.Value.Data)
			if err != nil {
				return fmt.Errorf("canonicalize comparison value for %s: %w", comparison.Field.Display(), err)
			}
			value.Data = canonicalBytes
		}
		literal, err := storage.LiteralFromValue(value)
		if err != nil {
			return fmt.Errorf("canonicalize comparison value for %s: %w", comparison.Field.Display(), err)
		}
		comparison.Value = literal
	}
	if predicate.Junction != nil {
		for _, operand := range predicate.Junction.Operands {
			if err := p.canonicalizePredicateValues(operand); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *plan) preparePredicateComparisons(predicate *storage.Predicate) error {
	if predicate == nil {
		return nil
	}
	if comparison := predicate.Comparison; comparison != nil {
		prepared, err := p.adapter.prepareComparison(p.columns[comparison.Field.Root()], comparison)
		if err != nil {
			return fmt.Errorf("normalize comparison value for %s: %w", comparison.Field.Display(), err)
		}
		p.comparisons[comparison] = prepared
	}
	if predicate.Junction != nil {
		for _, operand := range predicate.Junction.Operands {
			if err := p.preparePredicateComparisons(operand); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *plan) validateLocalComparisons(predicate *storage.Predicate) error {
	if predicate == nil {
		return nil
	}
	if predicate.Comparison != nil {
		field := predicate.Comparison.Field
		column := p.columns[field.Root()]
		isJSON := column.typeOID == 114 || column.typeOID == 3802
		if len(field.Segments()) == 1 && !isJSON && !locallyComparable(column) {
			return fmt.Errorf("comparison field %s uses PostgreSQL type %d or collation %q whose semantics cannot be reproduced safely", field.Display(), column.typeOID, column.collation)
		}
		if len(field.Segments()) > 1 && column.typeOID != 114 && column.typeOID != 3802 {
			return fmt.Errorf("nested comparison field %s does not start at a JSON column", field.Display())
		}
	}
	if predicate.Junction != nil {
		for _, operand := range predicate.Junction.Operands {
			if err := p.validateLocalComparisons(operand); err != nil {
				return err
			}
		}
	}
	return nil
}

func locallyComparable(column column) bool {
	switch column.typeOID {
	case 16, 17, 20, 21, 23, 26, 700, 701, 1082, 1114, 1184, 1700, 2950:
		return true
	case 25, 1043:
		return column.collation == "C" || column.collation == "POSIX"
	default:
		return false
	}
}

// Output fields and window boundaries do not change query membership or
// ordering, so they must not prevent live-query sharing or cursor reuse.
func queryFingerprint(query storage.QuerySpec, relationID uint32, schemaVersion ...string) (string, error) {
	filter := clonePredicate(query.Filter)
	if err := canonicalizeFingerprintPredicate(filter); err != nil {
		return "", err
	}
	canonical := struct {
		RelationID uint32
		Schema     []string
		Query      storage.QuerySpec
	}{
		RelationID: relationID,
		Schema:     schemaVersion,
		Query: storage.QuerySpec{
			Relation: query.Relation,
			Filter:   filter,
			Order:    query.Order,
		},
	}
	fingerprintBytes, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(fingerprintBytes)
	return fmt.Sprintf("%x", sum[:]), nil
}

func relationSchemaFingerprint(columns map[string]column) (string, error) {
	type schemaColumn struct {
		Name      string
		OID       uint32
		Primary   bool
		Collation string
		Supported bool
	}
	values := make([]schemaColumn, 0, len(columns))
	for _, value := range columns {
		values = append(values, schemaColumn{Name: value.name, OID: value.typeOID, Primary: value.primary, Collation: value.collation, Supported: value.supported})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", digest[:]), nil
}

func canonicalizeFingerprintPredicate(predicate *storage.Predicate) error {
	if predicate == nil {
		return nil
	}
	if comparison := predicate.Comparison; comparison != nil {
		if comparison.Value.Type == storage.TypeDateTime {
			encoded, err := valueencoding.ScalarBytes(comparison.Value.Value())
			if err != nil {
				return err
			}
			comparison.Value.Data = encoded
			comparison.Value.DateTime = time.Time{}
		} else {
			comparison.Value.Data = append([]byte(nil), comparison.Value.Data...)
		}
	}
	if predicate.Junction != nil {
		for _, operand := range predicate.Junction.Operands {
			if err := canonicalizeFingerprintPredicate(operand); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *plan) requireColumn(name string) error {
	column, ok := p.columns[name]
	if !ok || name == "" {
		return fmt.Errorf("unknown column %q", name)
	}
	if !column.supported {
		return fmt.Errorf("column %q uses unsupported PostgreSQL type %d", name, column.typeOID)
	}
	return nil
}

func (p *plan) requirePath(path storage.FieldPath) error {
	if err := path.Validate(); err != nil {
		return err
	}
	segments := path.Segments()
	if err := p.requireColumn(segments[0]); err != nil {
		return err
	}
	if len(segments) > 1 {
		oid := p.columns[segments[0]].typeOID
		if oid != 114 && oid != 3802 {
			return fmt.Errorf("field path %s traverses non-JSON column %q", path.Display(), segments[0])
		}
	}
	return nil
}

func (p *plan) validatePredicate(predicate *storage.Predicate, needed map[string]struct{}, limits QueryLimits) error {
	nodes := 0
	return p.validatePredicateAt(predicate, needed, 1, &nodes, limits)
}

func (p *plan) validatePredicateAt(predicate *storage.Predicate, needed map[string]struct{}, depth int, nodes *int, limits QueryLimits) error {
	if predicate == nil {
		return nil
	}
	*nodes++
	if depth > limits.MaxPredicateDepth || *nodes > limits.MaxPredicateNodes {
		return errors.New("predicate exceeds depth or node limit")
	}
	set := 0
	if predicate.Comparison != nil {
		set++
	}
	if predicate.Null != nil {
		set++
	}
	if predicate.Junction != nil {
		set++
	}
	if set != 1 {
		return errors.New("predicate must contain exactly one expression")
	}
	if comparison := predicate.Comparison; comparison != nil {
		name := comparison.Field.Root()
		if err := p.requirePath(comparison.Field); err != nil {
			return err
		}
		if comparison.Operator < storage.ComparisonEqual || comparison.Operator > storage.ComparisonGreaterOrEqual {
			return errors.New("comparison operator is required")
		}
		if len(comparison.Value.Data) > limits.MaxPredicateValueBytes {
			return fmt.Errorf("comparison value exceeds %d bytes", limits.MaxPredicateValueBytes)
		}
		needed[name] = struct{}{}
	} else if nullTest := predicate.Null; nullTest != nil {
		name := nullTest.Field.Root()
		if err := p.requirePath(nullTest.Field); err != nil {
			return err
		}
		needed[name] = struct{}{}
	} else if junction := predicate.Junction; junction != nil {
		if (junction.Operator != storage.JunctionAnd && junction.Operator != storage.JunctionOr) || len(junction.Operands) == 0 {
			return errors.New("junction operator and operands are required")
		}
		for _, operand := range junction.Operands {
			if err := p.validatePredicateAt(operand, needed, depth+1, nodes, limits); err != nil {
				return err
			}
		}
	}
	return nil
}

func quote(name string) string { return pgx.Identifier{name}.Sanitize() }

func (p *plan) predicateSQL(predicate *storage.Predicate, args *[]any) (string, error) {
	if predicate == nil {
		return "TRUE", nil
	}
	if comparison := predicate.Comparison; comparison != nil {
		op := map[storage.ComparisonOperator]string{
			storage.ComparisonEqual: "=", storage.ComparisonNotEqual: "<>",
			storage.ComparisonLess: "<", storage.ComparisonLessOrEqual: "<=",
			storage.ComparisonGreater: ">", storage.ComparisonGreaterOrEqual: ">=",
		}[comparison.Operator]
		if op == "" {
			return "", errors.New("unsupported comparison")
		}
		prepared, ok := p.comparisons[comparison]
		if !ok {
			return "", errors.New("comparison was not prepared")
		}
		column := p.columns[comparison.Field.Root()]
		switch prepared.kind {
		case postgresComparisonJSON:
			return p.jsonComparisonSQL(comparison, prepared, op, args)
		case postgresComparisonMismatch:
			if comparison.Operator == storage.ComparisonNotEqual {
				return quote(column.name) + " IS NOT NULL", nil
			}
			return "FALSE", nil
		case postgresComparisonBoundedConstant:
			return boundedIntegerConstantSQL(column, comparison)
		case postgresComparisonDate:
			return datePredicateSQL(column, comparison, prepared, args)
		case postgresComparisonScalar:
			return scalarComparisonSQL(p.adapter, column, comparison, prepared, op, args)
		default:
			return "", errors.New("comparison has an unknown PostgreSQL strategy")
		}
	}
	if nullTest := predicate.Null; nullTest != nil {
		segments := nullTest.Field.Segments()
		column := p.columns[segments[0]]
		if len(segments) > 1 || column.typeOID == 114 || column.typeOID == 3802 {
			expression, err := p.jsonPathSQL(nullTest.Field, args)
			if err != nil {
				return "", err
			}
			typeExpression := p.jsonTypeSQL(nullTest.Field, expression)
			if nullTest.IsNull {
				if len(segments) == 1 {
					return "(" + quote(segments[0]) + " IS NULL OR " + typeExpression + " = 'null')", nil
				}
				return "(" + expression + " IS NOT NULL AND " + typeExpression + " = 'null')", nil
			}
			return "(" + expression + " IS NOT NULL AND " + typeExpression + " <> 'null')", nil
		}
		if nullTest.IsNull {
			return quote(segments[0]) + " IS NULL", nil
		}
		return quote(segments[0]) + " IS NOT NULL", nil
	}
	if junction := predicate.Junction; junction != nil {
		parts := make([]string, 0, len(junction.Operands))
		for _, operand := range junction.Operands {
			part, err := p.predicateSQL(operand, args)
			if err != nil {
				return "", err
			}
			parts = append(parts, "("+part+")")
		}
		op := " AND "
		if junction.Operator == storage.JunctionOr {
			op = " OR "
		}
		return strings.Join(parts, op), nil
	}
	return "", errors.New("invalid predicate")
}

func datePredicateSQL(column column, comparison *storage.Comparison, prepared postgresComparison, args *[]any) (string, error) {
	nativeType, err := postgresParameterType(prepared.parameterOID)
	if err != nil {
		return "", err
	}
	argument, err := encodePostgresDateTime(prepared.parameterOID, comparison.Value.DateTime)
	if err != nil {
		return "", err
	}
	*args = append(*args, argument)
	placeholder := fmt.Sprintf("$%d::%s", len(*args), nativeType)
	timestamp := "(" + placeholder + " AT TIME ZONE 'UTC')"
	date := "(" + timestamp + ")::date"
	exact := "(" + timestamp + " = " + date + "::timestamp)"
	field := quote(column.name)
	switch comparison.Operator {
	case storage.ComparisonEqual:
		return "(" + exact + " AND " + field + " = " + date + ")", nil
	case storage.ComparisonNotEqual:
		return "(" + field + " IS NOT NULL AND NOT (" + exact + " AND " + field + " = " + date + "))", nil
	case storage.ComparisonLess:
		return field + " < (CASE WHEN " + exact + " THEN " + date + " ELSE " + date + " + 1 END)", nil
	case storage.ComparisonLessOrEqual:
		return field + " <= " + date, nil
	case storage.ComparisonGreater:
		return field + " > " + date, nil
	case storage.ComparisonGreaterOrEqual:
		return field + " >= (CASE WHEN " + exact + " THEN " + date + " ELSE " + date + " + 1 END)", nil
	default:
		return "", errors.New("unsupported date comparison")
	}
}

func scalarComparisonSQL(adapter *postgresTypeAdapter, column column, comparison *storage.Comparison, prepared postgresComparison, op string, args *[]any) (string, error) {
	argument := any(string(comparison.Value.Data))
	if comparison.Value.Type == storage.TypeBytes {
		argument = append([]byte(nil), comparison.Value.Data...)
	} else if comparison.Value.Type == storage.TypeDateTime {
		var err error
		argument, err = adapter.encodeScalar(prepared.parameterOID, comparison.Value.Value())
		if err != nil {
			return "", err
		}
	}
	nativeType, err := postgresParameterType(prepared.parameterOID)
	if err != nil {
		return "", err
	}
	*args = append(*args, argument)
	return fmt.Sprintf("%s %s $%d::%s", quote(column.name), op, len(*args), nativeType), nil
}

func boundedIntegerConstantSQL(column column, comparison *storage.Comparison) (string, error) {
	value, err := strconv.ParseInt(string(comparison.Value.Data), 10, 64)
	if err != nil {
		return "", err
	}
	minimum, maximum, supported := boundedIntegerDomain(column.typeOID)
	if !supported {
		return "", fmt.Errorf("PostgreSQL type %d is not a bounded integer", column.typeOID)
	}
	field := quote(column.name)
	matchesEveryNonNull := false
	switch comparison.Operator {
	case storage.ComparisonNotEqual:
		matchesEveryNonNull = true
	case storage.ComparisonLess, storage.ComparisonLessOrEqual:
		matchesEveryNonNull = value > maximum
	case storage.ComparisonGreater, storage.ComparisonGreaterOrEqual:
		matchesEveryNonNull = value < minimum
	}
	if matchesEveryNonNull {
		return field + " IS NOT NULL", nil
	}
	return "FALSE", nil
}

func boundedIntegerDomain(oid uint32) (minimum, maximum int64, supported bool) {
	switch oid {
	case pgtype.Int2OID:
		return -32768, 32767, true
	case pgtype.Int4OID:
		return -2147483648, 2147483647, true
	case pgtype.OIDOID:
		return 0, 4294967295, true
	default:
		return 0, 0, false
	}
}

func (p *plan) jsonPathSQL(path storage.FieldPath, args *[]any) (string, error) {
	segments := path.Segments()
	if len(segments) == 0 {
		return "", errors.New("JSON path is empty")
	}
	if len(segments) == 1 {
		return "(" + quote(segments[0]) + ")", nil
	}
	// The text overload extracts object keys only, unlike #> which treats
	// numeric segments as array indexes. Numeric object keys remain valid.
	expression := quote(segments[0])
	for _, segment := range segments[1:] {
		*args = append(*args, segment)
		expression += fmt.Sprintf(" -> $%d::text", len(*args))
	}
	return "(" + expression + ")", nil
}

func (p *plan) jsonTypeSQL(path storage.FieldPath, expression string) string {
	if p.columns[path.Root()].typeOID == 114 {
		return "json_typeof(" + expression + ")"
	}
	return "jsonb_typeof(" + expression + ")"
}

func (p *plan) jsonComparisonSQL(comparison *storage.Comparison, prepared postgresComparison, op string, args *[]any) (string, error) {
	expression, err := p.jsonPathSQL(comparison.Field, args)
	if err != nil {
		return "", err
	}
	typeExpression := p.jsonTypeSQL(comparison.Field, expression)
	scalarExpression := "(" + expression + " #>> '{}')"
	argument := any(string(comparison.Value.Data))
	valueExpression := scalarExpression
	switch comparison.Value.Type {
	case storage.TypeBool:
		valueExpression += "::boolean"
	case storage.TypeInteger, storage.TypeDecimal:
		valueExpression += "::numeric"
	case storage.TypeText:
		// Engine text ordering is bytewise. Make PostgreSQL use the matching C
		// collation instead of the database's locale-dependent default.
		valueExpression = "(" + scalarExpression + " COLLATE \"C\")"
	default:
		if comparison.Operator == storage.ComparisonNotEqual {
			return "(" + expression + " IS NOT NULL AND " + typeExpression + " <> 'null')", nil
		}
		return "FALSE", nil
	}
	nativeType, err := postgresParameterType(prepared.parameterOID)
	if err != nil {
		return "", err
	}
	*args = append(*args, argument)
	placeholder := fmt.Sprintf("$%d::%s", len(*args), nativeType)
	typeGuard := ""
	// JSON integers use the same lexical and int64-range rules as
	// logicalJSONValue. Keep the numeric cast inside CASE: PostgreSQL can
	// reorder AND operands, including for nonnumeric JSON values.
	integerGuard := "CASE WHEN " + typeExpression + " = 'number' THEN (" + scalarExpression + " ~ '^-?(0|[1-9][0-9]*)$' AND " + scalarExpression + "::numeric BETWEEN -9223372036854775808 AND 9223372036854775807) ELSE FALSE END"
	switch comparison.Value.Type {
	case storage.TypeBool:
		typeGuard = typeExpression + " = 'boolean'"
	case storage.TypeText:
		typeGuard = typeExpression + " = 'string'"
	case storage.TypeInteger:
		typeGuard = integerGuard
	case storage.TypeDecimal:
		typeGuard = typeExpression + " = 'number' AND NOT (" + integerGuard + ")"
	case storage.TypeFloat:
		if comparison.Operator == storage.ComparisonNotEqual {
			return "(" + expression + " IS NOT NULL AND " + typeExpression + " <> 'null')", nil
		}
		return "FALSE", nil // JSON numbers decode as integer or decimal, never float.
	}
	// PostgreSQL may reorder boolean operands, so an AND type guard does not
	// make a cast safe. Put the cast-bearing comparison inside CASE; a value of
	// another JSON type must never reach ::numeric or ::boolean.
	guardedMatch := "CASE WHEN " + typeGuard + " THEN " + valueExpression + " " + op + " " + placeholder + " ELSE FALSE END"
	if comparison.Operator == storage.ComparisonNotEqual {
		guardedMatch = "CASE WHEN " + typeGuard + " THEN " + valueExpression + " = " + placeholder + " ELSE FALSE END"
		return "(" + expression + " IS NOT NULL AND " + typeExpression + " <> 'null' AND NOT (" + guardedMatch + "))", nil
	}
	return "(" + guardedMatch + ")", nil
}
