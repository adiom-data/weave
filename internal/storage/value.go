package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Canonicalize converts source text into the engine representation for a
// supported scalar type. The returned bytes are stable for equal values.
func Canonicalize(valueType ValueType, data []byte) ([]byte, error) {
	text := string(data)
	switch valueType {
	case TypeBool:
		value, err := strconv.ParseBool(text)
		if err != nil {
			if text == "t" {
				return []byte("true"), nil
			}
			if text == "f" {
				return []byte("false"), nil
			}
			return nil, fmt.Errorf("invalid bool %q", text)
		}
		return []byte(strconv.FormatBool(value)), nil
	case TypeInteger:
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q", text)
		}
		return []byte(strconv.FormatInt(value, 10)), nil
	case TypeDecimal:
		return canonicalDecimal(text)
	case TypeFloat:
		return canonicalFloat(text)
	case TypeText, TypeBytes:
		return append([]byte(nil), data...), nil
	case TypeUUID:
		value := strings.ToLower(strings.ReplaceAll(text, "-", ""))
		if len(value) != 32 {
			return nil, fmt.Errorf("invalid UUID %q", text)
		}
		if _, ok := new(big.Int).SetString(value, 16); !ok {
			return nil, fmt.Errorf("invalid UUID %q", text)
		}
		return []byte(value[:8] + "-" + value[8:12] + "-" + value[12:16] + "-" + value[16:20] + "-" + value[20:]), nil
	case TypeDateTime:
		return nil, errors.New("datetime values use their time.Time representation")
	case TypeList, TypeObject:
		return nil, fmt.Errorf("%s values must be constructed recursively", valueType)
	default:
		return nil, fmt.Errorf("unsupported canonical value type %q", valueType)
	}
}

// ParseScalar converts a source-neutral textual scalar into a logical value.
// Datastore adapters should prefer constructing datetimes from their native
// temporal values and calling NewDateTime.
func ParseScalar(valueType ValueType, data []byte) (Value, error) {
	if valueType == TypeDateTime {
		instant, err := ParseDateTime(string(data))
		if err != nil {
			return Value{}, err
		}
		return NewDateTime(instant), nil
	}
	canonical, err := Canonicalize(valueType, data)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: valueType, State: ValuePresent, Data: canonical}, nil
}

// ParseDateTime accepts the source-neutral textual forms used by tests and
// non-protobuf callers. The public protobuf API uses google.protobuf.Timestamp.
func ParseDateTime(text string) (time.Time, error) {
	layouts := [...]string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00"}
	for _, layout := range layouts {
		instant, err := time.Parse(layout, text)
		if err == nil {
			return NormalizeDateTime(instant), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid datetime %q", text)
}

// NewDateTime constructs a canonical finite UTC datetime. Range restrictions
// belong to datastore and wire adapters, not to the storage-neutral model.
func NewDateTime(instant time.Time) Value {
	return Value{Type: TypeDateTime, State: ValuePresent, DateTime: NormalizeDateTime(instant)}
}

func NormalizeDateTime(instant time.Time) time.Time {
	return instant.UTC().Round(0)
}

func IsCanonicalDateTime(instant time.Time) bool {
	return instant == NormalizeDateTime(instant)
}

func CloneValue(value Value) Value {
	value.Data = append([]byte(nil), value.Data...)
	if value.List != nil {
		source := value.List
		value.List = make([]Value, len(source))
		for i := range source {
			value.List[i] = CloneValue(source[i])
		}
	}
	if value.Object != nil {
		source := value.Object
		value.Object = make(map[string]Value, len(source))
		for key, child := range source {
			value.Object[key] = CloneValue(child)
		}
	}
	return value
}

func EqualValues(left, right Value) bool {
	if left.Type != right.Type || left.State != right.State {
		return false
	}
	switch left.Type {
	case TypeList:
		if len(left.List) != len(right.List) {
			return false
		}
		for i := range left.List {
			if !EqualValues(left.List[i], right.List[i]) {
				return false
			}
		}
		return true
	case TypeObject:
		if len(left.Object) != len(right.Object) {
			return false
		}
		for key, value := range left.Object {
			other, ok := right.Object[key]
			if !ok || !EqualValues(value, other) {
				return false
			}
		}
		return true
	case TypeDateTime:
		a, _ := left.DateTime.MarshalBinary()
		b, _ := right.DateTime.MarshalBinary()
		return bytes.Equal(a, b)
	default:
		return bytes.Equal(left.Data, right.Data)
	}
}

// Lookup resolves an exact object-only path. List positions are deliberately
// not part of the query model.
func Lookup(record Record, path FieldPath) (Value, bool) {
	segments := path.Segments()
	if len(segments) == 0 {
		return Value{State: ValueMissing}, false
	}
	value, ok := record[segments[0]]
	for _, segment := range segments[1:] {
		if ok && value.State == ValueUnavailable {
			return value, true
		}
		if !ok || value.State != ValuePresent || value.Type != TypeObject {
			return Value{State: ValueMissing}, false
		}
		value, ok = value.Object[segment]
	}
	if !ok {
		return Value{State: ValueMissing}, false
	}
	return value, true
}

// SetPath materializes one selected object path into a record. Intermediate
// objects are created as needed; list traversal is intentionally unsupported.
func SetPath(record Record, path FieldPath, value Value) error {
	segments := path.Segments()
	if len(segments) == 0 {
		return errors.New("field path is empty")
	}
	if len(segments) == 1 {
		record[segments[0]] = CloneValue(value)
		return nil
	}
	root, exists := record[segments[0]]
	if !exists {
		root = Value{Type: TypeObject, State: ValuePresent, Object: make(map[string]Value)}
	}
	if root.State != ValuePresent || root.Type != TypeObject {
		return fmt.Errorf("selected path root %q is not an object", segments[0])
	}
	current := root.Object
	for _, segment := range segments[1 : len(segments)-1] {
		child, ok := current[segment]
		if !ok {
			child = Value{Type: TypeObject, State: ValuePresent, Object: make(map[string]Value)}
		}
		if child.State != ValuePresent || child.Type != TypeObject {
			return fmt.Errorf("selected path segment %q is not an object", segment)
		}
		current[segment] = child
		current = child.Object
	}
	current[segments[len(segments)-1]] = CloneValue(value)
	record[segments[0]] = root
	return nil
}

// EvaluatePredicate applies the engine's type-strict, source-independent
// predicate semantics. Different types are unequal and never ordered.
func EvaluatePredicate(predicate *Predicate, record Record) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	if comparison := predicate.Comparison; comparison != nil {
		actual, exists := Lookup(record, comparison.Field)
		if actual.State == ValueUnavailable {
			return false, ErrIncompleteRecord
		}
		if !exists || actual.State != ValuePresent {
			return false, nil
		}
		expected := comparison.Value.Value()
		if actual.Type != expected.Type {
			return comparison.Operator == ComparisonNotEqual, nil
		}
		compared, comparable := CompareValues(actual, expected)
		if !comparable {
			return false, fmt.Errorf("type %q does not support comparisons", actual.Type)
		}
		switch comparison.Operator {
		case ComparisonEqual:
			return compared == 0, nil
		case ComparisonNotEqual:
			return compared != 0, nil
		case ComparisonLess:
			return compared < 0, nil
		case ComparisonLessOrEqual:
			return compared <= 0, nil
		case ComparisonGreater:
			return compared > 0, nil
		case ComparisonGreaterOrEqual:
			return compared >= 0, nil
		default:
			return false, errors.New("unknown comparison operator")
		}
	}
	if nullTest := predicate.Null; nullTest != nil {
		value, exists := Lookup(record, nullTest.Field)
		if value.State == ValueUnavailable {
			return false, ErrIncompleteRecord
		}
		isNull := exists && value.State == ValueNull
		if nullTest.IsNull {
			return isNull, nil
		}
		return exists && value.State == ValuePresent, nil
	}
	if junction := predicate.Junction; junction != nil {
		switch junction.Operator {
		case JunctionAnd:
			for _, operand := range junction.Operands {
				matched, err := EvaluatePredicate(operand, record)
				if err != nil || !matched {
					return matched, err
				}
			}
			return true, nil
		case JunctionOr:
			for _, operand := range junction.Operands {
				matched, err := EvaluatePredicate(operand, record)
				if err != nil {
					return false, err
				}
				if matched {
					return true, nil
				}
			}
			return false, nil
		}
	}
	return false, errors.New("invalid predicate")
}

// CompareValues implements the ordering of canonical scalar values. Values of
// different types are deliberately incomparable; query preparation must make
// predicate endpoints and relation fields agree.
func CompareValues(left, right Value) (int, bool) {
	if left.State != ValuePresent || right.State != ValuePresent || left.Type != right.Type {
		return 0, false
	}
	switch left.Type {
	case TypeInteger:
		// The canonical integer domain is signed int64, not an arbitrary
		// rational. Tree/cursor comparisons must not allocate big numbers.
		x, errX := strconv.ParseInt(string(left.Data), 10, 64)
		y, errY := strconv.ParseInt(string(right.Data), 10, 64)
		if errX != nil || errY != nil {
			return 0, false
		}
		if x < y {
			return -1, true
		}
		if x > y {
			return 1, true
		}
		return 0, true
	case TypeDecimal:
		return compareCanonicalDecimals(string(left.Data), string(right.Data))
	case TypeFloat:
		x, okX := parseFloat(string(left.Data))
		y, okY := parseFloat(string(right.Data))
		if !okX || !okY {
			return 0, false
		}
		if math.IsNaN(x) || math.IsNaN(y) {
			if math.IsNaN(x) && math.IsNaN(y) {
				return 0, true
			}
			if math.IsNaN(x) {
				return 1, true
			}
			return -1, true
		}
		if x < y {
			return -1, true
		}
		if x > y {
			return 1, true
		}
		return 0, true
	case TypeBool, TypeText, TypeBytes, TypeUUID:
		return bytes.Compare(left.Data, right.Data), true
	case TypeDateTime:
		if left.DateTime.Before(right.DateTime) {
			return -1, true
		}
		if left.DateTime.After(right.DateTime) {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

func canonicalDecimal(text string) ([]byte, error) {
	switch text {
	case "NaN", "Infinity", "-Infinity":
		return []byte(text), nil
	}
	sign := ""
	if strings.HasPrefix(text, "+") || strings.HasPrefix(text, "-") {
		if text[0] == '-' {
			sign = "-"
		}
		text = text[1:]
	}
	mantissa, exponentText, hasExponent := strings.Cut(strings.ToLower(text), "e")
	exponent := 0
	maxInt := int(^uint(0) >> 1)
	minInt := -maxInt - 1
	extremeExponent := 0
	if hasExponent {
		parsedExponent, ok := new(big.Int).SetString(exponentText, 10)
		if !ok {
			return nil, fmt.Errorf("invalid decimal exponent %q", exponentText)
		}
		maximum := big.NewInt(int64(maxInt))
		minimum := big.NewInt(int64(minInt))
		switch {
		case parsedExponent.Cmp(maximum) > 0:
			extremeExponent = 1
		case parsedExponent.Cmp(minimum) < 0:
			extremeExponent = -1
		default:
			exponent = int(parsedExponent.Int64())
		}
	}
	integer, fraction, hasPoint := strings.Cut(mantissa, ".")
	if !hasPoint {
		fraction = ""
	}
	digits := integer + fraction
	if digits == "" {
		return nil, errors.New("empty decimal")
	}
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return nil, fmt.Errorf("invalid decimal %q", text)
		}
	}
	if extremeExponent > 0 {
		if strings.Trim(digits, "0") == "" {
			return []byte("0"), nil
		}
		return []byte(sign + "Infinity"), nil
	}
	if extremeExponent < 0 {
		return []byte("0"), nil
	}
	if exponent < minInt+len(fraction) {
		return nil, errors.New("decimal exponent is outside the supported range")
	}
	exponent -= len(fraction)
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return []byte("0"), nil
	}
	trimmed := strings.TrimRight(digits, "0")
	trailingZeros := len(digits) - len(trimmed)
	if exponent > maxInt-trailingZeros {
		return nil, errors.New("decimal exponent is outside the supported range")
	}
	exponent += trailingZeros
	digits = trimmed
	delta := len(digits) - 1
	if exponent > maxInt-delta || exponent < minInt+delta {
		return nil, errors.New("decimal exponent is outside the supported range")
	}
	scientificExponent := exponent + delta
	coefficient := digits[:1]
	if len(digits) > 1 {
		coefficient += "." + digits[1:]
	}
	if scientificExponent != 0 {
		coefficient += "e" + strconv.Itoa(scientificExponent)
	}
	return []byte(sign + coefficient), nil
}

func compareCanonicalDecimals(left, right string) (int, bool) {
	if special, ok := compareSpecialNumber(left, right); ok {
		return special, true
	}
	leftNegative, leftDigits, leftExponent, okLeft := decimalParts(left)
	rightNegative, rightDigits, rightExponent, okRight := decimalParts(right)
	if !okLeft || !okRight {
		return 0, false
	}
	if leftDigits == "0" || rightDigits == "0" {
		if leftDigits == rightDigits {
			return 0, true
		}
		if leftDigits == "0" {
			if rightNegative {
				return 1, true
			}
			return -1, true
		}
		if leftNegative {
			return -1, true
		}
		return 1, true
	}
	if leftNegative != rightNegative {
		if leftNegative {
			return -1, true
		}
		return 1, true
	}
	compared := 0
	if leftExponent < rightExponent {
		compared = -1
	} else if leftExponent > rightExponent {
		compared = 1
	} else {
		width := max(len(leftDigits), len(rightDigits))
		for index := 0; index < width; index++ {
			leftDigit, rightDigit := byte('0'), byte('0')
			if index < len(leftDigits) {
				leftDigit = leftDigits[index]
			}
			if index < len(rightDigits) {
				rightDigit = rightDigits[index]
			}
			if leftDigit < rightDigit {
				compared = -1
				break
			}
			if leftDigit > rightDigit {
				compared = 1
				break
			}
		}
	}
	if leftNegative {
		compared = -compared
	}
	return compared, true
}

func decimalParts(value string) (negative bool, digits string, exponent int, ok bool) {
	if strings.HasPrefix(value, "-") {
		negative = true
		value = value[1:]
	}
	coefficient, exponentText, hasExponent := strings.Cut(value, "e")
	if hasExponent {
		var err error
		exponent, err = strconv.Atoi(exponentText)
		if err != nil {
			return false, "", 0, false
		}
	}
	digits = strings.ReplaceAll(coefficient, ".", "")
	if digits == "" {
		return false, "", 0, false
	}
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return false, "", 0, false
		}
	}
	return negative, digits, exponent, true
}

func canonicalFloat(text string) ([]byte, error) {
	value, ok := parseFloat(text)
	if !ok {
		return nil, fmt.Errorf("invalid float %q", text)
	}
	if math.IsNaN(value) {
		return []byte("NaN"), nil
	}
	if math.IsInf(value, 1) {
		return []byte("Infinity"), nil
	}
	if math.IsInf(value, -1) {
		return []byte("-Infinity"), nil
	}
	if value == 0 {
		return []byte("0"), nil
	}
	return []byte(strconv.FormatFloat(value, 'g', -1, 64)), nil
}

func parseFloat(value string) (float64, bool) {
	switch value {
	case "Infinity":
		return math.Inf(1), true
	case "-Infinity":
		return math.Inf(-1), true
	case "NaN":
		return math.NaN(), true
	default:
		parsed, err := strconv.ParseFloat(value, 64)
		return parsed, err == nil
	}
}

func compareSpecialNumber(a, b string) (int, bool) {
	rank := func(value string) (int, bool) {
		switch value {
		case "-Infinity":
			return -1, true
		case "Infinity":
			return 1, true
		case "NaN":
			return 2, true
		default:
			return 0, false
		}
	}
	ra, oka := rank(a)
	rb, okb := rank(b)
	if !oka && !okb {
		return 0, false
	}
	if !oka {
		ra = 0
	}
	if !okb {
		rb = 0
	}
	return sign(ra - rb), true
}

func sign(value int) int {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
}
