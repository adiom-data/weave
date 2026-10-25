// Package value defines policy operations on the engine's canonical values.
// It has no dependency on CEL or either expression execution strategy.
package value

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/adiom-data/weave/internal/storage"
)

var (
	True    = storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")}
	False   = storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("false")}
	Null    = storage.Value{State: storage.ValueNull}
	Unknown = storage.Value{State: storage.ValueUnavailable}
)

func Bool(b bool) storage.Value {
	if b {
		return True
	}
	return False
}
func Int(n int64) storage.Value {
	return storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: strconv.AppendInt(nil, n, 10)}
}
func IsTrue(v storage.Value) bool {
	return v.State == storage.ValuePresent && v.Type == storage.TypeBool && string(v.Data) == "true"
}

// Equal is type-strict and structural. Missing reads are null-like, but an
// absent object key is not an explicitly null key. Unequal structure can settle
// equality even if another child is unresolved.
func Equal(a, b storage.Value) (storage.Value, error) {
	if a.State == storage.ValueUnavailable || b.State == storage.ValueUnavailable {
		return Unknown, nil
	}
	if a.State != storage.ValuePresent || b.State != storage.ValuePresent {
		return Bool(a.State != storage.ValuePresent && b.State != storage.ValuePresent), nil
	}
	if a.Type != b.Type {
		return False, nil
	}
	switch a.Type {
	case storage.TypeList:
		if len(a.List) != len(b.List) {
			return False, nil
		}
		pending := false
		for i := range a.List {
			v, err := Equal(a.List[i], b.List[i])
			if err != nil {
				return Null, err
			}
			if v.State == storage.ValueUnavailable {
				pending = true
			} else if !IsTrue(v) {
				return False, nil
			}
		}
		if pending {
			return Unknown, nil
		}
		return True, nil
	case storage.TypeObject:
		if len(a.Object) != len(b.Object) {
			return False, nil
		}
		pending := false
		for k, av := range a.Object {
			bv, found := b.Object[k]
			if !found {
				return False, nil
			}
			v, err := Equal(av, bv)
			if err != nil {
				return Null, err
			}
			if v.State == storage.ValueUnavailable {
				pending = true
			} else if !IsTrue(v) {
				return False, nil
			}
		}
		if pending {
			return Unknown, nil
		}
		return True, nil
	case storage.TypeFloat:
		x, err := strconv.ParseFloat(string(a.Data), 64)
		if err != nil {
			return Null, err
		}
		y, err := strconv.ParseFloat(string(b.Data), 64)
		if err != nil {
			return Null, err
		}
		return Bool(x == y), nil
	default:
		order, err := Compare(a, b)
		return Bool(order == 0), err
	}
}

// Compare never coerces operands. Float NaN is handled by Relational rather
// than the datastore's total ordering, which is intended for query indexes.
func Compare(a, b storage.Value) (int, error) {
	if a.State != storage.ValuePresent || b.State != storage.ValuePresent || a.Type != b.Type {
		return 0, fmt.Errorf("incompatible policy comparison: %s and %s", a.Type, b.Type)
	}
	switch a.Type {
	case storage.TypeInteger:
		x, e := strconv.ParseInt(string(a.Data), 10, 64)
		if e != nil {
			return 0, e
		}
		y, e := strconv.ParseInt(string(b.Data), 10, 64)
		if e != nil {
			return 0, e
		}
		if x < y {
			return -1, nil
		}
		if x > y {
			return 1, nil
		}
		return 0, nil
	case storage.TypeText, storage.TypeUUID, storage.TypeBytes:
		return bytes.Compare(a.Data, b.Data), nil
	default:
		if order, ok := storage.CompareValues(a, b); ok {
			return order, nil
		}
		return 0, fmt.Errorf("unsupported policy comparison: %s", a.Type)
	}
}

func Relational(op string, a, b storage.Value) (storage.Value, error) {
	if a.State == storage.ValueUnavailable || b.State == storage.ValueUnavailable {
		return Unknown, nil
	}
	order, err := Compare(a, b)
	if err != nil {
		return Null, err
	}
	if a.Type == storage.TypeFloat {
		x, _ := strconv.ParseFloat(string(a.Data), 64)
		y, _ := strconv.ParseFloat(string(b.Data), 64)
		if math.IsNaN(x) || math.IsNaN(y) {
			return False, nil
		}
	}
	switch op {
	case "<":
		return Bool(order < 0), nil
	case "<=":
		return Bool(order <= 0), nil
	case ">":
		return Bool(order > 0), nil
	case ">=":
		return Bool(order >= 0), nil
	}
	return Null, fmt.Errorf("unknown comparison %q", op)
}

func Contains(collection, needle storage.Value) (storage.Value, error) {
	if collection.State == storage.ValueUnavailable || needle.State == storage.ValueUnavailable {
		return Unknown, nil
	}
	if collection.State != storage.ValuePresent {
		return Null, fmt.Errorf("membership requires a collection")
	}
	switch collection.Type {
	case storage.TypeObject:
		if needle.Type != storage.TypeText || needle.State != storage.ValuePresent {
			return False, nil
		}
		_, ok := collection.Object[string(needle.Data)]
		return Bool(ok), nil
	case storage.TypeList:
		pending := false
		for _, child := range collection.List {
			result, err := Equal(needle, child)
			if err != nil {
				return Null, err
			}
			if IsTrue(result) {
				return True, nil
			}
			pending = pending || result.State == storage.ValueUnavailable
		}
		if pending {
			return Unknown, nil
		}
		return False, nil
	default:
		return Null, fmt.Errorf("membership requires a collection")
	}
}

func Size(v storage.Value) (int64, error) {
	if v.State != storage.ValuePresent {
		return 0, fmt.Errorf("size requires a present value")
	}
	switch v.Type {
	case storage.TypeText:
		return int64(utf8.RuneCount(v.Data)), nil
	case storage.TypeBytes:
		return int64(len(v.Data)), nil
	case storage.TypeList:
		return int64(len(v.List)), nil
	case storage.TypeObject:
		return int64(len(v.Object)), nil
	}
	return 0, fmt.Errorf("size unsupported for %s", v.Type)
}

func StringPredicate(op string, a, b storage.Value) (storage.Value, error) {
	if a.State == storage.ValueUnavailable || b.State == storage.ValueUnavailable {
		return Unknown, nil
	}
	if a.State != storage.ValuePresent || b.State != storage.ValuePresent || a.Type != storage.TypeText || b.Type != storage.TypeText {
		return Null, fmt.Errorf("%s requires text operands", op)
	}
	x, y := string(a.Data), string(b.Data)
	switch op {
	case "startsWith":
		return Bool(strings.HasPrefix(x, y)), nil
	case "endsWith":
		return Bool(strings.HasSuffix(x, y)), nil
	case "contains":
		return Bool(strings.Contains(x, y)), nil
	}
	return Null, fmt.Errorf("unknown string predicate %q", op)
}
