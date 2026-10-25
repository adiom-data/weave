package identity

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/adiom-data/weave/pkg/storage"
)

// Config and JWT values use JSON types, without application-specific inference
// (a UUID-looking string, for example, remains a string). Numbers remain exact.
func logicalValue(input any) (storage.Value, error) {
	v := storage.Value{State: storage.ValuePresent}
	switch x := input.(type) {
	case nil:
		v.State = storage.ValueNull
	case string:
		v.Type, v.Data = storage.TypeText, []byte(x)
	case bool:
		v.Type, v.Data = storage.TypeBool, []byte(strconv.FormatBool(x))
	case json.Number:
		t := storage.TypeDecimal
		if _, err := x.Int64(); err == nil {
			t = storage.TypeInteger
		}
		return storage.ParseScalar(t, []byte(x))
	case []any:
		v.Type = storage.TypeList
		v.List = make([]storage.Value, len(x))
		for i, item := range x {
			child, err := logicalValue(item)
			if err != nil {
				return v, err
			}
			v.List[i] = child
		}
	case map[string]any:
		v.Type = storage.TypeObject
		v.Object = make(map[string]storage.Value, len(x))
		for key, item := range x {
			child, err := logicalValue(item)
			if err != nil {
				return v, fmt.Errorf("field %q: %w", key, err)
			}
			v.Object[key] = child
		}
	default:
		return v, fmt.Errorf("unsupported value type %T", input)
	}
	return v, nil
}
