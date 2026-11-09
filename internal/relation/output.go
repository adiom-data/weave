package relation

import (
	"errors"
	"fmt"

	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"github.com/adiom-data/weave/internal/storage"
)

// canonicalOutput is the single CEL-to-canonical boundary. It does not coerce
// types. Returned canonical containers are borrowed immutable evidence.
func canonicalOutput(value ref.Val, remaining *int) (storage.Value, error) {
	if types.IsError(value) {
		return storage.Value{}, fmt.Errorf("computed expression: %v", value)
	}
	if v, ok := fromCEL(value); ok {
		return v, nil
	}
	consume := func() error {
		*remaining--
		if *remaining < 0 {
			return errors.New("relation output work limit exceeded")
		}
		return nil
	}
	switch v := value.(type) {
	case traits.Lister:
		result := storage.Value{Type: storage.TypeList, State: storage.ValuePresent}
		for i := types.Int(0); i < v.Size().(types.Int); i++ {
			if err := consume(); err != nil {
				return storage.Value{}, err
			}
			x, err := canonicalOutput(v.Get(i), remaining)
			if err != nil {
				return storage.Value{}, err
			}
			if x.State == storage.ValueUnavailable {
				return x, nil
			}
			result.List = append(result.List, x)
		}
		return result, nil
	case traits.Mapper:
		result := storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{}}
		iterator := v.Iterator()
		for iterator.HasNext() == types.True {
			if err := consume(); err != nil {
				return storage.Value{}, err
			}
			key, ok := iterator.Next().(types.String)
			if !ok {
				return storage.Value{}, errors.New("computed objects require string keys")
			}
			x, err := canonicalOutput(v.Get(key), remaining)
			if err != nil {
				return storage.Value{}, err
			}
			if x.State == storage.ValueUnavailable {
				return x, nil
			}
			result.Object[string(key)] = x
		}
		return result, nil
	default:
		return storage.Value{}, fmt.Errorf("computed expression returned unsupported type %s", value.Type().TypeName())
	}
}
