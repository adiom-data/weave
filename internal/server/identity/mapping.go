package identity

import (
	"fmt"
	"sort"

	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/storage"
)

type claimMapping struct {
	name string
	list bool
}
type claimsMapper []claimMapping

// The caller validates AuthConfig before compiling. The immutable plan does not
// retain the caller's map, and conversion is never inferred from string contents.
func newClaimsMapper(types map[string]string) claimsMapper {
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make(claimsMapper, 0, len(names))
	for _, name := range names {
		result = append(result, claimMapping{name: name, list: types[name] == config.ClaimTypeUUIDList})
	}
	return result
}

func (m claimsMapper) decode(raw map[string]any) (accesscontrol.Claims, error) {
	result, err := claimsFromJSON(raw)
	if err != nil {
		return nil, err
	}
	for _, mapping := range m {
		value, exists := result[mapping.name]
		if !exists {
			continue
		}
		if !mapping.list {
			value, err = uuidClaim(value)
			if err != nil {
				return nil, fmt.Errorf("claim %q must be a UUID string", mapping.name)
			}
		} else {
			if value.State != storage.ValuePresent || value.Type != storage.TypeList {
				return nil, fmt.Errorf("claim %q must be a list of UUID strings", mapping.name)
			}
			for index, element := range value.List {
				value.List[index], err = uuidClaim(element)
				if err != nil {
					return nil, fmt.Errorf("claim %q[%d] must be a UUID string", mapping.name, index)
				}
			}
		}
		result[mapping.name] = value
	}
	return result, nil
}

func uuidClaim(value storage.Value) (storage.Value, error) {
	if value.State != storage.ValuePresent || value.Type != storage.TypeText {
		return storage.Value{}, fmt.Errorf("expected UUID string")
	}
	return storage.ParseScalar(storage.TypeUUID, value.Data)
}

// ValidateFixedClaims performs the same mapping as admission, without creating
// an OIDC verifier or doing network IO. Invalid fixed values fail startup/check-config.
func ValidateFixedClaims(c config.AuthConfig) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.Mode != "fixed" {
		return nil
	}
	_, err := newClaimsMapper(c.ClaimTypes).decode(map[string]any(c.FixedClaims))
	return err
}
