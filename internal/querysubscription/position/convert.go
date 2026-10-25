package position

import (
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// FromSource strips adapter-specific progress into the consumer coordinate.
func FromSource(source storage.SourcePosition) *querysubscription.Position {
	if source == nil {
		return nil
	}
	return &querysubscription.Position{SourceID: source.Source(), Value: source.Order()}
}
