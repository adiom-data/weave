package datastore

import (
	"context"

	"github.com/adiom-data/weave/internal/storage"
)

// MetadataManager is optional. Invalidation forgets successful discovery for
// future preparations only. A nil relation means all tables. Existing prepared
// plans remain immutable and valid under the source's schema-stability contract.
type MetadataManager interface {
	InvalidateMetadata(context.Context, *storage.RelationName) error
}
