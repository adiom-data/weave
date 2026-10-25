// Package mutation provides optional, transport-independent mutation correlation.
package mutation

import impl "github.com/adiom-data/weave/internal/mutation"

type Context = impl.Context
type HeaderNames = impl.HeaderNames

const ConnectionContextHeader = impl.ConnectionContextHeader
const MutationIDHeader = impl.MutationIDHeader
const ReceiptKind = impl.ReceiptKind

func DefaultHeaderNames() HeaderNames { return impl.DefaultHeaderNames() }
func ReadHeaders(get func(string) string, names HeaderNames) (Context, error) {
	return impl.ReadHeaders(get, names)
}
