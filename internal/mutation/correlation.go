// Package mutation defines optional mutation correlation, not authentication.
package mutation

import (
	"encoding/base64"
	"errors"
	"strings"

	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

const ConnectionContextHeader = "X-Weave-Connection-Context"
const MutationIDHeader = "X-Weave-Mutation-Id"
const ReceiptKind = "weave.mutation.committed"

// Context identifies a mutation and its reactive recipient. It is not proof of
// identity or permission. The zero context means no receipt was requested.
type Context struct {
	ConnectionContext string
	MutationID        string
}

// HeaderNames allows applications to override either default header name.
type HeaderNames struct {
	ConnectionContext string
	MutationID        string
}

func DefaultHeaderNames() HeaderNames {
	return HeaderNames{ConnectionContext: ConnectionContextHeader, MutationID: MutationIDHeader}
}

// ReadHeaders works with http.Header.Get (including Connect requests) or a
// getter over gRPC metadata. It checks format, not whether a connection exists.
func ReadHeaders(get func(string) string, names HeaderNames) (Context, error) {
	if get == nil {
		return Context{}, errors.New("header getter is required")
	}
	if names.ConnectionContext == "" {
		names.ConnectionContext = ConnectionContextHeader
	}
	if names.MutationID == "" {
		names.MutationID = MutationIDHeader
	}
	if strings.EqualFold(names.ConnectionContext, names.MutationID) {
		return Context{}, errors.New("correlation header names must differ")
	}
	c := Context{ConnectionContext: get(names.ConnectionContext), MutationID: get(names.MutationID)}
	return c, c.Validate()
}

func (c Context) Validate() error {
	if c == (Context{}) {
		return nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(c.ConnectionContext)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != c.ConnectionContext {
		return errors.New("connection context must be the opaque context from the reactive handshake")
	}
	if len(c.MutationID) != 36 {
		return errors.New("mutation ID must be a canonical UUID")
	}
	for i, ch := range c.MutationID {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if ch == '-' {
				continue
			}
		} else if ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f' {
			continue
		}
		return errors.New("mutation ID must be a canonical UUID")
	}
	return nil
}

// Receipt returns nil for a nonreactive request. The payload is the mutation
// ID's UTF-8 bytes; the surrounding message routes it to the connection.
func (c Context) Receipt() (*reactivev1.RoutedTransactionMessage, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c == (Context{}) {
		return nil, nil
	}
	return &reactivev1.RoutedTransactionMessage{ConnectionContext: c.ConnectionContext, Kind: ReceiptKind, Payload: []byte(c.MutationID)}, nil
}
