package storage

import "errors"

// CursorBounds is an opaque-key (left, right] interval. Native key decoding and
// ordering belong to the datastore's prepared query, not its consumers.
type CursorBoundaryKind uint8

const (
	CursorStart CursorBoundaryKind = iota + 1
	CursorKey
	CursorEnd
)

type CursorBoundary struct {
	Kind   CursorBoundaryKind
	Cursor []byte
}
type CursorBounds struct{ Left, Right CursorBoundary }

func (b CursorBounds) Validate() error {
	for _, boundary := range []CursorBoundary{b.Left, b.Right} {
		switch boundary.Kind {
		case CursorStart, CursorEnd:
			if len(boundary.Cursor) > 0 {
				return errors.New("sentinel boundary cannot contain a cursor")
			}
		case CursorKey:
			if len(boundary.Cursor) == 0 {
				return errors.New("cursor boundary requires a key")
			}
		default:
			return errors.New("invalid cursor boundary kind")
		}
	}
	return nil
}
func (b CursorBounds) Clone() *CursorBounds {
	b.Left.Cursor = append([]byte(nil), b.Left.Cursor...)
	b.Right.Cursor = append([]byte(nil), b.Right.Cursor...)
	return &b
}
