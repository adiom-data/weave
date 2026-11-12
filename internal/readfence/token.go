// Package readfence owns opaque, versioned read-after-write tokens. Tokens are
// ordering requirements, not authorization credentials or durable receipts.
package readfence

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

type token struct {
	Version  int     `json:"v"`
	Position *uint64 `json:"p"`
}

func Encode(position uint64) string {
	data, _ := json.Marshal(token{Version: 1, Position: &position})
	return base64.RawURLEncoding.EncodeToString(data)
}

func Decode(value string) (uint64, error) {
	if len(value) == 0 || len(value) > 1024 {
		return 0, errors.New("invalid read fence length")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, errors.New("invalid read fence encoding")
	}
	var parsed token
	if err := json.Unmarshal(data, &parsed); err != nil || parsed.Version != 1 || parsed.Position == nil {
		return 0, errors.New("invalid or unsupported read fence")
	}
	return *parsed.Position, nil
}
