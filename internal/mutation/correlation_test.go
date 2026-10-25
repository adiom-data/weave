package mutation

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"
)

func validContext() Context {
	return Context{ConnectionContext: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), MutationID: "12345678-1234-1234-1234-123456789abc"}
}

func TestReadHeaders(t *testing.T) {
	want := validContext()
	for _, names := range []HeaderNames{{}, {ConnectionContext: "X-Connection", MutationID: "X-Mutation"}} {
		h := http.Header{}
		resolved := names
		if resolved == (HeaderNames{}) {
			resolved = DefaultHeaderNames()
		}
		h.Set(resolved.ConnectionContext, want.ConnectionContext)
		h.Set(resolved.MutationID, want.MutationID)
		got, err := ReadHeaders(h.Get, names)
		if err != nil || got != want {
			t.Fatalf("HTTP: %+v %v", got, err)
		}
		md := metadata.Pairs(strings.ToLower(resolved.ConnectionContext), want.ConnectionContext, strings.ToLower(resolved.MutationID), want.MutationID)
		got, err = ReadHeaders(func(name string) string {
			values := md.Get(name)
			if len(values) == 0 {
				return ""
			}
			return values[0]
		}, names)
		if err != nil || got != want {
			t.Fatalf("gRPC: %+v %v", got, err)
		}
	}
	if _, err := ReadHeaders(http.Header{}.Get, HeaderNames{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadHeaders(nil, HeaderNames{}); err == nil {
		t.Fatal("nil getter accepted")
	}
	if _, err := ReadHeaders(http.Header{}.Get, HeaderNames{ConnectionContext: "X-ID", MutationID: "x-id"}); err == nil {
		t.Fatal("colliding names accepted")
	}
}

func TestContextValidationAndReceipt(t *testing.T) {
	c := validContext()
	for _, invalid := range []Context{
		{ConnectionContext: c.ConnectionContext}, {MutationID: c.MutationID},
		{ConnectionContext: "invalid", MutationID: c.MutationID},
		{ConnectionContext: c.ConnectionContext, MutationID: strings.ToUpper(c.MutationID)},
		{ConnectionContext: c.ConnectionContext, MutationID: "12345678_1234-1234-1234-123456789abc"},
	} {
		if _, err := invalid.Receipt(); err == nil {
			t.Fatalf("accepted %+v", invalid)
		}
	}
	if message, err := (Context{}).Receipt(); err != nil || message != nil {
		t.Fatalf("empty receipt: %v %v", message, err)
	}
	message, err := c.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != ReceiptKind || message.ConnectionContext != c.ConnectionContext || string(message.Payload) != c.MutationID {
		t.Fatal(message)
	}
}
