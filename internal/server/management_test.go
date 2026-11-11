package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestManagementAuthenticationIsIndependentAndRejectsAmbiguousCredentials(t *testing.T) {
	const secret = "separate-admin-secret"
	called := 0
	handler := managementAuthentication(secret)(func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) {
		called++
		return connect.NewResponse(&emptypb.Empty{}), nil
	})
	for _, headers := range [][]string{nil, {"Bearer user-jwt"}, {"Bearer " + secret, "Bearer " + secret}, {"Basic " + secret}, {"Bearer"}} {
		req := connect.NewRequest(&emptypb.Empty{})
		for _, value := range headers {
			req.Header().Add("Authorization", value)
		}
		if _, err := handler(context.Background(), req); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatal(headers, err)
		}
	}
	if called != 0 {
		t.Fatal("unauthorized handler called")
	}
	req := connect.NewRequest(&emptypb.Empty{})
	req.Header().Set("Authorization", "Bearer "+secret)
	if _, err := handler(context.Background(), req); err != nil || called != 1 {
		t.Fatal(called, err)
	}
}
