package websocket

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// AuthenticationOptions enables transport-only authentication leases. The host
// verifies credentials; no JWT parsing or trust policy is built into this edge.
// Authenticate runs at admission and on token updates (with Authorization replaced by
// Bearer <token>). It must honor context cancellation and never mutate returned
// claims. Omit changing credential metadata such as exp/iat from Claims: these
// are the stable authorization inputs compared on every renewal. Include the
// subject/tenant identity as well as every claim used by authorization.
type AuthenticationOptions struct {
	Authenticate      func(context.Context, http.Header) (Authentication, error)
	ValidationTimeout time.Duration
	MaxTokenBytes     int
	// Shared by admission and refresh across all connections to this handler.
	// Admission rejects immediately when saturated. Existing connections wait
	// asynchronously, retaining only their latest pending token.
	MaxConcurrentValidations int
}

// Authentication binds a verified credential to canonical policy claims and a
// finite deadline. A refresh may extend the deadline but cannot change Claims.
type Authentication struct {
	Claims    accesscontrol.Claims
	ExpiresAt time.Time
}

const defaultMaxTokenBytes = 64 << 10

// DefaultAuthenticationOptions returns bounds without enabling a verifier.
func DefaultAuthenticationOptions() AuthenticationOptions {
	return AuthenticationOptions{ValidationTimeout: 5 * time.Second, MaxTokenBytes: defaultMaxTokenBytes, MaxConcurrentValidations: 32}
}

func (o AuthenticationOptions) withDefaults() AuthenticationOptions {
	d := DefaultAuthenticationOptions()
	if o.ValidationTimeout <= 0 {
		o.ValidationTimeout = d.ValidationTimeout
	}
	if o.MaxTokenBytes <= 0 {
		o.MaxTokenBytes = d.MaxTokenBytes
	}
	if o.MaxConcurrentValidations <= 0 {
		o.MaxConcurrentValidations = d.MaxConcurrentValidations
	}
	return o
}

type authenticationLease struct {
	mu      sync.Mutex
	claims  accesscontrol.Claims
	expires time.Time
	changed chan struct{}
}

func (h *handler) validate(ctx context.Context, headers http.Header) (Authentication, error) {
	if err := ctx.Err(); err != nil {
		return Authentication{}, err
	}
	o := h.socketConfig.Authentication
	if o.Authenticate == nil {
		return Authentication{}, errors.New("missing authenticator")
	}
	ctx, cancel := context.WithTimeout(ctx, o.ValidationTimeout)
	defer cancel()
	result, err := o.Authenticate(ctx, headers)
	if err != nil {
		return Authentication{}, err
	}
	if err := ctx.Err(); err != nil {
		return Authentication{}, err
	}
	if !result.ExpiresAt.After(time.Now()) {
		return Authentication{}, errors.New("authentication requires a valid future expiry")
	}
	for _, value := range result.Claims {
		if err := value.ValidateComplete(); err != nil {
			return Authentication{}, err
		}
	}
	result.Claims = accesscontrol.Claims(storage.CloneRecord(storage.Record(result.Claims)))
	return result, nil
}

func (h *handler) authenticate(ctx context.Context, headers http.Header) (*authenticationLease, error) {
	select {
	case h.validationSlots <- struct{}{}:
		defer func() { <-h.validationSlots }()
	default:
		return nil, errors.New("authentication capacity exhausted")
	}
	result, err := h.validate(ctx, headers)
	if err != nil {
		return nil, err
	}
	return &authenticationLease{claims: result.Claims, expires: result.ExpiresAt, changed: make(chan struct{}, 1)}, nil
}

func (a *authenticationLease) context(ctx context.Context) context.Context {
	return accesscontrol.ContextWithClaims(ctx, a.claims)
}
func (a *authenticationLease) current() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return time.Now().Before(a.expires)
}

type renewal uint8

const (
	renewed renewal = iota
	renewalRejected
	renewalReconnect
)

func (a *authenticationLease) renew(result Authentication) renewal {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !time.Now().Before(a.expires) || !time.Now().Before(result.ExpiresAt) {
		return renewalRejected
	}
	if len(a.claims) != len(result.Claims) {
		return renewalReconnect
	}
	for key, value := range a.claims {
		other, exists := result.Claims[key]
		if !exists || !storage.EqualValues(value, other) {
			return renewalReconnect
		}
	}
	a.expires = result.ExpiresAt
	select {
	case a.changed <- struct{}{}:
	default:
	}
	return renewed
}
func (a *authenticationLease) watch(ctx context.Context, cancel context.CancelFunc) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		a.mu.Lock()
		remaining := time.Until(a.expires)
		a.mu.Unlock()
		if remaining <= 0 {
			cancel()
			return
		}
		timer.Reset(remaining)
		select {
		case <-ctx.Done():
			return
		case <-a.changed:
		case <-timer.C:
		}
	}
}

// tokenUpdates is a latest-value mailbox, never a queue of credentials. One
// worker consumes it; the reader can replace pending work without waiting for
// verification or shared verifier capacity.
type tokenUpdates struct {
	mu      sync.Mutex
	pending string
	version uint64
	wake    chan struct{}
}

func newTokenUpdates() *tokenUpdates { return &tokenUpdates{wake: make(chan struct{}, 1)} }
func (u *tokenUpdates) put(token string) {
	u.mu.Lock()
	u.pending = token
	u.version++
	u.mu.Unlock()
	select {
	case u.wake <- struct{}{}:
	default:
	}
}
func (u *tokenUpdates) take() (string, uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	token := u.pending
	u.pending = ""
	return token, u.version
}
func (u *tokenUpdates) isLatest(version uint64) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.version == version
}

func (s *socketStream) updateAuthToken(update *reactivev1.WebSocketAuthToken) error {
	if update == nil || update.Token == "" {
		return errors.New("missing authentication token")
	}
	maxBytes := defaultMaxTokenBytes
	if s.config.Authentication != nil {
		maxBytes = s.config.Authentication.MaxTokenBytes
	}
	if len(update.Token) > maxBytes {
		return errors.New("authentication token too large")
	}
	s.touch()
	if s.tokens != nil {
		s.tokens.put(update.Token)
	}
	return nil
}

func (s *socketStream) validateTokens() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.tokens.wake:
		}
		// Acquire capacity before taking the token so updates received while
		// waiting replace obsolete work. Neither wait blocks the socket reader.
		select {
		case <-s.ctx.Done():
			return
		case s.handler.validationSlots <- struct{}{}:
		}
		token, version := s.tokens.take()
		if token == "" {
			<-s.handler.validationSlots
			continue
		}
		headers := s.headers.Clone()
		headers.Set("Authorization", "Bearer "+token)
		result, err := s.handler.validate(s.ctx, headers)
		// A slow socket writer must not occupy shared verifier capacity.
		<-s.handler.validationSlots
		if s.ctx.Err() != nil {
			return
		}
		outcome := renewalRejected
		if err == nil {
			// A valid same-claims token can renew the lease even when another
			// update is pending, avoiding starvation under frequent submissions.
			outcome = s.auth.renew(result)
		}
		if outcome == renewed || !s.tokens.isLatest(version) {
			continue
		}
		status := reactivev1.WebSocketAuthStatus_STATUS_REJECTED
		if outcome == renewalReconnect {
			status = reactivev1.WebSocketAuthStatus_STATUS_RECONNECT
			defer s.cancel()
		}
		if err := s.write(&reactivev1.WebSocketServerFrame{Payload: &reactivev1.WebSocketServerFrame_AuthStatus{AuthStatus: &reactivev1.WebSocketAuthStatus{Status: status}}}); err != nil {
			s.cancel()
			return
		}
		if outcome == renewalReconnect {
			return
		}
	}
}
