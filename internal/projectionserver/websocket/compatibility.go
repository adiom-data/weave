package websocket

import (
	"errors"
	"sync"

	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// ClientVersionPolicy is an application compatibility floor. Zero disables a
// floor. It is unrelated to package versions and is not an authentication check.
type ClientVersionPolicy struct {
	MinimumRevision     uint64
	RecommendedRevision uint64
}

// Current optionally replaces Policy with a live application-owned snapshot.
// It must be fast, nonblocking and safe for concurrent calls.
type ClientVersionOptions struct {
	Policy  ClientVersionPolicy
	Current func() ClientVersionPolicy
}

type clientVersion struct {
	mu       sync.Mutex
	known    bool
	revision uint64
	last     *pb.WebSocketClientVersionStatus
}

func (s *socketStream) declareVersion(info *pb.WebSocketClientInfo) error {
	if info == nil {
		return errors.New("missing client revision")
	}
	s.version.mu.Lock()
	if s.version.known {
		s.version.mu.Unlock()
		return errors.New("client revision must be declared once before reactive requests")
	}
	s.version.known, s.version.revision = true, info.Revision
	s.version.mu.Unlock()
	return s.checkVersion(false)
}

// Recheck on requests, heartbeats and output, so an application can retire
// existing connections by publishing a newer immutable policy. The getter must
// be fast, nonblocking and safe for concurrent calls; it performs no network IO.
func (s *socketStream) checkVersion(admit bool) error {
	if s.config.ClientVersions == nil {
		if admit {
			s.version.mu.Lock()
			s.version.known = true
			s.version.mu.Unlock()
		}
		return nil
	}
	policy := s.config.ClientVersions.Policy
	if current := s.config.ClientVersions.Current; current != nil {
		policy = current()
	}
	s.version.mu.Lock()
	defer s.version.mu.Unlock()
	if admit {
		s.version.known = true // Legacy clients implicitly announce revision zero.
	}
	if !s.version.known {
		return nil // The opening context contains no subscribed data.
	}
	status := pb.WebSocketClientVersionStatus_STATUS_UNSPECIFIED
	if s.version.revision < policy.MinimumRevision {
		status = pb.WebSocketClientVersionStatus_STATUS_UPGRADE_REQUIRED
	} else if s.version.revision < policy.RecommendedRevision {
		status = pb.WebSocketClientVersionStatus_STATUS_DEPRECATED
	}
	notice := &pb.WebSocketClientVersionStatus{Status: status, MinimumRevision: policy.MinimumRevision, RecommendedRevision: policy.RecommendedRevision}
	previous := s.version.last
	send := status != pb.WebSocketClientVersionStatus_STATUS_UNSPECIFIED && (previous == nil || previous.Status != status || previous.MinimumRevision != policy.MinimumRevision || previous.RecommendedRevision != policy.RecommendedRevision)
	if send {
		s.version.last = notice
	}
	// Serialize the notice with its terminal action. A concurrent output check
	// must not cancel the socket before this check has written the notice.
	if send {
		if err := s.write(&pb.WebSocketServerFrame{Payload: &pb.WebSocketServerFrame_ClientVersionStatus{ClientVersionStatus: notice}}); err != nil {
			s.cancel()
			return err
		}
	}
	if status == pb.WebSocketClientVersionStatus_STATUS_UPGRADE_REQUIRED {
		s.cancel()
		return errors.New("client upgrade required")
	}
	return nil
}
