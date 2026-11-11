package engine

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/engine/windowloader"
)

// materializationRequest is the immutable handoff from actor-owned window
// staging to asynchronous snapshot IO. Keeping the prepared query and exact
// ranges together prevents the read from being reconstructed by a later phase.
type materializationRequest struct {
	query     datastore.PreparedQuery
	reads     []datastore.Bounds
	reference datastore.PreparedReference
}

// materializedWindow binds a completed snapshot read to the request that
// produced it. Activation validates this request against actor-owned lifecycle
// state before allowing the snapshot to make coverage ready.
type materializedWindow struct {
	request materializationRequest
	pending *windowloader.PendingWindow
}

func (m *materializedWindow) cancel(cause error) {
	if m != nil && m.pending != nil {
		m.pending.Cancel(cause)
	}
}

func newMaterializationRequest(query datastore.PreparedQuery, reads []datastore.Bounds) *materializationRequest {
	if len(reads) == 0 {
		return nil
	}
	return &materializationRequest{query: query, reads: append([]datastore.Bounds(nil), reads...)}
}

func (c *Coordinator) readMaterialization(ctx context.Context, request *materializationRequest) (*materializedWindow, error) {
	if request == nil || request.query == nil || len(request.reads) == 0 {
		return nil, errors.New("materialization request is empty")
	}
	var capture *referenceRead
	var err error
	if request.reference != nil {
		capture, err = c.beginReferenceRead(ctx, request.reference)
		if err != nil {
			return nil, err
		}
		defer c.abortReferenceRead(capture)
	}
	read, err := c.beginWindowRead(ctx, request.query)
	if err != nil {
		return nil, err
	}
	readOwned := true
	defer func() {
		if readOwned {
			c.abortWindowRead(read)
		}
	}()
	var pending *windowloader.PendingWindow
	if identity, ok := request.query.(datastore.IdentityQuery); ok && request.reference == nil {
		pending, err = c.loader.ReadIdentity(ctx, identity)
	} else {
		if loader, ok := c.loader.(interface {
			ReadRangesWithReference(context.Context, datastore.PreparedQuery, []datastore.Bounds, int, datastore.ReferenceFetch) (*windowloader.PendingWindow, error)
		}); request.reference != nil && ok {
			pending, err = loader.ReadRangesWithReference(ctx, request.query, request.reads, c.options.SnapshotPageRows, request.reference.Fetch())
		} else {
			pending, err = c.loader.ReadRanges(ctx, request.query, request.reads, c.options.SnapshotPageRows)
		}
	}
	if err != nil {
		return nil, err
	}
	if err := c.stageWindowRead(ctx, read, pending); err != nil {
		pending.Cancel(err)
		return nil, err
	}
	if err := windowloader.WaitForExternalLoads(ctx, []*windowloader.PendingWindow{pending}, func(window *windowloader.PendingWindow, indexes []int) error {
		return c.retryWindowRead(ctx, read, window, indexes)
	}); err != nil {
		pending.Cancel(err)
		return nil, err
	}
	if err := c.finishWindowRead(ctx, read); err != nil {
		pending.Cancel(err)
		return nil, err
	}
	readOwned = false
	if capture != nil {
		if err := c.finishReferenceRead(ctx, capture, pending.References, pending.Fence); err != nil {
			pending.Cancel(err)
			return nil, err
		}
		pending.References = nil
	}
	return &materializedWindow{request: *request, pending: pending}, nil
}

func (m *materializedWindow) validate(query datastore.PreparedQuery, reads []datastore.Bounds) error {
	if m == nil || m.pending == nil || m.request.query == nil {
		return errors.New("materialization is empty")
	}
	if m.request.query.Fingerprint() != query.Fingerprint() || m.request.query.Relation() != query.Relation() ||
		m.pending.Query().Fingerprint() != query.Fingerprint() || m.pending.Query().Relation() != query.Relation() {
		return errors.New("materialization belongs to another query")
	}
	equal, err := sameMaterializationReads(query, m.request.reads, reads)
	if err != nil {
		return err
	}
	if !equal {
		return errors.New("materialization does not match the staged ranges")
	}
	return nil
}

func sameMaterializationReads(query datastore.PreparedQuery, left, right []datastore.Bounds) (bool, error) {
	if len(left) != len(right) {
		return false, nil
	}
	for index := range left {
		leftBoundary, err := querywindow.CompareBoundary(query, left[index].Left, right[index].Left)
		if err != nil {
			return false, err
		}
		rightBoundary, err := querywindow.CompareBoundary(query, left[index].Right, right[index].Right)
		if err != nil {
			return false, err
		}
		if leftBoundary != 0 || rightBoundary != 0 {
			return false, nil
		}
	}
	return true, nil
}
