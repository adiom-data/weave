package projection

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adiom-data/weave/internal/projection/retry"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/streamqueue"
)

var projectionSequence atomic.Uint64

type Limits struct {
	MaxDefinitionDepth      int
	MaxDefinitionNodes      int
	MaxActiveInstances      int
	MaxConcurrentExpansions int
	MaxPendingExpansions    int
	RecoveryInitialDelay    time.Duration
	RecoveryMaxDelay        time.Duration
	RecoveryMaxAttempts     int
}

func (limits Limits) WithDefaults() Limits {
	d := DefaultLimits()
	if limits.RecoveryInitialDelay <= 0 {
		limits.RecoveryInitialDelay = d.RecoveryInitialDelay
	}
	if limits.RecoveryMaxDelay <= 0 {
		limits.RecoveryMaxDelay = d.RecoveryMaxDelay
	}
	if limits.RecoveryMaxAttempts <= 0 {
		limits.RecoveryMaxAttempts = d.RecoveryMaxAttempts
	}
	if limits.MaxDefinitionDepth <= 0 {
		limits.MaxDefinitionDepth = d.MaxDefinitionDepth
	}
	if limits.MaxDefinitionNodes <= 0 {
		limits.MaxDefinitionNodes = d.MaxDefinitionNodes
	}
	if limits.MaxActiveInstances <= 0 {
		limits.MaxActiveInstances = d.MaxActiveInstances
	}
	if limits.MaxConcurrentExpansions <= 0 {
		limits.MaxConcurrentExpansions = d.MaxConcurrentExpansions
	}
	if limits.MaxPendingExpansions <= 0 {
		limits.MaxPendingExpansions = d.MaxPendingExpansions
	}
	return limits
}

func DefaultLimits() Limits {
	return Limits{MaxDefinitionDepth: 8, MaxDefinitionNodes: 64, MaxActiveInstances: 1000,
		MaxConcurrentExpansions: 8, MaxPendingExpansions: 1024, RecoveryInitialDelay: time.Second, RecoveryMaxDelay: 30 * time.Second, RecoveryMaxAttempts: 5}
}

func (limits Limits) recoveryPolicy() retry.Policy {
	return retry.Policy{InitialDelay: limits.RecoveryInitialDelay, MaxDelay: limits.RecoveryMaxDelay, MaxAttempts: limits.RecoveryMaxAttempts}
}

// ProjectionNode maps one parent record into one immutable table query. Each
// parent row receives its own child subscription (N+1 by design). Entity
// nodes use the entity interface; its implementation may use a count-one
// query without exposing window controls here.
type ProjectionNode struct {
	Name   string
	Entity bool
	// BuildQuery is a pure binding operation over a borrowed immutable record.
	// It must not mutate parent or reuse mutable scratch storage in its result.
	// Each parent image's result is shared by expansion and selector comparison.
	// matchable=false represents an empty branch and opens no subscription.
	BuildQuery     func(parent storage.Record) (query storage.QuerySpec, matchable bool, err error)
	Window         querysubscription.InitialWindow
	Children       []*ProjectionNode
	Reference      *storage.ReferenceFetch
	ReferenceChild string
}

type ProjectionEventType uint8

const (
	ProjectionRowChanged ProjectionEventType = iota + 1
	ProjectionInstanceOpened
	ProjectionInstanceClosed
	// ProjectionBranchChanged carries a branch-local error, or clears one.
	// A zero Key addresses the whole instance rather than a single row.
	ProjectionBranchChanged
)

type ProjectionEvent struct {
	Type             ProjectionEventType
	Path             []string
	Lineage          []PathStep
	Node             string
	EntityNode       bool
	InstanceID       string
	ParentInstanceID string
	ParentEntityKey  storage.EntityKey
	Kind             querysubscription.ChangeKind
	Key              storage.EntityKey
	Record           storage.Record
	// Computed is set only by the outer publication layer, never the query actor.
	Computed    storage.Record
	Pending     bool
	Cursor      []byte
	Bounds      querysubscription.Bounds
	Initial     bool
	ChangeIndex uint32
	Revision    uint64
	Fence       *querysubscription.Position
	Checkpoint  *querysubscription.Position
	// Version belongs to Record, not to the transaction publishing or
	// authorizing it. Zero is a valid bootstrap version, not "unversioned".
	Version querysubscription.RecordVersion
	Failure error
}

// PathStep identifies one concrete row occurrence on the directional
// expansion path that produced an event. Node is descriptive; authorization
// policies should match canonical relation identities and validate each edge.
type PathStep struct {
	Node string
	Key  storage.EntityKey
}

type ProjectionBatch struct {
	Delivery querysubscription.DeliveryMetadata
	Events   []ProjectionEvent
	// Membership contains ordered list deltas for the query instances changed
	// by this batch. Entity instances have no list membership delta.
	Membership map[string]querysubscription.MembershipDelta
	Window     *ProjectionWindowTransition
	// publication is resolved only by OutputScheduler, at the point the
	// subtree's parent effects have been consumed.
	publication *projectionPublication
	// WindowFailure acknowledges a window request that became obsolete because
	// its projection instance was retired before the queued transition could be
	// delivered. Transaction metadata is still carried so connection ordering
	// can advance without exposing stale instance data.
	WindowFailure *ProjectionWindowFailure
}

type projectionPublication struct {
	InstanceIDs    []string
	CausalRevision uint64
}

type ProjectionWindowTransition struct {
	Token      string
	InstanceID string
	Bounds     querysubscription.Bounds
	Fence      *querysubscription.Position
	Checkpoint *querysubscription.Position
}

type ProjectionWindowFailure struct {
	Token      string
	InstanceID string
	Cause      error
}

type Projection struct {
	queue       *streamqueue.Queue[ProjectionBatch]
	outputLimit int

	ctx     context.Context
	cancel  context.CancelFunc
	queries querysubscription.Feed
	prefix  string
	limits  Limits

	mu          sync.Mutex
	processMu   sync.Mutex
	receiveMu   sync.Mutex
	instances   map[string]*projectionInstance
	activeSlots int // Failed placeholder/replacement pairs share one logical slot.
	// latestCheckpoints follows feed observation order per source. It advances
	// authorization views for dynamic publications without falsifying row
	// revisions or replacing row data.
	latestCheckpoints map[storage.SourceID]*querysubscription.Position
	latestRevision    uint64
	initialChanges    *initialChanges
	progressChanged   chan struct{}
	rootID            string
	ready             chan struct{}
	readyOnce         sync.Once
	readyEvents       []ProjectionEvent
	readyPositions    map[storage.SourceID]*querysubscription.Position
	initialTaken      bool
	readyErr          error
	terminalErr       error
	releaseMu         sync.Mutex
	outputReleased    bool
	awaitingAdmission bool
	pendingOutput     []ProjectionBatch
	initializing      bool
	expansions        *expansionScheduler
	expansionLimiter  *ExpansionLimiter
	partialErrors     bool
	failed            map[string]*projectionInstance
	recoveryWake      chan struct{}
	closeOnce         sync.Once
}

type projectionInstance struct {
	id        string
	slot      *instanceSlot
	node      *ProjectionNode
	spec      storage.QuerySpec
	depth     int
	path      []string
	lineage   []PathStep
	parentID  string
	parentKey storage.EntityKey
	// opening owns this interest until the entire sibling expansion attaches.
	// Presence in instances alone does not make an obsolete load current.
	opening      *projectionExpansion
	subscription querysubscription.Subscription
	query        querysubscription.QuerySubscription
	initial      projectionInitialState
	order        []storage.EntityKey
	members      map[storage.EntityKey]*projectionMember
	// localEmpty is proved by an unmatched binding, without a source read.
	// Its parent remains part of the projection's snapshot/fencing checks.
	localEmpty bool
	// published is the structural publication boundary. Feed delivery may
	// advance an instance while its snapshot is loading, but no row from that
	// instance is exposed until a current image has introduced the instance.
	published  bool
	revision   uint64
	checkpoint *querysubscription.Position
	fence      *querysubscription.Position
	failure    error
	recovery   retry.State
	replaces   string // root predecessor, retired at the replacement publication
}

// snapshotPosition distinguishes source snapshots from locally derived empty
// topology and failed placeholders. Source-backed instances still require a
// real checkpoint or fence, even when their result contains no rows.
// The caller holds Projection.mu.
func (instance *projectionInstance) snapshotPosition() (*querysubscription.Position, error) {
	if instance.localEmpty || instance.failure != nil {
		return nil, nil
	}
	return querysubscription.SnapshotPosition(instance.checkpoint, instance.fence)
}

// A member owns one immutable row image and its expansion state, all guarded
// by Projection.mu. Row images are replaced on upsert; expansion tokens change
// only when selectors change or the row is removed/readded.
type projectionMember struct {
	record         storage.Record
	version        querysubscription.RecordVersion
	cursor         []byte
	pending        bool
	children       []string
	expanded       bool // An empty completed expansion differs from one not yet opened.
	expansionToken *expansionGeneration
	// Bindings are computed once from this immutable image. Expansion workers
	// and the feed consumer may request them concurrently.
	bindings *memberBindings
	// Published predecessors remain resident and count toward instance limits
	// until the replacement for their particular child slot is published.
	replacementRoots map[*ProjectionNode][]string
}

// Equivalent immutable row images share this selector generation. The loading
// signal serializes inline and queued passes without holding actor locks on IO.
// All access is guarded by Projection.mu.
type expansionGeneration struct{ loading chan struct{} }

func newProjectionMember(record storage.Record, version querysubscription.RecordVersion, cursor []byte, pending bool) *projectionMember {
	return &projectionMember{record: storage.CloneRecord(record), version: version, cursor: append([]byte(nil), cursor...), pending: pending, expansionToken: &expansionGeneration{}}
}

// capture requires Projection.mu and owns its payloads independently of future
// updates or expansion.
func (m *projectionMember) capture() projectionMember {
	return projectionMember{record: storage.CloneRecord(m.record), version: m.version, cursor: append([]byte(nil), m.cursor...), pending: m.pending, children: append([]string(nil), m.children...), expanded: m.expanded}
}

// projectionInitialState is the common snapshot information needed while a
// projection instance is opening. Bounds are meaningful only for query nodes;
// entity nodes do not manufacture a query snapshot merely to share this path.
type projectionInitialState struct {
	Bounds     querysubscription.Bounds
	Rows       []querysubscription.QueryRow
	Checkpoint *querysubscription.Position
	Fence      *querysubscription.Position
	Revision   uint64
}

type projectionExpansion struct {
	parent   *projectionInstance
	key      storage.EntityKey
	member   *projectionMember
	revision uint64
}

type StartOptions struct {
	Prefix           string
	QueueLimit       int
	Limits           Limits
	ExpansionLimiter *ExpansionLimiter
	// ConsumedCheckpoint/Revision seed coverage already consumed on this same
	// feed before this projection existed. Snapshots are never source proof.
	ConsumedCheckpoint *querysubscription.Position
	ConsumedRevision   uint64
	// WaitForSync delays the initial image until ordered feed progress covers
	// every snapshot discovered during expansion. Pre-cut changes are folded
	// into that image; only later changes are released as live output.
	WaitForSync bool
	// CurrentImageAdmission gives TryAdmitInitial ownership of the first public
	// image. Until acceptance, consume feed events into current state without
	// retaining superseded catch-up batches. WaitReady signals load completion,
	// not permission to publish its image or release live output in this mode.
	CurrentImageAdmission bool
	AllowPartialErrors    bool
}

func (p *Projection) PartialErrors() bool { return p.partialErrors }

// BeginProjection reserves Prefix and returns before loading begins. Consumers
// must drain Next immediately, then call TakeInitial to obtain the reconciled
// initial image and OutputScheduler.Release only after that image is installed publicly.
func BeginProjection(ctx context.Context, queries querysubscription.Feed, root *ProjectionNode, options StartOptions) (*Projection, error) {
	if queries == nil {
		return nil, errors.New("query subscription feed is required")
	}
	if root == nil || root.BuildQuery == nil || root.Name == "" {
		return nil, errors.New("projection root name and query builder are required")
	}
	if options.Prefix == "" {
		options.Prefix = fmt.Sprintf("projection-%d", projectionSequence.Add(1))
	}
	if options.QueueLimit <= 0 {
		options.QueueLimit = 1
	}
	options.Limits = options.Limits.WithDefaults()
	if err := options.Limits.recoveryPolicy().Validate(); err != nil {
		return nil, err
	}
	if err := validateProjection(root, 1, new(int), options.Limits); err != nil {
		return nil, err
	}
	projectionCtx, cancel := context.WithCancel(ctx)
	p := &Projection{
		queue: streamqueue.New[ProjectionBatch](options.QueueLimit, querysubscription.ErrBackpressure, querysubscription.ErrClosed), ctx: projectionCtx, cancel: cancel,
		outputLimit: options.QueueLimit,
		queries:     queries, prefix: options.Prefix,
		limits: options.Limits, instances: make(map[string]*projectionInstance),
		latestCheckpoints: make(map[storage.SourceID]*querysubscription.Position),
		progressChanged:   make(chan struct{}),
		ready:             make(chan struct{}), initializing: true,
		awaitingAdmission: options.CurrentImageAdmission,
		initialTaken:      options.CurrentImageAdmission,
		expansionLimiter:  options.ExpansionLimiter,
		partialErrors:     options.AllowPartialErrors, failed: make(map[string]*projectionInstance), recoveryWake: make(chan struct{}, 1),
	}
	if options.CurrentImageAdmission {
		p.initialChanges = newInitialChanges()
	}
	p.rememberCheckpointLocked(options.ConsumedCheckpoint)
	p.latestRevision = options.ConsumedRevision
	p.expansions = newExpansionScheduler(
		projectionCtx,
		options.Limits.MaxConcurrentExpansions,
		options.Limits.MaxPendingExpansions,
		p.expandChildren,
	)
	go p.initialize(root, options.WaitForSync)
	if options.AllowPartialErrors {
		go p.recoverBranches()
	}
	return p, nil
}

func (p *Projection) initialize(root *ProjectionNode, synchronize bool) {
	spec, matchable, err := root.BuildQuery(nil)
	var opened *projectionInstance
	if err == nil {
		opened, err = p.openNode(root, boundQuery{spec: spec, matchable: matchable}, []string{root.Name}, nil)
	}
	if err != nil && p.partialErrors && p.ctx.Err() == nil && !errors.Is(err, querysubscription.ErrBackpressure) {
		if failed := p.failedNode(root, spec, []string{root.Name}, nil, err); failed != nil {
			opened, err = failed, nil
		}
	}
	if err != nil {
		p.finishReady(nil, nil, err)
		p.shutdown(err)
		return
	}
	rootID := opened.id
	p.mu.Lock()
	p.rootID = rootID
	p.mu.Unlock()
	if synchronize {
		initial, positions, initialErr := p.synchronizedInitialImage(p.ctx)
		p.finishReady(initial, positions, initialErr)
		if initialErr != nil {
			p.shutdown(initialErr)
		}
		return
	}
	for {
		if err := p.expansions.waitIdle(p.ctx); err != nil {
			p.finishReady(nil, nil, err)
			p.shutdown(err)
			return
		}
		p.processMu.Lock()
		// ApplyDelivery schedules expansion work before releasing processMu. If
		// work appeared after waitIdle observed an empty scheduler, retry rather
		// than declaring the projection initialized across that handoff.
		if !p.expansions.isIdle() {
			p.processMu.Unlock()
			continue
		}
		var initial []ProjectionEvent
		var initialErr error
		// Admission owns the first capture. Readiness is only a completion
		// signal in this mode, not an image to copy and immediately discard.
		if p.initialChanges == nil {
			initial, initialErr = p.currentEvents(rootID)
		}
		if initialErr == nil {
			p.mu.Lock()
			for _, instance := range p.instances {
				instance.published = true
			}
			p.initializing = false
			p.mu.Unlock()
		}
		p.processMu.Unlock()
		p.finishReady(initial, nil, initialErr)
		if initialErr != nil {
			p.shutdown(initialErr)
		}
		return
	}
}

func (p *Projection) finishReady(events []ProjectionEvent, positions map[storage.SourceID]*querysubscription.Position, err error) {
	p.readyOnce.Do(func() {
		p.mu.Lock()
		// Capture already owns its records and metadata. Retain that ownership
		// only until TakeInitial transfers it to the initial-image consumer.
		if p.terminalErr == nil && !p.initialTaken {
			p.readyEvents = events
			p.readyPositions = positions
		}
		p.readyErr = err
		p.mu.Unlock()
		close(p.ready)
	})
}

// WaitReady waits for load completion without taking an image. Admission uses
// this signal before TryAdmitInitial; ordinary consumers use TakeInitial.
func (p *Projection) WaitReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-p.ready:
		p.mu.Lock()
		err := p.readyErr
		if p.terminalErr != nil {
			err = p.terminalErr
		}
		p.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TakeInitial transfers the reconciled initial image exactly once. The caller
// owns its mutable payloads and must install it before OutputScheduler.Release.
// Positions describe the same captured cut when WaitForSync was requested;
// an unsynchronized image does not advertise a common cut.
// CurrentImageAdmission consumers obtain their image through TryAdmitInitial.
func (p *Projection) TakeInitial(ctx context.Context) ([]ProjectionEvent, map[storage.SourceID]*querysubscription.Position, error) {
	if err := p.WaitReady(ctx); err != nil {
		return nil, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminalErr != nil {
		return nil, nil, p.terminalErr
	}
	if p.awaitingAdmission || p.initialTaken {
		return nil, nil, errors.New("projection initial image is unavailable or already taken")
	}
	p.initialTaken = true
	events, positions := p.readyEvents, p.readyPositions
	p.readyEvents = nil
	p.readyPositions = nil
	return events, positions, nil
}

// synchronizedInitialImage waits until expansion is idle and the ordered feed
// covers every reachable snapshot. Both one-shot and reactive consumers take
// this same initial image and its cut; there is no later recapture path.
// Capturing the image and enabling live delivery share processMu.
func (p *Projection) synchronizedInitialImage(ctx context.Context) ([]ProjectionEvent, map[storage.SourceID]*querysubscription.Position, error) {
	requested := make(map[storage.SourceID]*querysubscription.Position)
	for {
		if err := p.expansions.waitIdle(ctx); err != nil {
			return nil, nil, err
		}
		p.processMu.Lock()
		if !p.expansions.isIdle() {
			p.processMu.Unlock()
			continue
		}
		p.mu.Lock()
		if err := p.ctx.Err(); err != nil {
			p.mu.Unlock()
			p.processMu.Unlock()
			return nil, nil, err
		}
		targets := make(map[storage.SourceID]*querysubscription.Position)
		checkpoints := make(map[storage.SourceID]*querysubscription.Position)
		snapshotCount := 0
		for _, instance := range p.instances {
			position, err := instance.snapshotPosition()
			if err != nil {
				p.mu.Unlock()
				p.processMu.Unlock()
				return nil, nil, err
			}
			if position == nil {
				continue
			}
			snapshotCount++
			if checkpoint := instance.checkpoint; checkpoint != nil && (checkpoints[checkpoint.SourceID] == nil || checkpoints[checkpoint.SourceID].Value < checkpoint.Value) {
				checkpoints[checkpoint.SourceID] = clonePosition(checkpoint)
			}
			if targets[position.SourceID] == nil || targets[position.SourceID].Value < position.Value {
				targets[position.SourceID] = clonePosition(position)
			}
		}
		// Local empty branches add no independent snapshot to synchronize.
		covered := snapshotCount <= 1
		if !covered {
			covered = true
			for source, target := range targets {
				if current := p.latestCheckpoints[source]; current == nil || current.Value < target.Value {
					covered = false
					break
				}
			}
		}
		changed := p.progressChanged
		rootID := p.rootID
		cut := make(map[storage.SourceID]*querysubscription.Position, len(targets))
		for source, target := range targets {
			position := checkpoints[source]
			if current := p.latestCheckpoints[source]; current != nil && (position == nil || current.Value > position.Value) {
				position = current
			}
			// A single table's fence can establish its own snapshot without
			// naming a committed feed checkpoint. Keep that distinction for
			// snapshot authorization rather than fabricating a checkpoint.
			if position != nil && position.Value >= target.Value {
				cut[source] = clonePosition(position)
			}
		}
		progressTargets := make(map[storage.SourceID]*querysubscription.Position)
		if !covered {
			for source, target := range targets {
				current := p.latestCheckpoints[source]
				previous := requested[source]
				if (current == nil || current.Value < target.Value) && (previous == nil || previous.Value < target.Value) {
					progressTargets[source] = clonePosition(target)
				}
			}
		}
		p.mu.Unlock()
		if !covered {
			p.processMu.Unlock()
			if len(progressTargets) > 0 {
				if err := p.queries.RequestProgress(ctx); err != nil {
					return nil, nil, fmt.Errorf("request projection progress: %w", err)
				}
				for source, target := range progressTargets {
					requested[source] = target
				}
				continue
			}
			select {
			case <-changed:
				continue
			case <-p.ctx.Done():
				return nil, nil, p.ctx.Err()
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
		var events []ProjectionEvent
		var err error
		if p.initialChanges == nil {
			events, err = p.currentEvents(rootID)
		}
		if err == nil {
			p.mu.Lock()
			for _, instance := range p.instances {
				instance.published = true
			}
			p.initializing = false
			p.mu.Unlock()
		}
		p.processMu.Unlock()
		if err != nil {
			return nil, nil, err
		}
		// A synchronized image incorporates every source through its observed
		// cut. Stamp instance events with that common source position so visibility
		// is evaluated against the same cut rather than an older local snapshot.
		for index := range events {
			source := storage.SourceID("")
			if events[index].Checkpoint != nil {
				source = events[index].Checkpoint.SourceID
			} else if events[index].Fence != nil {
				source = events[index].Fence.SourceID
			}
			if position := cut[source]; position != nil {
				events[index].Checkpoint = clonePosition(position)
			}
		}
		return events, cut, nil
	}
}

// releaseOutput synchronously drains staged batches after the caller has
// installed the initial image. The same serialized consumer that calls
// ApplyEvent must process each batch in consume before processing another feed
// event. New output stays staged until this drain is complete. consume runs
// outside projection locks and may complete subtree publications.
func (p *Projection) releaseOutput(consume func(ProjectionBatch) error) error {
	defer p.signalRecovery()
	if consume == nil {
		return errors.New("projection output consumer is required")
	}
	select {
	case <-p.ready:
	default:
		return errors.New("projection is not ready")
	}
	p.releaseMu.Lock()
	defer p.releaseMu.Unlock()
	for {
		p.processMu.Lock()
		p.mu.Lock()
		err := p.readyErr
		if p.terminalErr != nil {
			err = p.terminalErr
		}
		if err != nil || p.outputReleased {
			p.mu.Unlock()
			p.processMu.Unlock()
			return err
		}
		if p.awaitingAdmission {
			p.mu.Unlock()
			p.processMu.Unlock()
			return errors.New("projection initial image has not been admitted")
		}
		pending := p.pendingOutput
		p.pendingOutput = nil
		if len(pending) == 0 {
			p.outputReleased = true
		}
		p.mu.Unlock()
		p.processMu.Unlock()
		if len(pending) == 0 {
			return nil
		}
		for _, batch := range pending {
			if err := consume(batch); err != nil {
				p.shutdown(err)
				return err
			}
			if err := p.Err(); err != nil {
				return err
			}
		}
	}
}

func validateProjection(node *ProjectionNode, depth int, count *int, limits Limits) error {
	if node == nil || node.Name == "" || node.BuildQuery == nil {
		return errors.New("projection node name and query builder are required")
	}
	if depth > limits.MaxDefinitionDepth {
		return fmt.Errorf("projection exceeds maximum depth of %d", limits.MaxDefinitionDepth)
	}
	*count++
	if *count > limits.MaxDefinitionNodes {
		return fmt.Errorf("projection exceeds maximum definition size of %d nodes", limits.MaxDefinitionNodes)
	}
	for _, child := range node.Children {
		if err := validateProjection(child, depth+1, count, limits); err != nil {
			return err
		}
	}
	return nil
}

func (p *Projection) Close() {
	p.shutdown(querysubscription.ErrClosed)
}

// Err returns the terminal projection failure, including failures that occurred
// after its initial image was captured but before the consumer published it.
func (p *Projection) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.terminalErr
}

func (p *Projection) stageOutput(batch ProjectionBatch) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminalErr != nil {
		return p.terminalErr
	}
	if p.awaitingAdmission {
		// The admission owner will capture current rows and topology under
		// processMu. These batches are already incorporated there; source
		// transaction messages are owned by the connection independently.
		return nil
	}
	if len(p.pendingOutput) >= p.outputLimit {
		return querysubscription.ErrBackpressure
	}
	p.pendingOutput = append(p.pendingOutput, batch)
	return nil
}

// Next returns asynchronous subtree publications and terminal errors. Source
// deliveries are returned synchronously by ApplyEvent or drained by OutputScheduler.Release.
// Calls are serialized so concurrent consumers cannot split the ordered stream.
// Route these batches through OutputScheduler before consuming them.
func (p *Projection) Next(ctx context.Context) (ProjectionBatch, error) {
	p.receiveMu.Lock()
	defer p.receiveMu.Unlock()
	return p.queue.Next(ctx)
}

func (p *Projection) shutdown(reason error) {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.terminalErr = reason
		p.readyEvents = nil
		p.readyPositions = nil
		p.pendingOutput = nil
		p.mu.Unlock()
		p.finishReady(nil, nil, reason)
		p.queue.Close(reason)
		p.cancel()
		p.expansions.stop()
		p.mu.Lock()
		ids := make([]string, 0, len(p.instances))
		for id := range p.instances {
			ids = append(ids, id)
		}
		p.mu.Unlock()
		for _, id := range ids {
			p.closeInstance(id)
		}
	})
}

// openNode installs raw state and topology only. Publication captures a current
// image after loading/catch-up; snapshots encountered during expansion are not
// events and must not be materialized into discarded event trees.
func (p *Projection) openNode(node *ProjectionNode, binding boundQuery, path []string, owner *projectionExpansion) (*projectionInstance, error) {
	instance, err := p.loadNode(node, binding, path, owner)
	if err != nil && p.partialErrors && p.ctx.Err() == nil && !errors.Is(err, querysubscription.ErrBackpressure) {
		if failed := p.failedNode(node, binding.spec, path, owner, err); failed != nil {
			return failed, nil
		}
	}
	return instance, err
}

func (p *Projection) loadNode(node *ProjectionNode, binding boundQuery, path []string, owner *projectionExpansion) (*projectionInstance, error) {
	spec := binding.spec
	if node == nil || node.Name == "" || node.BuildQuery == nil {
		return nil, errors.New("projection node name and query builder are required")
	}
	p.processMu.Lock()
	p.mu.Lock()
	tooMany := !p.canInstallInstanceLocked(node, owner)
	current := owner == nil || p.expansionCurrentLocked(*owner)
	// Resolve the window from the same reconciled ownership image as admission.
	// A parent row replacement may temporarily carry a new selector generation
	// until reconcileChildren restores an unchanged binding under processMu.
	bounds := p.replacementBoundsLocked(node, spec, owner)
	p.mu.Unlock()
	p.processMu.Unlock()
	if !current {
		return nil, querysubscription.ErrClosed
	}
	if tooMany {
		return nil, fmt.Errorf("projection exceeds maximum of %d active instances", p.limits.MaxActiveInstances)
	}
	var err error
	parentID, parentKey, depth := "", storage.EntityKey{}, 1
	if owner != nil {
		parentID, parentKey, depth = owner.parent.id, owner.key, owner.parent.depth+1
	}
	id := fmt.Sprintf("%s:%d", p.prefix, projectionSequence.Add(1))
	window := node.Window
	if window.Count == 0 {
		window.Count = 100
	}
	if bounds != nil {
		window = querysubscription.InitialWindow{Bounds: bounds}
	}
	lineage := []PathStep(nil)
	var fetchFrom *querysubscription.ReferenceSource
	if parentID != "" {
		p.mu.Lock()
		parentInstance := p.instances[parentID]
		if parentInstance != nil {
			if parentInstance.node.ReferenceChild == node.Name {
				fetchFrom = &querysubscription.ReferenceSource{SubscriptionID: parentID, ParentKey: parentKey}
			}
			lineage = append(lineage, parentInstance.lineage...)
			lineage = append(lineage, PathStep{Node: parentInstance.node.Name, Key: parentKey})
		}
		p.mu.Unlock()
	}
	var instance *projectionInstance
	var opening []projectionExpansion
	install := func(subscription querysubscription.Subscription, query querysubscription.QuerySubscription, initial projectionInitialState) error {
		installed := &projectionInstance{id: id, node: node, spec: spec, depth: depth, path: append([]string(nil), path...), lineage: append([]PathStep(nil), lineage...), parentID: parentID, parentKey: parentKey, opening: owner, subscription: subscription, query: query, initial: initial, members: make(map[storage.EntityKey]*projectionMember), revision: initial.Revision, checkpoint: initial.Checkpoint, fence: initial.Fence, localEmpty: !binding.matchable}
		// A row replacement and selector reconciliation share processMu. An
		// installer must see that whole transition, not its temporary new token.
		p.processMu.Lock()
		defer p.processMu.Unlock()
		p.mu.Lock()
		defer p.mu.Unlock()
		if owner != nil && !p.expansionCurrentLocked(*owner) {
			return querysubscription.ErrClosed
		}
		if err := p.ctx.Err(); err != nil {
			return err
		}
		if err := p.installInstanceLocked(installed); err != nil {
			return err
		}
		for _, entity := range initial.Rows {
			member := newProjectionMember(entity.Record, querysubscription.RecordVersion{Revision: initial.Revision, ChangeIndex: entity.ChangeIndex}, entity.Cursor, entity.Pending)
			installed.members[entity.Key] = member
			installed.order = append(installed.order, entity.Key)
			if !member.pending && len(node.Children) > 0 {
				opening = append(opening, projectionExpansion{parent: installed, key: entity.Key, member: member})
			}
		}
		installed.initial.Rows = nil
		instance = installed
		return nil
	}
	if !binding.matchable {
		// Retain an ordinary empty instance so publication and later rebinding
		// use the same branch lifecycle, without asking storage for any rows.
		initial := projectionInitialState{Bounds: querysubscription.Bounds{
			Left:  querysubscription.Boundary{Kind: querysubscription.BoundaryStart},
			Right: querysubscription.Boundary{Kind: querysubscription.BoundaryEnd},
		}}
		err = install(nil, nil, initial)
		return instance, err
	}
	if err := p.expansionLimiter.acquire(p.ctx); err != nil {
		return nil, err
	}
	if node.Entity {
		err = p.queries.SubscribeEntity(p.ctx, querysubscription.EntitySubscribeRequest{ID: id, Query: spec, FetchFrom: fetchFrom}, func(subscription querysubscription.EntitySubscription, snapshot querysubscription.EntitySnapshot) error {
			initial := projectionInitialState{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision}
			if snapshot.Row != nil {
				initial.Rows = []querysubscription.QueryRow{{Row: *snapshot.Row}}
			}
			return install(subscription, nil, initial)
		})
	} else {
		err = p.queries.SubscribeQuery(p.ctx, querysubscription.QuerySubscribeRequest{ID: id, Query: spec, Window: window, Reference: node.Reference}, func(subscription querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
			return install(subscription, subscription, projectionInitialState{
				Bounds: snapshot.Bounds, Rows: snapshot.Rows, Checkpoint: snapshot.Checkpoint,
				Fence: snapshot.Fence, Revision: snapshot.Revision,
			})
		})
	}
	p.expansionLimiter.release()
	if err != nil {
		p.closeInstance(id)
		return nil, err
	}
	for _, expansion := range opening {
		if _, err := p.loadChildren(expansion); err != nil {
			p.closeInstance(id)
			return nil, err
		}
	}
	return instance, nil
}

func (p *Projection) openChildren(expansion projectionExpansion, opened []string) ([]string, error) {
	parent, parentKey := expansion.parent, expansion.key
	bindings, err := p.childBindings(parent.node, expansion.member)
	if err != nil {
		return nil, err
	}
	p.processMu.Lock()
	p.mu.Lock()
	if !p.expansionCurrentLocked(expansion) {
		p.mu.Unlock()
		p.processMu.Unlock()
		return nil, querysubscription.ErrClosed
	}
	retained := p.coveredChildrenLocked(parent.members[parentKey], opened)
	p.mu.Unlock()
	p.processMu.Unlock()
	var ids []string
	for _, child := range parent.node.Children {
		if retained[child] {
			continue
		}
		path := append(append([]string(nil), parent.path...), parentKey.Value, child.Name)
		var instance *projectionInstance
		var err error
		if cause := expansion.member.bindings.errors[child]; cause != nil {
			instance = p.failedNode(child, bindings[child].spec, path, &expansion, cause)
			if instance == nil {
				err = querysubscription.ErrClosed
			}
		} else {
			instance, err = p.openNode(child, bindings[child], path, &expansion)
		}
		if err != nil {
			for _, id := range ids {
				p.closeInstance(id)
			}
			return nil, err
		}
		ids = append(ids, instance.id)
	}
	return ids, nil
}

// ApplyEvent routes an ordered feed event into this projection. Callers may
// offer the same event to multiple projections; unrelated effects are ignored.
func (p *Projection) ApplyEvent(event querysubscription.Event) (ProjectionBatch, bool, error) {
	if event.Entity != nil {
		return ProjectionBatch{}, false, nil
	} // installed by the query client's reference view
	if event.Failure != nil {
		p.processMu.Lock()
		instance := p.currentInstance(event.Failure.SubscriptionID)
		if instance != nil && p.partialErrors && !errors.Is(event.Failure.Cause, querysubscription.ErrBackpressure) {
			batch, applied := p.failInstance(instance, event.Failure.Cause)
			if !applied {
				p.processMu.Unlock()
				return ProjectionBatch{}, false, nil
			}
			p.mu.Lock()
			publish := !p.initializing && !p.awaitingAdmission && instance.published
			released := p.outputReleased
			p.mu.Unlock()
			if publish && !released {
				if err := p.stageOutput(batch); err != nil {
					p.processMu.Unlock()
					p.shutdown(err)
					return ProjectionBatch{}, false, err
				}
				publish = false
			}
			p.processMu.Unlock()
			return batch, publish, nil
		}
		if instance != nil {
			p.shutdown(event.Failure.Cause)
		}
		p.processMu.Unlock()
		if instance != nil {
			return ProjectionBatch{}, false, event.Failure.Cause
		}
		return ProjectionBatch{}, false, nil
	}
	if event.Progress != nil {
		p.mu.Lock()
		if event.Progress.Revision > p.latestRevision {
			p.latestRevision = event.Progress.Revision
		}
		p.rememberCheckpointLocked(event.Progress.Position)
		p.mu.Unlock()
		return ProjectionBatch{}, false, nil
	}
	if event.Delivery == nil {
		return ProjectionBatch{}, false, errors.New("query feed event has no payload")
	}
	return p.ApplyDelivery(*event.Delivery)
}

// ApplyDelivery applies every effect owned by this projection as one unit.
func (p *Projection) ApplyDelivery(envelope querysubscription.DeliveryEnvelope) (ProjectionBatch, bool, error) {
	p.processMu.Lock()
	p.mu.Lock()
	if envelope.Delivery.Revision > p.latestRevision {
		p.latestRevision = envelope.Delivery.Revision
	}
	if envelope.Delivery.Kind == querysubscription.DeliveryCommit {
		p.rememberCheckpointLocked(envelope.Delivery.Checkpoint)
	}
	// Upstream subscription order is not projection dependency order. Settle
	// each parent's topology before classifying its descendants in this same
	// delivery, including children whose installer ran before sibling loads
	// finished. Retired interests still retain their delivery/window metadata.
	type ownedEffect struct {
		querysubscription.SubscriptionEffect
		depth int
	}
	owned := make([]ownedEffect, 0, len(envelope.Effects))
	for effect := range envelope.SubscriptionEffects() {
		if !p.OwnsSubscription(effect.SubscriptionID) {
			continue
		}
		depth := 0
		if instance := p.instances[effect.SubscriptionID]; instance != nil {
			depth = instance.depth
		}
		owned = append(owned, ownedEffect{SubscriptionEffect: effect, depth: depth})
	}
	initializing := p.initializing
	outputReleased := p.outputReleased
	p.mu.Unlock()
	// After the initial cut, an otherwise empty commit can still revoke or
	// grant visibility. Retain its clock until the consumer installs the image.
	// Once released, the connection already processes that clock for all scopes.
	if len(owned) == 0 && (initializing || outputReleased || envelope.Delivery.Kind != querysubscription.DeliveryCommit) {
		p.processMu.Unlock()
		return ProjectionBatch{}, false, nil
	}
	combined := ProjectionBatch{Delivery: cloneDeliveryMetadata(envelope.Delivery)}
	var expansions []projectionExpansion
	if len(owned) > 1 {
		sort.SliceStable(owned, func(i, j int) bool { return owned[i].depth < owned[j].depth })
	}
	for _, effect := range owned {
		instance := p.currentInstance(effect.SubscriptionID)
		if instance == nil || instance.failure != nil {
			retired := retiredProjectionBatch(effect.SubscriptionID, envelope.Delivery, effect.Batch)
			if retired.WindowFailure != nil {
				combined.WindowFailure = retired.WindowFailure
			}
			continue
		}
		p.mu.Lock()
		published := instance.published
		p.mu.Unlock()
		part, pending, err := p.processBatch(instance, envelope.Delivery, effect.Batch)
		if err != nil {
			p.processMu.Unlock()
			return ProjectionBatch{}, false, err
		}
		if published {
			combined.Events = append(combined.Events, part.Events...)
			if part.Membership != nil {
				if combined.Membership == nil {
					combined.Membership = make(map[string]querysubscription.MembershipDelta)
				}
				for id, delta := range part.Membership {
					combined.Membership[id] = delta
				}
			}
			if part.Window != nil {
				combined.Window = part.Window
			}
		}
		expansions = append(expansions, pending...)
	}
	var outgoing bool
	if initializing {
		outgoing = false
	} else if len(combined.Events) > 0 || combined.Window != nil || combined.WindowFailure != nil || combined.Delivery.Kind != 0 {
		if outputReleased {
			outgoing = true
		} else {
			if err := p.stageOutput(combined); err != nil {
				p.processMu.Unlock()
				p.shutdown(err)
				return ProjectionBatch{}, false, err
			}
		}
	}
	if err := p.startExpansions(expansions); err != nil {
		p.processMu.Unlock()
		return ProjectionBatch{}, false, err
	}
	p.processMu.Unlock()
	return combined, outgoing, nil
}

func (p *Projection) currentEvents(rootID string) ([]ProjectionEvent, error) {
	var events []ProjectionEvent
	var visit func(string) error
	visit = func(id string) error {
		p.mu.Lock()
		instance := p.instances[id]
		if instance == nil {
			p.mu.Unlock()
			return nil
		}
		records := make(map[storage.EntityKey]projectionMember, len(instance.members))
		for key, member := range instance.members {
			records[key] = member.capture()
		}
		ordered := append([]storage.EntityKey(nil), instance.order...)
		revision := instance.revision
		checkpoint := instance.checkpoint
		fence := instance.fence
		// An image includes every delivery processed before capture, including
		// authorization-only commits. Its evaluation position is independent of
		// the version of each unchanged record. This applies to initial images
		// and subsequent subtree publications alike.
		if current := p.publicationCheckpointLocked(instance); current != nil {
			checkpoint = current
		}
		open := ProjectionEvent{
			Type: ProjectionInstanceOpened, Path: append([]string(nil), instance.path...), Lineage: cloneLineage(instance.lineage), Node: instance.node.Name,
			EntityNode: instance.node.Entity, InstanceID: instance.id, ParentInstanceID: instance.parentID,
			ParentEntityKey: instance.parentKey, Bounds: instance.initial.Bounds,
		}
		failure := instance.failure
		p.mu.Unlock()
		open.Initial, open.Revision, open.Fence, open.Checkpoint = true, revision, fence, checkpoint
		events = append(events, open)
		if failure != nil {
			status := open
			status.Type, status.Failure = ProjectionBranchChanged, failure
			events = append(events, status)
		}
		for _, key := range ordered {
			record := records[key]
			events = append(events, ProjectionEvent{
				Type: ProjectionRowChanged, Path: append([]string(nil), open.Path...), Lineage: cloneLineage(open.Lineage), Node: open.Node,
				EntityNode: open.EntityNode, InstanceID: open.InstanceID, ParentInstanceID: open.ParentInstanceID,
				ParentEntityKey: open.ParentEntityKey, Kind: querysubscription.Added, Key: key,
				// The capture above already owns this record independently of
				// instance state. Transfer it into the event rather than clone twice.
				Record: record.record, Cursor: record.cursor, Pending: record.pending, Initial: true, Revision: revision,
				Fence: fence, Checkpoint: checkpoint,
				Version: record.version,
			})
			for _, childID := range record.children {
				if err := visit(childID); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return events, visit(rootID)
}

// completePublication is called by the serialized output scheduler. Feed
// changes processed before this call are already reflected in the returned
// current image; feed changes processed afterward observe the instances as
// published and are emitted incrementally.
func (p *Projection) completePublication(publication projectionPublication) (ProjectionBatch, error) {
	defer p.signalRecovery()
	p.processMu.Lock()
	result := ProjectionBatch{}
	for _, id := range publication.InstanceIDs {
		p.mu.Lock()
		instance := p.instances[id]
		eligible := instance != nil && !instance.published
		p.mu.Unlock()
		if !eligible {
			continue
		}
		result.Events = append(result.Events, p.retireReplacedChildren(instance)...)
		if instance.replaces != "" {
			result.Events = append(result.Events, p.closeInstanceEvents(instance.replaces)...)
			instance.replaces = ""
		}
		events, err := p.currentEvents(id)
		if err != nil {
			p.processMu.Unlock()
			return ProjectionBatch{}, err
		}
		result.Events = append(result.Events, events...)
	}
	p.mu.Lock()
	for _, event := range result.Events {
		if event.Type == ProjectionInstanceOpened {
			if instance := p.instances[event.InstanceID]; instance != nil {
				instance.published = true
			}
		}
	}
	expansions := p.missingPublishedExpansionsLocked(publication.InstanceIDs)
	p.mu.Unlock()
	if err := p.startExpansions(expansions); err != nil {
		p.processMu.Unlock()
		return ProjectionBatch{}, err
	}
	p.processMu.Unlock()
	return result, nil
}

func retiredProjectionBatch(instanceID string, metadata querysubscription.DeliveryMetadata, batch querysubscription.Batch) ProjectionBatch {
	result := ProjectionBatch{
		Delivery: cloneDeliveryMetadata(metadata),
	}
	if batch.Window != nil {
		result.WindowFailure = &ProjectionWindowFailure{
			Token: batch.Window.Token, InstanceID: instanceID,
			Cause: errors.New("projection instance closed or failed before its window update was delivered"),
		}
	}
	return result
}

func (p *Projection) processBatch(instance *projectionInstance, metadata querysubscription.DeliveryMetadata, batch querysubscription.Batch) (ProjectionBatch, []projectionExpansion, error) {
	projectionBatch := ProjectionBatch{Delivery: cloneDeliveryMetadata(metadata)}
	p.mu.Lock()
	if c := p.initialChanges; c != nil {
		for _, change := range batch.Changes {
			c.row(instance, change.Key)
		}
		for _, key := range batch.Membership.Removed {
			c.move(instance, key)
		}
		for _, insert := range batch.Membership.Insertions {
			c.move(instance, insert.Key)
		}
		if batch.Clear {
			for key := range instance.members {
				c.row(instance, key)
				c.move(instance, key)
			}
		}
	}
	if metadata.Revision >= instance.revision {
		instance.revision = metadata.Revision
		if metadata.Checkpoint != nil {
			instance.checkpoint = metadata.Checkpoint
		}
		if metadata.Fence != nil {
			instance.fence = metadata.Fence
		}
	}
	p.mu.Unlock()
	var expansions []projectionExpansion
	events := batch.Changes
	if batch.Clear {
		p.mu.Lock()
		cleared := make([]storage.EntityKey, 0, len(instance.members))
		for key := range instance.members {
			cleared = append(cleared, key)
		}
		p.mu.Unlock()
		events = make([]querysubscription.Change, 0, len(cleared)+len(batch.Changes))
		for _, key := range cleared {
			events = append(events, querysubscription.Change{Kind: querysubscription.Removed, Row: querysubscription.Row{Key: key, ChangeIndex: batch.ClearChangeIndex}})
		}
		events = append(events, batch.Changes...)
	}
	for _, event := range events {
		projectionEvent := ProjectionEvent{Type: ProjectionRowChanged, Path: append([]string(nil), instance.path...), Lineage: cloneLineage(instance.lineage), Node: instance.node.Name, EntityNode: instance.node.Entity, InstanceID: instance.id, ParentInstanceID: instance.parentID, ParentEntityKey: instance.parentKey, Kind: event.Kind, Key: event.Key, Record: storage.CloneRecord(event.Record), Pending: event.Pending, ChangeIndex: event.ChangeIndex, Version: querysubscription.RecordVersion{Revision: metadata.Revision, ChangeIndex: event.ChangeIndex}}
		if event.Kind != querysubscription.Removed {
			projectionEvent.Cursor = append([]byte(nil), event.Cursor...)
		}
		p.mu.Lock()
		previous := instance.members[event.Key]
		var member *projectionMember
		if event.Kind == querysubscription.Removed {
			delete(instance.members, event.Key)
		} else {
			member = newProjectionMember(event.Record, projectionEvent.Version, event.Cursor, event.Pending)
			instance.members[event.Key] = member
		}
		p.mu.Unlock()
		closed, preserved, err := p.reconcileChildren(instance, previous, member)
		if err != nil {
			return ProjectionBatch{}, nil, err
		}
		projectionBatch.Events = append(projectionBatch.Events, closed...)
		if event.Kind == querysubscription.Removed && p.expansions != nil {
			p.expansions.discard(instance.id, event.Key)
		}
		projectionBatch.Events = append(projectionBatch.Events, projectionEvent)
		p.mu.Lock()
		mayExpand := p.initializing || p.awaitingAdmission || instance.published
		p.mu.Unlock()
		if event.Kind != querysubscription.Removed && !event.Pending && len(instance.node.Children) > 0 && mayExpand && !preserved {
			revision := uint64(0)
			if metadata.Kind != 0 {
				revision = metadata.Revision
			}
			expansions = append(expansions, projectionExpansion{parent: instance, key: event.Key, member: member, revision: revision})
		}
	}
	if instance.node.Entity {
		// Entity subscriptions intentionally omit query membership deltas. Their
		// zero-or-one ordered state follows directly from row changes.
		p.mu.Lock()
		for _, event := range events {
			switch event.Kind {
			case querysubscription.Removed:
				if len(instance.order) == 1 && instance.order[0] == event.Key {
					instance.order = nil
				}
			case querysubscription.Added, querysubscription.Updated:
				instance.order = []storage.EntityKey{event.Key}
			}
		}
		p.mu.Unlock()
	} else {
		membership := batch.Membership
		p.mu.Lock()
		if batch.Clear {
			// Clear is compact on the private engine queue, but ProjectionBatch has
			// no reset primitive. Expand it once at this boundary so visibility and
			// connection models receive a self-contained ordered delta.
			membership.Removed = append([]storage.EntityKey(nil), instance.order...)
		}
		if len(membership.Removed) > 0 || len(membership.Insertions) > 0 {
			order, err := querysubscription.ApplyMembership(instance.order, membership)
			if err != nil {
				p.mu.Unlock()
				return ProjectionBatch{}, nil, fmt.Errorf("apply query membership delta: %w", err)
			}
			instance.order = order
		}
		p.mu.Unlock()
		if len(membership.Removed) > 0 || len(membership.Insertions) > 0 {
			projectionBatch.Membership = map[string]querysubscription.MembershipDelta{instance.id: cloneMembershipDelta(membership)}
		}
	}
	if batch.Window != nil {
		projectionBatch.Window = &ProjectionWindowTransition{
			Token: batch.Window.Token, InstanceID: instance.id, Bounds: batch.Window.Bounds,
			Fence: batch.Window.Fence, Checkpoint: batch.Window.Checkpoint,
		}
		p.mu.Lock()
		instance.initial.Bounds = batch.Window.Bounds
		p.mu.Unlock()
	}
	return projectionBatch, expansions, nil
}

func (p *Projection) startExpansions(expansions []projectionExpansion) error {
	return p.expansions.enqueue(expansions)
}

func (p *Projection) expandChildren(expansion projectionExpansion) {
	p.mu.Lock()
	materializing := p.initializing || p.awaitingAdmission
	p.mu.Unlock()
	ids, err := p.loadChildren(expansion)
	if err != nil {
		p.processMu.Lock()
		p.mu.Lock()
		current := p.expansionCurrentLocked(expansion)
		p.mu.Unlock()
		if current && p.ctx.Err() == nil {
			p.shutdown(err)
		}
		p.processMu.Unlock()
		return
	}
	p.mu.Lock()
	parentPublished := p.instances[expansion.parent.id] == expansion.parent && expansion.parent.published
	p.mu.Unlock()
	if parentPublished && !materializing {
		if err := p.publishSubtrees(ids, expansion.revision); err != nil && p.ctx.Err() == nil {
			p.shutdown(err)
		}
	}
}

// publishSubtrees exposes a dynamically opened subtree once all currently
// reachable descendants have loaded. Feed events continue updating unpublished
// instances while reads run, and processMu makes the final current-image cut
// atomic with respect to applying the next feed event.
func (p *Projection) publishSubtrees(rootIDs []string, causalRevision uint64) error {
	if len(rootIDs) == 0 {
		return nil
	}
	for {
		p.processMu.Lock()
		reachable, missing := p.publicationState(rootIDs)
		if len(reachable) == 0 {
			p.processMu.Unlock()
			return nil
		}
		if len(missing) == 0 {
			batch := ProjectionBatch{publication: &projectionPublication{
				InstanceIDs: append([]string(nil), rootIDs...), CausalRevision: causalRevision,
			}}
			p.mu.Lock()
			outputReleased := p.outputReleased
			p.mu.Unlock()
			var err error
			if outputReleased {
				err = p.queue.Offer(batch)
			} else {
				err = p.stageOutput(batch)
			}
			p.processMu.Unlock()
			return err
		}
		p.processMu.Unlock()
		for _, expansion := range missing {
			if _, err := p.loadChildren(expansion); err != nil {
				if !p.anyInstance(rootIDs) {
					return nil
				}
				return err
			}
		}
	}
}

// publicationState returns the current reachable unpublished subtree and rows
// whose descendants were invalidated while that subtree was catching up.
// processMu must be held by the caller.
func (p *Projection) publicationState(rootIDs []string) ([]*projectionInstance, []projectionExpansion) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var reachable []*projectionInstance
	var missing []projectionExpansion
	seen := make(map[string]struct{})
	var visit func(string)
	visit = func(id string) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		instance := p.instances[id]
		if instance == nil || instance.published {
			return
		}
		reachable = append(reachable, instance)
		for key, member := range instance.members {
			if len(instance.node.Children) > 0 && !member.pending && !member.expanded {
				missing = append(missing, projectionExpansion{parent: instance, key: key, member: member})
			}
			for _, childID := range member.children {
				visit(childID)
			}
		}
	}
	for _, id := range rootIDs {
		visit(id)
	}
	sort.Slice(reachable, func(i, j int) bool { return reachable[i].id < reachable[j].id })
	return reachable, missing
}

// missingPublishedExpansionsLocked finds descendants invalidated after the
// current-image cut but before the serialized consumer installed the publication.
// Publishing the current parent row first is safe: projection references may be
// temporarily unresolved, and the follow-up expansion publishes its own current
// image. p.mu and processMu must be held by the caller.
func (p *Projection) missingPublishedExpansionsLocked(rootIDs []string) []projectionExpansion {
	var missing []projectionExpansion
	seen := make(map[string]struct{})
	var visit func(string)
	visit = func(id string) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		instance := p.instances[id]
		if instance == nil || !instance.published {
			return
		}
		for key, member := range instance.members {
			if len(instance.node.Children) > 0 && !member.pending && !member.expanded {
				missing = append(missing, projectionExpansion{parent: instance, key: key, member: member, revision: instance.revision})
			}
			for _, childID := range member.children {
				visit(childID)
			}
		}
	}
	for _, id := range rootIDs {
		visit(id)
	}
	return missing
}

// expansionCurrentLocked fences both replacement rows and completed loads.
// Member identities cannot be reused while an older worker still holds one.
func (p *Projection) expansionCurrentLocked(expansion projectionExpansion) bool {
	return expansion.member != nil && p.instanceCurrentLocked(expansion.parent) &&
		expansion.parent.members[expansion.key] != nil &&
		expansion.parent.members[expansion.key].expansionToken == expansion.member.expansionToken &&
		!expansion.parent.members[expansion.key].pending && !expansion.parent.members[expansion.key].expanded
}

func (p *Projection) anyInstance(ids []string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range ids {
		if p.instances[id] != nil {
			return true
		}
	}
	return false
}

func (p *Projection) rememberCheckpointLocked(checkpoint *querysubscription.Position) {
	if checkpoint == nil {
		return
	}
	if p.latestCheckpoints == nil {
		p.latestCheckpoints = make(map[storage.SourceID]*querysubscription.Position)
	}
	if current := p.latestCheckpoints[checkpoint.SourceID]; current != nil && current.Value >= checkpoint.Value {
		return
	}
	p.latestCheckpoints[checkpoint.SourceID] = clonePosition(checkpoint)
	if p.progressChanged != nil {
		close(p.progressChanged)
	}
	p.progressChanged = make(chan struct{})
}

func (p *Projection) publicationCheckpointLocked(instance *projectionInstance) *querysubscription.Position {
	source := storage.SourceID("")
	if instance.checkpoint != nil {
		source = instance.checkpoint.SourceID
	} else if instance.fence != nil {
		source = instance.fence.SourceID
	} else {
		return nil
	}
	position := instance.checkpoint
	floor, err := querysubscription.SnapshotPosition(instance.checkpoint, instance.fence)
	if err != nil {
		return nil
	}
	// A fence-only image must keep snapshot authorization until consumed
	// progress covers its fence. An arbitrary older checkpoint is not proof.
	if current := p.latestCheckpoints[source]; current != nil && current.Value >= floor.Value && (position == nil || current.Value > position.Value) {
		position = current
	}
	return clonePosition(position)
}

func clonePosition(position *querysubscription.Position) *querysubscription.Position {
	if position == nil {
		return nil
	}
	return &querysubscription.Position{SourceID: position.SourceID, Value: position.Value}
}

// QueueWindowUpdate delivers the resolved transition through the instance's
// CDC queue. This preserves its order relative to changes already in flight.
func (p *Projection) QueueWindowUpdate(ctx context.Context, instanceID string, changes []querysubscription.BoundaryChange, token string) error {
	p.mu.Lock()
	if p.awaitingAdmission {
		p.mu.Unlock()
		return errors.New("projection initial image has not been admitted")
	}
	instance := p.instances[instanceID]
	if instance == nil {
		p.mu.Unlock()
		return errors.New("projection instance not found")
	}
	// Failure can retire this handle concurrently. Capture it under the
	// instance lock, but keep potentially blocking IO outside that lock.
	query := instance.query
	p.mu.Unlock()
	if instance.node.Entity {
		return errors.New("entity projection nodes do not have mutable windows")
	}
	if query == nil {
		return errors.New("query projection instance has no window subscription")
	}
	return query.UpdateWindow(ctx, changes, token)
}

// OwnsSubscription reports whether id belongs to this projection generation,
// including instances that have just closed while a committed delivery is
// still in flight.
func (p *Projection) OwnsSubscription(id string) bool {
	return strings.HasPrefix(id, p.prefix+":")
}

func (p *Projection) closeInstance(id string) {
	_ = p.closeInstanceEvents(id)
}

func (p *Projection) closeInstanceEvents(id string) []ProjectionEvent {
	p.mu.Lock()
	instance := p.instances[id]
	if instance == nil {
		p.mu.Unlock()
		return nil
	}
	if p.initialChanges != nil {
		p.initialChanges.close(ProjectionEvent{Type: ProjectionInstanceClosed, InstanceID: id, Path: append([]string(nil), instance.path...), Node: instance.node.Name, EntityNode: instance.node.Entity, ParentInstanceID: instance.parentID, ParentEntityKey: instance.parentKey})
	}
	delete(p.instances, id)
	p.releaseInstanceSlotLocked(instance)
	delete(p.failed, id)
	handle := instance.takeSubscriptionLocked()
	var children []string
	for _, member := range instance.members {
		children = append(children, member.children...)
		for _, roots := range member.replacementRoots {
			children = append(children, roots...)
		}
	}
	p.mu.Unlock()
	if p.expansions != nil {
		p.expansions.discardParent(id)
	}
	var events []ProjectionEvent
	for _, childID := range children {
		events = append(events, p.closeInstanceEvents(childID)...)
	}
	if handle != nil {
		_ = handle.Close(context.WithoutCancel(p.ctx))
	}
	events = append(events, ProjectionEvent{Type: ProjectionInstanceClosed, Path: append([]string(nil), instance.path...), Lineage: cloneLineage(instance.lineage), Node: instance.node.Name, EntityNode: instance.node.Entity, InstanceID: instance.id, ParentInstanceID: instance.parentID, ParentEntityKey: instance.parentKey})
	return events
}

func cloneMembershipDelta(delta querysubscription.MembershipDelta) querysubscription.MembershipDelta {
	return querysubscription.MembershipDelta{
		Removed:    append([]storage.EntityKey(nil), delta.Removed...),
		Insertions: append([]querysubscription.Insertion(nil), delta.Insertions...),
	}
}

func cloneDeliveryMetadata(metadata querysubscription.DeliveryMetadata) querysubscription.DeliveryMetadata {
	return metadata
}

func cloneLineage(path []PathStep) []PathStep {
	return append([]PathStep(nil), path...)
}
