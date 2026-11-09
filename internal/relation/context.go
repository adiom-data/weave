package relation

import (
	"iter"

	"github.com/adiom-data/weave/internal/storage"
)

// Claims are opaque policy inputs. Authentication and claim validation belong
// to the application edge. No identity claim or separate subject ID is required.
type Claims map[string]storage.Value

// ProjectionStep identifies one concrete row occurrence on the directional
// expansion path that produced a candidate. Policies must explicitly approve
// every edge in this order; merely sharing a projection or a connection never
// grants visibility. Node is descriptive. Key.Relation is the canonical object
// namespace policies should use when matching an edge. Record and Complete
// carry that occurrence's immutable ancestor image, not a global latest lookup.
type ProjectionStep struct {
	Node     string
	Key      storage.EntityKey
	Record   storage.Record
	Complete bool
}

// ProjectionPath is an immutable, occurrence-specific path node. Siblings can
// share their parent without copying its records or rebuilding a flat path.
// Records returned by Step/Steps are read-only, like Candidate.Record.
type ProjectionPath struct {
	parent *ProjectionPath
	step   ProjectionStep
	length int
}

// NewProjectionPath captures one record image and retains an immutable parent.
// Later mutations of the supplied record cannot alter the captured context.
func NewProjectionPath(parent *ProjectionPath, step ProjectionStep) *ProjectionPath {
	step.Record = storage.CloneRecord(step.Record)
	return &ProjectionPath{parent: parent, step: step, length: parent.Len() + 1}
}
func (p *ProjectionPath) Parent() *ProjectionPath {
	if p == nil {
		return nil
	}
	return p.parent
}
func (p *ProjectionPath) Step() ProjectionStep {
	if p == nil {
		return ProjectionStep{}
	}
	return p.step
}
func (p *ProjectionPath) Len() int {
	if p == nil {
		return 0
	}
	return p.length
}

// Steps visits the path root first and honors early termination. No flat copy
// is allocated, and retaining a path keeps all its captured ancestors alive.
func (p *ProjectionPath) Steps() iter.Seq[ProjectionStep] {
	return func(yield func(ProjectionStep) bool) { p.visit(yield) }
}
func (p *ProjectionPath) visit(yield func(ProjectionStep) bool) bool {
	return p == nil || p.parent.visit(yield) && yield(p.step)
}

// Scope describes server-derived context for a prepared policy. Projection is
// occurrence-specific and directional, so Project -> ProjectTask -> Task cannot
// accidentally authorize Task -> Project or an unrelated Task. Values may carry
// additional application-defined context without teaching the query engine its
// meaning.
type Scope struct {
	// Computed names are selected projection outputs, not datastore columns.
	Computed []string
	// InstanceID identifies the concrete consumer-owned query occurrence, not
	// a relation or an entity. It is never supplied by an untrusted policy.
	InstanceID string
	Projection *ProjectionPath
	Values     map[string]storage.Value
}

// Candidate is an immutable entity image considered for public delivery.
// Context is server-derived, but the projection definition that caused it may
// be client supplied. A policy must validate the complete, ordered expansion
// path against its configured directional edge rules.
type Candidate struct {
	Key    storage.EntityKey
	Record storage.Record
	// Complete is false for an unresolved reference whose authoritative entity
	// image will arrive in a later hydration delivery. Policies that require row
	// facts should fail closed; policies based only on trusted Context may still
	// permit the reference.
	Complete bool
	Context  Scope
}
