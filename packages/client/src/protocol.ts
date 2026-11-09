// Application input types derive from the generated schema. The normalized
// runtime model below deliberately stays independent of protobuf message objects.
import { fromBinary, fromJson, toJson } from '@bufbuild/protobuf'
import type { JsonValue } from '@bufbuild/protobuf'
import * as pb from './gen/reactive/v1/reactive_pb.js'

type DeepReadonly<T> = T extends readonly (infer U)[] ? readonly DeepReadonly<U>[] :
  T extends object ? { readonly [K in keyof T]: DeepReadonly<T[K]> } : T
type PresentField<T> = { [K in keyof T]-?: Readonly<Required<Pick<T, K>>> }[keyof T]
export type Value = PresentField<DeepReadonly<pb.ValueJson>>
export type RecordData = Readonly<Record<string, Value>>
export type FieldPath = DeepReadonly<pb.FieldPathJson>
export type Operator = NonNullable<pb.ComparisonJson['operator']>
export type Predicate = DeepReadonly<pb.PredicateJson>
export type Query = DeepReadonly<pb.QueryJson>
export type Binding = DeepReadonly<pb.ProjectionBindingJson>
export type InitialWindow = DeepReadonly<pb.InitialWindowJson>
export type ProjectionNode = Omit<DeepReadonly<pb.ProjectionNodeJson>, 'nodeId' | 'children'> & {
  readonly nodeId: string
  readonly children?: readonly ProjectionNode[]
}
export type ProjectionRequest = Omit<DeepReadonly<pb.SubscribeProjectionJson>, 'root'> & { readonly root: ProjectionNode }
export type BoundaryUpdate = DeepReadonly<pb.BoundaryUpdateJson>
export type Boundary = DeepReadonly<pb.BoundaryJson>
export interface Bounds { readonly left?: Boundary; readonly right?: Boundary }
export type ClientMessage = DeepReadonly<pb.ClientMessageJson>

export type EntityChange = { readonly id: string } & (
  { readonly type: 'upsert'; readonly values: RecordData } | { readonly type: 'pending' } | { readonly type: 'released' }
)
export type QueryChange = { readonly id: string; readonly nodeId: string } & (
  | { readonly type: 'reset'; readonly ids: readonly string[]; readonly version: bigint; readonly bounds: Bounds }
  | { readonly type: 'delta'; readonly base: bigint; readonly version: bigint; readonly removed: readonly string[];
      readonly insertions: readonly { readonly id: string; readonly index: number }[] }
  | { readonly type: 'window'; readonly bounds: Bounds }
  | { readonly type: 'closed' }
)
export interface Occurrence {
  readonly id: string
  readonly subscriptionId: string
  readonly nodeId: string
  readonly parentOccurrenceId: string
  readonly parentEntityId: string
  readonly target: Readonly<{ type: 'entity' | 'query'; id: string }>
}
export type OccurrenceChange = { readonly id: string; readonly type: 'detached' } | { readonly type: 'attached'; readonly value: Occurrence }
/** Opaque application payload encoded as base64, matching protobuf JSON bytes. */
export interface TransactionMessage { readonly kind: string; readonly payload: string }
export interface ModelBatch {
  readonly computed: readonly ComputedChange[]
  readonly messages: readonly TransactionMessage[]
  readonly entities: readonly EntityChange[]
  readonly queries: readonly QueryChange[]
  readonly occurrences: readonly OccurrenceChange[]
  readonly kind: string
  readonly atomic: boolean
  readonly checkpoint: string
}
export type ComputedChange = Readonly<{ occurrenceId: string; entityId: string }> & (
  | Readonly<{ type: 'upsert'; values: RecordData }>
  | Readonly<{ type: 'released' }>
)
export interface ServerMessage {
  readonly subscriptionId: string
  readonly requestId: string
  readonly sequence: bigint
  readonly connectionSequence: bigint
  readonly event:
    | { readonly type: 'connectionOpened'; readonly context: string }
    | { readonly type: 'batch'; readonly batch: ModelBatch }
    | { readonly type: 'ready' }
    | { readonly type: 'error'; readonly code: string; readonly message: string }
}

export class ProtocolError extends Error {}
export interface DecodeLimits { readonly maxDepth?: number; readonly maxNodes?: number }

// Validate and own application inputs before creating subscriptions. Encoding
// errors must not be misclassified as transport failures and cause retry loops.
export function prepareProjectionRequest(input: ProjectionRequest): ProjectionRequest {
  // The generated validator accepts JSON values; readonly input arrays have
  // the same runtime shape but deliberately do not satisfy mutable JsonValue.
  const message = fromJson(pb.SubscribeProjectionSchema, input as unknown as JsonValue)
  if (!message.root) throw new ProtocolError('Projection root required')
  const visit = (node: pb.ProjectionNode): void => {
    id(node.nodeId)
    if (!node.selection.case) throw new ProtocolError('Projection selection required')
    node.children.forEach(visit)
  }
  visit(message.root)
  return immutable(toJson(pb.SubscribeProjectionSchema, message)) as ProjectionRequest
}

/** Validate, normalize, and own local record values through the same codec as CDC. */
export function prepareRecordData(values: RecordData): RecordData {
  const message = fromJson(pb.EntityUpsertSchema, { values } as unknown as JsonValue)
  return immutable(record(message.values))
}

export function immutable<T>(value: T): T {
  if (value && typeof value === 'object') {
    for (const item of Object.values(value)) immutable(item)
    Object.freeze(value)
  }
  return value
}
function id(value: string): string {
  if (!value) throw new ProtocolError('Empty identifier')
  return value
}
// Applies to decoded message graphs, not bytes within a bytes field. The
// transport separately bounds frame size, and the codec bounds parse recursion.
function checkStructure(value: unknown, limits: DecodeLimits): void {
  let remaining = limits.maxNodes ?? 100000
  const visit = (item: unknown, depth: number): void => {
    if (--remaining < 0 || depth > (limits.maxDepth ?? 64)) throw new ProtocolError('Message structure limit exceeded')
    if (item && typeof item === 'object' && !(item instanceof Uint8Array)) {
      for (const [key, child] of Object.entries(item)) if (key !== '$typeName') visit(child, depth + 1)
    }
  }
  visit(value, 0)
}
function record(values: Record<string, pb.Value>): RecordData {
  const result: Record<string, Value> = Object.create(null)
  for (const [key, value] of Object.entries(values)) result[key] = field(value)
  return result
}
function field(value: pb.Value): Value {
  switch (value.kind.case) {
    case undefined: throw new ProtocolError('Missing value kind')
    case 'listValue': return { listValue: { values: value.kind.value.values.map(field) } }
    case 'objectValue': return { objectValue: { fields: record(value.kind.value.fields) } }
    // Generated conversion preserves strings, base64 bytes, and nanosecond
    // timestamps. No handwritten numeric or temporal wire parsing.
    default: return toJson(pb.ValueSchema, value) as Value
  }
}
function boundary(value: pb.Boundary | undefined): Boundary | undefined {
  if (!value) return undefined
  if (!value.value.case || (value.value.case === 'sentinel' &&
      value.value.value !== pb.Boundary_Sentinel.START && value.value.value !== pb.Boundary_Sentinel.END)) throw new ProtocolError('Invalid boundary')
  return toJson(pb.BoundarySchema, value)
}
function bounds(value: { left?: pb.Boundary; right?: pb.Boundary }): Bounds {
  return { left: boundary(value.left), right: boundary(value.right) }
}
export function decodeServerMessage(input: Uint8Array, limits: DecodeLimits = {}): ServerMessage {
  return normalizeServerMessage(fromBinary(pb.ServerMessageSchema, input, {
    recursionLimit: limits.maxDepth ?? 64, readUnknownFields: false,
  }), limits)
}
export function normalizeServerMessage(message: pb.ServerMessage, limits: DecodeLimits = {}): ServerMessage {
  checkStructure(message, limits)
  let event: ServerMessage['event']
  switch (message.event.case) {
    case 'connectionOpened': {
      if (message.subscriptionId || message.requestId || message.sequence !== 0n || message.connectionSequence !== 0n) throw new ProtocolError('Invalid connection-opened envelope')
      event = { type: 'connectionOpened', context: id(message.event.value.connectionContext) }
      break
    }
    case 'ready': event = { type: 'ready' }; break
    case 'error': event = { type: 'error', code: id(message.event.value.code), message: message.event.value.message }; break
    case 'modelBatch': {
      const batch = message.event.value
      const entities = batch.entities.map((change): EntityChange => {
        const entityId = id(change.entityId)
        switch (change.event.case) {
          case 'upsert': return { id: entityId, type: 'upsert', values: record(change.event.value.values) }
          case 'pending': return { id: entityId, type: 'pending' }
          case 'released': return { id: entityId, type: 'released' }
          default: throw new ProtocolError('Missing entity event')
        }
      })
      const queries = batch.queries.map((change): QueryChange => {
        const base = { id: id(change.querySubscriptionId), nodeId: change.nodeId }
        switch (change.event.case) {
          case 'closed': return { ...base, type: 'closed' }
          case 'windowChanged': return { ...base, type: 'window', bounds: bounds(change.event.value) }
          case 'reset': {
            const value = change.event.value
            return { ...base, type: 'reset', ids: value.entityIds.map(id), version: value.version, bounds: bounds(value) }
          }
          case 'delta': {
            const value = change.event.value
            return { ...base, type: 'delta', base: value.baseVersion, version: value.resultVersion,
              removed: value.removedEntityIds.map(id), insertions: value.insertions.map(item => ({ id: id(item.entityId), index: item.finalIndex })) }
          }
          default: throw new ProtocolError('Missing query event')
        }
      })
      const occurrences = batch.projections.map((change): OccurrenceChange => {
        const occurrenceId = id(change.occurrenceId)
        if (change.event.case === 'detached') return { id: occurrenceId, type: 'detached' }
        if (change.event.case !== 'attached') throw new ProtocolError('Missing occurrence event')
        const target = change.event.value.target
        if (!target.case) throw new ProtocolError('Missing occurrence target')
        return { type: 'attached', value: {
          id: occurrenceId, subscriptionId: id(change.subscriptionId), nodeId: id(change.nodeId),
          parentOccurrenceId: change.parentOccurrenceId, parentEntityId: change.parentEntityId,
          target: { type: target.case === 'entityId' ? 'entity' : 'query', id: id(target.value) },
        } }
      })
      const kind = pb.ModelBatch_Kind[batch.kind]
      if (kind === undefined) throw new ProtocolError('Unknown model batch kind')
      if (batch.messages.length && kind !== 'COMMIT') throw new ProtocolError('Transaction messages require a commit batch')
      const messages = batch.messages.map(message => ({ kind: id(message.kind), payload: toJson(pb.TransactionMessageSchema, message).payload ?? '' }))
      const computed = batch.computed.map((change): ComputedChange => {
        const base = { occurrenceId: id(change.occurrenceId), entityId: id(change.entityId) }
        switch (change.event.case) {
          case 'upsert': return { ...base, type: 'upsert', values: record(change.event.value.fields) }
          case 'released': return { ...base, type: 'released' }
          default: throw new ProtocolError('Missing computed event')
        }
      })
      event = { type: 'batch', batch: { entities, queries, occurrences, computed, messages, kind, atomic: batch.atomic, checkpoint: batch.checkpoint } }
      break
    }
    default: throw new ProtocolError('Missing server event')
  }
  return immutable({ subscriptionId: message.subscriptionId, requestId: message.requestId,
    sequence: message.sequence, connectionSequence: message.connectionSequence, event })
}

export function scalar(values: RecordData, name: string): string {
  const value = values[name]
  if (!value) return ''
  if ('textValue' in value) return value.textValue ?? ''
  if ('uuidValue' in value) return value.uuidValue ?? ''
  if ('integerValue' in value) return value.integerValue ?? ''
  if ('decimalValue' in value) return value.decimalValue ?? ''
  if ('floatValue' in value) return value.floatValue ?? ''
  if ('datetimeValue' in value) return value.datetimeValue ?? ''
  return ''
}
