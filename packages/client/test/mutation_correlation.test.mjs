import test from 'node:test'
import assert from 'node:assert/strict'
import { decodeMutationReceipts, mutationHeaders, mutationReceiptKind, connectionContextHeader, mutationIdHeader } from '../dist/index.js'

const id = '12345678-1234-1234-1234-123456789abc'
test('mutation headers use configurable, distinct transport-independent names', () => {
  const context = { connectionContext: 'opaque-context', mutationId: id }
  assert.deepEqual(mutationHeaders(context), { [connectionContextHeader]: context.connectionContext, [mutationIdHeader]: id })
  assert.deepEqual(mutationHeaders(context, { connectionContext: 'x-context', mutationId: 'x-id' }), { 'x-context': context.connectionContext, 'x-id': id })
  assert.throws(() => mutationHeaders(context, { connectionContext: 'x-id', mutationId: 'X-ID' }))
})

test('standard receipt decoder ignores unrelated, malformed, and non-atomic messages', () => {
  const payload = mutationId => Buffer.from(mutationId, 'utf8').toString('base64')
  const transaction = { atomic: true, messages: [
    { kind: 'other', payload: payload(id) },
    { kind: mutationReceiptKind, payload: '/w==' },
    { kind: mutationReceiptKind, payload: payload('not-a-uuid') },
    { kind: mutationReceiptKind, payload: payload('') },
    { kind: mutationReceiptKind, payload: payload(id.toUpperCase()) },
    { kind: mutationReceiptKind, payload: payload(id + '\n') },
    { kind: mutationReceiptKind, payload: payload('\uFEFF' + id) },
    { kind: mutationReceiptKind, payload: 'not base64!' },
    // The removed protobuf wrapper is not a second supported receipt format.
    { kind: mutationReceiptKind, payload: Buffer.concat([Buffer.from([0x0a, 36]), Buffer.from(id)]).toString('base64') },
    { kind: mutationReceiptKind, payload: payload(id) },
  ] }
  assert.deepEqual(decodeMutationReceipts(transaction), [id])
  assert.deepEqual(decodeMutationReceipts({ ...transaction, atomic: false }), [])
})
