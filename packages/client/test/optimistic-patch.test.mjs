import test from 'node:test'
import assert from 'node:assert/strict'
import { mergeValue } from '../dist/index.js'
import { prepareProjectedPatch } from '../dist/optimistic_patch.js'

const text = { kind: 'text' }
const field = (name, path = [name], type = text, computed = false) => ({ name, path, type, computed })

test('wide typed merges index shared field metadata once per patch, not per edited member or branch', () => {
  const size = 3000
  let reads = 0
  const fields = Array.from({ length: size }, (_, i) => ({
    get name() { reads++; return `alias${i}` },
    path: [`stored${i}`], type: text,
  }))
  const type = { kind: 'object', fields }
  const shape = [field('left', ['left'], type), field('right', ['right'], type)]
  const members = Object.fromEntries(Array.from({ length: size }, (_, i) => [`alias${i}`, `value${i}`]))
  const input = { left: mergeValue(members), right: mergeValue(members) }
  const paths = prepareProjectedPatch(input, shape, [], 'root')
  assert.equal(reads, size, 'both branches must share one metadata index')
  assert.equal(paths.length, size * 2)
  assert.deepEqual(paths[0], { path: ['left', 'stored0'], value: { textValue: 'value0' } })
  assert.deepEqual(paths.at(-1), { path: ['right', `stored${size - 1}`], value: { textValue: `value${size - 1}` } })
  assert.ok(Object.isFrozen(paths))
  // Index lifetime is one compilation; independent calls do not retain it.
  prepareProjectedPatch(input, shape, [], 'root')
  assert.equal(reads, size * 2)
})

test('root indexing preserves mixed sets, unsets, and disjointness validation', () => {
  const size = 1000
  let reads = 0
  const fields = Array.from({ length: size }, (_, i) => ({
    get name() { reads++; return `field${i}` }, path: [`stored${i}`], type: text,
  }))
  const set = Object.fromEntries(Array.from({ length: size / 2 }, (_, i) => [`field${i}`, 'value']))
  const unset = Array.from({ length: size / 2 }, (_, i) => `field${i + size / 2}`)
  const paths = prepareProjectedPatch(set, fields, unset, 'root')
  assert.equal(reads, size)
  assert.equal(paths.length, size)
  assert.deepEqual(paths.at(-1), { path: [`stored${size - 1}`], unset: true })
  assert.throws(() => prepareProjectedPatch(set, fields, ['field0'], 'root'), /disjoint/)
  assert.deepEqual(prepareProjectedPatch({ field0: undefined }, fields, ['field0'], 'root'), [{ path: ['stored0'], unset: true }])
})

test('indexed lookup keeps stored-only validation, first-match aliases, and literal field names', () => {
  const fields = [field('computed', ['computed'], text, true), field('alias', ['first']), field('alias', ['second']),
    field('__proto__'), field('constructor'), field('a.b')]
  const input = Object.fromEntries(['alias', '__proto__', 'constructor', 'a.b'].map(name => [name, name]))
  const expected = ['first', '__proto__', 'constructor', 'a.b']
  const paths = prepareProjectedPatch(input, fields, [], 'root')
  assert.deepEqual(paths.map(change => change.path), expected.map(name => [name]))
  for (const name of ['computed', 'missing']) {
    assert.throws(() => prepareProjectedPatch({ [name]: undefined }, fields, [], 'root'), /Unknown stored field/)
    assert.throws(() => prepareProjectedPatch({}, fields, [name], 'root'), /Unknown stored field/)
    const nested = [field('object', ['object'], { kind: 'object', fields })]
    assert.throws(() => prepareProjectedPatch({ object: mergeValue({ [name]: undefined }) }, nested, [], 'root'), /Unknown stored field/)
  }
})
