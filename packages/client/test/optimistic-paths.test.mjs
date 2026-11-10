import test from 'node:test'
import assert from 'node:assert/strict'
import { preparePaths, applyPaths } from '../dist/optimistic_paths.js'
import { prepareRecordData } from '../dist/protocol.js'

const text = textValue => ({ textValue })
const object = fields => ({ objectValue: { fields } })

test('path indexing coalesces equal writes and rejects conflicts in either insertion order', () => {
  const a = { path: ['metadata', 'color'], value: text('red') }
  const parent = { path: ['metadata'], value: object({}) }
  const child = { path: ['metadata', 'color', 'nested'], unset: true }
  assert.equal(preparePaths([a, { ...a, path: [...a.path] }]).length, 1)
  assert.equal(preparePaths([{ path: ['gone'], unset: true }, { path: ['gone'], unset: true }]).length, 1)
  for (const other of [parent, child]) for (const pair of [[a, other], [other, a]]) {
    assert.throws(() => preparePaths(pair), /Overlapping/)
  }
  assert.throws(() => preparePaths([a, { ...a, value: text('blue') }]), /Conflicting/)
  assert.throws(() => preparePaths([a, { path: a.path, unset: true }]), /Conflicting/)
  // Literal segments do not alias dotted paths, empty keys, or prototype names.
  const paths = preparePaths([
    { path: ['a.b'], value: text('literal') },
    { path: ['a', 'b'], value: text('nested') },
    { path: ['', '__proto__'], value: text('safe') },
  ])
  const result = applyPaths(prepareRecordData({}), paths)
  assert.equal(result['a.b'].textValue, 'literal')
  assert.equal(result.a.objectValue.fields.b.textValue, 'nested')
  assert.equal(result[''].objectValue.fields.__proto__.textValue, 'safe')
  assert.equal(Object.getPrototypeOf(result[''].objectValue.fields), null)
})

test('wide merges copy each shared object once and retain untouched immutable subtrees', () => {
  const size = 3000
  const fields = Object.freeze(Object.fromEntries(Array.from({ length: size }, (_, i) => [`k${i}`, Object.freeze(text('old'))])))
  let copies = 0
  const observed = new Proxy(fields, { ownKeys(target) { copies++; return Reflect.ownKeys(target) } })
  const untouched = Object.freeze(object(Object.freeze({ title: Object.freeze(text('unchanged')) })))
  const base = Object.freeze({ payload: Object.freeze(object(observed)), untouched })
  const paths = preparePaths(Array.from({ length: size }, (_, i) => ({ path: ['payload', `k${i}`], value: text('new') })))
  const result = applyPaths(base, paths)
  assert.equal(copies, 1, 'the shared parent must not be recopied for every leaf')
  assert.equal(result.untouched, untouched)
  assert.equal(result.payload.objectValue.fields.k0.textValue, 'new')
  assert.equal(result.payload.objectValue.fields[`k${size - 1}`].textValue, 'new')
  assert.equal(base.payload.objectValue.fields.k0.textValue, 'old')
  assert.ok(Object.isFrozen(result))
  assert.ok(Object.isFrozen(result.payload))
  assert.ok(Object.isFrozen(result.payload.objectValue))
  assert.ok(Object.isFrozen(result.payload.objectValue.fields))
})

test('prepared paths own inputs and no-op removals preserve their original containers', () => {
  const edit = { path: ['payload', 'nested', 'color'], value: text('red') }
  const paths = preparePaths([edit])
  edit.path[0] = 'wrong'; edit.value.textValue = 'wrong'
  const base = prepareRecordData({ payload: { nullValue: true } })
  const result = applyPaths(base, paths)
  assert.equal(result.payload.objectValue.fields.nested.objectValue.fields.color.textValue, 'red')
  const removal = preparePaths([{ path: ['payload', 'missing', 'color'], unset: true }])
  assert.equal(applyPaths(base, removal), base)
  assert.equal(applyPaths(base, []), base)
  const existing = prepareRecordData({ payload: object({ keep: text('kept') }) })
  assert.equal(applyPaths(existing, removal), existing)
})

// Independent sequential reference: group application must preserve the existing
// object-only semantics, regardless of sibling edit order or container types.
function sequential(base, changes) {
  const result = Object.assign(Object.create(null), base)
  const apply = (fields, path, index, change) => {
    const name = path[index]
    if (index === path.length - 1) {
      if (change.unset) delete fields[name]
      else fields[name] = change.value
      return
    }
    const value = fields[name]
    if (change.unset && (!value || !('objectValue' in value))) return
    const children = Object.assign(Object.create(null), value && 'objectValue' in value ? value.objectValue.fields : undefined)
    apply(children, path, index + 1, change)
    fields[name] = object(children)
  }
  for (const change of changes) apply(result, change.path, 0, change)
  return result
}

test('grouped updates match sequential semantics for mixed sets, removals, and non-object bases', () => {
  let seed = 0x435de21
  const next = n => { seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5; return (seed >>> 0) % n }
  const containers = [undefined, { nullValue: true }, text('scalar'), { listValue: { values: [text('item')] } }, object({}), object({ a: text('old'), nested: object({ a: text('old') }), keep: text('keep') })]
  for (let attempt = 0; attempt < 200; attempt++) {
    const base = prepareRecordData(Object.fromEntries(['left', 'right', '__proto__'].flatMap(key => {
      const value = containers[next(containers.length)]
      return value ? [[key, value]] : []
    })))
    const edits = ['left', 'right', '__proto__'].flatMap(key => ['a', 'b', 'nested'].flatMap(name => {
      const path = name === 'nested' ? [key, name, 'a'] : [key, name]
      return next(3) === 0 ? [] : [{ path, ...(next(2) ? { value: text(`new-${attempt}`) } : { unset: true }) }]
    }))
    for (let i = edits.length - 1; i > 0; i--) { const j = next(i + 1); [edits[i], edits[j]] = [edits[j], edits[i]] }
    const paths = preparePaths(edits)
    assert.deepEqual(applyPaths(base, paths), sequential(base, paths), `case ${attempt}`)
  }
})
