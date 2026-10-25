# Embedded projection output benchmarks

These benchmarks compare recursive nested selection with sending a selected
column's whole value. They require no PostgreSQL server and do not modify data.

```sh
go test ./internal/projectionserver -run '^$' -bench '^BenchmarkEmbedded' \
  -benchmem -benchtime=100ms -count=3

# Longer measurements for the representative 1,000-element array.
go test ./internal/projectionserver -run '^$' \
  -bench '^BenchmarkEmbedded(Publication|Update)$/^array$/^1000$' \
  -benchmem -benchtime=500ms -count=5
```

## What is measured

- `BenchmarkEmbeddedPublication`: output selection, owned output copies,
  canonical protobuf construction, and binary serialization, starting from an
  already accepted complete image. This is not the complete snapshot RPC.
- `BenchmarkEmbeddedUpdate`: the real connection model accepting a new version,
  cloning the full record, selecting/comparing the outgoing image, and encoding
  a model batch when the entity changes. Alternating immutable inputs change
  `name` (selected in every mode) or `payload` (`unused_0000`, omitted in sparse
  and dense modes). Emission counts are declared explicitly by the fixture,
  never calculated using the selector or publication code under measurement.
  Whole and recursive-all modes must emit for both changes; sparse and dense
  must emit only for `name`. These replace the old `selected`/`unselected`
  update-case names, which were ambiguous for fully selected values.
- `BenchmarkEmbeddedSelectionReferences`: add/remove one overlapping selection
  reference while 1, 10, 100, or 1,000 existing references retain the same mask.
  This measures the already-covered path, not creating/removing the final tree
  branch or the full subscription lifecycle.

Wide objects contain `name` and 16/256/1,024 additional payload fields. Arrays
contain 100/1,000/10,000 object elements, each with `name` and eight 64-byte text
payload fields. Every mode keeps every array element. Modes are:

- `whole`: request the metadata column without nested trimming.
- `selected`: sparse mask selecting only `name` within each object/element.
- `dense`: select every field except `unused_0000` (8 of 9 per array element).
- `all`: explicitly select every field recursively, producing the same data as
  `whole` while exercising recursive-mask overhead rather than a whole-value
  shortcut. Dense/all array masks also include the outer metadata `name`.

These are internal output-path benchmarks, bypassing wire validation. Large
wide-object dense/all masks exceed the server's default selection limits; real
applications would need to increase those configured limits to request them.

There is no database decoding, CDC transport, authorization, query routing,
projection expansion, socket I/O, or browser work in these measurements. They
are single-entity output-path measurements, not end-to-end throughput estimates.
`B/op` is allocated bytes, not retained memory. `wire-bytes/op` counts emitted
protobuf payloads: a suppressed entity update reports zero, but a real stream
may still carry transaction/progress envelopes or other subscription effects.
Publication measures `EntityEvent`; updates measure `ModelBatch`, so their
payload sizes have slightly different envelope overhead.

## Results: November 1, 2026

Apple M4 Pro, macOS 26.3, Go 1.26.10, darwin/arm64, default GOMAXPROCS 14.
The table uses the median of three 500 ms samples for 1,000 array elements,
run sequentially without concurrent benchmark or test processes.
MB below means decimal megabytes. Results are indicative local measurements,
not statistically established performance guarantees or historical-version
comparisons.

| Mode | Publish image | Live `name` change | Live `payload` change | Publish allocated/op | Initial payload |
| --- | ---: | ---: | ---: | ---: | ---: |
| Whole value | 2.88 ms | 4.69 ms | 4.70 ms | 5.63 MB | 689,959 bytes |
| Sparse selection | 1.03 ms | 1.92 ms | 1.40 ms, suppressed | 2.91 MB | 23,943 bytes |
| Dense selection | 2.71 ms | 4.39 ms | 3.11 ms, suppressed | 5.18 MB | 606,959 bytes |
| Recursive all fields | 3.50 ms | 5.27 ms | 5.29 ms | 7.88 MB | 689,959 bytes |

Initial entity payloads fall from 689,959 to 23,943 bytes (about 96.5% smaller).
A selected-field live change emits roughly 690 KB versus 24 KB; an unselected
change emits one whole-value entity event versus no sparse or dense entity
event. Sparse live updates allocate 5.98 MB when emitted and 4.57 MB when
suppressed, versus 10.26 MB for whole-value updates.

Trimming is not universally cheaper. Explicitly traversing all selected fields
adds about 22% publication time and 40% publication allocation in this fixture,
without saving any wire bytes compared with the whole-value shortcut. Dense
selection shows much smaller gains than sparse selection for emitted changes,
but still suppresses changes to its one omitted payload field. These cases make
the cost of near-full selection visible instead of assuming sparse results apply
to every projection.

The original sparse/whole three-sample 100 ms sweep also showed:

- Selecting one field from a 1,025-field object took about 1.74 microseconds
  for publication versus 307 microseconds for the whole object.
- For 10,000 array elements, publication was about 11.6 ms narrowed versus
  29.7 ms whole, with payloads around 249 KB versus 6.91 MB. This stress case
  had few iterations per short sample; use longer samples before tuning to it.
- Updating overlapping selection references took about 0.25–0.26 microseconds
  per add/remove pair with zero allocations across 1–1,000 existing references.

## Where the remaining allocation cost goes

A separate three-second allocation profile of the narrowed 1,000-element
unselected-update case attributed approximately:

- 60% of allocated bytes to full-record ownership copying (`CloneValue`).
- 26% to selected object/array scaffolding.
- 14% to `reflect.DeepEqual` and its helpers.

So suppression saves serialization and wire traffic, but does not make large
JSONB changes free. Array projection still traverses all elements, and accepting
the new complete record still copies the unselected data. The current design
is faster and allocates less for sparse selection in these fixtures; dense
selection saves less and recursive-all selection adds overhead. There remains
meaningful output-path allocation headroom.

Reasonable follow-up experiments are a canonical-value comparator instead of
reflection, and comparison against the selection without first materializing
all selected containers. Reducing full-record ownership copies is a separate
ownership/interface exercise: do not simply remove them from mutable images.
No production optimization was made as part of these measurements.

To profile a case, supply temporary paths for `-cpuprofile`, `-memprofile`, and
`-o` to the second command, then inspect the profiles using `go tool pprof`
(or an installed `pprof` binary if the selected toolchain omits that tool).
Use `-alloc_space` for total allocated bytes. Profiled timings should not be
mixed with the unprofiled timing table.
