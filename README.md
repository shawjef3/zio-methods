# me.jeffshaw.zio methods

Standalone home for stream combinators built on published ZIO, extracted from a
ZIO fork rather than merged upstream. Depends on `dev.zio %% zio-streams %
2.1.26`; targets the same Java (11) and Scala (2.12/2.13/3.3) versions as ZIO.

## What's here

- `runForeachPar` — added to `ZStream` as an extension method
  (`me.jeffshaw.zio.methods.ZStreamMethods`). Consumes all elements, running up
  to `n` invocations of `f` concurrently, without emitting results downstream.
  Semantics match `mapZIOParUnordered`: up to `n` concurrent, unordered, results
  discarded, first failure interrupts the rest and fails fast.
- `ChunkCursorDistributor` — the chunk-transport / element-dispatch engine
  behind `runForeachPar`.

## Design

`runForeachPar` exists because `mapZIOParUnordered(n)(f).runDrain` pays to
buffer and re-chunk results that are then thrown away. Discarding them up front
is worth roughly two orders of magnitude in throughput.

Three decisions shape the implementation.

### Transport is chunk-granular; dispatch is element-granular

A pool of `n` long-lived worker fibers is fed by a producer fiber writing
`Take[E, A]` chunks into a bounded queue. Workers do *not* claim whole chunks:
they claim individual elements out of the current chunk via a shared
`AtomicInteger` cursor.

Keeping these two granularities separate is the central idea. Chunk transport
keeps queue traffic proportional to chunks rather than elements, and avoids
boxing every element into an `Exit`. Element dispatch preserves the concurrency
contract: with a single chunk of 1000 elements and `n = 64`, all 64 workers run
concurrently, because they claim elements rather than chunks. A design where a
worker owns a whole chunk starves workers whenever there are fewer chunks than
`n`, and is what the "single chunk saturates all workers" test guards against.

There is no barrier at chunk boundaries — a worker that finishes an element
immediately claims the next — so one slow `f` never idles the other workers.

The round/cursor protocol itself: a *round* holds a chunk, a cursor, and a
promise for the next round. Each worker does `i = cursor.getAndIncrement()`;
`i < length` runs `f(chunk(i))`, `i == length` makes that worker the designated
fetcher (exactly one observes the boundary, by construction), and `i > length`
awaits the next round. Terminal rounds carry the end-of-stream or failure
signal, and are detected before the cursor is touched.

### The producer fiber and queue are deliberate

A queue-less variant, where the fetching worker pulls the stream directly
(`ZStream#toPull`), allocates ~29% less and is CPU-neutral on in-memory
sources. It was measured and rejected: on a source that *parks* — a socket, a
queue, a JDBC cursor, any async API — it loses ~44% throughput. When the
fetcher is itself a worker, nothing is queued behind it while it is parked, so
the remaining workers idle once the current chunk drains. The producer fiber
keeps filling the queue across a parked pull, and that is worth more than the
allocation it costs. `StreamParBenchmark.zioRunForeachParBlockingUpstream`
guards against reintroducing the queue-less shape.

### The fetcher drains the whole buffer, not one chunk

The designated fetcher takes *every* buffered chunk (`takeBetween(1,
bufferSize)`) and fuses them into a single round.

This matters when `n` is much larger than the chunk size. With single-chunk
rounds, a round holds fewer elements than there are workers, so every round
boundary wakes all the overflow workers at once to race for the next chunk — a
thundering herd, hundreds of times per second at high `n`. Fusing multiplies the
elements per round by the number of buffered chunks, making those boundaries
proportionally rarer. Dispatch stays element-granular, so load balance and the
concurrency contract are unchanged. A single-chunk batch is returned as-is, so
the low-`n` regime pays nothing for the fusion path. A terminal `Take` arriving
mid-batch is split off and parked for the next fetch.

`bufferSize` therefore sets both the pipelining depth and the fusion window.

### Notes

- `n <= 1` short-circuits to `runForeach`.
- Everything here uses public ZIO API. `Promise#done(Exit.unit)` stands in for
  the `private[zio]` `succeedUnit` (`Exit.unit` is a singleton, so it allocates
  nothing either), and terminal rounds simply leave their unused `next` promise
  uncompleted rather than reaching for `Promise#unsafe.done`.

## Performance

Throughput is the optimization target; allocation is treated as a diagnostic.
The two rank differently often enough that scoring on allocation alone is
misleading — a `ZIO.whileLoop` worker loop, for instance, cut allocation by
23–38% while costing ~30% throughput, and was reverted.

Measured on 32 cores, JMH throughput mode.

Combinator overhead, 500k elements, no-op `f` (`StreamParBenchmark`):

| Approach | ops/s |
|---|---|
| `runForeachPar` | 158 |
| `runForeachChunk` + `foreachParDiscard` | 23 |
| `mapZIOParUnordered().runDrain` | 0.6 |

High-concurrency IO-like `f` — 200k elements, 2000-element chunks,
`f = ZIO.sleep(5ms)` (`RealisticParBenchmark`):

| `n` | elements/s |
|---|---|
| 2,048 | 350k |
| 16,384 | 861k |
| 40,960 | 726k |

At that scale the binding constraint is the ZIO runtime's own fiber wake and
timer path, not this combinator: `runForeachPar` runs at or slightly above a
stream-free `ZIO.foreachParDiscard(...).withParallelism(n)` control. The dip
from 16k to 40k is the runtime degrading past ~16k fibers.

Note that with a no-op or very cheap `f`, sequential `runForeach` is faster than
any parallel variant — the workers are pure coordination overhead with nothing
to divide. Parallelism starts paying somewhere around a `BigDecimal.pow(3)` per
element, and the advantage grows with the cost of `f`.

## Layout

- `methods` (root) — the library + `zio-test` spec.
- `benchmarks` — JMH subproject (sbt-jmh), ZIO-only benchmarks (the
  Akka/fs2/cats-effect comparisons from the original ZIO benchmark were
  dropped).
  - `StreamParBenchmark` — combinator overhead against alternatives, plus
    slow-upstream and blocking-upstream regression guards.
  - `RealisticParBenchmark` — high-concurrency IO-like `f`, with a stream-free
    control benchmark for the runtime ceiling.

## Running

```
sbt test
sbt "benchmarks/Jmh/run -f 2 -wi 5 -i 5 StreamParBenchmark"
sbt "benchmarks/Jmh/run -f 1 RealisticParBenchmark"
```
