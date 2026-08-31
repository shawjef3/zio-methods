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

The round/cursor protocol itself: a *round* holds a chunk, a cursor, a stride,
and a promise for the next round. Each worker claims a range with
`i = cursor.getAndAdd(stride)`; `i < length` runs `f` over
`[i, min(i + stride, length))`, `i >= length` means the round is drained and
elects one worker as the designated fetcher, and a worker that is not elected
awaits the next round. Terminal rounds carry the end-of-stream or failure
signal, and are detected before the cursor is touched.

### Claims are batched only where batching is free

Once `f` is cheap, the cost of dispatch is the cursor's atomic operation, paid
per element. A worker therefore claims a contiguous *range* of `stride` elements
per atomic and runs them without returning to the cursor.

The stride is what makes this safe. It is derived per round as
`length / (n * 8)`, capped at 16: every worker is left at least eight claims, so
a worker that draws one oversized claim is at most an eighth of the round behind
the rest, whatever `f` costs. Below `n * 8` elements per round the quotient is
zero, the stride pins to 1, and dispatch is exactly per-element again — so the
"single chunk saturates all `n` workers" guarantee holds unchanged, and a slow
`f`, where a round rarely has that many elements per worker, never batches at
all.

Sizing the stride to give each worker *one* claim (`length / n`) was tried first
and measured 7–9% **slower** at `n` in the thousands with a 5 ms `f`: with one
claim apiece the round ends when the slowest single claim ends, so a 16-element
claim serialized 80 ms behind everyone else. Requiring several claims per worker
keeps the amortization where `f` is cheap and restores fine-grained balance
where it is not.

Stride 1 is kept as a literal fast path — `getAndIncrement` rather than
`getAndAdd(1)`, `f` invoked directly rather than through the range loop, and no
election flag allocated — so the slow-`f` regime runs the pre-batching code with
no added work. Without that fast path it measured ~2–4% slower at `n = 16384`.

A stride above 1 also changes how the fetcher is elected. With unit strides the
cursor's values are consecutive, so exactly one worker sees `i == length` and
that test elects it for free. A larger stride makes the values skip, so none need
land on `length` at all and the same test would elect *nobody* and hang the run;
batched rounds elect by CAS on a per-round flag instead. `claims partition the
chunk at every length/n ratio` is the regression test for this — reverting the
election to `i == length` makes it deadlock rather than fail quietly.

### The producer fiber and queue are deliberate

A queue-less variant, where the fetching worker pulls the stream directly
(`ZStream#toPull`), allocates ~29% less and is CPU-neutral on in-memory
sources. It was measured and rejected.

The precondition is a source that cannot keep the workers fed — the stream
producing more slowly than the pool consumes. When the fetcher is itself a
worker, nothing is queued behind it while it waits, so the remaining workers
idle once the current chunk drains. The producer fiber keeps filling the queue
across that wait, holding up to `bufferSize` of work in flight, and that is
worth more than the allocation it costs.

But a slow producer alone does not reproduce the collapse. `slowUpstream` is
already producer-limited — throughput falls monotonically with `upstreamCost`
(226 / 137 / 57 / 7.1 ops/s at 0 / 200 / 2000 / 20000, `n = 4`) — and the
queue-less variant is CPU-neutral on it. What the ~44% loss needs in addition
is a pull that *parks*: a socket, a queue, a JDBC cursor, any async API. A
suspended pull costs a scheduler wake to resume on top of the wait, where a
slow on-CPU pull merely occupies the fiber.

`StreamParBenchmark.zioRunForeachParBlockingUpstream` supplies that shape — a
bounded queue with a small buffer, so the consumer really does out-run it and
the pull really does suspend — and guards against reintroducing the queue-less
design. Keeping it alongside `zioRunForeachParSlowUpstream` is what separates
"the producer is slow" from "the producer parks"; only the second sinks the
queue-less shape.

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

Measured on 32 cores, JMH throughput mode. Scores below are ± the JMH error over
several forks; the `n = 4` benchmarks in particular vary enough fork to fork that
single-fork runs are not comparable — read them across forks or not at all.

Combinator overhead, 500k elements, no-op `f`, `n = 4` (`StreamParBenchmark`):

| Approach | ops/s |
|---|---|
| `runForeachPar` | 165 ± 1 |
| `runForeachChunk` + `foreachParDiscard` | 19.2 ± 0.6 |
| `mapZIOParUnordered().runDrain` | 0.65 ± 0.03 |

Batched claims are what moved the first row; against the same combinator with
per-element claims:

| `f`, 500k elements, `n = 4` | per-element | batched |
|---|---|---|
| no-op | 127 ± 10 | 165 ± 1 |
| `BigDecimal.pow(3)` | 35.7 ± 4.6 | 46.4 ± 4.9 |
| no-op, CPU-bound producer | 134 ± 11 | 181 ± 5 |
| no-op, parking producer | 96.9 ± 8.5 | 114 ± 3 |

High-concurrency IO-like `f` — 200k elements, 2000-element chunks,
`f = ZIO.sleep(5ms)` (`RealisticParBenchmark`):

| `n` | elements/s |
|---|---|
| 2,048 | 351k |
| 16,384 | 641k |

(Measured at `-f 3 -wi 3 -i 5`. This benchmark is sensitive to the warmup
settings — a longer warmup reaches ~860k at `n = 16384` — so compare variants
only within one configuration.)

At that scale the binding constraint is the ZIO runtime's own fiber wake and
timer path, not this combinator: `runForeachPar` runs at or slightly above a
stream-free `ZIO.foreachParDiscard(...).withParallelism(n)` control. The dip
from 16k to 40k is the runtime degrading past ~16k fibers. Batching does not
engage in this regime at all — rounds hold fewer than `n * 8` elements, so the
stride is 1 — and the stride-1 fast paths exist to keep it costing nothing
there; measured against per-element claims it is a wash (3.21 ± 0.11 vs
3.24 ± 0.05 ops/s at `n = 16384`).

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
