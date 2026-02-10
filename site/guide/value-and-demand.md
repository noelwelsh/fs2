# Values Flow Downstream; Demand Flows Upstream

We can think of a `Stream` as a sequence of values flowing downstream from a source to a sink. No values will flow, however, without demand from the sink. This demand is known as a pull, and flows from downstream to upstream.

The sink will not pull values if it has not finished processing the current value (unless we explicitly say we want concurrency). This means that only as many values will be processed as the sink needs, and they are processed in the order they are produced.

```scala mdoc
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream

// This stream can produce infinite values
val infinite = Stream.iterate(0)(_ + 1)

// But only 5 are actually produced because we only take 5
val result = infinite.take(5).compile.toList

// Demand flows upstream: the take(5) send only 5 pulls upstream,
// so iterate only produces 5 values

// Processing happens one value at a time
Stream(1, 2, 3)
  .evalMap(n => IO.println(s"Produced $n").as(n))
  .evalMap(n => IO.println(s"Consuming $n").as(n))
  .compile.drain.unsafeRunSync()
// Output:
// Produced 1
// Consuming 1
// Produced 2
// Consuming 2
// Produced 3
// Consuming 3
```

Another implication of demand flowing upstream is that only the parts of our program reachable from the sink will run. If we write

```scala
val a = Stream(1, 2, 3)

a.map(x => x + 3)

a.map(x => x * 2).compile.toList
```

the `map(x => x + 3)` fragment *does not* run, as it cannot be reached from the compiled program.

This pull-based model provides backpressure automatically. If a downstream consumer is slow, upstream producers naturally slow down to match.
