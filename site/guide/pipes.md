# Pipes are Reusable Stream Fragments

A `Pipe` is just a type alias for a function with type `Stream[F, I] => Stream[F, O]`. Remember that a `Stream` is a program. The input to a `Pipe` is a program describing a source of values. The result is a `Stream` producing some output `O`. Thus a `Pipe` is a reusable fragment of a `Stream` that we can attach to the end of a source.

We connect a `Stream` to a `Pipe` using `through`.

```scala mdoc
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.{Pipe, Pure, Stream}

// Define a reusable pipe
val doubleAndLog: Pipe[IO, Int, Int] = 
  _.map(_ * 2)
   .evalMap(n => IO.println(s"Doubled: $n").as(n))

// Use it with different sources
val source1 = Stream(1, 2, 3)
val source2 = Stream(10, 20, 30)

source1.through(doubleAndLog).compile.toList.unsafeRunSync()
// Doubled: 2
// Doubled: 4
// Doubled: 6

source2.through(doubleAndLog).compile.toList.unsafeRunSync()
// Doubled: 20
// Doubled: 40
// Doubled: 60

// Pipes can be composed
// This Pipe has no effects. We could use IO as the effect type,
// but we've made it polymorphic over the effect type F to
// demonstrate how this is done.
def filterEven[F[_]]: Pipe[F, Int, Int] = _.filter(_ % 2 == 0)

val pipeline: Pipe[IO, Int, Int] = 
  _.through(doubleAndLog).through(filterEven)

Stream(1, 2, 3, 4).through(pipeline).compile.toList.unsafeRunSync()
```

Pipes promote code reuse and composability, allowing you to build libraries of stream transformations.
