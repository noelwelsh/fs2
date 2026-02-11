# Concurrency is Explicit

Concurrency is explicit in FS2. If we want some part of a stream to run concurrently, we must explicitly state this using a method like `parEvalMap`. The actual implementation of concurrency is deferred to the effect type we use in our `Stream`. So if we're using Cats Effect `IO`, a `parEvalMap` will turn into calls to `IO` and run on the Cats Effect scheduler. A `Stream` is only a program; it doesn't implement the effects itself.


```scala mdoc
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration._
import fs2.Stream

def slowTask(n: Int): IO[Int] = 
  IO.sleep(1.second) >> IO.println(s"Processed $n") >> IO.pure(n * 2)

// Sequential processing - takes ~5 seconds
Stream(1, 2, 3, 4, 5)
  .evalMap(slowTask)
  .compile.toList

// Concurrent processing - takes ~1 second (with maxConcurrent = 5)
Stream(1, 2, 3, 4, 5)
  // Tell type inference we're working with an IO
  .covary[IO]
  .parEvalMap(maxConcurrent = 5)(slowTask)
  .compile.toList

// Other concurrent operations
val s1 = Stream.awakeEvery[IO](1.second).map(_ => "A")
val s2 = Stream.awakeEvery[IO](2.seconds).map(_ => "B")

// merge runs both streams concurrently
// We should see more As than Bs in the output
s1.merge(s2).take(5).compile.toList.unsafeRunSync()

// parJoin for a stream of streams
Stream(
  Stream.eval(IO.pure("Stream 1")).repeat.take(3),
  Stream.eval(IO.pure("Stream 2")).repeat.take(3)
).parJoin(maxOpen = 2).compile.toList.unsafeRunSync()
```

The `maxConcurrent` or `maxOpen` parameters give you fine-grained control over resource usage.
