# Finalization and Interruption 
A stream is finalized when it finishes, regardless of whether it finished successfully or not. Any resources in scope are released during finalization. Additional actions be registered using `onFinalize`.

Streams can be interrupted, either explicitly or due to concurrent operations completing. When a stream is interrupted, finalization still occurs, ensuring resources are cleaned up.

```scala mdoc
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import scala.concurrent.duration._

// Explicit interruption
val neverEnding = Stream.awakeEvery[IO](1.second)
  .map(d => s"Tick: $d")
  .onFinalize(IO.println("Stream finalized"))

neverEnding
  .interruptAfter(5.seconds)
  .compile
  .toList
  .unsafeRunSync()
// Prints "Stream finalized"

// Interruption via concurrent operations
val s1 = Stream.sleep[IO](1.second) >> Stream("S1 done")
val s2 = Stream.sleep[IO](5.seconds).onFinalize(IO.println("S2 done"))

s1.concurrently(s2).compile.toList.unsafeRunSync()
// When s1 completes (or fails), s2 is interrupted
```
