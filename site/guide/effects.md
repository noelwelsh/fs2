# Effects are Explicit

In FS2 we represent effects explicitly with some effect type. This is often `cats.effect.IO`, but you can use other effect types. We must tell `Stream` when we want to include an effect, by using `Stream.eval`, `Stream.evalMap` or similar methods. Here are some examples.

```scala mdoc:silent
import cats.effect.IO
import fs2.Stream
import scala.io.Source

// Pure stream - no effects
val pureStream = Stream(1, 2, 3)

// To include an effect, we must be explicit
val withEffect = Stream.eval(IO.println("Starting!")) ++
  pureStream.evalMap(n => IO.println(s"Processing $n"))

// evalMap takes a function that returns an effect
val readFiles = Stream("file1.txt", "file2.txt", "file3.txt")
  .evalMap(filename => IO(Source.fromFile(filename).mkString))

// Stream.eval lifts a single effect into a stream
val randomNumber = Stream.eval(IO(scala.util.Random.nextInt(100)))
```

The explicitness means you can easily see where side effects occur in your stream pipeline. Pure transformations like `map` and `filter` don't perform effects, while `eval` and `evalMap` clearly mark effectful operations.
