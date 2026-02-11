# Scopes Manage Resource Lifecycles

Scopes in FS2 define regions where resources are valid. When a scope closes, all resources acquired within that scope are released. This happens automatically—you typically don't need to manage scopes explicitly, but understanding them helps explain resource behavior.

One way to create a scope is with `Stream.bracket`:

```scala
Stream.bracket(acquire)(release)
```

If you're using Cats Effect you'll probably prefer

```scala
Stream.resource(Resource.make(acquire)(release))
```

In this case the stream defines the scope. The resource is acquired when the stream starts and released when the stream ends, stops due to error, or is interrupted.

Here's a more complete example showing how we might use a file as a resource. In real code we'd probably want to use `fs2.io.file.Files`, which will do this for us.

```scala mdoc:silent
import cats.effect.IO
import fs2.Stream

// Resources are automatically scoped
Stream.bracket(IO(scala.io.Source.fromFile("file.txt")))(
  source => IO(source.close())
).flatMap { source =>
  Stream.fromIterator[IO](source.getLines(), chunkSize = 1024)
}
// The file is closed when the stream terminates
```

Scopes nest so we can create more than one resource if needed.

```scala
// With multiple resources, each has its own scope
Stream.resource(fileResource1)
  .flatMap { file1 =>
    Stream.resource(fileResource2).flatMap { file2 =>
      // Both files are open here
      processFiles(file1, file2)
    } // file2 closed here
  } // file1 closed here
```


## Scopes and Concurrency

It is very important to note that scopes do not extend across concurrency boundaries, except in special cases. If we use, say, `parMapEval` on a resource we probably will not get the resource management we expect.

There is an exception to this. If we have a stream of resources we can use `parJoin` or `parJoinUnbounded` to process this stream in parallel, and each resource will be closed when its associated processing finishes.

This pattern is very common in FS2. For example, when creating a server with the `fs2.io.net` package we'll deal with a stream of sockets.

```scala
val server: Stream[IO, Socket[IO]] = ???
```

We typically convert each `Socket` to a stream of bytes (using the `reads` or `writes` method) and we must make sure we close each `Socket` when we've finished with it. `parJoin` is just the tool for this job.

Here's the outline of the canoncial echo server written in this style.

```scala
server
  .map { socket =>
    // Read bytes from the socket...
    socket.reads
      //...and send them back out
      .through(socket.writes)
  }
  // We have at most 5 open connections at once
  // parJoin ensures each socket is released when its reads stream finishes
  .parJoin(5)
```
