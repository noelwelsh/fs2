# Error Handling

If a `Stream` encounters an error, no further upstream values are pulled and no further downstream processing takes place. Errors are propagated immediately and trigger cleanup, ensuring resources are not leaked.

You can raise an error with `Stream.raiseError` and handle errors with `handleErrorWith`, `attempt`, and other combinators.

This example shows a stream with an error in its middle.

```scala mdoc:crash
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream

// Define an exception type that doesn't capture a stack trace
// to make it easier to read when printed
class StacklessException(message: String) extends 
  Exception(message, null, false, false)

// A stream that fails
val failing = Stream(1, 2, 3) ++ 
  Stream.raiseError[IO](new StacklessException("Boom!")) ++
  Stream(4, 5, 6)

failing.compile.toList.unsafeRunSync()
```

As we can see, the error is raised when we run the `IO` the `Stream` compiles into and we get no output. If we handle the error we can return a new stream that is run in place of the remaining failed stream.

```scala mdoc:invisible
// The failing block above means these are no in scope below, so redefine them here
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream

class StacklessException(message: String) extends 
  Exception(message, null, false, false)

val failing = Stream(1, 2, 3) ++ 
  Stream.raiseError[IO](new StacklessException("Boom!")) ++
  Stream(4, 5, 6)
```

```scala mdoc
// Handle errors with handleErrorWith
failing.handleErrorWith(error => Stream(-1) )
  .compile.toList.unsafeRunSync()

// Or use attempt to convert errors to Either
failing.attempt.compile.toList.unsafeRunSync()

// We can also recover from specific errors
Stream.raiseError[IO](new IllegalArgumentException("Bad!"))
  .handleErrorWith {
    case _: IllegalArgumentException => Stream(0)
    case e => Stream.raiseError[IO](e)
  }
  .compile.toList.unsafeRunSync()

// Errors also close resources properly
Stream.bracket(IO.println("Acquiring"))(_ => IO.println("Releasing"))
  .flatMap { _ =>
    Stream(1, 2) ++ Stream.raiseError[IO](new Exception("Fail!"))
  }
  .compile.drain
// Output:
// Acquiring
// Releasing
```
