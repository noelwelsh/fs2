# Streams are Programs

A `Stream[F, A]` is a program describing how to produce values of type `A` using effects of type `F`.  As a program it doesn't do anything until it is compiled and run. The examples below illustrate this.

```scala mdoc
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream

// This is just a description. Nothing happens yet.
val stream = Stream(1, 2, 3, 4, 5)

// Still just a description. We've added a transformation.
val doubled = stream.map(_ * 2)

// Now we compile it to a List. This actually runs the program.
val result: List[Int] = doubled.compile.toList

// We need an effect type to represent effects.
// We use Cats Effect IO.
val effectfulStream = Stream.eval(IO.println("Hello!"))

// Compile to an IO. 
// Nothing happens yet because an IO is also a program
val program: IO[Unit] = effectfulStream.compile.drain

// Now we run the IO and effects happen
program.unsafeRunSync()  
// "Hello!" is printed
```

This separation between description and execution is crucial. It means we can build up complex stream programs, pass them around, combine them, and only run them when we're ready.


## Compiling Streams

If a stream doesn't have any effects it will have type `Stream[Pure, A]`, for some type `A`. In these cases we can compile directly into a value like a `List[A]`. However, if the `Stream` has effects it will compile into the effect type. The effect type, such as `IO`, will then have to run.

Here are some of the common ways to compile a `Stream`:

```scala
// Throw away all the values produced by the stream
stream.compile.drain

// Count the number of elements produced by the stream
stream.compile.count

// Produce a `List` of all the elements produced by the stream
stream.compile.toList
```
