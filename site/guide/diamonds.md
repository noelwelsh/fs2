# Be Aware of Diamonds

There are a number of ways to join together two or more `Streams`: `merge`, `interleave`, `either`, `zip` and more. However, be careful when you want to implement a diamond pattern. Remember, a `Stream` is a program. Look at the following code.

```scala mdoc:silent
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import scala.util.Random

val randomInts = Stream.eval(IO(Random.nextInt())).repeat

val left = randomInts.map(x => s"left: $x")
val right = randomInts.map(x => s"right: $x")

val stream = left.zip(right)
```

You might think when we run this that the `left` and `right` branches will both process the *same* values. However, look at the output when we run the code.

```scala mdoc
stream.take(3).compile.toList.unsafeRunSync()
```

It produces tuples where the left element is a *different* random number to the right element because we have executed the program that generates random numbers twice—once for the left side and once for the right side.

If we wanted the *same* random numbers on the left and right side, which means splitting and then joining one stream of effects, we have a few options. One approach is to use `broadcastThrough`, which connects a source to any numbers of pipes. The pipes will all receive the same values from the source, and output of the pipes is combined in an arbitrary order.

```scala mdoc:nest:silent
import fs2.Pipe

val left: Pipe[IO, Int, String] = _.map(x => s"left: $x")
val right: Pipe[IO, Int, String] = _.map(x => s"right: $x")

val stream = randomInts.broadcastThrough(left, right)
```

Now when we run the stream we see that both branches receive the same values.

```scala mdoc
stream.take(5).compile.toList.unsafeRunSync()
```

If a branch is running purely for effect, we can use `evalTap` to run these effects.

```scala mdoc:nest
// Using evalTap to print values as they pass through the Stream.
val stream = Stream(1, 2, 3).evalTap(x => IO.println(s"Saw $x")).map(x => x * 2)

stream.compile.toList.unsafeRunSync()
// Output:
// Saw 1
// Saw 2
// Saw 3
```

We can achieve the same thing with a `Pipe` using `observe` and variants.

Finally, we can build our own system using concurrency primitives, such as those that come with Cats Effect, or with the tools in `fs2.concurrent`. The example below pushes the value from a `Stream` to two `Queues`, which in turn push values to a final `Queue`.

```scala mdoc:nest:silent
import cats.syntax.all.*
import cats.effect.std.Queue

val intQueue = Queue.bounded[IO, Int](5)
val stringQueue = Queue.bounded[IO, String](5)

val result =
  (intQueue, intQueue, stringQueue).tupled.flatMap { 
    case (left, right, output) =>
      val producer = randomInts
        .take(5)
        .evalTap(left.offer)
        .evalTap(right.offer)
        .compile
        .drain
      
      val leftConsumer = Stream.fromQueueUnterminated(left)
        .evalMap(x => output.offer(s"left: $x"))
        .take(5)
        .compile
        .drain
    
      val rightConsumer = Stream.fromQueueUnterminated(right)
        .evalMap(x => output.offer(s"right: $x"))
        .take(5)
        .compile
        .drain
        
      val results = Stream.fromQueueUnterminated(output)
        .take(5)
        .compile
        .toList
      
      // Run all in parallel
      (producer, leftConsumer, rightConsumer, results)
        .parMapN((_, _, _, r) => r)
  }
```

When we run this we see that the left and right side are processing the same values.

```scala mdoc
result.unsafeRunSync()
```

We can build the same setup using a `Topic` to share values between multiple streams. The code below is essentially a reimplementation of `broadcastThrough`.

```scala mdoc:nest
import cats.effect.std.CountDownLatch
import fs2.concurrent.Topic

(CountDownLatch[IO](2), Topic[IO, Int]).tupled.flatMap{ case (latch, topic) =>
  val randomInts = Stream
    .eval(IO(Random.nextInt()))
    .repeat
    .take(5)

  // Use the latch to wait for the subscribers to be setup before we 
  // start publishing. This ensures they don't miss any data.
  //
  // Appending the `topic.close` effect makes sure we close the topic 
  // when we have generated all the data.
  val source = Stream
    .exec(latch.await)
    .append(randomInts.through(topic.publish))
    .append(Stream.eval(topic.close))

  // Sets up a subscription to the topic. We use the latch to notify
  // when we are subscribed, and also wait for all other subscribers
  // to be ready.
  def subscription(pipe: Pipe[IO, Int, String]): Stream[IO, String] =
    Stream
      .resource(topic.subscribeAwait(1))
      .flatMap(subscription =>
        Stream
          .exec(latch.release >> latch.await)
          .append(subscription.through(pipe))
      )

  val left =
    subscription(_.map(x => s"left: $x"))
  val right =
    subscription(_.map(x => s"right: $x"))

  val joined = Stream(left, right).parJoinUnbounded
  val results = joined.concurrently(source).compile.toList

  results
}.unsafeRunSync()
```
