# Chunks Optimize Performance

FS2 processes elements in chunks rather than one at a time, which significantly improves performance for many operations. A `Chunk` is an immutable, strict sequence that provides efficient indexed access and various bulk operations.

```scala mdoc
import cats.effect.IO
import fs2.Chunk
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.Path

// Streams work with chunks internally
val stream = Stream.chunk(Chunk.array(Array(1, 2, 3, 4, 5)))

// You can see chunks explicitly
Stream(1, 2, 3)
  .chunks
  .map(chunk => s"Chunk of size ${chunk.size}")
  .compile
  .toList

// Files.readAll produces a Stream with chunked data
Files.forIO.readAll(Path("/some/file.txt"))

// When you need to work with individual elements, FS2 handles 
// chunking for you
Stream.chunk(Chunk.array(Array(1, 2, 3, 4, 5)))
  // This works element-wise but internally uses chunks
  .map(_ * 2)  
  .compile
  .toList
```

Most of the time you don't need to think about chunks, but understanding them helps explain FS2's performance characteristics.
