# flink-memory-buffer

An in-memory buffer for Apache Flink which can be used to achieve higher throughput in certain data skewness 
scenarios.

Write up: https://ariskk.com/flink-memory-buffers

Usage:
```scala
import org.apache.flink.streaming.api.scala._
import extensions._
import FlinkOps._

val env: StreamExecutionEnvironment = ???
val words = Seq("this", "is", "a", "list", "of", "words")

env.fromCollection(words)
  .map(word => (word, 1))
  .keyingBy { case (word, _) => word }
  .buffer(
    Time.milliseconds(100),
    maxCount = 100,
    keySelector = _._1,
    _.reduce { case ((word, count), _) => (word, count + 1)}
  )
  .keyingBy { case (word, _) => word }
  .reduceWith { 
    case ((word, count), (_, partialCount)) => 
      (word, count + partialCount)
  }
```

