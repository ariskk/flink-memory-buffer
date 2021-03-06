package com.ariskk.membuffer

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

final case class BenchSource[T](
  iterations: Int,
  generator: () => T
) extends RichSourceFunction[T] {

  override def run(ctx: SourceFunction.SourceContext[T]): Unit =
    Stream
      .continually(generator())
      .take(iterations)
      .foreach(ctx.collect)

  override def cancel(): Unit = {}
}
