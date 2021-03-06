package com.ariskk.membuffer

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

trait KeyedStreamOps[T, K] {
  def ks: KeyedStream[T, K]

  def buffer[R: TypeInformation](
    triggerInterval: Time,
    maxCount: Int,
    keySelector: T => K,
    processor: Vector[T] => R
  ): DataStream[R] =
    ks.process(new KeyedProcessFunction[K, T, R] {
      type KContext = KeyedProcessFunction[K, T, R]

      private val triggerMillis = triggerInterval.toMilliseconds

      /**
        * For this to work in the real world, the function needs to implement the CheckpointedFunction
        * interface
        */
      val buffer: ConcurrentHashMap[K, Vector[T]] = new ConcurrentHashMap[K, Vector[T]]()
      val started: AtomicBoolean = new AtomicBoolean()
      val count: AtomicLong = new AtomicLong(0L)
      @volatile var collector: Collector[R] = _

      private def emptyBuffer(out: Collector[R]): Unit =
        Option(buffer).foreach { bu =>
          if (!bu.isEmpty) {
            bu.asScala.foreach {
              case (_, vector) =>
                out.collect(processor(vector))
            }
            bu.clear()
            count.set(0L)
          }
        }

      def processElement(
        value: T,
        ctx: KContext#Context,
        out: Collector[R]
      ): Unit = {
        Option(buffer).foreach { bu =>
          val key = keySelector(value)
          val previousVector = bu.getOrDefault(key, Vector.empty)
          bu.put(key, previousVector :+ value)
          val newCount = count.incrementAndGet()
          if (newCount >= maxCount) emptyBuffer(out)
        }
        if (!started.get()) {
          val now = ctx.timerService().currentProcessingTime()
          ctx.timerService().registerProcessingTimeTimer(now + triggerMillis)
          started.set(true)
          collector = out
        }
      }

      override def onTimer(
        timestamp: Long,
        ctx: KContext#OnTimerContext,
        out: Collector[R]
      ): Unit = {
        emptyBuffer(out)
        ctx.timerService().registerProcessingTimeTimer(timestamp + triggerMillis)
      }

      override def close(): Unit = {
        super.close()
        Option(collector).foreach(out => emptyBuffer(out))
      }
    })

}

object FlinkOps {
  implicit class KStreamOps[T, K](val ks: KeyedStream[T, K]) extends KeyedStreamOps[T, K]
}
