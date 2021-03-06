package com.ariskk.membuffer

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import FlinkOps._
import KeyedStreamOpsSpec.KV

class KeyedStreamOpsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    FlinkExecutor.startCluster()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FlinkExecutor.stopCluster()
  }

  def log(message: String): Unit = println(message)

  def stopwatchRun[T](stream: DataStream[T], streamDescription: String): Long = {
    log(s"Running: $streamDescription")
    val start = System.currentTimeMillis()
    val iterator = stream.executeAndCollect()
    iterator.size // trigger
    val took = System.currentTimeMillis() - start
    iterator.close()
    log(s"Finished running $streamDescription. Took: $took")
    took
  }

  def source(
    totalViews: Int,
    totalUsers: Int,
    totalTweets: Int
  ): BenchSource[View] =
    BenchSource[View](
      iterations = totalViews,
      () => View.generate(totalUsers, totalTweets)
    )

  def roundToTwoDecimals(d: Double): BigDecimal =
    BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_DOWN)

  def env: StreamExecutionEnvironment = FlinkExecutor.newEnv

  describe("KeyedStreamOps") {

    it("produces correct results") {

      val stream = (1 to 100).map(i => KV(s"key-${i % 10}", 1))

      val unbuffered = env
        .fromCollection(stream)
        .keyBy(_.key)
        .reduce { (acc, in) =>
          KV(acc.key, acc.value + in.value)
        }
        .executeAndCollect()

      val unbufferedCounts = unbuffered.toList.groupBy(_.key).mapValues(_.last)
      unbuffered.close()

      val buffered = env
        .fromCollection(stream)
        .keyBy(_.key)
        .buffer(
          Time.milliseconds(100),
          maxCount = 100,
          _.key,
          _.reduce((acc, in) => KV(acc.key, acc.value + in.value))
        )
        .keyBy(_.key)
        .reduce { (acc, in) =>
          KV(acc.key, acc.value + in.value)
        }
        .executeAndCollect()

      val bufferedCounts = buffered.toList.groupBy(_.key).mapValues(_.last)
      buffered.close()

      unbufferedCounts should equal(bufferedCounts)
    }

    it("buffers records and produces results faster") {

      val totalViews = 100000
      val totalUsers = 25000

      def streamSource(totalTweets: Int) = source(totalViews, totalUsers, totalTweets)

      def unbufferedStream(totalTweets: Int) =
        ViewCountReducer.computeUnbufferedStats(env.addSource(streamSource(totalTweets)))

      def bufferedStream(totalTweets: Int) =
        ViewCountReducer.computeBufferedStats(env.addSource(streamSource(totalTweets)))

      val reps = 5
      val tweetCounts = List(20000, 10000, 5000, 1000, 500, 100, 50, 20, 10, 1)

      val unbufferedTook = tweetCounts.map { tc =>
        (1 to reps)
          .map(i =>
            stopwatchRun(
              unbufferedStream(tc),
              s"Unbuffered Stream of $totalViews views over $tc tweets. Rep: $i"
            )
          )
          .sum
          .toDouble / reps
      }

      val bufferedTook = tweetCounts.map { tc =>
        (1 to reps)
          .map(i =>
            stopwatchRun(
              bufferedStream(tc),
              s"Buffered Stream of of $totalViews views over $tc tweets. Rep: $i"
            )
          )
          .sum
          .toDouble / reps
      }

      unbufferedTook.zip(bufferedTook).zip(tweetCounts).foreach {
        case ((unBuffered, buffered), tweetCount) =>
          log(s"TweetCount,Unbuffered,Buffered")
          log(s"$tweetCount,$unBuffered,$buffered")
          log(s"Speedup over unbuffered for $tweetCount tweet keys: ${roundToTwoDecimals(unBuffered / buffered)}")
      }

      unbufferedTook.zip(bufferedTook).forall { case (unbuff, buff) => unbuff > buff } shouldBe true
    }

  }

}

object KeyedStreamOpsSpec {
  case class KV(key: String, value: Int)
}
