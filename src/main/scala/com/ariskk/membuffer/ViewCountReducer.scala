package com.ariskk.membuffer

import com.twitter.algebird.{HLL, HyperLogLogAggregator, Semigroup}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import View.TweetId
import FlinkOps._

import scala.util.Random

object ViewCountReducer {

  case class StatsState(tweetId: TweetId, viewCounter: HLL, viewerCounter: HLL) {
    def toStats: TweetStats =
      TweetStats(
        tweetId,
        viewCounter.approximateSize.estimate,
        viewerCounter.approximateSize.estimate
      )

    def salt(splits: Int) =
      this.copy(
        tweetId = s"$tweetId-${Random.nextInt(10)}"
      )

    def unsalt = this.copy(tweetId = tweetId.split("-").head)
  }

  object StatsState {
    private val agg = HyperLogLogAggregator.withErrorGeneric[String](0.01)

    def fromView(view: View) =
      StatsState(
        view.tweetId,
        agg.prepare(view.viewId),
        agg.prepare(view.userId)
      )

    implicit val semigroup = new Semigroup[StatsState] {
      final def plus(x: StatsState, y: StatsState): StatsState =
        StatsState(
          x.tweetId,
          x.viewCounter + y.viewCounter,
          x.viewerCounter + y.viewerCounter
        )
    }
  }

  def computeUnbufferedStats(views: DataStream[View]): DataStream[TweetStats] =
    views
      .map(StatsState.fromView(_))
      .keyBy(_.tweetId)
      .reduce((acc, in) => Semigroup.plus[StatsState](acc, in))
      .map(_.toStats)

  def computeBufferedStats(views: DataStream[View]): DataStream[TweetStats] =
    views
      .map(StatsState.fromView(_))
      .keyBy(_.tweetId)
      .buffer(
        Time.milliseconds(100),
        maxCount = 1000,
        keySelector = _.tweetId,
        processor = _.reduce(Semigroup.plus[StatsState](_, _))
      )
      .keyBy(_.tweetId)
      .reduce { (acc, in) =>
        Semigroup.plus(acc, in)
      }
      .map(_.toStats)

  // Too slow to include in the benchmark.
  def computeSaltedStats(views: DataStream[View], splits: Int): DataStream[TweetStats] =
    views
      .map(StatsState.fromView(_).salt(splits))
      .keyBy(_.tweetId)
      .reduce { (acc, in) =>
        Semigroup.plus(acc, in)
      }
      .map(_.unsalt)
      .keyBy(_.tweetId)
      .reduce { (acc, in) =>
        Semigroup.plus(acc, in)
      }
      .map(_.toStats)

}
