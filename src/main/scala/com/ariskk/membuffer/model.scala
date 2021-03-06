package com.ariskk.membuffer

import View._

import scala.util.Random

case class View(viewId: ViewId, userId: UserId, tweetId: TweetId)

object View {
  type UserId = String
  type TweetId = String
  type ViewId = String

  def generate(
    userCardinality: Int,
    tweetCardinality: Int
  ): View =
    View(
      viewId = s"view-${Random.nextString(10)}",
      userId = s"user-${Random.nextInt(userCardinality)}",
      tweetId = s"tweet-${Random.nextInt(tweetCardinality)}"
    )

}

case class TweetStats(tweetId: TweetId, viewCount: Long, viewerCount: Long)
