package com.ariskk.membuffer

import scala.util.Random

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend

object FlinkExecutor {

  private var flinkCluster: MiniClusterWithClientResource = _

  def startCluster(): Unit = {
    flinkCluster = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(2)
        .setNumberTaskManagers(2)
        .build
    )

    flinkCluster.before()
  }

  def stopCluster(): Unit =
    flinkCluster.after()

  def newEnv: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val rocks = new RocksDBStateBackend(s"file:///tmp/flink-${Random.nextString(10)}")
    env.setStateBackend(rocks)
    env
  }

}
