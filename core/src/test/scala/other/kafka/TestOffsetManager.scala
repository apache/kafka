/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package other.kafka

import kafka.api._
import kafka.utils.{ZkUtils, ShutdownableThread}
import org.apache.kafka.common.protocol.Errors
import scala.collection._
import kafka.client.ClientUtils
import joptsimple.OptionParser
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.network.BlockingChannel
import scala.util.Random
import java.io.IOException
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.nio.channels.ClosedByInterruptException


object TestOffsetManager {

  val random = new Random
  val SocketTimeoutMs = 10000

  class StatsThread(reportingIntervalMs: Long, commitThreads: Seq[CommitThread], fetchThread: FetchThread)
        extends ShutdownableThread("stats-thread") {

    def printStats() {
      println("--------------------------------------------------------------------------------")
      println("Aggregate stats for commits:")
      println("Error count: %d; Max:%f; Min: %f; Mean: %f; Commit count: %d".format(
        commitThreads.map(_.numErrors.get).sum,
        commitThreads.map(_.timer.max()).max,
        commitThreads.map(_.timer.min()).min,
        commitThreads.map(_.timer.mean()).sum / commitThreads.size,
        commitThreads.map(_.numCommits.get).sum))
      println("--------------------------------------------------------------------------------")
      commitThreads.foreach(t => println(t.stats))
      println(fetchThread.stats)
    }

    override def doWork() {
      printStats()
      Thread.sleep(reportingIntervalMs)
    }

  }

  class CommitThread(id: Int, partitionCount: Int, commitIntervalMs: Long, zkUtils: ZkUtils)
        extends ShutdownableThread("commit-thread")
        with KafkaMetricsGroup {

    private val groupId = "group-" + id
    private val metadata = "Metadata from commit thread " + id
    private var offsetsChannel = ClientUtils.channelToOffsetManager(groupId, zkUtils, SocketTimeoutMs)
    private var offset = 0L
    val numErrors = new AtomicInteger(0)
    val numCommits = new AtomicInteger(0)
    val timer = newTimer("commit-thread", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
    private val commitTimer = new KafkaTimer(timer)
    val shutdownLock = new Object

    private def ensureConnected() {
      if (!offsetsChannel.isConnected)
        offsetsChannel = ClientUtils.channelToOffsetManager(groupId, zkUtils, SocketTimeoutMs)
    }

    override def doWork() {
      val commitRequest = OffsetCommitRequest(groupId, immutable.Map((1 to partitionCount).map(TopicAndPartition("topic-" + id, _) -> OffsetAndMetadata(offset, metadata)):_*))
      try {
        ensureConnected()
        offsetsChannel.send(commitRequest)
        numCommits.getAndIncrement
        commitTimer.time {
          val response = OffsetCommitResponse.readFrom(offsetsChannel.receive().payload())
          if (response.commitStatus.exists(_._2 != Errors.NONE.code)) numErrors.getAndIncrement
        }
        offset += 1
      }
      catch {
        case e1: ClosedByInterruptException =>
          offsetsChannel.disconnect()
        case e2: IOException =>
          println("Commit thread %d: Error while committing offsets to %s:%d for group %s due to %s.".format(id, offsetsChannel.host, offsetsChannel.port, groupId, e2))
          offsetsChannel.disconnect()
      }
      finally {
        Thread.sleep(commitIntervalMs)
      }
    }

    override def shutdown() {
      super.shutdown()
      awaitShutdown()
      offsetsChannel.disconnect()
      println("Commit thread %d ended. Last committed offset: %d.".format(id, offset))
    }

    def stats = {
      "Commit thread %d :: Error count: %d; Max:%f; Min: %f; Mean: %f; Commit count: %d"
      .format(id, numErrors.get(), timer.max(), timer.min(), timer.mean(), numCommits.get())
    }
  }

  class FetchThread(numGroups: Int, fetchIntervalMs: Long, zkUtils: ZkUtils)
        extends ShutdownableThread("fetch-thread")
        with KafkaMetricsGroup {

    private val timer = newTimer("fetch-thread", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
    private val fetchTimer = new KafkaTimer(timer)

    private val channels = mutable.Map[Int, BlockingChannel]()
    private var metadataChannel = ClientUtils.channelToAnyBroker(zkUtils, SocketTimeoutMs)

    private val numErrors = new AtomicInteger(0)

    override def doWork() {
      val id = random.nextInt().abs % numGroups
      val group = "group-" + id
      try {
        metadataChannel.send(GroupCoordinatorRequest(group))
        val coordinatorId = GroupCoordinatorResponse.readFrom(metadataChannel.receive().payload()).coordinatorOpt.map(_.id).getOrElse(-1)

        val channel = if (channels.contains(coordinatorId))
          channels(coordinatorId)
        else {
          val newChannel = ClientUtils.channelToOffsetManager(group, zkUtils, SocketTimeoutMs)
          channels.put(coordinatorId, newChannel)
          newChannel
        }

        try {
          // send the offset fetch request
          val fetchRequest = OffsetFetchRequest(group, Seq(TopicAndPartition("topic-"+id, 1)))
          channel.send(fetchRequest)

          fetchTimer.time {
            val response = OffsetFetchResponse.readFrom(channel.receive().payload())
            if (response.requestInfo.exists(_._2.error != Errors.NONE.code)) {
              numErrors.getAndIncrement
            }
          }
        }
        catch {
          case e1: ClosedByInterruptException =>
            channel.disconnect()
            channels.remove(coordinatorId)
          case e2: IOException =>
            println("Error while fetching offset from %s:%d due to %s.".format(channel.host, channel.port, e2))
            channel.disconnect()
            channels.remove(coordinatorId)
        }
      }
      catch {
        case e: IOException =>
          println("Error while querying %s:%d - shutting down query channel.".format(metadataChannel.host, metadataChannel.port))
          metadataChannel.disconnect()
          println("Creating new query channel.")
          metadataChannel = ClientUtils.channelToAnyBroker(zkUtils, SocketTimeoutMs)
      }
      finally {
        Thread.sleep(fetchIntervalMs)
      }

    }

    override def shutdown() {
      super.shutdown()
      awaitShutdown()
      channels.foreach(_._2.disconnect())
      metadataChannel.disconnect()
    }

    def stats = {
      "Fetch thread :: Error count: %d; Max:%f; Min: %f; Mean: %f; Fetch count: %d"
      .format(numErrors.get(), timer.max(), timer.min(), timer.mean(), timer.count())
    }
  }

  def main(args: Array[String]) {
    val parser = new OptionParser
    val zookeeperOpt = parser.accepts("zookeeper", "The ZooKeeper connection URL.")
      .withRequiredArg
      .describedAs("ZooKeeper URL")
      .ofType(classOf[java.lang.String])
      .defaultsTo("localhost:2181")

    val commitIntervalOpt = parser.accepts("commit-interval-ms", "Offset commit interval.")
      .withRequiredArg
      .describedAs("interval")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)

    val fetchIntervalOpt = parser.accepts("fetch-interval-ms", "Offset fetch interval.")
      .withRequiredArg
      .describedAs("interval")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1000)

    val numPartitionsOpt = parser.accepts("partition-count", "Number of partitions per commit.")
      .withRequiredArg
      .describedAs("interval")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)

    val numThreadsOpt = parser.accepts("thread-count", "Number of commit threads.")
      .withRequiredArg
      .describedAs("threads")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)

    val reportingIntervalOpt = parser.accepts("reporting-interval-ms", "Interval at which stats are reported.")
      .withRequiredArg
      .describedAs("interval (ms)")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3000)

    val helpOpt = parser.accepts("help", "Print this message.")

    val options = parser.parse(args : _*)

    if (options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      System.exit(0)
    }

    val commitIntervalMs = options.valueOf(commitIntervalOpt).intValue()
    val fetchIntervalMs = options.valueOf(fetchIntervalOpt).intValue()
    val threadCount = options.valueOf(numThreadsOpt).intValue()
    val partitionCount = options.valueOf(numPartitionsOpt).intValue()
    val zookeeper = options.valueOf(zookeeperOpt)
    val reportingIntervalMs = options.valueOf(reportingIntervalOpt).intValue()
    println("Commit thread count: %d; Partition count: %d, Commit interval: %d ms; Fetch interval: %d ms; Reporting interval: %d ms"
            .format(threadCount, partitionCount, commitIntervalMs, fetchIntervalMs, reportingIntervalMs))

    var zkUtils: ZkUtils = null
    var commitThreads: Seq[CommitThread] = Seq()
    var fetchThread: FetchThread = null
    var statsThread: StatsThread = null
    try {
      zkUtils = ZkUtils(zookeeper, 6000, 2000, false)
      commitThreads = (0 to (threadCount-1)).map { threadId =>
        new CommitThread(threadId, partitionCount, commitIntervalMs, zkUtils)
      }

      fetchThread = new FetchThread(threadCount, fetchIntervalMs, zkUtils)

      val statsThread = new StatsThread(reportingIntervalMs, commitThreads, fetchThread)

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() {
          cleanShutdown()
          statsThread.printStats()
        }
      })

      commitThreads.foreach(_.start())

      fetchThread.start()

      statsThread.start()

      commitThreads.foreach(_.join())
      fetchThread.join()
      statsThread.join()
    }
    catch {
      case e: Throwable =>
        println("Error: ", e)
    }
    finally {
      cleanShutdown()
    }

    def cleanShutdown() {
      commitThreads.foreach(_.shutdown())
      commitThreads.foreach(_.join())
      if (fetchThread != null) {
        fetchThread.shutdown()
        fetchThread.join()
      }
      if (statsThread != null) {
        statsThread.shutdown()
        statsThread.join()
      }
      zkUtils.close()
    }

  }
}

