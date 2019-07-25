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

package kafka.server

import kafka.network._
import kafka.utils._
import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.yammer.metrics.core.Meter
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.{KafkaThread, Time}

import scala.collection.mutable

/**
 * A thread that answers kafka requests.
 */
class KafkaRequestHandler(id: Int,
                          brokerId: Int,
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: AtomicInteger,
                          val requestChannel: RequestChannel,
                          apis: KafkaApis,
                          time: Time) extends Runnable with Logging {
  this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "
  private val shutdownComplete = new CountDownLatch(1)
  @volatile private var stopped = false

  def run() {
    while (!stopped) {
      // We use a single meter for aggregate idle percentage for the thread pool.
      // Since meter is calculated as total_recorded_value / time_window and
      // time_window is independent of the number of threads, each recorded idle
      // time should be discounted by # threads.
      val startSelectTime = time.nanoseconds

      val req = requestChannel.receiveRequest(300)
      val endTime = time.nanoseconds
      val idleTime = endTime - startSelectTime
      aggregateIdleMeter.mark(idleTime / totalHandlerThreads.get)

      req match {
        case RequestChannel.ShutdownRequest =>
          debug(s"Kafka request handler $id on broker $brokerId received shut down command")
          shutdownComplete.countDown()
          return

        case request: RequestChannel.Request =>
          try {
            request.requestDequeueTimeNanos = endTime
            trace(s"Kafka request handler $id on broker $brokerId handling request $request")
            apis.handle(request)
          } catch {
            case e: FatalExitError =>
              shutdownComplete.countDown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            request.releaseBuffer()
          }

        case null => // continue
      }
    }
    shutdownComplete.countDown()
  }

  def stop(): Unit = {
    stopped = true
  }

  def initiateShutdown(): Unit = requestChannel.sendShutdownRequest()

  def awaitShutdown(): Unit = shutdownComplete.await()

}

class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              time: Time,
                              numThreads: Int,
                              requestHandlerAvgIdleMetricName: String,
                              logAndThreadNamePrefix : String) extends Logging with KafkaMetricsGroup {

  private val threadPoolSize: AtomicInteger = new AtomicInteger(numThreads)
  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = newMeter(requestHandlerAvgIdleMetricName, "percent", TimeUnit.NANOSECONDS)

  this.logIdent = "[" + logAndThreadNamePrefix + " Kafka Request Handler on Broker " + brokerId + "], "
  val runnables = new mutable.ArrayBuffer[KafkaRequestHandler](numThreads)
  for (i <- 0 until numThreads) {
    createHandler(i)
  }

  def createHandler(id: Int): Unit = synchronized {
    runnables += new KafkaRequestHandler(id, brokerId, aggregateIdleMeter, threadPoolSize, requestChannel, apis, time)
    KafkaThread.daemon(logAndThreadNamePrefix + "-kafka-request-handler-" + id, runnables(id)).start()
  }

  def resizeThreadPool(newSize: Int): Unit = synchronized {
    val currentSize = threadPoolSize.get
    info(s"Resizing request handler thread pool size from $currentSize to $newSize")
    if (newSize > currentSize) {
      for (i <- currentSize until newSize) {
        createHandler(i)
      }
    } else if (newSize < currentSize) {
      for (i <- 1 to (currentSize - newSize)) {
        runnables.remove(currentSize - i).stop()
      }
    }
    threadPoolSize.set(newSize)
  }

  def shutdown(): Unit = synchronized {
    info("shutting down")
    for (handler <- runnables)
      handler.initiateShutdown()
    for (handler <- runnables)
      handler.awaitShutdown()
    info("shut down completely")
  }
}

class BrokerTopicMetrics(name: Option[String]) extends KafkaMetricsGroup {
  val tags: scala.collection.Map[String, String] = name match {
    case None => Map.empty
    case Some(topic) => Map("topic" -> topic)
  }

  // an internal map for "lazy initialization" of certain metrics
  private val metricTypeMap = new Pool[String, Meter]

  def messagesInRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.MessagesInPerSec,
    newMeter(BrokerTopicStats.MessagesInPerSec, "messages", TimeUnit.SECONDS, tags))
  def bytesInRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.BytesInPerSec,
    newMeter(BrokerTopicStats.BytesInPerSec, "bytes", TimeUnit.SECONDS, tags))
  def bytesOutRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.BytesOutPerSec,
    newMeter(BrokerTopicStats.BytesOutPerSec, "bytes", TimeUnit.SECONDS, tags))
  def bytesRejectedRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.BytesRejectedPerSec,
    newMeter(BrokerTopicStats.BytesRejectedPerSec, "bytes", TimeUnit.SECONDS, tags))
  private[server] def replicationBytesInRate =
    if (name.isEmpty) Some(metricTypeMap.getAndMaybePut(
      BrokerTopicStats.ReplicationBytesInPerSec,
      newMeter(BrokerTopicStats.ReplicationBytesInPerSec, "bytes", TimeUnit.SECONDS, tags)))
    else None
  private[server] def replicationBytesOutRate =
    if (name.isEmpty) Some(metricTypeMap.getAndMaybePut(
      BrokerTopicStats.ReplicationBytesOutPerSec,
      newMeter(BrokerTopicStats.ReplicationBytesOutPerSec, "bytes", TimeUnit.SECONDS, tags)))
    else None
  def failedProduceRequestRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.FailedProduceRequestsPerSec,
    newMeter(BrokerTopicStats.FailedProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags))
  def failedFetchRequestRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.FailedFetchRequestsPerSec,
    newMeter(BrokerTopicStats.FailedFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags))
  def totalProduceRequestRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.TotalProduceRequestsPerSec,
    newMeter(BrokerTopicStats.TotalProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags))
  def totalFetchRequestRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.TotalFetchRequestsPerSec,
    newMeter(BrokerTopicStats.TotalFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags))
  def fetchMessageConversionsRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.FetchMessageConversionsPerSec,
    newMeter(BrokerTopicStats.FetchMessageConversionsPerSec, "requests", TimeUnit.SECONDS, tags))
  def produceMessageConversionsRate = metricTypeMap.getAndMaybePut(BrokerTopicStats.ProduceMessageConversionsPerSec,
    newMeter(BrokerTopicStats.ProduceMessageConversionsPerSec, "requests", TimeUnit.SECONDS, tags))

  // this method helps check with metricTypeMap first before deleting a metric
  def removeMetricHelper(metricType: String, tags: scala.collection.Map[String, String]): Unit = {
    val metric: Meter = metricTypeMap.remove(metricType)
    if (metric != null) {
      removeMetric(metricType, tags)
    }
  }

  def close() {
    removeMetricHelper(BrokerTopicStats.MessagesInPerSec, tags)
    removeMetricHelper(BrokerTopicStats.BytesInPerSec, tags)
    removeMetricHelper(BrokerTopicStats.BytesOutPerSec, tags)
    removeMetricHelper(BrokerTopicStats.BytesRejectedPerSec, tags)
    if (replicationBytesInRate.isDefined)
      removeMetricHelper(BrokerTopicStats.ReplicationBytesInPerSec, tags)
    if (replicationBytesOutRate.isDefined)
      removeMetricHelper(BrokerTopicStats.ReplicationBytesOutPerSec, tags)
    removeMetricHelper(BrokerTopicStats.FailedProduceRequestsPerSec, tags)
    removeMetricHelper(BrokerTopicStats.FailedFetchRequestsPerSec, tags)
    removeMetricHelper(BrokerTopicStats.TotalProduceRequestsPerSec, tags)
    removeMetricHelper(BrokerTopicStats.TotalFetchRequestsPerSec, tags)
    removeMetricHelper(BrokerTopicStats.FetchMessageConversionsPerSec, tags)
    removeMetricHelper(BrokerTopicStats.ProduceMessageConversionsPerSec, tags)
  }
}

object BrokerTopicStats {
  val MessagesInPerSec = "MessagesInPerSec"
  val BytesInPerSec = "BytesInPerSec"
  val BytesOutPerSec = "BytesOutPerSec"
  val BytesRejectedPerSec = "BytesRejectedPerSec"
  val ReplicationBytesInPerSec = "ReplicationBytesInPerSec"
  val ReplicationBytesOutPerSec = "ReplicationBytesOutPerSec"
  val FailedProduceRequestsPerSec = "FailedProduceRequestsPerSec"
  val FailedFetchRequestsPerSec = "FailedFetchRequestsPerSec"
  val TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec"
  val TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec"
  val FetchMessageConversionsPerSec = "FetchMessageConversionsPerSec"
  val ProduceMessageConversionsPerSec = "ProduceMessageConversionsPerSec"
  private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k))
}

class BrokerTopicStats {
  import BrokerTopicStats._

  private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  val allTopicsStats = new BrokerTopicMetrics(None)

  def topicStats(topic: String): BrokerTopicMetrics =
    stats.getAndMaybePut(topic)

  def updateReplicationBytesIn(value: Long) {
    allTopicsStats.replicationBytesInRate.foreach { metric =>
      metric.mark(value)
    }
  }

  private def updateReplicationBytesOut(value: Long) {
    allTopicsStats.replicationBytesOutRate.foreach { metric =>
      metric.mark(value)
    }
  }

  // This method only removes metrics only used for leader
  def removeOldLeaderMetrics(topic: String) {
    val topicMetrics = topicStats(topic)
    if (topicMetrics != null) {
      topicMetrics.removeMetricHelper(BrokerTopicStats.MessagesInPerSec, topicMetrics.tags)
      topicMetrics.removeMetricHelper(BrokerTopicStats.BytesInPerSec, topicMetrics.tags)
      topicMetrics.removeMetricHelper(BrokerTopicStats.BytesRejectedPerSec, topicMetrics.tags)
      topicMetrics.removeMetricHelper(BrokerTopicStats.FailedProduceRequestsPerSec, topicMetrics.tags)
      topicMetrics.removeMetricHelper(BrokerTopicStats.TotalProduceRequestsPerSec, topicMetrics.tags)
      topicMetrics.removeMetricHelper(BrokerTopicStats.ProduceMessageConversionsPerSec, topicMetrics.tags)
      topicMetrics.removeMetricHelper(BrokerTopicStats.ReplicationBytesOutPerSec, topicMetrics.tags)
    }
  }

  // This method only removes metrics only used for follower
  def removeOldFollowerMetrics(topic: String): Unit = {
    val topicMetrics = topicStats(topic)
    if (topicMetrics != null)
      topicMetrics.removeMetricHelper(BrokerTopicStats.ReplicationBytesInPerSec, topicMetrics.tags)
  }

  def removeMetrics(topic: String) {
    val metrics = stats.remove(topic)
    if (metrics != null)
      metrics.close()
  }

  def updateBytesOut(topic: String, isFollower: Boolean, value: Long) {
    if (isFollower) {
      updateReplicationBytesOut(value)
    } else {
      topicStats(topic).bytesOutRate.mark(value)
      allTopicsStats.bytesOutRate.mark(value)
    }
  }

  def close(): Unit = {
    allTopicsStats.close()
    stats.values.foreach(_.close())
  }

}
