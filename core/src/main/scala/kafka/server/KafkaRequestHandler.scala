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
import kafka.server.KafkaRequestHandler.{threadCurrentRequest, threadRequestChannel}

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import com.yammer.metrics.core.Meter
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.{KafkaThread, Time}
import org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util.Collections
import scala.collection.mutable
import scala.jdk.CollectionConverters._

trait ApiRequestHandler {
  def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit
  def tryCompleteActions(): Unit = {}
}

object KafkaRequestHandler {
  // Support for scheduling callbacks on a request thread.
  private val threadRequestChannel = new ThreadLocal[RequestChannel]
  private val threadCurrentRequest = new ThreadLocal[RequestChannel.Request]

  // For testing
  @volatile private var bypassThreadCheck = false
  def setBypassThreadCheck(bypassCheck: Boolean): Unit = {
    bypassThreadCheck = bypassCheck
  }

  /**
   * Wrap callback to schedule it on a request thread.
   * NOTE: this function must be called on a request thread.
   * @param fun Callback function to execute
   * @return Wrapped callback that would execute `fun` on a request thread
   */
  def wrap[T](fun: T => Unit): T => Unit = {
    val requestChannel = threadRequestChannel.get()
    val currentRequest = threadCurrentRequest.get()
    if (requestChannel == null || currentRequest == null) {
      if (!bypassThreadCheck)
        throw new IllegalStateException("Attempted to reschedule to request handler thread from non-request handler thread.")
      T => fun(T)
    } else {
      T => {
        if (threadCurrentRequest.get() != null) {
          // If the callback is actually executed on a request thread, we can directly execute
          // it without re-scheduling it.
          fun(T)
        } else {
          // The requestChannel and request are captured in this lambda, so when it's executed on the callback thread
          // we can re-schedule the original callback on a request thread and update the metrics accordingly.
          requestChannel.sendCallbackRequest(RequestChannel.CallbackRequest(() => fun(T), currentRequest))
        }
      }
    }
  }
}

/**
 * A thread that answers kafka requests.
 */
class KafkaRequestHandler(id: Int,
                          brokerId: Int,
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: AtomicInteger,
                          val requestChannel: RequestChannel,
                          apis: ApiRequestHandler,
                          time: Time) extends Runnable with Logging {
  this.logIdent = s"[Kafka Request Handler $id on Broker $brokerId], "
  private val shutdownComplete = new CountDownLatch(1)
  private val requestLocal = RequestLocal.withThreadConfinedCaching
  @volatile private var stopped = false

  def run(): Unit = {
    threadRequestChannel.set(requestChannel)
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
          completeShutdown()
          return

        case callback: RequestChannel.CallbackRequest =>
          val originalRequest = callback.originalRequest
          try {

            // If we've already executed a callback for this request, reset the times and subtract the callback time from the 
            // new dequeue time. This will allow calculation of multiple callback times.
            // Otherwise, set dequeue time to now.
            if (originalRequest.callbackRequestDequeueTimeNanos.isDefined) {
              val prevCallbacksTimeNanos = originalRequest.callbackRequestCompleteTimeNanos.getOrElse(0L) - originalRequest.callbackRequestDequeueTimeNanos.getOrElse(0L)
              originalRequest.callbackRequestCompleteTimeNanos = None
              originalRequest.callbackRequestDequeueTimeNanos = Some(time.nanoseconds() - prevCallbacksTimeNanos)
            } else {
              originalRequest.callbackRequestDequeueTimeNanos = Some(time.nanoseconds())
            }
            
            threadCurrentRequest.set(originalRequest)
            callback.fun()
          } catch {
            case e: FatalExitError =>
              completeShutdown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            // When handling requests, we try to complete actions after, so we should try to do so here as well.
            apis.tryCompleteActions()
            if (originalRequest.callbackRequestCompleteTimeNanos.isEmpty)
              originalRequest.callbackRequestCompleteTimeNanos = Some(time.nanoseconds())
            threadCurrentRequest.remove()
          }

        case request: RequestChannel.Request =>
          try {
            request.requestDequeueTimeNanos = endTime
            trace(s"Kafka request handler $id on broker $brokerId handling request $request")
            threadCurrentRequest.set(request)
            apis.handle(request, requestLocal)
          } catch {
            case e: FatalExitError =>
              completeShutdown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            threadCurrentRequest.remove()
            request.releaseBuffer()
          }

        case RequestChannel.WakeupRequest => 
          // We should handle this in receiveRequest by polling callbackQueue.
          warn("Received a wakeup request outside of typical usage.")

        case null => // continue
      }
    }
    completeShutdown()
  }

  private def completeShutdown(): Unit = {
    requestLocal.close()
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
                              val apis: ApiRequestHandler,
                              time: Time,
                              numThreads: Int,
                              requestHandlerAvgIdleMetricName: String,
                              logAndThreadNamePrefix : String) extends Logging {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  private val threadPoolSize: AtomicInteger = new AtomicInteger(numThreads)
  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = metricsGroup.newMeter(requestHandlerAvgIdleMetricName, "percent", TimeUnit.NANOSECONDS)

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

class BrokerTopicMetrics(name: Option[String], configOpt: java.util.Optional[KafkaConfig]) {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val tags: java.util.Map[String, String] = name match {
    case None => Collections.emptyMap()
    case Some(topic) => Map("topic" -> topic).asJava
  }

  case class MeterWrapper(metricType: String, eventType: String) {
    @volatile private var lazyMeter: Meter = _
    private val meterLock = new Object

    def meter(): Meter = {
      var meter = lazyMeter
      if (meter == null) {
        meterLock synchronized {
          meter = lazyMeter
          if (meter == null) {
            meter = metricsGroup.newMeter(metricType, eventType, TimeUnit.SECONDS, tags)
            lazyMeter = meter
          }
        }
      }
      meter
    }

    def close(): Unit = meterLock synchronized {
      if (lazyMeter != null) {
        metricsGroup.removeMetric(metricType, tags)
        lazyMeter = null
      }
    }

    if (tags.isEmpty) // greedily initialize the general topic metrics
      meter()
  }

  // an internal map for "lazy initialization" of certain metrics
  private val metricTypeMap = new Pool[String, MeterWrapper]()
  metricTypeMap.putAll(Map(
    BrokerTopicStats.MessagesInPerSec -> MeterWrapper(BrokerTopicStats.MessagesInPerSec, "messages"),
    BrokerTopicStats.BytesInPerSec -> MeterWrapper(BrokerTopicStats.BytesInPerSec, "bytes"),
    BrokerTopicStats.BytesOutPerSec -> MeterWrapper(BrokerTopicStats.BytesOutPerSec, "bytes"),
    BrokerTopicStats.BytesRejectedPerSec -> MeterWrapper(BrokerTopicStats.BytesRejectedPerSec, "bytes"),
    BrokerTopicStats.FailedProduceRequestsPerSec -> MeterWrapper(BrokerTopicStats.FailedProduceRequestsPerSec, "requests"),
    BrokerTopicStats.FailedFetchRequestsPerSec -> MeterWrapper(BrokerTopicStats.FailedFetchRequestsPerSec, "requests"),
    BrokerTopicStats.TotalProduceRequestsPerSec -> MeterWrapper(BrokerTopicStats.TotalProduceRequestsPerSec, "requests"),
    BrokerTopicStats.TotalFetchRequestsPerSec -> MeterWrapper(BrokerTopicStats.TotalFetchRequestsPerSec, "requests"),
    BrokerTopicStats.FetchMessageConversionsPerSec -> MeterWrapper(BrokerTopicStats.FetchMessageConversionsPerSec, "requests"),
    BrokerTopicStats.ProduceMessageConversionsPerSec -> MeterWrapper(BrokerTopicStats.ProduceMessageConversionsPerSec, "requests"),
    BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec -> MeterWrapper(BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec, "requests"),
    BrokerTopicStats.InvalidMagicNumberRecordsPerSec -> MeterWrapper(BrokerTopicStats.InvalidMagicNumberRecordsPerSec, "requests"),
    BrokerTopicStats.InvalidMessageCrcRecordsPerSec -> MeterWrapper(BrokerTopicStats.InvalidMessageCrcRecordsPerSec, "requests"),
    BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec -> MeterWrapper(BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec, "requests")
  ).asJava)

  if (name.isEmpty) {
    metricTypeMap.put(BrokerTopicStats.ReplicationBytesInPerSec, MeterWrapper(BrokerTopicStats.ReplicationBytesInPerSec, "bytes"))
    metricTypeMap.put(BrokerTopicStats.ReplicationBytesOutPerSec, MeterWrapper(BrokerTopicStats.ReplicationBytesOutPerSec, "bytes"))
    metricTypeMap.put(BrokerTopicStats.ReassignmentBytesInPerSec, MeterWrapper(BrokerTopicStats.ReassignmentBytesInPerSec, "bytes"))
    metricTypeMap.put(BrokerTopicStats.ReassignmentBytesOutPerSec, MeterWrapper(BrokerTopicStats.ReassignmentBytesOutPerSec, "bytes"))
  }

  configOpt.ifPresent(config =>
    if (config.remoteLogManagerConfig.enableRemoteStorageSystem()) {
      metricTypeMap.putAll(Map(
        RemoteStorageMetrics.REMOTE_COPY_BYTES_PER_SEC_METRIC.getName -> MeterWrapper(RemoteStorageMetrics.REMOTE_COPY_BYTES_PER_SEC_METRIC.getName, "bytes"),
        RemoteStorageMetrics.REMOTE_FETCH_BYTES_PER_SEC_METRIC.getName -> MeterWrapper(RemoteStorageMetrics.REMOTE_FETCH_BYTES_PER_SEC_METRIC.getName, "bytes"),
        RemoteStorageMetrics.REMOTE_FETCH_REQUESTS_PER_SEC_METRIC.getName -> MeterWrapper(RemoteStorageMetrics.REMOTE_FETCH_REQUESTS_PER_SEC_METRIC.getName, "requests"),
        RemoteStorageMetrics.REMOTE_COPY_REQUESTS_PER_SEC_METRIC.getName -> MeterWrapper(RemoteStorageMetrics.REMOTE_COPY_REQUESTS_PER_SEC_METRIC.getName, "requests"),
        RemoteStorageMetrics.FAILED_REMOTE_FETCH_PER_SEC_METRIC.getName -> MeterWrapper(RemoteStorageMetrics.FAILED_REMOTE_FETCH_PER_SEC_METRIC.getName, "requests"),
        RemoteStorageMetrics.FAILED_REMOTE_COPY_PER_SEC_METRIC.getName -> MeterWrapper(RemoteStorageMetrics.FAILED_REMOTE_COPY_PER_SEC_METRIC.getName, "requests")
      ).asJava)
    })

  // used for testing only
  def metricMap: Map[String, MeterWrapper] = metricTypeMap.toMap

  def messagesInRate: Meter = metricTypeMap.get(BrokerTopicStats.MessagesInPerSec).meter()

  def bytesInRate: Meter = metricTypeMap.get(BrokerTopicStats.BytesInPerSec).meter()

  def bytesOutRate: Meter = metricTypeMap.get(BrokerTopicStats.BytesOutPerSec).meter()

  def bytesRejectedRate: Meter = metricTypeMap.get(BrokerTopicStats.BytesRejectedPerSec).meter()

  private[server] def replicationBytesInRate: Option[Meter] =
    if (name.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReplicationBytesInPerSec).meter())
    else None

  private[server] def replicationBytesOutRate: Option[Meter] =
    if (name.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReplicationBytesOutPerSec).meter())
    else None

  private[server] def reassignmentBytesInPerSec: Option[Meter] =
    if (name.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReassignmentBytesInPerSec).meter())
    else None

  private[server] def reassignmentBytesOutPerSec: Option[Meter] =
    if (name.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReassignmentBytesOutPerSec).meter())
    else None

  def failedProduceRequestRate: Meter = metricTypeMap.get(BrokerTopicStats.FailedProduceRequestsPerSec).meter()

  def failedFetchRequestRate: Meter = metricTypeMap.get(BrokerTopicStats.FailedFetchRequestsPerSec).meter()

  def totalProduceRequestRate: Meter = metricTypeMap.get(BrokerTopicStats.TotalProduceRequestsPerSec).meter()

  def totalFetchRequestRate: Meter = metricTypeMap.get(BrokerTopicStats.TotalFetchRequestsPerSec).meter()

  def fetchMessageConversionsRate: Meter = metricTypeMap.get(BrokerTopicStats.FetchMessageConversionsPerSec).meter()

  def produceMessageConversionsRate: Meter = metricTypeMap.get(BrokerTopicStats.ProduceMessageConversionsPerSec).meter()

  def noKeyCompactedTopicRecordsPerSec: Meter = metricTypeMap.get(BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec).meter()

  def invalidMagicNumberRecordsPerSec: Meter = metricTypeMap.get(BrokerTopicStats.InvalidMagicNumberRecordsPerSec).meter()

  def invalidMessageCrcRecordsPerSec: Meter = metricTypeMap.get(BrokerTopicStats.InvalidMessageCrcRecordsPerSec).meter()

  def invalidOffsetOrSequenceRecordsPerSec: Meter = metricTypeMap.get(BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec).meter()

  def remoteCopyBytesRate: Meter = metricTypeMap.get(RemoteStorageMetrics.REMOTE_COPY_BYTES_PER_SEC_METRIC.getName).meter()

  def remoteFetchBytesRate: Meter = metricTypeMap.get(RemoteStorageMetrics.REMOTE_FETCH_BYTES_PER_SEC_METRIC.getName).meter()

  def remoteFetchRequestRate: Meter = metricTypeMap.get(RemoteStorageMetrics.REMOTE_FETCH_REQUESTS_PER_SEC_METRIC.getName).meter()

  def remoteCopyRequestRate: Meter = metricTypeMap.get(RemoteStorageMetrics.REMOTE_COPY_REQUESTS_PER_SEC_METRIC.getName).meter()

  def failedRemoteFetchRequestRate: Meter = metricTypeMap.get(RemoteStorageMetrics.FAILED_REMOTE_FETCH_PER_SEC_METRIC.getName).meter()

  def failedRemoteCopyRequestRate: Meter = metricTypeMap.get(RemoteStorageMetrics.FAILED_REMOTE_COPY_PER_SEC_METRIC.getName).meter()

  def closeMetric(metricType: String): Unit = {
    val meter = metricTypeMap.get(metricType)
    if (meter != null)
      meter.close()
  }

  def close(): Unit = metricTypeMap.values.foreach(_.close())
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
  val ReassignmentBytesInPerSec = "ReassignmentBytesInPerSec"
  val ReassignmentBytesOutPerSec = "ReassignmentBytesOutPerSec"
  // These following topics are for LogValidator for better debugging on failed records
  val NoKeyCompactedTopicRecordsPerSec = "NoKeyCompactedTopicRecordsPerSec"
  val InvalidMagicNumberRecordsPerSec = "InvalidMagicNumberRecordsPerSec"
  val InvalidMessageCrcRecordsPerSec = "InvalidMessageCrcRecordsPerSec"
  val InvalidOffsetOrSequenceRecordsPerSec = "InvalidOffsetOrSequenceRecordsPerSec"
}

class BrokerTopicStats(configOpt: java.util.Optional[KafkaConfig] = java.util.Optional.empty()) extends Logging {

  private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k), configOpt)
  private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  val allTopicsStats = new BrokerTopicMetrics(None, configOpt)

  def topicStats(topic: String): BrokerTopicMetrics =
    stats.getAndMaybePut(topic)

  def updateReplicationBytesIn(value: Long): Unit = {
    allTopicsStats.replicationBytesInRate.foreach { metric =>
      metric.mark(value)
    }
  }

  private def updateReplicationBytesOut(value: Long): Unit = {
    allTopicsStats.replicationBytesOutRate.foreach { metric =>
      metric.mark(value)
    }
  }

  def updateReassignmentBytesIn(value: Long): Unit = {
    allTopicsStats.reassignmentBytesInPerSec.foreach { metric =>
      metric.mark(value)
    }
  }

  def updateReassignmentBytesOut(value: Long): Unit = {
    allTopicsStats.reassignmentBytesOutPerSec.foreach { metric =>
      metric.mark(value)
    }
  }

  // This method only removes metrics only used for leader
  def removeOldLeaderMetrics(topic: String): Unit = {
    val topicMetrics = topicStats(topic)
    if (topicMetrics != null) {
      topicMetrics.closeMetric(BrokerTopicStats.MessagesInPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.BytesInPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.BytesRejectedPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.FailedProduceRequestsPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.TotalProduceRequestsPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ProduceMessageConversionsPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ReplicationBytesOutPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ReassignmentBytesOutPerSec)
      topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_COPY_BYTES_PER_SEC_METRIC.getName)
      topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_FETCH_BYTES_PER_SEC_METRIC.getName)
      topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_FETCH_REQUESTS_PER_SEC_METRIC.getName)
      topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_COPY_REQUESTS_PER_SEC_METRIC.getName)
      topicMetrics.closeMetric(RemoteStorageMetrics.FAILED_REMOTE_FETCH_PER_SEC_METRIC.getName)
      topicMetrics.closeMetric(RemoteStorageMetrics.FAILED_REMOTE_COPY_PER_SEC_METRIC.getName)
    }
  }

  // This method only removes metrics only used for follower
  def removeOldFollowerMetrics(topic: String): Unit = {
    val topicMetrics = topicStats(topic)
    if (topicMetrics != null) {
      topicMetrics.closeMetric(BrokerTopicStats.ReplicationBytesInPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ReassignmentBytesInPerSec)
    }
  }

  def removeMetrics(topic: String): Unit = {
    val metrics = stats.remove(topic)
    if (metrics != null)
      metrics.close()
  }

  def updateBytesOut(topic: String, isFollower: Boolean, isReassignment: Boolean, value: Long): Unit = {
    if (isFollower) {
      if (isReassignment)
        updateReassignmentBytesOut(value)
      updateReplicationBytesOut(value)
    } else {
      topicStats(topic).bytesOutRate.mark(value)
      allTopicsStats.bytesOutRate.mark(value)
    }
  }

  def close(): Unit = {
    allTopicsStats.close()
    stats.values.foreach(_.close())

    info("Broker and topic stats closed")
  }
}
