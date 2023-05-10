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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import com.yammer.metrics.core.Meter
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.{KafkaThread, Time}
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util.Collections
import scala.collection.mutable
import scala.jdk.CollectionConverters._

trait ApiRequestHandler {
  def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit
}

/**
 * A thread that answers kafka requests.
 * comment by shuai 20230510
 * 请求处理线程类。每个请求处理线程实例，负责从 SocketServer 的 RequestChannel 的请求队列中获取请求对象，并进行处理。
 * @param id 请求处理线程的序号，类似于 Processor 线程的 ID 序号，仅仅用于标识这是线程池中的第几个线程。
 * @param brokerId Broker 序号，用于标识这是哪个 Broker 上的请求处理线程。
 * @param aggregateIdleMeter
 * @param totalHandlerThreads
 * @param requestChannel SocketServer 中的请求通道对象。KafkaRequestHandler 对象为什么要定义这个字段呢？我们说过，它是负责处理请求的类，
 *                       请求恰恰是保存在 RequestChannel 中的请求队列中，因此，Kafka 在构造 KafkaRequestHandler 实例时，
 *                       必须关联 SocketServer 组件中的 RequestChannel 实例，也就是说，要让 I/O 线程能够找到请求被保存的地方。
 * @param apis 这是一个 KafkaApis 类。如果说 KafkaRequestHandler 是真正处理请求的，那么，KafkaApis 类就是真正执行请求处理逻辑的地方。
 *             它有个 handle 方法，用于执行请求处理逻辑。
 * @param time
 */
class KafkaRequestHandler(id: Int, // id: I/O线程序号；请求处理线程的序号，类似于 Processor 线程的 ID 序号，仅仅用于标识这是线程池中的第几个线程。
                          brokerId: Int, // brokerId：所在Broker序号，即broker.id值；用于标识这是哪个 Broker 上的请求处理线程。
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: AtomicInteger,// totalHandlerThreads：I/O线程池大小
                          val requestChannel: RequestChannel,// requestChannel：请求处理通道
                          apis: ApiRequestHandler,// apis：KafkaApis类，用于真正实现请求处理逻辑的类
                          time: Time) extends Runnable with Logging {
  this.logIdent = s"[Kafka Request Handler $id on Broker $brokerId], "
  private val shutdownComplete = new CountDownLatch(1)
  private val requestLocal = RequestLocal.withThreadConfinedCaching
  @volatile private var stopped = false

  // KafkaRequestHandler 是一个线程类，那么，除去常规的 close、stop、initiateShutdown 和 awaitShutdown 方法，
  // 最重要的当属 run 方法实现了
  /**
   * <img width="445" height="384" src="https://static001.geekbang.org/resource/image/b5/4d/b5f6d3b4ecea86a3e66a29953034dc4d.jpg" alt="">
   * 第 1 步是从请求队列中获取下一个待处理的请求，同时更新一些相关的统计指标。如果本次循环没取到，那么本轮循环结束，进入到下一轮。
   * 如果是 ShutdownRequest 请求，则说明该 Broker 发起了关闭操作。
   **/
  def run(): Unit = {
    // 只要该线程尚未关闭，循环运行处理逻辑
    //它的所有执行逻辑都在 while 循环之下，因此，只要标志线程关闭状态的 stopped 为 false，run 方法将一直循环执行 while 下的语句。
    while (!stopped) {
      // We use a single meter for aggregate idle percentage for the thread pool.
      // Since meter is calculated as total_recorded_value / time_window and
      // time_window is independent of the number of threads, each recorded idle
      // time should be discounted by # threads.
      val startSelectTime = time.nanoseconds

      // 从请求队列中获取下一个待处理的请求
      val req = requestChannel.receiveRequest(300)
      val endTime = time.nanoseconds
      // 统计线程空闲时间
      val idleTime = endTime - startSelectTime
      // 更新线程空闲百分比指标
      aggregateIdleMeter.mark(idleTime / totalHandlerThreads.get)

      req match {
        // 关闭线程请求
        case RequestChannel.ShutdownRequest =>
          debug(s"Kafka request handler $id on broker $brokerId received shut down command")
          // 关闭线程
          // 而 Broker 关闭时会调用 KafkaRequestHandler 的 shutdown 方法，进而调用 initiateShutdown 方法，以及 RequestChannel 的 sendShutdownRequest 方法，而后者就是将 ShutdownRequest 写入到请求队列。
          // 一旦从请求队列中获取到 ShutdownRequest，run 方法代码会调用 shutdownComplete 的 countDown 方法，正式完成对 KafkaRequestHandler 线程的关闭操作。
          completeShutdown()
          return

        // 普通请求
        case request: RequestChannel.Request =>
          try {
            request.requestDequeueTimeNanos = endTime
            trace(s"Kafka request handler $id on broker $brokerId handling request $request")
            // 由KafkaApis.handle方法执行相应处理逻辑
            apis.handle(request, requestLocal)
          } catch {
            // 如果出现严重错误，立即关闭线程
            case e: FatalExitError =>
              completeShutdown()
              Exit.exit(e.statusCode)

            // 如果是普通异常，记录错误日志
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            // 释放请求对象占用的内存缓冲区资源
            request.releaseBuffer()
          }

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

// KafkaRequestHandlerPool 组件就是我们常说的 I/O 线程池，里面定义了若干个 I/O 线程，用于执行真实的请求处理逻辑。
// KafkaRequestHandlerPool 线程池定义了多个 KafkaRequestHandler 线程，而 KafkaRequestHandler 线程是真正处理请求逻辑的地方。
// 请求处理线程池，负责创建、维护、管理和销毁下辖的请求处理线程。
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

  /**
   * 而 Broker 关闭时会调用 KafkaRequestHandler 的 shutdown 方法，
   * 进而调用 initiateShutdown 方法，以及 RequestChannel 的 sendShutdownRequest 方法，
   * 而后者就是将 ShutdownRequest 写入到请求队列。
   *
   * 一旦从请求队列中获取到 ShutdownRequest，run 方法代码会调用 shutdownComplete 的 countDown 方法，正式完成对 KafkaRequestHandler 线程的关闭操作。
   */
  def shutdown(): Unit = synchronized {
    info("shutting down")
    for (handler <- runnables)
      handler.initiateShutdown()
    for (handler <- runnables)
      handler.awaitShutdown()
    info("shut down completely")
  }
}

//Broker 端与主题相关的监控指标的管理类。
class BrokerTopicMetrics(name: Option[String]) {
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

  def closeMetric(metricType: String): Unit = {
    val meter = metricTypeMap.get(metricType)
    if (meter != null)
      meter.close()
  }

  def close(): Unit = metricTypeMap.values.foreach(_.close())
}

// 定义 Broker 端与主题相关的监控指标的管理操作。
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

  private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k))
}

// BrokerTopicStats 的伴生对象类，定义 Broker 端与主题相关的监控指标，
// 比如常见的 MessagesInPerSec 和 MessagesOutPerSec 等。
class BrokerTopicStats extends Logging {
  import BrokerTopicStats._

  private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  val allTopicsStats = new BrokerTopicMetrics(None)

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
