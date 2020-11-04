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

package kafka.server.metadata

import java.util
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{KafkaConfig, MetadataCache, QuotaFactory, ReplicaManager}
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.Time
import org.apache.kafka.controller.MetaLogManager

object BrokerMetadataListener {
  val ThreadNamePrefix = "broker-"
  val ThreadNameSuffix = "-metadata-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
  val ErrorCountMetricName = "ErrorCount"
  val DefaultEventQueueTimeoutMs = 300000

  def defaultProcessors(kafkaConfig: KafkaConfig,
                        clusterId: String,
                        metadataCache: MetadataCache,
                        groupCoordinator: GroupCoordinator,
                        quotaManagers: QuotaFactory.QuotaManagers,
                        replicaManager: ReplicaManager,
                        txnCoordinator: TransactionCoordinator): List[BrokerMetadataProcessor] = {
    List(new PartitionMetadataProcessor(kafkaConfig, clusterId, metadataCache, groupCoordinator, quotaManagers,
      replicaManager, txnCoordinator))
  }
}

/**
 * A metadata event appearing either in the metadata log or originating out-of-band from the broker's heartbeat
 */
sealed trait BrokerMetadataEvent

/**
 * A batch of messages from the metadata log
 *
 * @param apiMessages the batch of messages
 * @param lastOffset the metadata offset of the last message in the batch
 */
final case class MetadataLogEvent(apiMessages: util.List[ApiMessage], lastOffset: Long) extends BrokerMetadataEvent

/**
 * A register-broker event that occurs when the broker heartbeat receives a successful registration response.
 * It will only occur once in the lifetime of the broker process, and it will occur before
 * any metadata log message batches appear.  The listener injects this event into the event stream,
 * and processors will receive it immediately.
 *
 * @param brokerEpoch the epoch assigned to the broker by the active controller
 */
final case class RegisterBrokerEvent(brokerEpoch: Long) extends BrokerMetadataEvent

/**
 * A fence-broker event that occurs when either:
 *
 * 1) The local broker's heartbeat is unable to contact the active controller within the
 * defined lease duration and loses its lease.
 * 2) The local broker's heartbeat is told by the controller that it should be in the fenced state.
 *
 * The listener injects this event into the event stream such that processors receive it as soon as they finish their
 * current message batch, if any, otherwise they receive it immediately.
 *
 * @param brokerEpoch the broker epoch that was fenced
 */
final case class FenceBrokerEvent(brokerEpoch: Long) extends BrokerMetadataEvent

/**
 * Used to wakeup the polling thread in order to shutdown.
 */
case object ShutdownEvent extends BrokerMetadataEvent

class QueuedEvent(val event: BrokerMetadataEvent, val enqueueTimeMs: Long) {
  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

trait BrokerMetadataProcessor {
  def process(event: BrokerMetadataEvent): Unit
}

class BrokerMetadataListener(
  config: KafkaConfig,
  time: Time,
  processors: List[BrokerMetadataProcessor],
  eventQueueTimeoutMs: Long = BrokerMetadataListener.DefaultEventQueueTimeoutMs
) extends MetaLogManager.Listener with KafkaMetricsGroup  {

  if (processors.isEmpty) {
    throw new IllegalArgumentException(s"Empty processors list!")
  }

  private val queue = new LinkedBlockingQueue[QueuedEvent]()
  private val thread = new BrokerMetadataEventThread(
    s"${BrokerMetadataListener.ThreadNamePrefix}${config.brokerId}${BrokerMetadataListener.ThreadNameSuffix}")

  // metrics
  private val eventQueueTimeHist = newHistogram(BrokerMetadataListener.EventQueueTimeMetricName)
  newGauge(BrokerMetadataListener.EventQueueSizeMetricName, () => queue.size)

  @volatile private var _currentMetadataOffset: Long = -1

  def start(): Unit = {
    thread.start()
  }

  // For testing, it is useful to be able to drain all events synchronously
  private[metadata] def drain(): Unit = {
    while (!queue.isEmpty) {
      thread.doWork()
    }
  }

  // For testing, in cases where we want to block synchronously while we wait for an event
  private[metadata] def poll(): Unit = {
    thread.doWork()
  }

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      put(ShutdownEvent) // wake up the thread in case it is blocked on queue.take()
      thread.awaitShutdown()
    } finally {
      removeMetric(BrokerMetadataListener.EventQueueTimeMetricName)
      removeMetric(BrokerMetadataListener.ErrorCountMetricName)
      removeMetric(BrokerMetadataListener.EventQueueSizeMetricName)
    }
  }

  override def handleCommits(lastOffset: Long, messages: util.List[ApiMessage]): Unit = {
    put(MetadataLogEvent(messages, lastOffset))
  }

  def put(event: BrokerMetadataEvent): QueuedEvent = {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent)
    queuedEvent
  }

  def currentMetadataOffset(): Long = _currentMetadataOffset

  private class BrokerMetadataEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[BrokerMetadataEventThread] "

    private def process(event: BrokerMetadataEvent): Unit = {
      processors.foreach(_.process(event))
    }

    override def doWork(): Unit = {
      val dequeued = queue.poll(eventQueueTimeoutMs, TimeUnit.MILLISECONDS)
      if (dequeued == null) {
        // Clear the histogram after a timeout without any activity. This
        // ensures that the histogram does not continue to report stale telemetry.
        eventQueueTimeHist.clear()
        return
      }

      val currentTimeMs = time.milliseconds()
      eventQueueTimeHist.update(math.max(0, currentTimeMs - dequeued.enqueueTimeMs))

      trace(s"Processing event ${dequeued.event}")
      dequeued.event match {
        case ShutdownEvent =>
          // Do nothing since we are shutting down

        case logEvent: MetadataLogEvent =>
          if (logEvent.lastOffset < _currentMetadataOffset + logEvent.apiMessages.size) {
            throw new IllegalStateException(s"Non-monotonic offset found in $logEvent. Our " +
              s"current offset is ${_currentMetadataOffset}.")
          }

          process(logEvent)
          _currentMetadataOffset = logEvent.lastOffset

        case event =>
          process(event)
      }
    }
  }

}
