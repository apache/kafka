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
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.TimeUnit

import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{KafkaConfig, MetadataCache, QuotaFactory, ReplicaManager}
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.Time

import scala.collection.mutable.ListBuffer

object BrokerMetadataListener {
  val ThreadNamePrefix = "broker-"
  val ThreadNameSuffix = "-metadata-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
  val ErrorCountMetricName = "ErrorCount"

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
sealed trait BrokerMetadataEvent {}

final case object StartupEvent extends BrokerMetadataEvent

/**
 * A batch of messages from the metadata log
 *
 * @param apiMessages the batch of messages
 * @param lastMetadataOffset the metadata offset of the last message in the batch
 */
final case class MetadataLogEvent(apiMessages: List[ApiMessage], lastMetadataOffset: Long) extends BrokerMetadataEvent

/**
 * A register-broker event that occurs when the broker heartbeat receives a successful registration response.
 * It will only occur once in the lifetime of the broker process, and it will occur before
 * any metadata log message batches appear.  The listener injects this event into the event stream,
 * and processors will receive it immediately.
 *
 * @param brokerEpoch the epoch assigned to the broker by the active controller
 */
final case class OutOfBandRegisterLocalBrokerEvent(brokerEpoch: Long) extends BrokerMetadataEvent

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
final case class OutOfBandFenceLocalBrokerEvent(brokerEpoch: Long) extends BrokerMetadataEvent

/*
 * WakeupEvent is necessary for the case when the event thread is blocked in queue.take() and we need to wake it up.
 * This event has no other semantic meaning and should be ignored when seen.  It is useful in two specific situations:
 * 1) when an out-of-band message is available; and 2) when shutdown needs to occur.  The presence of this message
 * ensures these other messages are seen quickly when no other messages are in the queue.
 */
final case object WakeupEvent extends BrokerMetadataEvent

class QueuedEvent(val event: BrokerMetadataEvent, val enqueueTimeNanos: Long) {
  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeNanos=$enqueueTimeNanos)"
  }
}

trait BrokerMetadataProcessor {
  def processStartup(): Unit
  def process(metadataLogEvent: MetadataLogEvent): Unit
  def process(outOfBandRegisterLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent): Unit
  def process(outOfBandFenceLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent): Unit
}

class BrokerMetadataListener(config: KafkaConfig,
                             time: Time,
                             processors: List[BrokerMetadataProcessor],
                             eventQueueTimeHistogramTimeoutMs: Long = 300000) extends KafkaMetricsGroup {
  if (processors.isEmpty) {
    throw new IllegalArgumentException(s"Empty processors list!")
  }

  private val deque = new util.LinkedList[QueuedEvent]
  private val dequeLock = new ReentrantLock()
  private val dequeNonEmptyCondition = dequeLock.newCondition()

  @volatile private var dequeSize: Int = 0
  @volatile private var errorCount: Int = 0

  private val thread = new BrokerMetadataEventThread(
    s"${BrokerMetadataListener.ThreadNamePrefix}${config.brokerId}${BrokerMetadataListener.ThreadNameSuffix}")

  // metrics
  private val eventQueueTimeHist = newHistogram(BrokerMetadataListener.EventQueueTimeMetricName)
  newGauge(BrokerMetadataListener.EventQueueSizeMetricName, () => dequeSize)
  newGauge(BrokerMetadataListener.ErrorCountMetricName, () => errorCount)

  @volatile private var _currentMetadataOffset: Long = -1

  def start(): Unit = {
    put(StartupEvent)
    thread.start()
  }

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      put(WakeupEvent) // wake up the thread in case it is blocked on queue.take()
      thread.awaitShutdown()
    } finally {
      removeMetric(BrokerMetadataListener.EventQueueTimeMetricName)
      removeMetric(BrokerMetadataListener.ErrorCountMetricName)
      removeMetric(BrokerMetadataListener.EventQueueSizeMetricName)
    }
  }

  def put(event: BrokerMetadataEvent): QueuedEvent = {
    val queuedEvent = new QueuedEvent(event, time.nanoseconds())
    dequeLock.lock()
    try {
      event match {
        case _: OutOfBandRegisterLocalBrokerEvent |
             _: OutOfBandFenceLocalBrokerEvent =>
          // we have to check to see if any out-of-band events are already at the front of the deque;
          // if so, then we need to add this event after those
          val listBuffer: ListBuffer[QueuedEvent] = new ListBuffer()
          var nextEvent = deque.peekFirst()
          while (nextEvent != null && {
            nextEvent.event match {
              case _: OutOfBandRegisterLocalBrokerEvent |
                   _: OutOfBandFenceLocalBrokerEvent => true
              case _ => false
            }}) {
            listBuffer += deque.pollFirst()
            nextEvent = deque.peekFirst()
          }
          // we've removed any out-of-band events that were there
          // so now add the one we want to add
          deque.addFirst(queuedEvent)
          // and then put back any out-of-band events that were there before (keeping their order unchanged)
          listBuffer.reverseIterator.foreach(eventAlreadyThere => deque.addFirst(eventAlreadyThere))
        case _ =>
          // add this non-out-of-band message to the end of the deque
          deque.addLast(queuedEvent)
      }
      dequeNonEmptyCondition.signal()
      dequeSize += 1
    } finally {
      dequeLock.unlock()
    }
    queuedEvent
  }

  def currentMetadataOffset(): Long = _currentMetadataOffset

  private class BrokerMetadataEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[BrokerMetadataEventThread] "

    override def doWork(): Unit = {
      val dequeued: QueuedEvent = pollFromEventQueue()
      try {
        dequeued.event match {
          case StartupEvent =>
            processors.foreach(processor =>
              try {
                processor.processStartup()
              } catch {
                case e: FatalExitError => throw e
                case e: Exception =>
                  error(s"Uncaught error when processor $processor processed startup event", e)
                  errorCount += 1
              })
          case WakeupEvent => // Ignore since it serves solely to wake us up
          case metadataLogEvent: MetadataLogEvent =>
            eventQueueTimeHist.update(TimeUnit.MILLISECONDS.convert(time.nanoseconds() - dequeued.enqueueTimeNanos, TimeUnit.NANOSECONDS))

            val currentOffset = currentMetadataOffset()
            if (metadataLogEvent.lastMetadataOffset < currentOffset + metadataLogEvent.apiMessages.size) {
              error(s"Metadata offset of last message in batch of size ${metadataLogEvent.apiMessages.size}" +
                s" is too small for current metadata offset $currentOffset: ${metadataLogEvent.lastMetadataOffset}")
              errorCount += 1
            } else {
              // Give each processor an opportunity to process the batch.

              // We could introduce queues, run each processor in a separate thread, and pipeline message batches to them.
              // If we pipeline, then we would probably add a CompletableFuture to the process() method
              // and we would have a separate thread/queue waiting on each batch's futures to complete before applying the
              // corresponding metadata offset.
              processors.foreach(processor =>
                try {
                  processor.process(metadataLogEvent) // synchronous, see above for an alternative
                } catch {
                  case e: FatalExitError => throw e
                  case e: Exception =>
                    error(s"Uncaught error when processor $processor processed metadata log event: $metadataLogEvent", e)
                    errorCount += 1
                })
              // set the new offset now that all processors have processed the metadata log event
              if (isTraceEnabled) {
                trace(s"Setting current metadata offset to ${metadataLogEvent.lastMetadataOffset} after processing metadata log messages: ${metadataLogEvent.apiMessages}")
              }
              _currentMetadataOffset = metadataLogEvent.lastMetadataOffset
            }
          case outOfBandRegisterLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent =>
            processors.foreach(processor =>
              try {
                processor.process(outOfBandRegisterLocalBrokerEvent)
              } catch {
                case e: FatalExitError => throw e
                case e: Exception =>
                  error(s"Uncaught error when processor $processor processed out-of-band register-broker event: $outOfBandRegisterLocalBrokerEvent", e)
                  errorCount += 1
              })
          case outOfBandFenceLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent =>
            processors.foreach(processor =>
              try {
                processor.process(outOfBandFenceLocalBrokerEvent)
              } catch {
                case e: FatalExitError => throw e
                case e: Exception =>
                  error(s"Uncaught error when processor $processor processed out-of-band fence-broker event: $outOfBandFenceLocalBrokerEvent", e)
                  errorCount += 1
              })
        }
      } catch {
        case e: FatalExitError => throw e
        case e: Exception =>
          error(s"Uncaught error when processing dequeued event: $dequeued", e)
          errorCount += 1
      }
    }

    private def pollFromEventQueue(): QueuedEvent = {
      dequeLock.lock()
      try {
        var nanosRemaining = TimeUnit.NANOSECONDS.convert(eventQueueTimeHistogramTimeoutMs, TimeUnit.MILLISECONDS)
        while (deque.isEmpty()) { // takes care of spurious wakeups
          val hasRecordedValue = eventQueueTimeHist.count() > 0
          if (!hasRecordedValue) {
            // wait indefinitely for something to appear
            dequeNonEmptyCondition.await()
          } else {
            if (nanosRemaining <= 0) {
              // no time left, so clear the histogram and wait indefinitely
              eventQueueTimeHist.clear()
              dequeNonEmptyCondition.await()
            } else {
              // wait only up until the timeout so we can clear the histogram if nothing appears
              nanosRemaining = dequeNonEmptyCondition.awaitNanos(nanosRemaining)
            }
          }
        }
        dequeSize -= 1
        deque.poll()
      } finally {
        dequeLock.unlock()
      }
    }
  }
}
