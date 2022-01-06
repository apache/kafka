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


import com.yammer.metrics.core.Gauge
import kafka.cluster.BrokerEndPoint
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.utils.Time

import java.util.concurrent.TimeUnit
import scala.collection.Map

trait FetcherEventProcessor {
  def process(event: FetcherEvent)
  def fetcherStats: AsyncFetcherStats
  def fetcherLagStats : AsyncFetcherLagStats
  def sourceBroker: BrokerEndPoint
  def close(): Unit
}

object FetcherEventManager {
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
  val ScheduledEventQueueSizeMetricName = "ScheduledEventQueueSize"
}

/**
 * The FetcherEventManager can spawn a FetcherEventThread, whose main job is to take events from a
 * FetcherEventBus and executes them in a FetcherEventProcessor.
 * @param name
 * @param fetcherEventBus
 * @param processor
 * @param time
 */
class FetcherEventManager(name: String,
                          val fetcherEventBus: FetcherEventBus,
                          processor: FetcherEventProcessor,
                          time: Time) extends KafkaMetricsGroup {

  import FetcherEventManager._

  val rateAndTimeMetrics: Map[FetcherState, KafkaTimer] = FetcherState.values.flatMap { state =>
    state.rateAndTimeMetricName.map { metricName =>
      state -> new KafkaTimer(newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
    }
  }.toMap

  @volatile private var _state: FetcherState = FetcherState.Idle
  private[server] val thread = new FetcherEventThread(name)

  def fetcherStats: AsyncFetcherStats = processor.fetcherStats
  def fetcherLagStats : AsyncFetcherLagStats = processor.fetcherLagStats
  def sourceBroker: BrokerEndPoint = processor.sourceBroker
  def isThreadFailed: Boolean = thread.isThreadFailed

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(
    EventQueueSizeMetricName,
    new Gauge[Int] {
      def value: Int = {
        fetcherEventBus.eventQueueSize()
      }
    }
  )

  newGauge(
    ScheduledEventQueueSizeMetricName,
    new Gauge[Int] {
      def value: Int = {
        fetcherEventBus.scheduledEventQueueSize()
      }
    }
  )

  def state: FetcherState = _state

  def start(): Unit = {
    fetcherEventBus.put(TruncateAndFetch)
    thread.start()
  }

  def modifyPartitionsAndGetCount(partitionModifications: PartitionModifications): KafkaFuture[Int] = {
    val future = new KafkaFutureImpl[Int]{}
    fetcherEventBus.put(ModifyPartitionsAndGetCount(partitionModifications.partitionsToRemove,
      partitionModifications.partitionsToMakeFollowerWithOffsetAndEpoch, future))
    future
  }

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      fetcherEventBus.close()
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
      removeMetric(ScheduledEventQueueSizeMetricName)
    }

    processor.close()
  }


  class FetcherEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[FetcherEventThread fetcherId=$name] "


    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception
     */
    override def doWork(): Unit = {
      val nextEvent = fetcherEventBus.getNextEvent()
      if (nextEvent == null) {
        // a null value will be returned when the fetcherEventBus has started shutting down
        return
      }

      val fetcherEvent = nextEvent.event
      _state = fetcherEvent.state
      eventQueueTimeHist.update(time.milliseconds() - nextEvent.enqueueTimeMs)

      try {
        rateAndTimeMetrics(state).time {
          processor.process(fetcherEvent)
        }
      } catch {
        case e: Exception => error(s"Uncaught error processing event $fetcherEvent", e)
      }

      _state = FetcherState.Idle
    }
  }
}
