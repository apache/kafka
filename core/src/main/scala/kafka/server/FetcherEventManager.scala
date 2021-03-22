package kafka.server


import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Gauge
import kafka.cluster.BrokerEndPoint
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{KafkaFuture, TopicPartition}

import scala.collection.{Map, Set}

trait FetcherEventProcessor {
  def process(event: FetcherEvent)
  def fetcherStats: FetcherStats
  def fetcherLagStats : FetcherLagStats
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
                          fetcherEventBus: FetcherEventBus,
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

  def fetcherStats: FetcherStats = processor.fetcherStats
  def fetcherLagStats : FetcherLagStats = processor.fetcherLagStats
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

  def addPartitions(initialFetchStates: Map[TopicPartition, InitialFetchState]): KafkaFuture[Void] = {
    val future = new KafkaFutureImpl[Void] {}
    fetcherEventBus.put(AddPartitions(initialFetchStates, future))
    future
  }

  def removePartitions(topicPartitions: Set[TopicPartition]): KafkaFuture[Void] = {
    val future = new KafkaFutureImpl[Void] {}
    fetcherEventBus.put(RemovePartitions(topicPartitions, future))
    future
  }

  def getPartitionsCount(): KafkaFuture[Int] = {
    val future = new KafkaFutureImpl[Int]{}
    fetcherEventBus.put(GetPartitionCount(future))
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
