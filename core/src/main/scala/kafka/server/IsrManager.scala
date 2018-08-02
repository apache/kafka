package kafka.server

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

object IsrManager {
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L

  def apply(zkClient: KafkaZkClient) =
    new IsrManager(zkClient)
}

class IsrManager(zkClient: KafkaZkClient) extends Logging with KafkaMetricsGroup {

  private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())

  val isrExpandRate = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate = newMeter("FailedIsrUpdatesPerSec", "failedUpdates", TimeUnit.SECONDS)

  def recordIsrChange(topicPartition: TopicPartition) {
    isrChangeSet synchronized {
      isrChangeSet += topicPartition
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }

  /**
    * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
    * 1. There is ISR change not propagated yet.
    * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
    * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
    * other brokers when large amount of ISR change occurs.
    */
  def maybePropagateIsrChanges() {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + IsrManager.IsrChangePropagationBlackOut < now ||
          lastIsrPropagationMs.get() + IsrManager.IsrChangePropagationInterval < now)) {
        zkClient.propagateIsrChanges(isrChangeSet)
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }
}
