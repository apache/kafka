package kafka.server

import org.apache.kafka.common.TopicPartition
import scala.collection.{Map, Set}

trait FetcherManagerTrait {
  def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit
  def removeFetcherForPartitions(partitions: Set[TopicPartition]): Unit
  def shutdownIdleFetcherThreads(): Unit
  def shutdown(): Unit
}
