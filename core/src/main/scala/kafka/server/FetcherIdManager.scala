package kafka.server

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

object FetcherIdManager {
  val tpFetcherIdMap = new mutable.HashMap[TopicPartition, Int]
  val brokerAndLastComputedFetcherIdMap = new mutable.HashMap[BrokerEndPoint, Int]
}
