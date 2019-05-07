package kafka.server

import java.util

import kafka.admin.AdminClient
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import org.apache.kafka.common.{NewOffsetMetaData, Node, TopicPartition}
import org.apache.kafka.common.requests.GetStartOffsetResponse

import scala.collection.mutable

class SmartExtendManager (val config: KafkaConfig) extends Logging with KafkaMetricsGroup {
  private val SmartExtendChannel  = AdminClient.createSimplePlaintext(config.bootstarpServers)

  private def getLogTimestamp(partition: Partition, isLst: Boolean): Long = {
    if (isLst) {
      partition.logManager.getLog(partition.topicPartition).get.segments.firstEntry().getValue.log.creationTime()
    } else {
      partition.logManager.getLog(partition.topicPartition).get.segments.lastEntry().getValue.log.file.lastModified()
    }
  }

  def close: Unit = {
    SmartExtendChannel.close()
  }

  def sendRequest(broker: BrokerEndPoint, partitions: mutable.Set[Partition]): util.Map[TopicPartition, GetStartOffsetResponse.StartOffsetResponse] = {
    val partitionOffsetMetaDatas = new util.LinkedHashMap[TopicPartition, NewOffsetMetaData]
    partitions.map{ partition =>
      // local brokerid
      val brokerid:Int = config.brokerId
      val leo:Long = partition.getReplica().get.logEndOffset.messageOffset
      val lso:Long = partition.getReplica().get.logStartOffset
      val lst:Long = getLogTimestamp(partition, true)
      val let:Long = getLogTimestamp(partition, false)
      partitionOffsetMetaDatas.put(partition.topicPartition, new NewOffsetMetaData(brokerid, leo, lst, let ,lso))
    }
    val node = new Node(broker.id, broker.host, broker.port)
    debug("sendRequest broker=" + broker + " partitionOffsetMetaDatas=" + partitionOffsetMetaDatas)
    import scala.collection.JavaConversions.mapAsJavaMap
    SmartExtendChannel.getStartOffset(node, partitionOffsetMetaDatas)
  }
}
