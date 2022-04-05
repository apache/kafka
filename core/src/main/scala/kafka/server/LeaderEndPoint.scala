package kafka.server

import kafka.api.{KAFKA_0_10_1_IV2, KAFKA_0_11_0_IV0, KAFKA_2_0_IV0, KAFKA_2_0_IV1, KAFKA_2_1_IV1, KAFKA_2_2_IV1, KAFKA_2_3_IV1, KAFKA_2_8_IV0, KAFKA_3_0_IV1}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData}

import scala.collection.Map

trait LeaderEndPoint extends Logging {
  val brokerConfig: KafkaConfig
  val endpoint: BlockingSend = null // setting to null since only RemoteLeaderEndPoint needs to override

  type FetchData = FetchResponseData.PartitionData
  type EpochData = OffsetForLeaderEpochRequestData.OffsetForLeaderPartition

  // Visible for testing
  private[server] val listOffsetRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_3_0_IV1) 7
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_8_IV0) 6
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_2_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2) 1
    else 0

  // Visible for testing
  private[server] val offsetForLeaderEpochRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_8_IV0) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_3_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV0) 1
    else 0

  def initiateClose(): Unit = {}

  def close(): Unit = {}

  def fetch(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData]

  def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long

  def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long

  def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset]
}
