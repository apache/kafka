package unit.kafka.server

import java.util
import java.util.Properties

import kafka.log.LogConfig
import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.Assert._
import org.junit.Test

class FetchRequestDownConversionConfigTest extends BaseRequestTest {
  private var producer: KafkaProducer[String, String] = null
  override def numBrokers: Int = 1

  override def tearDown(): Unit = {
    if (producer != null)
      producer.close()
    super.tearDown()
  }

  override protected def propertyOverrides(properties: Properties): Unit = {
    super.propertyOverrides(properties)
    properties.put(KafkaConfig.LogMessageDownConversionEnableProp, "false")
  }

  private def initProducer(): Unit = {
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 5, keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
  }

  private def createTopics(numTopics: Int, numPartitions: Int,
                           configs: Map[String, String] = Map.empty, topicSuffixStart: Int = 0): Map[TopicPartition, Int] = {
    val topics = (0 until numTopics).map(t => s"topic${t + topicSuffixStart}")
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, 1.toString)
    configs.foreach { case (k, v) => topicConfig.setProperty(k, v) }
    topics.flatMap { topic =>
      val partitionToLeader = createTopic(topic, numPartitions = numPartitions, replicationFactor = 1,
        topicConfig = topicConfig)
      partitionToLeader.map { case (partition, leader) => new TopicPartition(topic, partition) -> leader }
    }.toMap
  }

  private def createPartitionMap(maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 offsetMap: Map[TopicPartition, Long] = Map.empty): util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData] = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    topicPartitions.foreach { tp =>
      partitionMap.put(tp, new FetchRequest.PartitionData(offsetMap.getOrElse(tp, 0), 0L, maxPartitionBytes))
    }
    partitionMap
  }

  private def sendFetchRequest(leaderId: Int, request: FetchRequest): FetchResponse[MemoryRecords] = {
    val response = connectAndSend(request, ApiKeys.FETCH, destination = brokerSocketServer(leaderId))
    FetchResponse.parse(response, request.version)
  }

  @Test
  def testV1FetchWithDownConversionDisabled(): Unit = {
    initProducer()
    val topicMap = createTopics(numTopics = 5, numPartitions = 1)
    val topics = topicMap.keySet.toSeq
    topics.foreach(topic => producer.send(new ProducerRecord(topic.topic(), "key", "value")))
    val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(1024,
      topics)).build(1)
    val fetchResponse = sendFetchRequest(topicMap.head._2, fetchRequest)
    topics.foreach(tp => assertEquals(Errors.UNSUPPORTED_VERSION, fetchResponse.responseData().get(tp).error))
  }

  @Test
  def testLatestFetchWithDownConversionDisabled(): Unit = {
    initProducer()
    val topicMap = createTopics(numTopics = 5, numPartitions = 1)
    val topics = topicMap.keySet.toSeq
    topics.foreach(topic => producer.send(new ProducerRecord(topic.topic(), "key", "value")))
    val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(1024,
      topics)).build()
    val fetchResponse = sendFetchRequest(topicMap.head._2, fetchRequest)
    topics.foreach(tp => assertEquals(Errors.NONE, fetchResponse.responseData().get(tp).error))
  }

  @Test
  def testV1FetchWithTopicLevelOverrides(): Unit = {
    initProducer()

    // create topics with default down-conversion configuration (i.e. conversion disabled)
    val conversionDisabledTopicsMap = createTopics(numTopics = 5, numPartitions = 1, topicSuffixStart = 0)
    val conversionDisabledTopics = conversionDisabledTopicsMap.keySet.toSeq

    // create topics with down-conversion configuration enabled
    val topicConfig = Map(LogConfig.MessageDownConversionEnableProp -> "true")
    val conversionEnabledTopicsMap = createTopics(numTopics = 5, numPartitions = 1, topicConfig, topicSuffixStart = 5)
    val conversionEnabledTopics = conversionEnabledTopicsMap.keySet.toSeq

    val allTopics = conversionDisabledTopics ++ conversionEnabledTopics
    val leaderId = conversionDisabledTopicsMap.head._2

    allTopics.foreach(topic => producer.send(new ProducerRecord(topic.topic(), "key", "value")))
    val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(1024,
      allTopics)).build(1)
    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)

    conversionDisabledTopics.foreach(tp => assertEquals(Errors.UNSUPPORTED_VERSION, fetchResponse.responseData().get(tp).error))
    conversionEnabledTopics.foreach(tp => assertEquals(Errors.NONE, fetchResponse.responseData().get(tp).error))
  }

  @Test
  def testV1FetchFromReplica(): Unit = {
    initProducer()

    // create topics with default down-conversion configuration (i.e. conversion disabled)
    val conversionDisabledTopicsMap = createTopics(numTopics = 5, numPartitions = 1, topicSuffixStart = 0)
    val conversionDisabledTopics = conversionDisabledTopicsMap.keySet.toSeq

    // create topics with down-conversion configuration enabled
    val topicConfig = Map(LogConfig.MessageDownConversionEnableProp -> "true")
    val conversionEnabledTopicsMap = createTopics(numTopics = 5, numPartitions = 1, topicConfig, topicSuffixStart = 5)
    val conversionEnabledTopics = conversionEnabledTopicsMap.keySet.toSeq

    val allTopics = conversionDisabledTopics ++ conversionEnabledTopics
    val leaderId = conversionDisabledTopicsMap.head._2

    allTopics.foreach(topic => producer.send(new ProducerRecord(topic.topic(), "key", "value")))
    val fetchRequest = FetchRequest.Builder.forReplica(1, 1, Int.MaxValue, 0, createPartitionMap(1024, allTopics)).build();
    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)

    allTopics.foreach(tp => assertEquals(Errors.NONE, fetchResponse.responseData().get(tp).error))
  }
}
