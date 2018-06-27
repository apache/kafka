/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util
import java.util.Properties

import kafka.log.LogConfig
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

  override def setUp(): Unit = {
    super.setUp()
    initProducer()
  }

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
    producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
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

  /**
   * Tests that fetch request that require down-conversion returns with an error response when down-conversion is disabled on broker.
   */
  @Test
  def testV1FetchWithDownConversionDisabled(): Unit = {
    val topicMap = createTopics(numTopics = 5, numPartitions = 1)
    val topicPartitions = topicMap.keySet.toSeq
    topicPartitions.foreach(tp => producer.send(new ProducerRecord(tp.topic(), "key", "value")).get())
    val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(1024,
      topicPartitions)).build(1)
    val fetchResponse = sendFetchRequest(topicMap.head._2, fetchRequest)
    topicPartitions.foreach(tp => assertEquals(Errors.UNSUPPORTED_VERSION, fetchResponse.responseData().get(tp).error))
  }

  /**
   * Tests that "message.downconversion.enable" has no effect when down-conversion is not required.
   */
  @Test
  def testLatestFetchWithDownConversionDisabled(): Unit = {
    val topicMap = createTopics(numTopics = 5, numPartitions = 1)
    val topicPartitions = topicMap.keySet.toSeq
    topicPartitions.foreach(tp => producer.send(new ProducerRecord(tp.topic(), "key", "value")).get())
    val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(1024,
      topicPartitions)).build()
    val fetchResponse = sendFetchRequest(topicMap.head._2, fetchRequest)
    topicPartitions.foreach(tp => assertEquals(Errors.NONE, fetchResponse.responseData().get(tp).error))
  }

  /**
   * Tests that "message.downconversion.enable" can be set at topic level, and its configuration is obeyed for client
   * fetch requests.
   */
  @Test
  def testV1FetchWithTopicLevelOverrides(): Unit = {
    // create topics with default down-conversion configuration (i.e. conversion disabled)
    val conversionDisabledTopicsMap = createTopics(numTopics = 5, numPartitions = 1, topicSuffixStart = 0)
    val conversionDisabledTopicPartitions = conversionDisabledTopicsMap.keySet.toSeq

    // create topics with down-conversion configuration enabled
    val topicConfig = Map(LogConfig.MessageDownConversionEnableProp -> "true")
    val conversionEnabledTopicsMap = createTopics(numTopics = 5, numPartitions = 1, topicConfig, topicSuffixStart = 5)
    val conversionEnabledTopicPartitions = conversionEnabledTopicsMap.keySet.toSeq

    val allTopics = conversionDisabledTopicPartitions ++ conversionEnabledTopicPartitions
    val leaderId = conversionDisabledTopicsMap.head._2

    allTopics.foreach(tp => producer.send(new ProducerRecord(tp.topic(), "key", "value")).get())
    val fetchRequest = FetchRequest.Builder.forConsumer(Int.MaxValue, 0, createPartitionMap(1024,
      allTopics)).build(1)
    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)

    conversionDisabledTopicPartitions.foreach(tp => assertEquals(Errors.UNSUPPORTED_VERSION, fetchResponse.responseData().get(tp).error))
    conversionEnabledTopicPartitions.foreach(tp => assertEquals(Errors.NONE, fetchResponse.responseData().get(tp).error))
  }

  /**
   * Tests that "message.downconversion.enable" has no effect on fetch requests from replicas.
   */
  @Test
  def testV1FetchFromReplica(): Unit = {
    // create topics with default down-conversion configuration (i.e. conversion disabled)
    val conversionDisabledTopicsMap = createTopics(numTopics = 5, numPartitions = 1, topicSuffixStart = 0)
    val conversionDisabledTopicPartitions = conversionDisabledTopicsMap.keySet.toSeq

    // create topics with down-conversion configuration enabled
    val topicConfig = Map(LogConfig.MessageDownConversionEnableProp -> "true")
    val conversionEnabledTopicsMap = createTopics(numTopics = 5, numPartitions = 1, topicConfig, topicSuffixStart = 5)
    val conversionEnabledTopicPartitions = conversionEnabledTopicsMap.keySet.toSeq

    val allTopicPartitions = conversionDisabledTopicPartitions ++ conversionEnabledTopicPartitions
    val leaderId = conversionDisabledTopicsMap.head._2

    allTopicPartitions.foreach(tp => producer.send(new ProducerRecord(tp.topic(), "key", "value")).get())
    val fetchRequest = FetchRequest.Builder.forReplica(1, 1, Int.MaxValue, 0,
      createPartitionMap(1024, allTopicPartitions)).build()
    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)

    allTopicPartitions.foreach(tp => assertEquals(Errors.NONE, fetchResponse.responseData().get(tp).error))
  }
}
