/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils.{createProducer, plaintextBootstrapServers}
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.utils.{MockTime, Time, Utils}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class ListOffsetsIntegrationTest extends KafkaServerTestHarness {

  val topicName = "foo"
  val topicNameWithCustomConfigs = "foo2"
  var adminClient: Admin = _
  var setOldMessageFormat: Boolean = false
  val mockTime: Time = new MockTime(1)

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopicWithConfig(topicName, new Properties())
    adminClient = Admin.create(Map[String, Object](
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers()
    ).asJava)
  }

  override def brokerTime(brokerId: Int): Time = mockTime

  @AfterEach
  override def tearDown(): Unit = {
    setOldMessageFormat = false
    Utils.closeQuietly(adminClient, "ListOffsetsAdminClient")
    super.tearDown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testThreeCompressedRecordsInOneBatch(quorum: String): Unit = {
    produceMessagesInOneBatch("gzip")
    verifyListOffsets()

    // test LogAppendTime case
    val props: Properties = new Properties()
    props.setProperty(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
    createTopicWithConfig(topicNameWithCustomConfigs, props)
    produceMessagesInOneBatch("gzip", topicNameWithCustomConfigs)
    // In LogAppendTime's case, the maxTimestampOffset should be the first message of the batch.
    // So in this one batch test, it'll be the first offset 0
    verifyListOffsets(topic = topicNameWithCustomConfigs, 0)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testThreeRecordsInSeparateBatch(quorum: String): Unit = {
    produceMessagesInSeparateBatch()
    verifyListOffsets()
  }

  // The message conversion test only run in ZK mode because KRaft mode doesn't support "inter.broker.protocol.version" < 3.0
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testThreeRecordsInOneBatchWithMessageConversion(quorum: String): Unit = {
    createOldMessageFormatBrokers()
    produceMessagesInOneBatch()
    verifyListOffsets()

    // test LogAppendTime case
    val props: Properties = new Properties()
    props.setProperty(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
    createTopicWithConfig(topicNameWithCustomConfigs, props)
    produceMessagesInOneBatch(topic = topicNameWithCustomConfigs)
    // In LogAppendTime's case, the maxTimestampOffset should be the first message of the batch.
    // So in this one batch test, it'll be the first offset 0
    verifyListOffsets(topic = topicNameWithCustomConfigs, 0)
  }

  // The message conversion test only run in ZK mode because KRaft mode doesn't support "inter.broker.protocol.version" < 3.0
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testThreeRecordsInSeparateBatchWithMessageConversion(quorum: String): Unit = {
    createOldMessageFormatBrokers()
    produceMessagesInSeparateBatch()
    verifyListOffsets()

    // test LogAppendTime case
    val props: Properties = new Properties()
    props.setProperty(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
    createTopicWithConfig(topicNameWithCustomConfigs, props)
    produceMessagesInSeparateBatch(topic = topicNameWithCustomConfigs)
    // In LogAppendTime's case, the maxTimestampOffset should be the first message of the batch.
    // So in this separate batch test, it'll be the last offset 2
    verifyListOffsets(topic = topicNameWithCustomConfigs, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testThreeRecordsInOneBatchHavingDifferentCompressionTypeWithServer(quorum: String): Unit = {
    val props: Properties = new Properties()
    props.setProperty(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    createTopicWithConfig(topicNameWithCustomConfigs, props)
    produceMessagesInOneBatch(topic = topicNameWithCustomConfigs)
    verifyListOffsets(topic = topicNameWithCustomConfigs)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testThreeRecordsInSeparateBatchHavingDifferentCompressionTypeWithServer(quorum: String): Unit = {
    val props: Properties = new Properties()
    props.setProperty(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    createTopicWithConfig(topicNameWithCustomConfigs, props)
    produceMessagesInSeparateBatch(topic = topicNameWithCustomConfigs)
    verifyListOffsets(topic = topicNameWithCustomConfigs)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testThreeCompressedRecordsInSeparateBatch(quorum: String): Unit = {
    produceMessagesInSeparateBatch("gzip")
    verifyListOffsets()

    // test LogAppendTime case
    val props: Properties = new Properties()
    props.setProperty(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
    createTopicWithConfig(topicNameWithCustomConfigs, props)
    produceMessagesInSeparateBatch("gzip", topicNameWithCustomConfigs)
    // In LogAppendTime's case, the maxTimestampOffset should be the first message of the batch.
    // So in this separate batch test, it'll be the last offset 2
    verifyListOffsets(topic = topicNameWithCustomConfigs, 2)
  }

  private def createOldMessageFormatBrokers(): Unit = {
    setOldMessageFormat = true
    recreateBrokers(reconfigure = true, startup = true)
    Utils.closeQuietly(adminClient, "ListOffsetsAdminClient")
    adminClient = Admin.create(Map[String, Object](
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers()
    ).asJava)
  }

  private def createTopicWithConfig(topic: String, props: Properties): Unit = {
    createTopic(topic, 1, 1.toShort, topicConfig = props)
  }

  private def verifyListOffsets(topic: String = topicName, expectedMaxTimestampOffset: Int = 1): Unit = {
    val earliestOffset = runFetchOffsets(adminClient, OffsetSpec.earliest(), topic)
    assertEquals(0, earliestOffset.offset())

    val latestOffset = runFetchOffsets(adminClient, OffsetSpec.latest(), topic)
    assertEquals(3, latestOffset.offset())

    val maxTimestampOffset = runFetchOffsets(adminClient, OffsetSpec.maxTimestamp(), topic)
    assertEquals(expectedMaxTimestampOffset, maxTimestampOffset.offset())
  }

  private def runFetchOffsets(adminClient: Admin,
                              offsetSpec: OffsetSpec,
                              topic: String): ListOffsetsResult.ListOffsetsResultInfo = {
    val tp = new TopicPartition(topic, 0)
    adminClient.listOffsets(Map(
      tp -> offsetSpec
    ).asJava, new ListOffsetsOptions()).all().get().get(tp)
  }

  private def produceMessagesInOneBatch(compressionType: String = "none", topic: String = topicName): Unit = {
    val records = Seq(
      new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, 100L,
        null, new Array[Byte](10)),
      new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, 999L,
        null, new Array[Byte](10)),
      new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, 200L,
        null, new Array[Byte](10)),
    )
    // create a producer with large linger.ms and enough batch.size (default is enough for three 10 bytes records),
    // so that we can confirm all records will be accumulated in producer until we flush them into one batch.
    val producer = createProducer(
      plaintextBootstrapServers(brokers),
      deliveryTimeoutMs = Int.MaxValue,
      lingerMs = Int.MaxValue,
      compressionType = compressionType)

    try {
      val futures = records.map(producer.send)
      producer.flush()
      futures.foreach(_.get)
    } finally {
      producer.close()
    }
  }

  private def produceMessagesInSeparateBatch(compressionType: String = "none", topic: String = topicName): Unit = {
    val records = Seq(new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, 100L,
        null, new Array[Byte](10)))
    val records2 = Seq(new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, 999L,
      null, new Array[Byte](10)))
    val records3 = Seq(new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, 200L,
      null, new Array[Byte](10)))

    val producer = createProducer(
      plaintextBootstrapServers(brokers),
      compressionType = compressionType)
    try {
      val futures = records.map(producer.send)
      futures.foreach(_.get)
      // advance the server time after each record sent to make sure the time changed when appendTime is used
      mockTime.sleep(100)
      val futures2 = records2.map(producer.send)
      futures2.foreach(_.get)
      mockTime.sleep(100)
      val futures3 = records3.map(producer.send)
      futures3.foreach(_.get)
    } finally {
      producer.close()
    }
  }

  def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(1, zkConnectOrNull).map{ props =>
      if (setOldMessageFormat) {
        props.setProperty("log.message.format.version", "0.10.0")
        props.setProperty("inter.broker.protocol.version", "0.10.0")
      }
      props
    }.map(KafkaConfig.fromProps)
  }
}

