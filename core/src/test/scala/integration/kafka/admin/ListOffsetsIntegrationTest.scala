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
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class ListOffsetsIntegrationTest extends KafkaServerTestHarness {

  val topicName = "foo"
  var adminClient: Admin = _
  var setOldMessageFormat: Boolean = false

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(topicName, 1, 1.toShort)
    adminClient = Admin.create(Map[String, Object](
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers()
    ).asJava)
  }

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
  }

  // The message conversion test only run in ZK mode because KRaft mode doesn't support "inter.broker.protocol.version" < 3.0
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testThreeRecordsInOneBatchWithMessageConversion(quorum: String): Unit = {
    createOldMessageFormatBrokers()
    produceMessagesInOneBatch()
    verifyListOffsets()
  }

  // The message conversion test only run in ZK mode because KRaft mode doesn't support "inter.broker.protocol.version" < 3.0
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testThreeRecordsInSeparateBatchWithMessageConversion(quorum: String): Unit = {
    createOldMessageFormatBrokers()
    produceMessagesInSeparateBatch()
    verifyListOffsets()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testThreeRecordsInSeparateBatch(quorum: String): Unit = {
    produceMessagesInSeparateBatch()
    verifyListOffsets()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testThreeCompressedRecordsInSeparateBatch(quorum: String): Unit = {
    produceMessagesInSeparateBatch("gzip")
    verifyListOffsets()
  }

  private def createOldMessageFormatBrokers(): Unit = {
    setOldMessageFormat = true
    recreateBrokers(reconfigure = true, startup = true)
    Utils.closeQuietly(adminClient, "ListOffsetsAdminClient")
    adminClient = Admin.create(Map[String, Object](
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers()
    ).asJava)
  }

  private def verifyListOffsets(): Unit = {
    val earliestOffset = runFetchOffsets(adminClient, OffsetSpec.earliest())
    assertEquals(0, earliestOffset.offset())

    val latestOffset = runFetchOffsets(adminClient, OffsetSpec.latest())
    assertEquals(3, latestOffset.offset())

    val maxTimestampOffset = runFetchOffsets(adminClient, OffsetSpec.maxTimestamp())
    assertEquals(1, maxTimestampOffset.offset())
  }

  private def runFetchOffsets(adminClient: Admin,
                              offsetSpec: OffsetSpec,
                              topic: String = topicName): ListOffsetsResult.ListOffsetsResultInfo = {
    val tp = new TopicPartition(topic, 0)
    adminClient.listOffsets(Map(
      tp -> offsetSpec
    ).asJava, new ListOffsetsOptions()).all().get().get(tp)
  }

  def produceMessagesInOneBatch(compressionType: String = "none"): Unit = {
    val records = Seq(
      new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, 100L,
        null, new Array[Byte](10)),
      new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, 999L,
        null, new Array[Byte](10)),
      new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, 200L,
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

  def produceMessagesInSeparateBatch(compressionType: String = "none"): Unit = {
    val records = Seq(new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, 100L,
        null, new Array[Byte](10)))
    val records2 = Seq(new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, 999L,
      null, new Array[Byte](10)))
    val records3 = Seq(new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, 200L,
      null, new Array[Byte](10)))

    val producer = createProducer(
      plaintextBootstrapServers(brokers),
      compressionType = compressionType)
    try {
      val futures = records.map(producer.send)
      futures.foreach(_.get)
      val futures2 = records2.map(producer.send)
      futures2.foreach(_.get)
      val futures3 = records3.map(producer.send)
      futures3.foreach(_.get)
    } finally {
      producer.close()
    }
  }

  def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(1, zkConnectOrNull).map(props => {
      if (setOldMessageFormat) {
        props.setProperty("log.message.format.version", "0.10.0")
        props.setProperty("inter.broker.protocol.version", "0.10.0")
      }
      props
    }).map(KafkaConfig.fromProps)
  }
}

