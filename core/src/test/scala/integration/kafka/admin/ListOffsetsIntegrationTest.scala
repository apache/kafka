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
import kafka.utils.TestUtils
import kafka.utils.TestUtils.{createProducer, plaintextBootstrapServers, tempDir, waitUntilTrue}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.requests.ListOffsetsResponse
import org.apache.kafka.common.utils.{MockTime, Time, Utils}
import org.apache.kafka.server.config.ServerLogConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.File
import java.util.{Optional, Properties}
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class ListOffsetsIntegrationTest extends KafkaServerTestHarness {

  private val topicName = "foo"
  private val topicNameWithCustomConfigs = "foo2"
  private var adminClient: Admin = _
  private val mockTime: Time = new MockTime(1)
  private val dataFolder = Seq(tempDir().getAbsolutePath, tempDir().getAbsolutePath)

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
    Utils.closeQuietly(adminClient, "ListOffsetsAdminClient")
    super.tearDown()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testListMaxTimestampWithEmptyLog(quorum: String): Unit = {
    val maxTimestampOffset = runFetchOffsets(adminClient, OffsetSpec.maxTimestamp(), topicName)
    assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, maxTimestampOffset.offset())
    assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, maxTimestampOffset.timestamp())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testThreeCompressedRecordsInOneBatch(quorum: String): Unit = {
    produceMessagesInOneBatch("gzip")
    verifyListOffsets()

    // test LogAppendTime case
    setUpForLogAppendTimeCase()
    produceMessagesInOneBatch("gzip", topicNameWithCustomConfigs)
    // In LogAppendTime's case, the maxTimestampOffset should be the first message of the batch.
    // So in this one batch test, it'll be the first offset 0
    verifyListOffsets(topic = topicNameWithCustomConfigs, 0)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testThreeNonCompressedRecordsInOneBatch(quorum: String): Unit = {
    produceMessagesInOneBatch()
    verifyListOffsets()

    // test LogAppendTime case
    setUpForLogAppendTimeCase()
    produceMessagesInOneBatch(topic=topicNameWithCustomConfigs)
    // In LogAppendTime's case, if the timestamps are the same, we choose the offset of the first record
    // thus, the maxTimestampOffset should be the first record of the batch.
    // So in this one batch test, it'll be the first offset which is 0
    verifyListOffsets(topic = topicNameWithCustomConfigs, 0)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testThreeNonCompressedRecordsInSeparateBatch(quorum: String): Unit = {
    produceMessagesInSeparateBatch()
    verifyListOffsets()

    // test LogAppendTime case
    setUpForLogAppendTimeCase()
    produceMessagesInSeparateBatch(topic = topicNameWithCustomConfigs)
    // In LogAppendTime's case, if the timestamp is different, it should be the last one
    verifyListOffsets(topic = topicNameWithCustomConfigs, 2)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testThreeRecordsInOneBatchHavingDifferentCompressionTypeWithServer(quorum: String): Unit = {
    val props: Properties = new Properties()
    props.setProperty(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    createTopicWithConfig(topicNameWithCustomConfigs, props)
    produceMessagesInOneBatch(topic = topicNameWithCustomConfigs)
    verifyListOffsets(topic = topicNameWithCustomConfigs)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testThreeRecordsInSeparateBatchHavingDifferentCompressionTypeWithServer(quorum: String): Unit = {
    val props: Properties = new Properties()
    props.setProperty(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    createTopicWithConfig(topicNameWithCustomConfigs, props)
    produceMessagesInSeparateBatch(topic = topicNameWithCustomConfigs)
    verifyListOffsets(topic = topicNameWithCustomConfigs)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testThreeCompressedRecordsInSeparateBatch(quorum: String): Unit = {
    produceMessagesInSeparateBatch("gzip")
    verifyListOffsets()

    // test LogAppendTime case
    setUpForLogAppendTimeCase()
    produceMessagesInSeparateBatch("gzip", topicNameWithCustomConfigs)
    // In LogAppendTime's case, the maxTimestampOffset is the message in the last batch since we advance the time
    // for each batch, So it'll be the last offset 2
    verifyListOffsets(topic = topicNameWithCustomConfigs, 2)
  }

  private def setUpForLogAppendTimeCase(): Unit = {
    val props: Properties = new Properties()
    props.setProperty(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
    createTopicWithConfig(topicNameWithCustomConfigs, props)
  }

  private def createTopicWithConfig(topic: String, props: Properties): Unit = {
    createTopic(topic, 1, 1.toShort, topicConfig = props)
  }

  private def verifyListOffsets(topic: String = topicName, expectedMaxTimestampOffset: Int = 1): Unit = {
    def check(): Unit = {
      val earliestOffset = runFetchOffsets(adminClient, OffsetSpec.earliest(), topic)
      assertEquals(0, earliestOffset.offset())

      val latestOffset = runFetchOffsets(adminClient, OffsetSpec.latest(), topic)
      assertEquals(3, latestOffset.offset())

      val maxTimestampOffset = runFetchOffsets(adminClient, OffsetSpec.maxTimestamp(), topic)
      assertEquals(expectedMaxTimestampOffset, maxTimestampOffset.offset())
      // the epoch is related to the returned offset.
      // Hence, it should be zero (the earliest leader epoch), regardless of new leader election
      assertEquals(Optional.of(0), maxTimestampOffset.leaderEpoch())
    }

    // case 0: test the offsets from leader's append path
    check()

    // case 1: test the offsets from follower's append path.
    // we make a follower be the new leader to handle the ListOffsetRequest
    def leader(): Int = adminClient.describeTopics(java.util.Collections.singletonList(topic))
      .allTopicNames().get().get(topic).partitions().get(0).leader().id()

    val previousLeader = leader()
    val newLeader = brokers.map(_.config.brokerId).find(_ != previousLeader).get

    // change the leader to new one
    adminClient.alterPartitionReassignments(java.util.Collections.singletonMap(new TopicPartition(topic, 0),
      Optional.of(new NewPartitionReassignment(java.util.Arrays.asList(newLeader))))).all().get()
    // wait for all reassignments get completed
    waitUntilTrue(() => adminClient.listPartitionReassignments().reassignments().get().isEmpty,
      s"There still are ongoing reassignments")
    // make sure we are able to see the new leader
    var lastLeader = -1
    TestUtils.waitUntilTrue(() => {
      lastLeader = leader()
      lastLeader == newLeader
    }, s"expected leader: $newLeader but actual: $lastLeader")
    check()

    // case 2: test the offsets from recovery path.
    // server will rebuild offset index according to log files if the index files are nonexistent
    val indexFiles = brokers.flatMap(_.config.logDirs).toSet
    brokers.foreach(b => killBroker(b.config.brokerId))
    indexFiles.foreach { root =>
      val files = new File(s"$root/$topic-0").listFiles()
      if (files != null) files.foreach { f =>
        if (f.getName.endsWith(".index")) f.delete()
      }
    }
    restartDeadBrokers()
    Utils.closeQuietly(adminClient, "ListOffsetsAdminClient")
    adminClient = Admin.create(java.util.Collections.singletonMap(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers().asInstanceOf[Object]))
    check()
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
    TestUtils.createBrokerConfigs(2).zipWithIndex.map{ case (props, index) =>
     // We use mock timer so the records can get removed if the test env is too busy to complete
     // tests before kafka-log-retention. Hence, we disable the retention to avoid failed tests
     props.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "-1")
     props.setProperty(ServerLogConfigs.LOG_DIR_CONFIG, dataFolder(index))
      props
    }.map(KafkaConfig.fromProps)
  }
}

