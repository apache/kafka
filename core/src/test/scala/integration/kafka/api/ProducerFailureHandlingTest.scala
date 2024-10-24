/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.api

import java.util.concurrent.ExecutionException
import java.util.Properties
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.{DefaultRecord, DefaultRecordBatch}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.{ServerConfigs, ReplicationConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{MethodSource, ValueSource}

class ProducerFailureHandlingTest extends KafkaServerTestHarness {
  private val producerBufferSize = 30000
  private val serverMessageMaxBytes =  producerBufferSize/2
  private val replicaFetchMaxPartitionBytes = serverMessageMaxBytes + 200
  private val replicaFetchMaxResponseBytes = replicaFetchMaxPartitionBytes + 200

  val numServers = 2

  val overridingProps = new Properties()
  overridingProps.put(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, false.toString)
  overridingProps.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, serverMessageMaxBytes.toString)
  overridingProps.put(ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG, replicaFetchMaxPartitionBytes.toString)
  overridingProps.put(ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC, replicaFetchMaxResponseBytes.toString)
  // Set a smaller value for the number of partitions for the offset commit topic (__consumer_offset topic)
  // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
  overridingProps.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, 1.toString)

  def generateConfigs =
    TestUtils.createBrokerConfigs(numServers, zkConnectOrNull, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

  private var producer1: KafkaProducer[Array[Byte], Array[Byte]] = _
  private var producer2: KafkaProducer[Array[Byte], Array[Byte]] = _
  private var producer3: KafkaProducer[Array[Byte], Array[Byte]] = _
  private var producer4: KafkaProducer[Array[Byte], Array[Byte]] = _

  private val topic1 = "topic-1"
  private val topic2 = "topic-2"

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    producer1 = TestUtils.createProducer(bootstrapServers(), acks = 0, retries = 0, requestTimeoutMs = 30000, maxBlockMs = 10000L,
      bufferSize = producerBufferSize)
    producer2 = TestUtils.createProducer(bootstrapServers(), acks = 1, retries = 0, requestTimeoutMs = 30000, maxBlockMs = 10000L,
      bufferSize = producerBufferSize)
    producer3 = TestUtils.createProducer(bootstrapServers(), acks = -1, retries = 0, requestTimeoutMs = 30000, maxBlockMs = 10000L,
      bufferSize = producerBufferSize)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (producer1 != null) producer1.close()
    if (producer2 != null) producer2.close()
    if (producer3 != null) producer3.close()
    if (producer4 != null) producer4.close()

    super.tearDown()
  }

  /**
   * With ack == 0 the future metadata will have no exceptions with offset -1
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testTooLargeRecordWithAckZero(quorum: String): Unit = {
    // create topic
    createTopic(topic1, replicationFactor = numServers)

    // send a too-large record
    val record = new ProducerRecord(topic1, null, "key".getBytes, new Array[Byte](serverMessageMaxBytes + 1))

    val recordMetadata = producer1.send(record).get()
    assertNotNull(recordMetadata)
    assertFalse(recordMetadata.hasOffset)
    assertEquals(-1L, recordMetadata.offset)
  }

  /**
   * With ack == 1 the future metadata will throw ExecutionException caused by RecordTooLargeException
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testTooLargeRecordWithAckOne(quorum: String): Unit = {
    // create topic
    createTopic(topic1, replicationFactor = numServers)

    // send a too-large record
    val record = new ProducerRecord(topic1, null, "key".getBytes, new Array[Byte](serverMessageMaxBytes + 1))
    assertThrows(classOf[ExecutionException], () => producer2.send(record).get)
  }

  private def checkTooLargeRecordForReplicationWithAckAll(maxFetchSize: Int): Unit = {
    val maxMessageSize = maxFetchSize + 100
    val topicConfig = new Properties
    topicConfig.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, numServers.toString)
    topicConfig.setProperty(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageSize.toString)

    // create topic
    val topic10 = "topic10"
    createTopic(topic10, numPartitions = brokers.size, replicationFactor = numServers, topicConfig)

    // send a record that is too large for replication, but within the broker max message limit
    val value = new Array[Byte](maxMessageSize - DefaultRecordBatch.RECORD_BATCH_OVERHEAD - DefaultRecord.MAX_RECORD_OVERHEAD)
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic10, null, value)
    val recordMetadata = producer3.send(record).get

    assertEquals(topic10, recordMetadata.topic)
  }

  /** This should succeed as the replica fetcher thread can handle oversized messages since KIP-74 */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testPartitionTooLargeForReplicationWithAckAll(quorum: String): Unit = {
    checkTooLargeRecordForReplicationWithAckAll(replicaFetchMaxPartitionBytes)
  }

  /** This should succeed as the replica fetcher thread can handle oversized messages since KIP-74 */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testResponseTooLargeForReplicationWithAckAll(quorum: String): Unit = {
    checkTooLargeRecordForReplicationWithAckAll(replicaFetchMaxResponseBytes)
  }

  /**
   * With non-exist-topic the future metadata should return ExecutionException caused by TimeoutException
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testNonExistentTopic(quorum: String): Unit = {
    // send a record with non-exist topic
    val record = new ProducerRecord(topic2, null, "key".getBytes, "value".getBytes)
    assertThrows(classOf[ExecutionException], () => producer1.send(record).get)
  }

  /**
   * With incorrect broker-list the future metadata should return ExecutionException caused by TimeoutException
   *
   * TODO: other exceptions that can be thrown in ExecutionException:
   *    UnknownTopicOrPartitionException
   *    NotLeaderOrFollowerException
   *    LeaderNotAvailableException
   *    CorruptRecordException
   *    TimeoutException
   */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testWrongBrokerList(quorum: String): Unit = {
    // create topic
    createTopic(topic1, replicationFactor = numServers)

    // producer with incorrect broker list
    producer4 = TestUtils.createProducer("localhost:8686,localhost:4242", acks = 1, maxBlockMs = 10000L, bufferSize = producerBufferSize)

    // send a record with incorrect broker list
    val record = new ProducerRecord(topic1, null, "key".getBytes, "value".getBytes)
    assertThrows(classOf[ExecutionException], () => producer4.send(record).get)
  }

  /**
   * Send with invalid partition id should return ExecutionException caused by TimeoutException
   * when partition is higher than the upper bound of partitions.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testInvalidPartition(quorum: String): Unit = {
    // create topic with a single partition
    createTopic(topic1, replicationFactor = numServers)

    // create a record with incorrect partition id (higher than the number of partitions), send should fail
    val higherRecord = new ProducerRecord(topic1, 1, "key".getBytes, "value".getBytes)
    val e = assertThrows(classOf[ExecutionException], () => producer1.send(higherRecord).get)
    assertEquals(classOf[TimeoutException], e.getCause.getClass)
  }

  /**
   * The send call after producer closed should throw IllegalStateException
   */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testSendAfterClosed(quorum: String): Unit = {
    // create topic
    createTopic(topic1, replicationFactor = numServers)

    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic1, null, "key".getBytes, "value".getBytes)

    // first send a message to make sure the metadata is refreshed
    producer1.send(record).get
    producer2.send(record).get
    producer3.send(record).get

    producer1.close()
    assertThrows(classOf[IllegalStateException], () => producer1.send(record))
    producer2.close()
    assertThrows(classOf[IllegalStateException], () => producer2.send(record))
    producer3.close()
    assertThrows(classOf[IllegalStateException], () =>  producer3.send(record))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCannotSendToInternalTopic(quorum: String): Unit = {

    createOffsetsTopic()
    val thrown = assertThrows(classOf[ExecutionException],
      () => producer2.send(new ProducerRecord(Topic.GROUP_METADATA_TOPIC_NAME, "test".getBytes, "test".getBytes)).get)
    assertTrue(thrown.getCause.isInstanceOf[InvalidTopicException], "Unexpected exception while sending to an invalid topic " + thrown.getCause)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testNotEnoughReplicasAfterBrokerShutdown(quorum: String): Unit = {
    val topicName = "minisrtest2"
    val topicProps = new Properties()
    topicProps.put("min.insync.replicas", numServers.toString)

    createTopic(topicName, replicationFactor = numServers, topicConfig = topicProps)

    val record = new ProducerRecord(topicName, null, "key".getBytes, "value".getBytes)
    // this should work with all brokers up and running
    producer3.send(record).get

    // shut down one broker
    brokers.head.shutdown()
    brokers.head.awaitShutdown()
    val e = assertThrows(classOf[ExecutionException], () => producer3.send(record).get)
    assertTrue(e.getCause.isInstanceOf[NotEnoughReplicasException] ||
      e.getCause.isInstanceOf[NotEnoughReplicasAfterAppendException] ||
      e.getCause.isInstanceOf[TimeoutException])

    // restart the server
    brokers.head.startup()
  }

}
