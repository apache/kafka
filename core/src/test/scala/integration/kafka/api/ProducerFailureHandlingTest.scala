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

import java.util.concurrent.{ExecutionException, TimeoutException}
import java.util.Properties

import kafka.common.Topic
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors._
import org.junit.Assert._
import org.junit.{After, Before, Test}

class ProducerFailureHandlingTest extends KafkaServerTestHarness {
  private val producerBufferSize = 30000
  private val serverMessageMaxBytes =  producerBufferSize/2
  private val replicaFetchMaxPartitionBytes = serverMessageMaxBytes + 200
  private val replicaFetchMaxResponseBytes = replicaFetchMaxPartitionBytes + 200

  val numServers = 2

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
  overridingProps.put(KafkaConfig.MessageMaxBytesProp, serverMessageMaxBytes.toString)
  overridingProps.put(KafkaConfig.ReplicaFetchMaxBytesProp, replicaFetchMaxPartitionBytes.toString)
  overridingProps.put(KafkaConfig.ReplicaFetchResponseMaxBytesDoc, replicaFetchMaxResponseBytes.toString)
  // Set a smaller value for the number of partitions for the offset commit topic (__consumer_offset topic)
  // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)

  def generateConfigs() =
    TestUtils.createBrokerConfigs(numServers, zkConnect, false).map(KafkaConfig.fromProps(_, overridingProps))

  private var producer1: KafkaProducer[Array[Byte], Array[Byte]] = null
  private var producer2: KafkaProducer[Array[Byte], Array[Byte]] = null
  private var producer3: KafkaProducer[Array[Byte], Array[Byte]] = null
  private var producer4: KafkaProducer[Array[Byte], Array[Byte]] = null

  private val topic1 = "topic-1"
  private val topic2 = "topic-2"

  @Before
  override def setUp() {
    super.setUp()

    producer1 = TestUtils.createNewProducer(brokerList, acks = 0, requestTimeoutMs = 30000L, maxBlockMs = 10000L,
      bufferSize = producerBufferSize)
    producer2 = TestUtils.createNewProducer(brokerList, acks = 1, requestTimeoutMs = 30000L, maxBlockMs = 10000L,
      bufferSize = producerBufferSize)
    producer3 = TestUtils.createNewProducer(brokerList, acks = -1, requestTimeoutMs = 30000L, maxBlockMs = 10000L,
      bufferSize = producerBufferSize)
  }

  @After
  override def tearDown() {
    if (producer1 != null) producer1.close()
    if (producer2 != null) producer2.close()
    if (producer3 != null) producer3.close()
    if (producer4 != null) producer4.close()

    super.tearDown()
  }

  /**
   * With ack == 0 the future metadata will have no exceptions with offset -1
   */
  @Test
  def testTooLargeRecordWithAckZero() {
    // create topic
    TestUtils.createTopic(zkUtils, topic1, 1, numServers, servers)

    // send a too-large record
    val record = new ProducerRecord(topic1, null, "key".getBytes, new Array[Byte](serverMessageMaxBytes + 1))
    assertEquals("Returned metadata should have offset -1", producer1.send(record).get.offset, -1L)
  }

  /**
   * With ack == 1 the future metadata will throw ExecutionException caused by RecordTooLargeException
   */
  @Test
  def testTooLargeRecordWithAckOne() {
    // create topic
    TestUtils.createTopic(zkUtils, topic1, 1, numServers, servers)

    // send a too-large record
    val record = new ProducerRecord(topic1, null, "key".getBytes, new Array[Byte](serverMessageMaxBytes + 1))
    intercept[ExecutionException] {
      producer2.send(record).get
    }
  }

  private def checkTooLargeRecordForReplicationWithAckAll(maxFetchSize: Int) {
    val maxMessageSize = maxFetchSize + 100
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, numServers.toString)
    topicConfig.setProperty(LogConfig.MaxMessageBytesProp, maxMessageSize.toString)

    // create topic
    val topic10 = "topic10"
    TestUtils.createTopic(zkUtils, topic10, servers.size, numServers, servers, topicConfig)

    // send a record that is too large for replication, but within the broker max message limit
    val record = new ProducerRecord(topic10, null, "key".getBytes, new Array[Byte](maxMessageSize - 50))
    val recordMetadata = producer3.send(record).get

    assertEquals(topic10, recordMetadata.topic())
  }

  /** This should succeed as the replica fetcher thread can handle oversized messages since KIP-74 */
  @Test
  def testPartitionTooLargeForReplicationWithAckAll() {
    checkTooLargeRecordForReplicationWithAckAll(replicaFetchMaxPartitionBytes)
  }

  /** This should succeed as the replica fetcher thread can handle oversized messages since KIP-74 */
  @Test
  def testResponseTooLargeForReplicationWithAckAll() {
    checkTooLargeRecordForReplicationWithAckAll(replicaFetchMaxResponseBytes)
  }

  /**
   * With non-exist-topic the future metadata should return ExecutionException caused by TimeoutException
   */
  @Test
  def testNonExistentTopic() {
    // send a record with non-exist topic
    val record = new ProducerRecord(topic2, null, "key".getBytes, "value".getBytes)
    intercept[ExecutionException] {
      producer1.send(record).get
    }
  }

  /**
   * With incorrect broker-list the future metadata should return ExecutionException caused by TimeoutException
   *
   * TODO: other exceptions that can be thrown in ExecutionException:
   *    UnknownTopicOrPartitionException
   *    NotLeaderForPartitionException
   *    LeaderNotAvailableException
   *    CorruptRecordException
   *    TimeoutException
   */
  @Test
  def testWrongBrokerList() {
    // create topic
    TestUtils.createTopic(zkUtils, topic1, 1, numServers, servers)

    // producer with incorrect broker list
    producer4 = TestUtils.createNewProducer("localhost:8686,localhost:4242", acks = 1, maxBlockMs = 10000L, bufferSize = producerBufferSize)

    // send a record with incorrect broker list
    val record = new ProducerRecord(topic1, null, "key".getBytes, "value".getBytes)
    intercept[ExecutionException] {
      producer4.send(record).get
    }
  }

  /**
    * Send with invalid partition id should throw KafkaException when partition is higher than the
    * upper bound of partitions and IllegalArgumentException when partition is negative
    */
  @Test
  def testInvalidPartition() {
    // create topic with a single partition
    TestUtils.createTopic(zkUtils, topic1, 1, numServers, servers)

    // create a record with incorrect partition id (higher than the number of partitions), send should fail
    val higherRecord = new ProducerRecord[Array[Byte], Array[Byte]](topic1, 1, "key".getBytes, "value".getBytes)
    intercept[KafkaException] {
      producer1.send(higherRecord)
    }
  }

  /**
   * The send call after producer closed should throw KafkaException cased by IllegalStateException
   */
  @Test
  def testSendAfterClosed() {
    // create topic
    TestUtils.createTopic(zkUtils, topic1, 1, numServers, servers)

    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, "key".getBytes, "value".getBytes)

    // first send a message to make sure the metadata is refreshed
    producer1.send(record).get
    producer2.send(record).get
    producer3.send(record).get

    intercept[IllegalStateException] {
      producer1.close()
      producer1.send(record)
    }
    intercept[IllegalStateException] {
      producer2.close()
      producer2.send(record)
    }
    intercept[IllegalStateException] {
      producer3.close()
      producer3.send(record)
    }

    // re-close producer is fine
  }

  @Test
  def testCannotSendToInternalTopic() {
    val thrown = intercept[ExecutionException] {
      producer2.send(new ProducerRecord[Array[Byte],Array[Byte]](Topic.InternalTopics.head, "test".getBytes, "test".getBytes)).get
    }
    assertTrue("Unexpected exception while sending to an invalid topic " + thrown.getCause, thrown.getCause.isInstanceOf[InvalidTopicException])
  }

  @Test
  def testNotEnoughReplicas() {
    val topicName = "minisrtest"
    val topicProps = new Properties()
    topicProps.put("min.insync.replicas",(numServers+1).toString)

    TestUtils.createTopic(zkUtils, topicName, 1, numServers, servers, topicProps)

    val record = new ProducerRecord(topicName, null, "key".getBytes, "value".getBytes)
    try {
      producer3.send(record).get
      fail("Expected exception when producing to topic with fewer brokers than min.insync.replicas")
    } catch {
      case e: ExecutionException =>
        if (!e.getCause.isInstanceOf[NotEnoughReplicasException]) {
          fail("Expected NotEnoughReplicasException when producing to topic with fewer brokers than min.insync.replicas")
        }
    }
  }

  @Test
  def testNotEnoughReplicasAfterBrokerShutdown() {
    val topicName = "minisrtest2"
    val topicProps = new Properties()
    topicProps.put("min.insync.replicas", numServers.toString)

    TestUtils.createTopic(zkUtils, topicName, 1, numServers, servers,topicProps)

    val record = new ProducerRecord(topicName, null, "key".getBytes, "value".getBytes)
    // this should work with all brokers up and running
    producer3.send(record).get

    // shut down one broker
    servers.head.shutdown()
    servers.head.awaitShutdown()
    try {
      producer3.send(record).get
      fail("Expected exception when producing to topic with fewer brokers than min.insync.replicas")
    } catch {
      case e: ExecutionException =>
        if (!e.getCause.isInstanceOf[NotEnoughReplicasException]  &&
            !e.getCause.isInstanceOf[NotEnoughReplicasAfterAppendException] &&
            !e.getCause.isInstanceOf[TimeoutException]) {
          fail("Expected NotEnoughReplicasException or NotEnoughReplicasAfterAppendException when producing to topic " +
            "with fewer brokers than min.insync.replicas, but saw " + e.getCause)
        }
    }

    // restart the server
    servers.head.startup()
  }

}
