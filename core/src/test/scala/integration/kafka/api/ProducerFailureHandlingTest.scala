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

import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}
import java.util.{Properties, Random}
import kafka.common.Topic
import kafka.consumer.SimpleConsumer
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{ShutdownableThread, TestUtils}
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.{InvalidTopicException, NotEnoughReplicasAfterAppendException, NotEnoughReplicasException}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.apache.kafka.common.internals.TopicConstants

class ProducerFailureHandlingTest extends KafkaServerTestHarness {
  private val producerBufferSize = 30000
  private val serverMessageMaxBytes =  producerBufferSize/2

  val numServers = 2

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
  overridingProps.put(KafkaConfig.MessageMaxBytesProp, serverMessageMaxBytes.toString)
  // Set a smaller value for the number of partitions for the offset commit topic (__consumer_offset topic)
  // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)

  def generateConfigs() =
    TestUtils.createBrokerConfigs(numServers, zkConnect, false).map(KafkaConfig.fromProps(_, overridingProps))

  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null

  private var producer1: KafkaProducer[Array[Byte],Array[Byte]] = null
  private var producer2: KafkaProducer[Array[Byte],Array[Byte]] = null
  private var producer3: KafkaProducer[Array[Byte],Array[Byte]] = null
  private var producer4: KafkaProducer[Array[Byte],Array[Byte]] = null

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
    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, "key".getBytes, new Array[Byte](serverMessageMaxBytes + 1))
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
    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, "key".getBytes, new Array[Byte](serverMessageMaxBytes + 1))
    intercept[ExecutionException] {
      producer2.send(record).get
    }
  }

  /**
   * With non-exist-topic the future metadata should return ExecutionException caused by TimeoutException
   */
  @Test
  def testNonExistentTopic() {
    // send a record with non-exist topic
    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic2, null, "key".getBytes, "value".getBytes)
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
    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, "key".getBytes, "value".getBytes)
    intercept[ExecutionException] {
      producer4.send(record).get
    }
  }

  /**
   *  The send call with invalid partition id should throw KafkaException caused by IllegalArgumentException
   */
  @Test
  def testInvalidPartition() {
    // create topic
    TestUtils.createTopic(zkUtils, topic1, 1, numServers, servers)

    // create a record with incorrect partition id, send should fail
    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic1, new Integer(1), "key".getBytes, "value".getBytes)
    intercept[IllegalArgumentException] {
      producer1.send(record)
    }
    intercept[IllegalArgumentException] {
      producer2.send(record)
    }
    intercept[IllegalArgumentException] {
      producer3.send(record)
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
      producer2.send(new ProducerRecord[Array[Byte],Array[Byte]](TopicConstants.INTERNAL_TOPICS.iterator.next, "test".getBytes, "test".getBytes)).get
    }
    assertTrue("Unexpected exception while sending to an invalid topic " + thrown.getCause, thrown.getCause.isInstanceOf[InvalidTopicException])
  }

  @Test
  def testNotEnoughReplicas() {
    val topicName = "minisrtest"
    val topicProps = new Properties()
    topicProps.put("min.insync.replicas",(numServers+1).toString)

    TestUtils.createTopic(zkUtils, topicName, 1, numServers, servers, topicProps)

    val record = new ProducerRecord[Array[Byte],Array[Byte]](topicName, null, "key".getBytes, "value".getBytes)
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
    topicProps.put("min.insync.replicas",numServers.toString)

    TestUtils.createTopic(zkUtils, topicName, 1, numServers, servers,topicProps)

    val record = new ProducerRecord[Array[Byte],Array[Byte]](topicName, null, "key".getBytes, "value".getBytes)
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

  private class ProducerScheduler extends ShutdownableThread("daemon-producer", false)
  {
    val numRecords = 1000
    var sent = 0
    var failed = false

    val producer = TestUtils.createNewProducer(brokerList, bufferSize = producerBufferSize, retries = 10)

    override def doWork(): Unit = {
      val responses =
        for (i <- sent+1 to sent+numRecords)
        yield producer.send(new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, null, i.toString.getBytes),
                            new ErrorLoggingCallback(topic1, null, null, true))
      val futures = responses.toList

      try {
        futures.map(_.get)
        sent += numRecords
      } catch {
        case e : Exception => failed = true
      }
    }

    override def shutdown(){
      super.shutdown()
      producer.close()
    }
  }
}
