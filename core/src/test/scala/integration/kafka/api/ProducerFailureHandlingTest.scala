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

import kafka.common.Topic
import org.apache.kafka.common.errors.{InvalidTopicException,NotEnoughReplicasException}
import org.scalatest.junit.JUnit3Suite
import org.junit.Test
import org.junit.Assert._

import java.util.{Properties, Random}
import java.lang.Integer
import java.util.concurrent.{TimeoutException, TimeUnit, ExecutionException}

import kafka.server.KafkaConfig
import kafka.utils.{TestZKUtils, ShutdownableThread, TestUtils}
import kafka.integration.KafkaServerTestHarness
import kafka.consumer.SimpleConsumer

import org.apache.kafka.common.KafkaException
import org.apache.kafka.clients.producer._

class ProducerFailureHandlingTest extends JUnit3Suite with KafkaServerTestHarness {
  private val producerBufferSize = 30000
  private val serverMessageMaxBytes =  producerBufferSize/2

  val numServers = 2
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numServers, false))
    yield new KafkaConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
      override val autoCreateTopicsEnable = false
      override val messageMaxBytes = serverMessageMaxBytes
      // TODO: Currently, when there is no topic in a cluster, the controller doesn't send any UpdateMetadataRequest to
      // the broker. As a result, the live broker list in metadataCache is empty. If the number of live brokers is 0, we
      // try to create the offset topic with the default offsets.topic.replication.factor of 3. The creation will fail
      // since there is not enough live brokers. This causes testCannotSendToInternalTopic() to fail. Temporarily fixing
      // the issue by overriding offsets.topic.replication.factor to 1 for now. When we fix KAFKA-1867, we need to
      // remove the following config override.
      override val offsetsTopicReplicationFactor = 1.asInstanceOf[Short]
    }


  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null

  private var producer1: KafkaProducer[Array[Byte],Array[Byte]] = null
  private var producer2: KafkaProducer[Array[Byte],Array[Byte]] = null
  private var producer3: KafkaProducer[Array[Byte],Array[Byte]] = null
  private var producer4: KafkaProducer[Array[Byte],Array[Byte]] = null

  private val topic1 = "topic-1"
  private val topic2 = "topic-2"

  override def setUp() {
    super.setUp()

    // TODO: we need to migrate to new consumers when 0.9 is final
    consumer1 = new SimpleConsumer("localhost", configs(0).port, 100, 1024*1024, "")
    consumer2 = new SimpleConsumer("localhost", configs(1).port, 100, 1024*1024, "")

    producer1 = TestUtils.createNewProducer(brokerList, acks = 0, blockOnBufferFull = false, bufferSize = producerBufferSize)
    producer2 = TestUtils.createNewProducer(brokerList, acks = 1, blockOnBufferFull = false, bufferSize = producerBufferSize)
    producer3 = TestUtils.createNewProducer(brokerList, acks = -1, blockOnBufferFull = false, bufferSize = producerBufferSize)
  }

  override def tearDown() {
    consumer1.close
    consumer2.close

    if (producer1 != null) producer1.close
    if (producer2 != null) producer2.close
    if (producer3 != null) producer3.close
    if (producer4 != null) producer4.close

    super.tearDown()
  }

  /**
   * With ack == 0 the future metadata will have no exceptions with offset -1
   */
  @Test
  def testTooLargeRecordWithAckZero() {
    // create topic
    TestUtils.createTopic(zkClient, topic1, 1, 2, servers)

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
    TestUtils.createTopic(zkClient, topic1, 1, 2, servers)

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
    TestUtils.createTopic(zkClient, topic1, 1, 2, servers)

    // producer with incorrect broker list
    producer4 = TestUtils.createNewProducer("localhost:8686,localhost:4242", acks = 1, blockOnBufferFull = false, bufferSize = producerBufferSize)

    // send a record with incorrect broker list
    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, "key".getBytes, "value".getBytes)
    intercept[ExecutionException] {
      producer4.send(record).get
    }
  }

  /**
   * 1. With ack=0, the future metadata should not be blocked.
   * 2. With ack=1, the future metadata should block,
   *    and subsequent calls will eventually cause buffer full
   */
  @Test
  def testNoResponse() {
    // create topic
    TestUtils.createTopic(zkClient, topic1, 1, 2, servers)

    // first send a message to make sure the metadata is refreshed
    val record1 = new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, "key".getBytes, "value".getBytes)
    producer1.send(record1).get
    producer2.send(record1).get

    // stop IO threads and request handling, but leave networking operational
    // any requests should be accepted and queue up, but not handled
    servers.foreach(server => server.requestHandlerPool.shutdown())

    producer1.send(record1).get(5000, TimeUnit.MILLISECONDS)

    intercept[TimeoutException] {
      producer2.send(record1).get(5000, TimeUnit.MILLISECONDS)
    }

    // TODO: expose producer configs after creating them
    // send enough messages to get buffer full
    val tooManyRecords = 10
    val msgSize = producerBufferSize / tooManyRecords
    val value = new Array[Byte](msgSize)
    new Random().nextBytes(value)
    val record2 = new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, "key".getBytes, value)

    intercept[KafkaException] {
      for (i <- 1 to tooManyRecords)
        producer2.send(record2)
    }

    // do not close produce2 since it will block
    // TODO: can we do better?
    producer2 = null
  }

  /**
   *  The send call with invalid partition id should throw KafkaException caused by IllegalArgumentException
   */
  @Test
  def testInvalidPartition() {
    // create topic
    TestUtils.createTopic(zkClient, topic1, 1, 2, servers)

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
    TestUtils.createTopic(zkClient, topic1, 1, 2, servers)

    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, "key".getBytes, "value".getBytes)

    // first send a message to make sure the metadata is refreshed
    producer1.send(record).get
    producer2.send(record).get
    producer3.send(record).get

    intercept[IllegalStateException] {
      producer1.close
      producer1.send(record)
    }
    intercept[IllegalStateException] {
      producer2.close
      producer2.send(record)
    }
    intercept[IllegalStateException] {
      producer3.close
      producer3.send(record)
    }

    // re-close producer is fine
  }

  /**
   * With replication, producer should able able to find new leader after it detects broker failure
   */
  @Test
  def testBrokerFailure() {
    // create topic
    val leaders = TestUtils.createTopic(zkClient, topic1, 1, 2, servers)
    val partition = 0
    assertTrue("Leader of partition 0 of the topic should exist", leaders(partition).isDefined)

    val scheduler = new ProducerScheduler()
    scheduler.start

    // rolling bounce brokers
    for (i <- 0 until 2) {
      for (server <- servers) {
        server.shutdown()
        server.awaitShutdown()
        server.startup

        Thread.sleep(2000)
      }

      // Make sure the producer do not see any exception
      // in returned metadata due to broker failures
      assertTrue(scheduler.failed == false)

      // Make sure the leader still exists after bouncing brokers
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic1, partition)
    }

    scheduler.shutdown

    // Make sure the producer do not see any exception
    // when draining the left messages on shutdown
    assertTrue(scheduler.failed == false)

    // double check that the leader info has been propagated after consecutive bounces
    val leader = TestUtils.waitUntilMetadataIsPropagated(servers, topic1, partition)

    val fetchResponse = if(leader == configs(0).brokerId) {
      consumer1.fetch(new FetchRequestBuilder().addFetch(topic1, partition, 0, Int.MaxValue).build()).messageSet(topic1, partition)
    } else {
      consumer2.fetch(new FetchRequestBuilder().addFetch(topic1, partition, 0, Int.MaxValue).build()).messageSet(topic1, partition)
    }

    val messages = fetchResponse.iterator.toList.map(_.message)
    val uniqueMessages = messages.toSet
    val uniqueMessageSize = uniqueMessages.size

    assertEquals("Should have fetched " + scheduler.sent + " unique messages", scheduler.sent, uniqueMessageSize)
  }

  @Test(expected = classOf[InvalidTopicException])
  def testCannotSendToInternalTopic() {
    producer1.send(new ProducerRecord[Array[Byte],Array[Byte]](Topic.InternalTopics.head, "test".getBytes, "test".getBytes)).get
  }

  @Test
  def testNotEnoughReplicas() {
    val topicName = "minisrtest"
    val topicProps = new Properties();
    topicProps.put("min.insync.replicas","3");


    TestUtils.createTopic(zkClient, topicName, 1, 2, servers,topicProps)

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
    val topicProps = new Properties();
    topicProps.put("min.insync.replicas","2");


    TestUtils.createTopic(zkClient, topicName, 1, 2, servers,topicProps)

    val record = new ProducerRecord[Array[Byte],Array[Byte]](topicName, null, "key".getBytes, "value".getBytes)
    // This should work
    producer3.send(record).get

    //shut down one broker
    servers.head.shutdown()
    servers.head.awaitShutdown()
    try {
      producer3.send(record).get
      fail("Expected exception when producing to topic with fewer brokers than min.insync.replicas")
    } catch {
      case e: ExecutionException =>
        if (!e.getCause.isInstanceOf[NotEnoughReplicasException]) {
          fail("Expected NotEnoughReplicasException when producing to topic with fewer brokers than min.insync.replicas")
        }
    }

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
        yield producer.send(new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, null, i.toString.getBytes))
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
      producer.close
    }
  }
}
