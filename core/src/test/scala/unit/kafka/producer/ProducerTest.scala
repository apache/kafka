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

package kafka.producer

import java.nio.ByteBuffer
import java.util
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.api.FetchRequestBuilder
import kafka.common.FailedToSendMessageException
import kafka.consumer.SimpleConsumer
import kafka.message.Message
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaRequestHandler, KafkaServer}
import kafka.utils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.TimestampType
import org.apache.log4j.{Level, Logger}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.exceptions.TestFailedException

@deprecated("This test has been deprecated and it will be removed in a future release.", "0.10.0.0")
class ProducerTest extends ZooKeeperTestHarness with Logging{
  private val brokerId1 = 0
  private val brokerId2 = 1
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
  private var servers = List.empty[KafkaServer]

  // Creation of consumers is deferred until they are actually needed. This allows us to kill brokers that use random
  // ports and then get a consumer instance that will be pointed at the correct port
  def getConsumer1() = {
    if (consumer1 == null)
      consumer1 = new SimpleConsumer("localhost", server1.boundPort(), 1000000, 64*1024, "")
    consumer1
  }

  def getConsumer2() = {
    if (consumer2 == null)
      consumer2 = new SimpleConsumer("localhost", server2.boundPort(), 100, 64*1024, "")
    consumer2
  }

  @Before
  override def setUp() {
    super.setUp()
    // set up 2 brokers with 4 partitions each
    val props1 = TestUtils.createBrokerConfig(brokerId1, zkConnect, false)
    props1.put("num.partitions", "4")
    val config1 = KafkaConfig.fromProps(props1)
    val props2 = TestUtils.createBrokerConfig(brokerId2, zkConnect, false)
    props2.put("num.partitions", "4")
    val config2 = KafkaConfig.fromProps(props2)
    server1 = TestUtils.createServer(config1)
    server2 = TestUtils.createServer(config2)
    servers = List(server1,server2)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", server1.boundPort().toString)

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)
  }

  @After
  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)

    if (consumer1 != null)
      consumer1.close()
    if (consumer2 != null)
      consumer2.close()

    server1.shutdown
    server2.shutdown
    CoreUtils.delete(server1.config.logDirs)
    CoreUtils.delete(server2.config.logDirs)
    super.tearDown()
  }

  @Test
  def testUpdateBrokerPartitionInfo() {
    val topic = "new-topic"
    TestUtils.createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 2, servers = servers)

    val props = new Properties()
    // no need to retry since the send will always fail
    props.put("message.send.max.retries", "0")
    val producer1 = TestUtils.createProducer[String, String](
        brokerList = "localhost:80,localhost:81",
        encoder = classOf[StringEncoder].getName,
        keyEncoder = classOf[StringEncoder].getName,
        producerProps = props)

    try{
      producer1.send(new KeyedMessage[String, String](topic, "test", "test1"))
      fail("Test should fail because the broker list provided are not valid")
    } catch {
      case e: FailedToSendMessageException => // this is expected
      case oe: Throwable => fail("fails with exception", oe)
    } finally {
      producer1.close()
    }

    val producer2 = TestUtils.createProducer[String, String](
      brokerList = "localhost:80," + TestUtils.getBrokerListStrFromServers(Seq(server1)),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName)

    try{
      producer2.send(new KeyedMessage[String, String](topic, "test", "test1"))
    } catch {
      case e: Throwable => fail("Should succeed sending the message", e)
    } finally {
      producer2.close()
    }

    val producer3 =  TestUtils.createProducer[String, String](
      brokerList = TestUtils.getBrokerListStrFromServers(Seq(server1, server2)),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName)

    try{
      producer3.send(new KeyedMessage[String, String](topic, "test", "test1"))
    } catch {
      case e: Throwable => fail("Should succeed sending the message", e)
    } finally {
      producer3.close()
    }
  }

  @Test
  def testSendToNewTopic() {
    val props1 = new util.Properties()
    props1.put("request.required.acks", "-1")

    val topic = "new-topic"
    // create topic with 1 partition and await leadership
    TestUtils.createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 2, servers = servers)

    val producer1 = TestUtils.createProducer[String, String](
      brokerList = TestUtils.getBrokerListStrFromServers(Seq(server1, server2)),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName,
      partitioner = classOf[StaticPartitioner].getName,
      producerProps = props1)
    val startTime = System.currentTimeMillis()
    // Available partition ids should be 0.
    producer1.send(new KeyedMessage[String, String](topic, "test", "test1"))
    producer1.send(new KeyedMessage[String, String](topic, "test", "test2"))
    val endTime = System.currentTimeMillis()
    // get the leader
    val leaderOpt = zkUtils.getLeaderForPartition(topic, 0)
    assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined)
    val leader = leaderOpt.get

    val messageSet = if(leader == server1.config.brokerId) {
      val response1 = getConsumer1().fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      response1.messageSet("new-topic", 0).iterator.toBuffer
    }else {
      val response2 = getConsumer2().fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      response2.messageSet("new-topic", 0).iterator.toBuffer
    }
    assertEquals("Should have fetched 2 messages", 2, messageSet.size)
    // Message 1
    assertTrue(ByteBuffer.wrap("test1".getBytes).equals(messageSet(0).message.payload))
    assertTrue(ByteBuffer.wrap("test".getBytes).equals(messageSet(0).message.key))
    assertTrue(messageSet(0).message.timestamp >= startTime && messageSet(0).message.timestamp < endTime)
    assertEquals(TimestampType.CREATE_TIME, messageSet(0).message.timestampType)
    assertEquals(Message.MagicValue_V1, messageSet(0).message.magic)

    // Message 2
    assertTrue(ByteBuffer.wrap("test2".getBytes).equals(messageSet(1).message.payload))
    assertTrue(ByteBuffer.wrap("test".getBytes).equals(messageSet(1).message.key))
    assertTrue(messageSet(1).message.timestamp >= startTime && messageSet(1).message.timestamp < endTime)
    assertEquals(TimestampType.CREATE_TIME, messageSet(1).message.timestampType)
    assertEquals(Message.MagicValue_V1, messageSet(1).message.magic)
    producer1.close()

    val props2 = new util.Properties()
    props2.put("request.required.acks", "3")
    // no need to retry since the send will always fail
    props2.put("message.send.max.retries", "0")

    try {
      val producer2 = TestUtils.createProducer[String, String](
        brokerList = TestUtils.getBrokerListStrFromServers(Seq(server1, server2)),
        encoder = classOf[StringEncoder].getName,
        keyEncoder = classOf[StringEncoder].getName,
        partitioner = classOf[StaticPartitioner].getName,
        producerProps = props2)
        producer2.close
        fail("we don't support request.required.acks greater than 1")
    }
    catch {
      case iae: IllegalArgumentException =>  // this is expected
      case e: Throwable => fail("Not expected", e)

    }
  }


  @Test
  def testSendWithDeadBroker() {
    val props = new Properties()
    props.put("request.required.acks", "1")
    // No need to retry since the topic will be created beforehand and normal send will succeed on the first try.
    // Reducing the retries will save the time on the subsequent failure test.
    props.put("message.send.max.retries", "0")

    val topic = "new-topic"
    // create topic
    TestUtils.createTopic(zkUtils, topic, partitionReplicaAssignment = Map(0->Seq(0), 1->Seq(0), 2->Seq(0), 3->Seq(0)),
                          servers = servers)

    val producer = TestUtils.createProducer[String, String](
      brokerList = TestUtils.getBrokerListStrFromServers(Seq(server1, server2)),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName,
      partitioner = classOf[StaticPartitioner].getName,
      producerProps = props)
    val startTime = System.currentTimeMillis()
    try {
      // Available partition ids should be 0, 1, 2 and 3, all lead and hosted only
      // on broker 0
      producer.send(new KeyedMessage[String, String](topic, "test", "test1"))
    } catch {
      case e: Throwable => fail("Unexpected exception: " + e)
    }
    val endTime = System.currentTimeMillis()
    // kill the broker
    server1.shutdown
    server1.awaitShutdown()

    try {
      // These sends should fail since there are no available brokers
      producer.send(new KeyedMessage[String, String](topic, "test", "test1"))
      fail("Should fail since no leader exists for the partition.")
    } catch {
      case e : TestFailedException => throw e // catch and re-throw the failure message
      case e2: Throwable => // otherwise success
    }

    // restart server 1
    server1.startup()
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilLeaderIsKnown(servers, topic, 0)

    try {
      // cross check if broker 1 got the messages
      val response1 = getConsumer1().fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      val messageSet1 = response1.messageSet(topic, 0).iterator
      assertTrue("Message set should have 1 message", messageSet1.hasNext)
      val message = messageSet1.next.message
      assertTrue(ByteBuffer.wrap("test1".getBytes).equals(message.payload))
      assertTrue(ByteBuffer.wrap("test".getBytes).equals(message.key))
      assertTrue(message.timestamp >= startTime && message.timestamp < endTime)
      assertEquals(TimestampType.CREATE_TIME, message.timestampType)
      assertEquals(Message.MagicValue_V1, message.magic)
      assertFalse("Message set should have another message", messageSet1.hasNext)
    } catch {
      case e: Exception => fail("Not expected", e)
    }
    producer.close
  }

  @Test
  def testAsyncSendCanCorrectlyFailWithTimeout() {
    val timeoutMs = 500
    val props = new Properties()
    props.put("request.timeout.ms", String.valueOf(timeoutMs))
    props.put("request.required.acks", "1")
    props.put("message.send.max.retries", "0")
    props.put("client.id","ProducerTest-testAsyncSendCanCorrectlyFailWithTimeout")
    val producer = TestUtils.createProducer[String, String](
      brokerList = TestUtils.getBrokerListStrFromServers(Seq(server1, server2)),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName,
      partitioner = classOf[StaticPartitioner].getName,
      producerProps = props)

    val topic = "new-topic"
    // create topics in ZK
    TestUtils.createTopic(zkUtils, topic, partitionReplicaAssignment = Map(0->Seq(0,1)), servers = servers)

    // do a simple test to make sure plumbing is okay
    try {
      // this message should be assigned to partition 0 whose leader is on broker 0
      producer.send(new KeyedMessage[String, String](topic, "test", "test"))
      // cross check if brokers got the messages
      val response1 = getConsumer1().fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      val messageSet1 = response1.messageSet("new-topic", 0).iterator
      assertTrue("Message set should have 1 message", messageSet1.hasNext)
      assertEquals(new Message("test".getBytes), messageSet1.next.message)
    } catch {
      case e: Throwable => case e: Exception => producer.close; fail("Not expected", e)
    }

    // stop IO threads and request handling, but leave networking operational
    // any requests should be accepted and queue up, but not handled
    server1.requestHandlerPool.shutdown()

    val t1 = SystemTime.milliseconds
    try {
      // this message should be assigned to partition 0 whose leader is on broker 0, but
      // broker 0 will not response within timeoutMs millis.
      producer.send(new KeyedMessage[String, String](topic, "test", "test"))
    } catch {
      case e: FailedToSendMessageException => /* success */
      case e: Exception => fail("Not expected", e)
    } finally {
      producer.close()
    }
    val t2 = SystemTime.milliseconds

    // make sure we don't wait fewer than timeoutMs
    assertTrue((t2-t1) >= timeoutMs)
  }

  @Test
  def testSendNullMessage() {
    val producer = TestUtils.createProducer[String, String](
      brokerList = TestUtils.getBrokerListStrFromServers(Seq(server1, server2)),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName,
      partitioner = classOf[StaticPartitioner].getName)

    try {

      // create topic
      AdminUtils.createTopic(zkUtils, "new-topic", 2, 1)
      TestUtils.waitUntilTrue(() =>
        AdminUtils.fetchTopicMetadataFromZk("new-topic", zkUtils).error != Errors.UNKNOWN_TOPIC_OR_PARTITION,
        "Topic new-topic not created after timeout",
        waitTime = zookeeper.tickTime)
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, "new-topic", 0)

      producer.send(new KeyedMessage[String, String]("new-topic", "key", null))
    } finally {
      producer.close()
    }
  }
}
