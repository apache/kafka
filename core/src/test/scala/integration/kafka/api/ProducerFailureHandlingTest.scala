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

import org.scalatest.junit.JUnit3Suite
import org.junit.Test
import org.junit.Assert._

import java.util.Properties
import java.lang.Integer
import java.util.concurrent.{TimeoutException, TimeUnit, ExecutionException}

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{ShutdownableThread, Utils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import kafka.consumer.SimpleConsumer

import org.apache.kafka.common.KafkaException
import org.apache.kafka.clients.producer._

class ProducerFailureHandlingTest extends JUnit3Suite with ZooKeeperTestHarness {
  private val brokerId1 = 0
  private val brokerId2 = 1
  private val ports = TestUtils.choosePorts(2)
  private val (port1, port2) = (ports(0), ports(1))
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var servers = List.empty[KafkaServer]

  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null

  private var producer1: KafkaProducer = null
  private var producer2: KafkaProducer = null
  private var producer3: KafkaProducer = null
  private var producer4: KafkaProducer = null

  private val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
  private val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
  props1.put("auto.create.topics.enable", "false")
  props2.put("auto.create.topics.enable", "false")
  private val config1 = new KafkaConfig(props1)
  private val config2 = new KafkaConfig(props2)
  private val brokerList = TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2))

  private val bufferSize = 2 * config1.messageMaxBytes

  private val topic1 = "topic-1"
  private val topic2 = "topic-2"

  // TODO: move this function to TestUtils after we have server dependant on clients
  private def makeProducer(brokerList: String, acks: Int, metadataFetchTimeout: Long,
                           blockOnBufferFull: Boolean, bufferSize: Long) : KafkaProducer = {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.ACKS_CONFIG, acks.toString)
    producerProps.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, metadataFetchTimeout.toString)
    producerProps.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, blockOnBufferFull.toString)
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString)
    return new KafkaProducer(producerProps)
  }

  override def setUp() {
    super.setUp()
    server1 = TestUtils.createServer(config1)
    server2 = TestUtils.createServer(config2)
    servers = List(server1,server2)

    // TODO: we need to migrate to new consumers when 0.9 is final
    consumer1 = new SimpleConsumer("localhost", port1, 100, 1024*1024, "")
    consumer2 = new SimpleConsumer("localhost", port2, 100, 1024*1024, "")

    producer1 = makeProducer(brokerList, 0, 3000, false, bufferSize); // produce with ack=0
    producer2 = makeProducer(brokerList, 1, 3000, false, bufferSize); // produce with ack=1
    producer3 = makeProducer(brokerList, -1, 3000, false, bufferSize); // produce with ack=-1
    producer4 = makeProducer("localhost:8686,localhost:4242", 1, 3000, false, bufferSize); // produce with incorrect broker list
  }

  override def tearDown() {
    consumer1.close
    consumer2.close

    if (producer1 != null) producer1.close
    if (producer2 != null) producer2.close
    if (producer3 != null) producer3.close
    if (producer4 != null) producer4.close

    server1.shutdown; Utils.rm(server1.config.logDirs)
    server2.shutdown; Utils.rm(server2.config.logDirs)

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
    val record = new ProducerRecord(topic1, null, "key".getBytes, new Array[Byte](config1.messageMaxBytes + 1))
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
    val record = new ProducerRecord(topic1, null, "key".getBytes, new Array[Byte](config1.messageMaxBytes + 1))
    intercept[ExecutionException] {
      producer2.send(record).get
    }
  }

  /**
   * With non-exist-topic the future metadata should return ExecutionException caused by TimeoutException
   */
  @Test
  def testNonExistTopic() {
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
    TestUtils.createTopic(zkClient, topic1, 1, 2, servers)

    // send a record with incorrect broker list
    val record = new ProducerRecord(topic1, null, "key".getBytes, "value".getBytes)
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
    val record = new ProducerRecord(topic1, null, "key".getBytes, "value".getBytes)
    producer1.send(record).get
    producer2.send(record).get

    // stop IO threads and request handling, but leave networking operational
    // any requests should be accepted and queue up, but not handled
    server1.requestHandlerPool.shutdown()
    server2.requestHandlerPool.shutdown()

    producer1.send(record).get(5000, TimeUnit.MILLISECONDS)

    intercept[TimeoutException] {
      producer2.send(record).get(5000, TimeUnit.MILLISECONDS)
    }

    // TODO: expose producer configs after creating them
    // send enough messages to get buffer full
    val tooManyRecords = bufferSize / ("key".getBytes.length + "value".getBytes.length)

    intercept[KafkaException] {
      for (i <- 1 to tooManyRecords)
        producer2.send(record)
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
    val record = new ProducerRecord(topic1, new Integer(1), "key".getBytes, "value".getBytes)
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

    val record = new ProducerRecord(topic1, null, "key".getBytes, "value".getBytes)

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

  /* Temporarily disables the test since it hangs occasionally on the following stacktrace. Also, the test takes too long.
"Test worker" prio=5 tid=7fb23bb48800 nid=0x10dc79000 waiting for monitor entry [10dc76000]
   java.lang.Thread.State: BLOCKED (on object monitor)
        at java.nio.HeapByteBuffer.slice(HeapByteBuffer.java:80)
        at kafka.message.ByteBufferMessageSet$$anon$1.makeNextOuter(ByteBufferMessageSet.scala:165)
        at kafka.message.ByteBufferMessageSet$$anon$1.makeNext(ByteBufferMessageSet.scala:191)
        at kafka.message.ByteBufferMessageSet$$anon$1.makeNext(ByteBufferMessageSet.scala:145)
        at kafka.utils.IteratorTemplate.maybeComputeNext(IteratorTemplate.scala:66)
        at kafka.utils.IteratorTemplate.hasNext(IteratorTemplate.scala:58)
        at scala.collection.Iterator$class.foreach(Iterator.scala:631)
        at kafka.utils.IteratorTemplate.foreach(IteratorTemplate.scala:32)
        at scala.collection.generic.Growable$class.$plus$plus$eq(Growable.scala:48)
        at scala.collection.TraversableOnce$class.toList(TraversableOnce.scala:399)
        at kafka.utils.IteratorTemplate.toList(IteratorTemplate.scala:32)
        at kafka.api.ProducerFailureHandlingTest.testBrokerFailure(ProducerFailureHandlingTest.scala:305)

  /**
   * With replication, producer should able able to find new leader after it detects broker failure
   */
  @Test
  def testBrokerFailure() {
    // create topic
    val leaders = TestUtils.createTopic(zkClient, topic1, 1, 2, servers)
    val partition = 0
    var leader = leaders(partition)
    assertTrue("Leader of partition 0 of the topic should exist", leader.isDefined)

    val scheduler = new ProducerScheduler()
    scheduler.start

    // rolling bounce brokers
    for (i <- 0 until 5) {
      server1.shutdown()
      server1.awaitShutdown()
      server1.startup

      Thread.sleep(2000)

      server2.shutdown()
      server2.awaitShutdown()
      server2.startup

      Thread.sleep(2000)

      assertTrue(scheduler.failed == false)
    }

    scheduler.shutdown
    leader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic1, partition, 500)

    val fetchResponse = if(leader.get == server1.config.brokerId) {
      consumer1.fetch(new FetchRequestBuilder().addFetch(topic1, partition, 0, Int.MaxValue).build()).messageSet(topic1, partition)
    } else {
      consumer2.fetch(new FetchRequestBuilder().addFetch(topic1, partition, 0, Int.MaxValue).build()).messageSet(topic1, partition)
    }

    val messages = fetchResponse.iterator.toList.map(_.message)
    val uniqueMessages = messages.toSet
    val uniqueMessageSize = uniqueMessages.size

    assertEquals("Should have fetched " + scheduler.sent + " unique messages", scheduler.sent, uniqueMessageSize)
  }
  */

  private class ProducerScheduler extends ShutdownableThread("daemon-producer", false)
  {
    val numRecords = 1000
    var sent = 0
    var failed = false

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.ACKS_CONFIG, (-1).toString)
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString)
    producerProps.put(ProducerConfig.RETRIES_CONFIG, 10.toString)
    producerProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000.toString)
    val producer = new KafkaProducer(producerProps)

    override def doWork(): Unit = {
      val responses =
        for (i <- sent+1 to sent+numRecords)
        yield producer.send(new ProducerRecord(topic1, null, null, i.toString.getBytes))
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