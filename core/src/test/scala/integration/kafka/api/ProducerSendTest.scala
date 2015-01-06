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

package kafka.api.test

import java.lang.{Integer, IllegalArgumentException}

import org.apache.kafka.clients.producer._
import org.scalatest.junit.JUnit3Suite
import org.junit.Test
import org.junit.Assert._

import kafka.server.KafkaConfig
import kafka.utils.{TestZKUtils, TestUtils}
import kafka.consumer.SimpleConsumer
import kafka.api.FetchRequestBuilder
import kafka.message.Message
import kafka.integration.KafkaServerTestHarness
import org.apache.kafka.common.errors.SerializationException
import java.util.Properties
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.serialization.ByteArraySerializer


class ProducerSendTest extends JUnit3Suite with KafkaServerTestHarness {
  val numServers = 2
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numServers, false))
    yield new KafkaConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
      override val numPartitions = 4
    }

  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null

  private val topic = "topic"
  private val numRecords = 100

  override def setUp() {
    super.setUp()

    // TODO: we need to migrate to new consumers when 0.9 is final
    consumer1 = new SimpleConsumer("localhost", configs(0).port, 100, 1024*1024, "")
    consumer2 = new SimpleConsumer("localhost", configs(1).port, 100, 1024*1024, "")
  }

  override def tearDown() {
    consumer1.close()
    consumer2.close()

    super.tearDown()
  }

  class CheckErrorCallback extends Callback {
    def onCompletion(metadata: RecordMetadata, exception: Exception) {
      if (exception != null)
        fail("Send callback returns the following exception", exception)
    }
  }

  /**
   * testSendOffset checks the basic send API behavior
   *
   * 1. Send with null key/value/partition-id should be accepted; send with null topic should be rejected.
   * 2. Last message of the non-blocking send should return the correct offset metadata
   */
  @Test
  def testSendOffset() {
    var producer = TestUtils.createNewProducer(brokerList)

    val callback = new CheckErrorCallback

    try {
      // create topic
      TestUtils.createTopic(zkClient, topic, 1, 2, servers)

      // send a normal record
      val record0 = new ProducerRecord[Array[Byte],Array[Byte]](topic, new Integer(0), "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 0", 0L, producer.send(record0, callback).get.offset)

      // send a record with null value should be ok
      val record1 = new ProducerRecord[Array[Byte],Array[Byte]](topic, new Integer(0), "key".getBytes, null)
      assertEquals("Should have offset 1", 1L, producer.send(record1, callback).get.offset)

      // send a record with null key should be ok
      val record2 = new ProducerRecord[Array[Byte],Array[Byte]](topic, new Integer(0), null, "value".getBytes)
      assertEquals("Should have offset 2", 2L, producer.send(record2, callback).get.offset)

      // send a record with null part id should be ok
      val record3 = new ProducerRecord[Array[Byte],Array[Byte]](topic, null, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 3", 3L, producer.send(record3, callback).get.offset)

      // send a record with null topic should fail
      try {
        val record4 = new ProducerRecord[Array[Byte],Array[Byte]](null, new Integer(0), "key".getBytes, "value".getBytes)
        producer.send(record4, callback)
        fail("Should not allow sending a record without topic")
      } catch {
        case iae: IllegalArgumentException => // this is ok
        case e: Throwable => fail("Only expecting IllegalArgumentException", e)
      }

      // non-blocking send a list of records
      for (i <- 1 to numRecords)
        producer.send(record0)

      // check that all messages have been acked via offset
      assertEquals("Should have offset " + (numRecords + 4), numRecords + 4L, producer.send(record0, callback).get.offset)

    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }

  @Test
  def testSerializer() {
    // send a record with a wrong type should receive a serialization exception
    try {
      val producer = createNewProducerWithWrongSerializer(brokerList)
      val record5 = new ProducerRecord[Array[Byte],Array[Byte]](topic, new Integer(0), "key".getBytes, "value".getBytes)
      producer.send(record5)
      fail("Should have gotten a SerializationException")
    } catch {
      case se: SerializationException => // this is ok
    }

    try {
      createNewProducerWithNoSerializer(brokerList)
      fail("Instantiating a producer without specifying a serializer should cause a ConfigException")
    } catch {
      case ce : ConfigException => // this is ok
    }

    // create a producer with explicit serializers should succeed
    createNewProducerWithExplicitSerializer(brokerList)
  }

  private def createNewProducerWithWrongSerializer(brokerList: String) : KafkaProducer[Array[Byte],Array[Byte]] = {
    import org.apache.kafka.clients.producer.ProducerConfig

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    return new KafkaProducer[Array[Byte],Array[Byte]](producerProps)
  }

  private def createNewProducerWithNoSerializer(brokerList: String) : KafkaProducer[Array[Byte],Array[Byte]] = {
    import org.apache.kafka.clients.producer.ProducerConfig

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    return new KafkaProducer[Array[Byte],Array[Byte]](producerProps)
  }

  private def createNewProducerWithExplicitSerializer(brokerList: String) : KafkaProducer[Array[Byte],Array[Byte]] = {
    import org.apache.kafka.clients.producer.ProducerConfig

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    return new KafkaProducer[Array[Byte],Array[Byte]](producerProps, new ByteArraySerializer, new ByteArraySerializer)
  }

  /**
   * testClose checks the closing behavior
   *
   * After close() returns, all messages should be sent with correct returned offset metadata
   */
  @Test
  def testClose() {
    var producer = TestUtils.createNewProducer(brokerList)

    try {
      // create topic
      TestUtils.createTopic(zkClient, topic, 1, 2, servers)

      // non-blocking send a list of records
      val record0 = new ProducerRecord[Array[Byte],Array[Byte]](topic, null, "key".getBytes, "value".getBytes)
      for (i <- 1 to numRecords)
        producer.send(record0)
      val response0 = producer.send(record0)

      // close the producer
      producer.close()
      producer = null

      // check that all messages have been acked via offset,
      // this also checks that messages with same key go to the same partition
      assertTrue("The last message should be acked before producer is shutdown", response0.isDone)
      assertEquals("Should have offset " + numRecords, numRecords.toLong, response0.get.offset)

    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }

  /**
   * testSendToPartition checks the partitioning behavior
   *
   * The specified partition-id should be respected
   */
  @Test
  def testSendToPartition() {
    var producer = TestUtils.createNewProducer(brokerList)

    try {
      // create topic
      val leaders = TestUtils.createTopic(zkClient, topic, 2, 2, servers)
      val partition = 1

      // make sure leaders exist
      val leader1 = leaders(partition)
      assertTrue("Leader for topic \"topic\" partition 1 should exist", leader1.isDefined)

      val responses =
        for (i <- 1 to numRecords)
        yield producer.send(new ProducerRecord[Array[Byte],Array[Byte]](topic, partition, null, ("value" + i).getBytes))
      val futures = responses.toList
      futures.map(_.get)
      for (future <- futures)
        assertTrue("Request should have completed", future.isDone)

      // make sure all of them end up in the same partition with increasing offset values
      for ((future, offset) <- futures zip (0 until numRecords)) {
        assertEquals(offset.toLong, future.get.offset)
        assertEquals(topic, future.get.topic)
        assertEquals(partition, future.get.partition)
      }

      // make sure the fetched messages also respect the partitioning and ordering
      val fetchResponse1 = if(leader1.get == configs(0).brokerId) {
        consumer1.fetch(new FetchRequestBuilder().addFetch(topic, partition, 0, Int.MaxValue).build())
      } else {
        consumer2.fetch(new FetchRequestBuilder().addFetch(topic, partition, 0, Int.MaxValue).build())
      }
      val messageSet1 = fetchResponse1.messageSet(topic, partition).iterator.toBuffer
      assertEquals("Should have fetched " + numRecords + " messages", numRecords, messageSet1.size)

      // TODO: also check topic and partition after they are added in the return messageSet
      for (i <- 0 to numRecords - 1) {
        assertEquals(new Message(bytes = ("value" + (i + 1)).getBytes), messageSet1(i).message)
        assertEquals(i.toLong, messageSet1(i).offset)
      }
    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }

  /**
   * testAutoCreateTopic
   *
   * The topic should be created upon sending the first message
   */
  @Test
  def testAutoCreateTopic() {
    var producer = TestUtils.createNewProducer(brokerList, retries = 5)

    try {
      // Send a message to auto-create the topic
      val record = new ProducerRecord[Array[Byte],Array[Byte]](topic, null, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 0", 0L, producer.send(record).get.offset)

      // double check that the topic is created with leader elected
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)

    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }
}
