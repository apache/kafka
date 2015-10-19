/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.util.Properties
import java.io.File

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import kafka.integration.KafkaServerTestHarness

import kafka.utils.{TestUtils, Logging}
import kafka.server.KafkaConfig

import java.util.ArrayList
import org.junit.{Test, Before, After}
import org.junit.Assert._

import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._
import kafka.coordinator.ConsumerCoordinator


/**
  * Integration tests for the new consumer that cover basic usage as well as server failures
  */
class SSLConsumerTest extends KafkaServerTestHarness with Logging {

  val trustStoreFile = File.createTempFile("truststore", ".jks")
  val numServers = 3
  val producerCount = 1
  val consumerCount = 2
  val producerConfig = new Properties
  val consumerConfig = new Properties

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, 4.toString)
  overridingProps.put(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
  overridingProps.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  overridingProps.put(KafkaConfig.ConsumerMinSessionTimeoutMsProp, "100") // set small enough session timeout

  val consumers = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
  val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  def generateConfigs() =
    TestUtils.createBrokerConfigs(numServers, zkConnect, false, enableSSL=true, trustStoreFile=Some(trustStoreFile)).map(KafkaConfig.fromProps(_, overridingProps))

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)

  // configure the servers and clients
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-test")
  this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096.toString)
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  @Before
  override def setUp() {
    super.setUp()
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.getSSLBrokerListStrFromServers(servers))
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.getSSLBrokerListStrFromServers(servers))
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "range")

    for (i <- 0 until producerCount)
      producers += TestUtils.createNewProducer(TestUtils.getSSLBrokerListStrFromServers(servers),
                                               acks = 1,
                                               enableSSL=true,
                                               trustStoreFile=Some(trustStoreFile))
    for (i <- 0 until consumerCount)
      consumers += TestUtils.createNewConsumer(TestUtils.getSSLBrokerListStrFromServers(servers),
                                               groupId = "my-test",
                                               partitionAssignmentStrategy= "range",
                                               enableSSL=true,
                                               trustStoreFile=Some(trustStoreFile))


    // create the consumer offset topic
    TestUtils.createTopic(zkUtils, ConsumerCoordinator.OffsetsTopicName,
      overridingProps.getProperty(KafkaConfig.OffsetsTopicPartitionsProp).toInt,
      overridingProps.getProperty(KafkaConfig.OffsetsTopicReplicationFactorProp).toInt,
      servers,
      servers(0).consumerCoordinator.offsetsTopicConfigs)

    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(zkUtils, topic, 1, numServers, this.servers)
  }

  @After
  override def tearDown() {
    producers.foreach(_.close())
    consumers.foreach(_.close())
    super.tearDown()
  }

  @Test
  def testSimpleConsumption() {
    val numRecords = 10000
    sendRecords(numRecords)
    assertEquals(0, this.consumers(0).assignment.size)
    this.consumers(0).assign(List(tp))
    assertEquals(1, this.consumers(0).assignment.size)
    this.consumers(0).seek(tp, 0)
    consumeRecords(this.consumers(0), numRecords = numRecords, startingOffset = 0)
  }

  @Test
  def testAutoOffsetReset() {
    sendRecords(1)
    this.consumers(0).assign(List(tp))
    consumeRecords(this.consumers(0), numRecords = 1, startingOffset = 0)
  }

  @Test
  def testSeek() {
    val consumer = this.consumers(0)
    val totalRecords = 50L
    sendRecords(totalRecords.toInt)
    consumer.assign(List(tp))

    consumer.seekToEnd(tp)
    assertEquals(totalRecords, consumer.position(tp))
    assertFalse(consumer.poll(totalRecords).iterator().hasNext)

    consumer.seekToBeginning(tp)
    assertEquals(0, consumer.position(tp), 0)
    consumeRecords(consumer, numRecords = 1, startingOffset = 0)

    val mid = totalRecords / 2
    consumer.seek(tp, mid)
    assertEquals(mid, consumer.position(tp))
    consumeRecords(consumer, numRecords = 1, startingOffset = mid.toInt)
  }

  @Test
  def testGroupConsumption() {
    sendRecords(10)
    this.consumers(0).subscribe(List(topic))
    consumeRecords(this.consumers(0), numRecords = 1, startingOffset = 0)
  }

  @Test
  def testPositionAndCommit() {
    sendRecords(5)

    // committed() on a partition with no committed offset returns null
    assertNull(this.consumers(0).committed(new TopicPartition(topic, 15)))

    // position() on a partition that we aren't subscribed to throws an exception
    intercept[IllegalArgumentException] {
      this.consumers(0).position(new TopicPartition(topic, 15))
    }

    this.consumers(0).assign(List(tp))

    assertEquals("position() on a partition that we are subscribed to should reset the offset", 0L, this.consumers(0).position(tp))
    this.consumers(0).commitSync()
    assertEquals(0L, this.consumers(0).committed(tp).offset)

    consumeRecords(this.consumers(0), 5, 0)
    assertEquals("After consuming 5 records, position should be 5", 5L, this.consumers(0).position(tp))
    this.consumers(0).commitSync()
    assertEquals("Committed offset should be returned", 5L, this.consumers(0).committed(tp).offset)

    sendRecords(1)

    // another consumer in the same group should get the same position
    this.consumers(1).assign(List(tp))
    consumeRecords(this.consumers(1), 1, 5)
  }

  @Test
  def testPartitionsFor() {
    val numParts = 2
    TestUtils.createTopic(zkUtils, "part-test", numParts, 1, this.servers)
    val parts = this.consumers(0).partitionsFor("part-test")
    assertNotNull(parts)
    assertEquals(2, parts.length)
    assertNull(this.consumers(0).partitionsFor("non-exist-topic"))
  }

  private def sendRecords(numRecords: Int) {
    val futures = (0 until numRecords).map { i =>
      this.producers(0).send(new ProducerRecord(topic, part, i.toString.getBytes, i.toString.getBytes))
    }
    futures.map(_.get)
  }

  private def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]], numRecords: Int, startingOffset: Int) {
    val records = new ArrayList[ConsumerRecord[Array[Byte], Array[Byte]]]()
    val maxIters = numRecords * 300
    var iters = 0
    while (records.size < numRecords) {
      for (record <- consumer.poll(50)) {
        records.add(record)
      }
      if (iters > maxIters)
        throw new IllegalStateException("Failed to consume the expected records after " + iters + " iterations.")
      iters += 1
    }
    for (i <- 0 until numRecords) {
      val record = records.get(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic())
      assertEquals(part, record.partition())
      assertEquals(offset.toLong, record.offset())
    }
  }

}
