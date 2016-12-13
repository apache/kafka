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

package kafka.tools

import java.util
import java.util.Collections.singletonList
import java.util.Properties

import kafka.consumer.{BaseConsumerRecord, ConsumerTimeoutException}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.tools.MirrorMaker.MirrorMakerNewConsumer
import kafka.utils.{CoreUtils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.directory.mavibot.btree.serializer.ByteSerializer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.record.{Record, TimestampType}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, BytesDeserializer, StringDeserializer}
import org.junit.Assert._
import org.junit.{After, Before, Test}

class MirrorMakerTest extends ZooKeeperTestHarness {

  private var server1: KafkaServer = null

  @Before
  override def setUp() {
    super.setUp()
    val props1 = TestUtils.createBrokerConfig(0, zkConnect, false)
    val config1 = KafkaConfig.fromProps(props1)
    server1 = TestUtils.createServer(config1)

    val topic = "new-topic"
    TestUtils.createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 1, servers = List(server1))
  }

  @After
  override def tearDown() {
    server1.shutdown
    CoreUtils.delete(server1.config.logDirs)
    super.tearDown()
  }

  @Test
  def testWhitelistTopic() {
    val topic = "new-topic"
    val msg = "test message"
    val brokerList = TestUtils.getBrokerListStrFromServers(Seq(server1))

    // Create a test producer to delivery a message
    val props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    val producer1 = new KafkaProducer[Array[Byte], Array[Byte]](props)
    producer1.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, msg.getBytes()))
    producer1.flush() // Explicitly invoke flush method to make effect immediately
    producer1.close() // Close the producer

    // Create a MirrorMaker consumer
    val config = new util.HashMap[String, AnyRef]
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-gropu")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config)
    MirrorMaker.createMirrorMakerProducer(brokerList)

    val whitelist = Some("another_topic|foo" ) // Test a regular expression
    val mirrorMakerConsumer = new MirrorMakerNewConsumer(consumer, None, whitelist)
    mirrorMakerConsumer.init()
    try {
      val data = mirrorMakerConsumer.receive()
      assertTrue("MirrorMaker consumer should get the correct test topic: " + topic, data.topic.equals(topic))
      assertTrue("MirrorMaker consumer should read the correct message.", new String(data.value).equals(msg))
    } catch {
      case e: RuntimeException =>
        fail("Unexpected exception: " + e)
    } finally {
      consumer.close()
    }
  }

  @Test
  def testDefaultMirrorMakerMessageHandler() {
    val now = 12345L
    val consumerRecord = BaseConsumerRecord("topic", 0, 1L, now, TimestampType.CREATE_TIME, "key".getBytes, "value".getBytes)

    val result = MirrorMaker.defaultMirrorMakerMessageHandler.handle(consumerRecord)
    assertEquals(1, result.size)

    val producerRecord = result.get(0)
    assertEquals(now, producerRecord.timestamp)
    assertEquals("topic", producerRecord.topic)
    assertNull(producerRecord.partition)
    assertEquals("key", new String(producerRecord.key))
    assertEquals("value", new String(producerRecord.value))
  }

  @Test
  def testDefaultMirrorMakerMessageHandlerWithNoTimestampInSourceMessage() {
    val consumerRecord = BaseConsumerRecord("topic", 0, 1L, Record.NO_TIMESTAMP, TimestampType.CREATE_TIME, "key".getBytes, "value".getBytes)

    val result = MirrorMaker.defaultMirrorMakerMessageHandler.handle(consumerRecord)
    assertEquals(1, result.size)

    val producerRecord = result.get(0)
    assertNull(producerRecord.timestamp)
    assertEquals("topic", producerRecord.topic)
    assertNull(producerRecord.partition)
    assertEquals("key", new String(producerRecord.key))
    assertEquals("value", new String(producerRecord.value))
  }

}
