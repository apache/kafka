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

package kafka.admin

import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.server.Defaults
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.utils.Utils
import org.junit.Test
import org.junit.Assert._

class DeleteOffsetsConsumerGroupCommandTest extends ConsumerGroupCommandTest {

  @Test
  def testDeleteOffsetsNonExistingGroup(): Unit = {
    val missingGroup = "missing.group"

    val cgcArgs = Array(
      "--bootstrap-server", brokerList,
      "--delete-offsets",
      "--group", missingGroup,
      "--topic", "foo:1")
    val service = getConsumerGroupService(cgcArgs)

    try {
      service.deleteOffsets()
      fail("GroupIdNotFoundException should have been raised")
    } catch {
      case e: ExecutionException =>
        if (e.getCause != Errors.GROUP_ID_NOT_FOUND.exception())
          throw e
    }
  }

  @Test
  def testDeleteOffsetsWithTopicPartition(): Unit = {
    val producer = createProducer()
    try {
      producer.send(new ProducerRecord(topic, 0, null, null)).get()
    } finally {
      Utils.closeQuietly(producer, "producer")
    }

    val consumer = createConsumer()
    try {
      TestUtils.subscribeAndWaitForRecords(topic, consumer)
      consumer.commitSync()

      val cgcArgs = Array(
        "--bootstrap-server", brokerList,
        "--delete-offsets",
        "--group", group,
        "--topic", topic + ":0")
      val service = getConsumerGroupService(cgcArgs)

      val partitions = service.deleteOffsets()
      // Unknown because the consumer has not committed any offsets yet.
      assertEquals(Errors.GROUP_SUBSCRIBED_TO_TOPIC.exception, partitions(new TopicPartition(topic, 0)).getCause)
    } finally {
      Utils.closeQuietly(consumer, "consumer")
    }
  }

  @Test
  def testDeleteOffsetsWithTopic(): Unit = {
    val producer = createProducer()
    try {
      producer.send(new ProducerRecord(topic, 0, null, null)).get()
    } finally {
      Utils.closeQuietly(producer, "producer")
    }

    val consumer = createConsumer()
    try {
      TestUtils.subscribeAndWaitForRecords(topic, consumer)
      consumer.commitSync()

      val cgcArgs = Array(
        "--bootstrap-server", brokerList,
        "--delete-offsets",
        "--group", group,
        "--topic", topic)
      val service = getConsumerGroupService(cgcArgs)

      val partitions = service.deleteOffsets()
      assertEquals(Errors.GROUP_SUBSCRIBED_TO_TOPIC.exception, partitions(new TopicPartition(topic, 0)).getCause)
    } finally {
      Utils.closeQuietly(consumer, "consumer")
    }
  }

  @Test
  def testDeleteOffsetsWithTopicEmpty(): Unit = {
    val producer = createProducer()
    try {
      producer.send(new ProducerRecord(topic, 0, null, null)).get()
    } finally {
      Utils.closeQuietly(producer, "producer")
    }

    val consumer = createConsumer()
    try {
      TestUtils.subscribeAndWaitForRecords(topic, consumer)
      consumer.commitSync()
    } finally {
      Utils.closeQuietly(consumer, "consumer")
    }

    val cgcArgs = Array(
      "--bootstrap-server", brokerList,
      "--delete-offsets",
      "--group", group,
      "--topic", topic)
    val service = getConsumerGroupService(cgcArgs)

    val partitions = service.deleteOffsets()
    assertNull(partitions(new TopicPartition(topic, 0)))
  }

  @Test
  def testDeleteOffsetsWithUnknownTopic(): Unit = {
    val producer = createProducer()
    try {
      producer.send(new ProducerRecord(topic, 0, null, null)).get()
    } finally {
      Utils.closeQuietly(producer, "producer")
    }

    val consumer = createConsumer()
    try {
      TestUtils.subscribeAndWaitForRecords(topic, consumer)
      consumer.commitSync()

      val cgcArgs = Array(
        "--bootstrap-server", brokerList,
        "--delete-offsets",
        "--group", group,
        "--topic", "foobar")
      val service = getConsumerGroupService(cgcArgs)

      val partitions = service.deleteOffsets()
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception, partitions(new TopicPartition("foobar", -1)).getCause)
    } finally {
      Utils.closeQuietly(consumer, "consumer")
    }
  }

  private def createProducer(config: Properties = new Properties()): KafkaProducer[Array[Byte], Array[Byte]] = {
    config.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.putIfAbsent(ProducerConfig.ACKS_CONFIG, "-1")
    config.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    config.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)

    new KafkaProducer(config)
  }

  private def createConsumer(config: Properties = new Properties()): KafkaConsumer[Array[Byte], Array[Byte]] = {
    config.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, group)
    config.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    config.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    // Increase timeouts to avoid having a rebalance during the test
    config.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE.toString)
    config.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Defaults.GroupMaxSessionTimeoutMs.toString)

    new KafkaConsumer(config)
  }

}
