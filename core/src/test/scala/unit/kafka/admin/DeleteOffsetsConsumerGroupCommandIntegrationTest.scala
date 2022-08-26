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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

class DeleteOffsetsConsumerGroupCommandIntegrationTest extends ConsumerGroupCommandTest {

  def getArgs(group: String, topic: String): Array[String] = {
    Array(
      "--bootstrap-server", bootstrapServers(),
      "--delete-offsets",
      "--group", group,
      "--topic", topic
    )
  }

  @Test
  def testDeleteOffsetsNonExistingGroup(): Unit = {
    val group = "missing.group"
    val topic = "foo:1"
    val service = getConsumerGroupService(getArgs(group, topic))

    val (error, _) = service.deleteOffsets(group, List(topic))
    assertEquals(Errors.GROUP_ID_NOT_FOUND, error)
  }

  @Test
  def testDeleteOffsetsOfStableConsumerGroupWithTopicPartition(): Unit = {
    testWithStableConsumerGroup(topic, 0, 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC)
  }

  @Test
  def testDeleteOffsetsOfStableConsumerGroupWithTopicOnly(): Unit = {
    testWithStableConsumerGroup(topic, -1, 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC)
  }

  @Test
  def testDeleteOffsetsOfStableConsumerGroupWithUnknownTopicPartition(): Unit = {
    testWithStableConsumerGroup("foobar", 0, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testDeleteOffsetsOfStableConsumerGroupWithUnknownTopicOnly(): Unit = {
    testWithStableConsumerGroup("foobar", -1, -1, Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testDeleteOffsetsOfEmptyConsumerGroupWithTopicPartition(): Unit = {
    testWithEmptyConsumerGroup(topic, 0, 0, Errors.NONE)
  }

  @Test
  def testDeleteOffsetsOfEmptyConsumerGroupWithTopicOnly(): Unit = {
    testWithEmptyConsumerGroup(topic, -1, 0, Errors.NONE)
  }

  @Test
  def testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicPartition(): Unit = {
    testWithEmptyConsumerGroup("foobar", 0, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicOnly(): Unit = {
    testWithEmptyConsumerGroup("foobar", -1, -1, Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  private def testWithStableConsumerGroup(inputTopic: String,
                                          inputPartition: Int,
                                          expectedPartition: Int,
                                          expectedError: Errors): Unit = {
    testWithConsumerGroup(
      withStableConsumerGroup,
      inputTopic,
      inputPartition,
      expectedPartition,
      expectedError)
  }

  private def testWithEmptyConsumerGroup(inputTopic: String,
                                         inputPartition: Int,
                                         expectedPartition: Int,
                                         expectedError: Errors): Unit = {
    testWithConsumerGroup(
      withEmptyConsumerGroup,
      inputTopic,
      inputPartition,
      expectedPartition,
      expectedError)
  }

  private def testWithConsumerGroup(withConsumerGroup: (=> Unit) => Unit,
                                    inputTopic: String,
                                    inputPartition: Int,
                                    expectedPartition: Int,
                                    expectedError: Errors): Unit = {
    produceRecord()
    withConsumerGroup {
      val topic = if (inputPartition >= 0) inputTopic + ":" + inputPartition else inputTopic
      val service = getConsumerGroupService(getArgs(group, topic))
      val (topLevelError, partitions) = service.deleteOffsets(group, List(topic))
      val tp = new TopicPartition(inputTopic, expectedPartition)
      // Partition level error should propagate to top level, unless this is due to a missed partition attempt.
      if (inputPartition >= 0) {
        assertEquals(expectedError, topLevelError)
      }
      if (expectedError == Errors.NONE)
        assertNull(partitions(tp))
      else
        assertEquals(expectedError.exception, partitions(tp).getCause)
    }
  }

  private def produceRecord(): Unit = {
    val producer = createProducer()
    try {
      producer.send(new ProducerRecord(topic, 0, null, null)).get()
    } finally {
      Utils.closeQuietly(producer, "producer")
    }
  }

  private def withStableConsumerGroup(body: => Unit): Unit = {
    val consumer = createConsumer()
    try {
      TestUtils.subscribeAndWaitForRecords(this.topic, consumer)
      consumer.commitSync()
      body
    } finally {
      Utils.closeQuietly(consumer, "consumer")
    }
  }

  private def withEmptyConsumerGroup(body: => Unit): Unit = {
    val consumer = createConsumer()
    try {
      TestUtils.subscribeAndWaitForRecords(this.topic, consumer)
      consumer.commitSync()
    } finally {
      Utils.closeQuietly(consumer, "consumer")
    }
    body
  }

  private def createProducer(config: Properties = new Properties()): KafkaProducer[Array[Byte], Array[Byte]] = {
    config.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    config.putIfAbsent(ProducerConfig.ACKS_CONFIG, "-1")
    config.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    config.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)

    new KafkaProducer(config)
  }

  private def createConsumer(config: Properties = new Properties()): KafkaConsumer[Array[Byte], Array[Byte]] = {
    config.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
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
