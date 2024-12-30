/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.api

import kafka.utils.TestInfoUtils
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util
import java.util.{Collections, Optional, Properties}
import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

class ConsumerWithLegacyMessageFormatIntegrationTest extends AbstractConsumerTest {

  @nowarn("cat=deprecation")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testOffsetsForTimes(quorum: String, groupProtocol: String): Unit = {
    val numParts = 2
    val topic1 = "part-test-topic-1"
    val topic2 = "part-test-topic-2"
    val topic3 = "part-test-topic-3"
    val props = new Properties()
    props.setProperty(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, "0.9.0")
    createTopic(topic1, numParts)
    // Topic2 is in old message format.
    createTopic(topic2, numParts, 1, props)
    createTopic(topic3, numParts)

    val consumer = createConsumer()

    // Test negative target time
    assertThrows(classOf[IllegalArgumentException],
      () => consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(topic1, 0), -1)))

    val producer = createProducer()
    val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]()
    var i = 0
    for (topic <- List(topic1, topic2, topic3)) {
      for (part <- 0 until numParts) {
        val tp = new TopicPartition(topic, part)
        // In sendRecords(), each message will have key, value and timestamp equal to the sequence number.
        sendRecords(producer, numRecords = 100, tp, startingTimestamp = 0)
        timestampsToSearch.put(tp, (i * 20).toLong)
        i += 1
      }
    }
    // The timestampToSearch map should contain:
    // (topic1Partition0 -> 0,
    //  topic1Partition1 -> 20,
    //  topic2Partition0 -> 40,
    //  topic2Partition1 -> 60,
    //  topic3Partition0 -> 80,
    //  topic3Partition1 -> 100)
    val timestampOffsets = consumer.offsetsForTimes(timestampsToSearch)

    val timestampTopic1P0 = timestampOffsets.get(new TopicPartition(topic1, 0))
    assertEquals(0, timestampTopic1P0.offset)
    assertEquals(0, timestampTopic1P0.timestamp)
    assertEquals(Optional.of(0), timestampTopic1P0.leaderEpoch)

    val timestampTopic1P1 = timestampOffsets.get(new TopicPartition(topic1, 1))
    assertEquals(20, timestampTopic1P1.offset)
    assertEquals(20, timestampTopic1P1.timestamp)
    assertEquals(Optional.of(0), timestampTopic1P1.leaderEpoch)

    // legacy message formats are supported for IBP version < 3.0 and KRaft runs on minimum version 3.0-IV1
    val timestampTopic2P0 = timestampOffsets.get(new TopicPartition(topic2, 0))
    assertEquals(40, timestampTopic2P0.offset)
    assertEquals(40, timestampTopic2P0.timestamp)
    assertEquals(Optional.of(0), timestampTopic2P0.leaderEpoch)

    val timestampTopic2P1 = timestampOffsets.get(new TopicPartition(topic2, 1))
    assertEquals(60, timestampTopic2P1.offset)
    assertEquals(60, timestampTopic2P1.timestamp)
    assertEquals(Optional.of(0), timestampTopic2P1.leaderEpoch)

    val timestampTopic3P0 = timestampOffsets.get(new TopicPartition(topic3, 0))
    assertEquals(80, timestampTopic3P0.offset)
    assertEquals(80, timestampTopic3P0.timestamp)
    assertEquals(Optional.of(0), timestampTopic3P0.leaderEpoch)

    assertNull(timestampOffsets.get(new TopicPartition(topic3, 1)))
  }

  @nowarn("cat=deprecation")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testEarliestOrLatestOffsets(quorum: String, groupProtocol: String): Unit = {
    val topic0 = "topicWithNewMessageFormat"
    val topic1 = "topicWithOldMessageFormat"
    val prop = new Properties()
    // idempotence producer doesn't support old version of messages
    prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    val producer = createProducer(configOverrides = prop)
    createTopicAndSendRecords(producer, topicName = topic0, numPartitions = 2, recordsPerPartition = 100)
    val props = new Properties()
    props.setProperty(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, "0.9.0")
    createTopic(topic1, numPartitions = 1, replicationFactor = 1, props)
    sendRecords(producer, numRecords = 100, new TopicPartition(topic1, 0))

    val t0p0 = new TopicPartition(topic0, 0)
    val t0p1 = new TopicPartition(topic0, 1)
    val t1p0 = new TopicPartition(topic1, 0)
    val partitions = Set(t0p0, t0p1, t1p0).asJava
    val consumer = createConsumer()

    val earliests = consumer.beginningOffsets(partitions)
    assertEquals(0L, earliests.get(t0p0))
    assertEquals(0L, earliests.get(t0p1))
    assertEquals(0L, earliests.get(t1p0))

    val latests = consumer.endOffsets(partitions)
    assertEquals(100L, latests.get(t0p0))
    assertEquals(100L, latests.get(t0p1))
    assertEquals(100L, latests.get(t1p0))
  }
}
