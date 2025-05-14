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

import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, NewPartitionReassignment, TopicDescription}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.config.{ReplicationConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import java.util
import java.util.Optional
import scala.jdk.CollectionConverters._


class ProducerSendWhileDeletionTest extends IntegrationTestHarness {
  val producerCount: Int = 1
  val brokerCount: Int = 2
  val defaultLingerMs: Int = 5

  serverConfig.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, 2.toString)
  serverConfig.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, 2.toString)
  serverConfig.put(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, false.toString)

  producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000L.toString)
  producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000.toString)
  producerConfig.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (10000 + defaultLingerMs).toString)

  /**
   * Tests that Producer gets self-recovered when a topic is deleted mid-way of produce.
   *
   * Producer will attempt to send messages to the partition specified in each record, and should
   * succeed as long as the partition is included in the metadata.
   */
  @Test
  def testSendWithTopicDeletionMidWay(): Unit = {
    val numRecords = 10
    val topic = "topic"

    // Create topic with leader as 0 for the 2 partitions.
    createTopicWithAssignment(topic, Map(0 -> Seq(0, 1), 1 -> Seq(0, 1)))

    val reassignment = Map(
      new TopicPartition(topic, 0) -> Optional.of(new NewPartitionReassignment(util.Arrays.asList(1, 0))),
      new TopicPartition(topic, 1) -> Optional.of(new NewPartitionReassignment(util.Arrays.asList(1, 0)))
    )

    // Change leader to 1 for both the partitions to increase leader epoch from 0 -> 1
    val admin = createAdminClient()
    admin.alterPartitionReassignments(reassignment.asJava).all().get()

    val producer = createProducer()

    (1 to numRecords).foreach { i =>
      val resp = producer.send(new ProducerRecord(topic, null, ("value" + i).getBytes(StandardCharsets.UTF_8))).get
      assertEquals(topic, resp.topic())
    }

    // Start topic deletion
    deleteTopic(topic, listenerName)

    // Verify that the topic is deleted when no metadata request comes in
    TestUtils.verifyTopicDeletion(topic, 2, brokers)

    // Producer should be able to send messages even after topic gets deleted and auto-created
    assertEquals(topic, producer.send(new ProducerRecord(topic, null, "value".getBytes(StandardCharsets.UTF_8))).get.topic())
  }

  /**
   * Tests that Producer produce to new topic id after recreation.
   *
   * Producer will attempt to send messages to the partition specified in each record, and should
   * succeed as long as the metadata has been updated with new topic id.
   */
  @Test
  def testSendWithRecreatedTopic(): Unit = {
    val numRecords = 10
    val topic = "topic"
    createTopic(topic)
    val admin = createAdminClient()
    val topicId = topicMetadata(admin, topic).topicId()
    val producer = createProducer()

    (1 to numRecords).foreach { i =>
      val resp = producer.send(new ProducerRecord(topic, null, ("value" + i).getBytes(StandardCharsets.UTF_8))).get
      assertEquals(topic, resp.topic())
    }
    // Start topic deletion
    deleteTopic(topic, listenerName)

    // Verify that the topic is deleted when no metadata request comes in
    TestUtils.verifyTopicDeletion(topic, 2, brokers)
    createTopic(topic)
    assertNotEquals(topicId, topicMetadata(admin, topic).topicId())

    // Producer should be able to send messages even after topic gets recreated
    val recordMetadata: RecordMetadata = producer.send(new ProducerRecord(topic, null, "value".getBytes(StandardCharsets.UTF_8))).get
    assertEquals(topic, recordMetadata.topic())
    assertEquals(0, recordMetadata.offset())
  }

  /**
   * Tests that Producer produce to topic during reassignment where topic metadata change on broker side.
   *
   * Producer will attempt to send messages to the partition specified in each record, and should
   * succeed as long as the metadata cache on the leader includes the partition topic id.
   */
  @Test
  def testSendWithTopicReassignmentIsMidWay(): Unit = {
    val numRecords = 10
    val topic = "topic"
    val partition0: TopicPartition = new TopicPartition(topic, 0)
    val admin: Admin = createAdminClient()

    // Create topic with leader as 0 for the 1 partition.
    createTopicWithAssignment(topic, Map(0 -> Seq(0)))
    TestUtils.assertLeader(admin, partition0, 0)

    val topicDetails = topicMetadata(admin, topic)
    val producer = createProducer()

    (1 to numRecords).foreach { i =>
      val resp = producer.send(new ProducerRecord(topic, null, ("value" + i).getBytes(StandardCharsets.UTF_8))).get
      assertEquals(topic, resp.topic())
    }

    val reassignment = Map(
      partition0 -> Optional.of(new NewPartitionReassignment(util.Arrays.asList(1))),
    )

    // Change replica assignment from 0 to 1. Leadership moves to 1.
    admin.alterPartitionReassignments(reassignment.asJava).all().get()
    TestUtils.assertLeader(admin, partition0, 1)
    assertEquals(topicDetails.topicId(), topicMetadata(admin, topic).topicId())

    // Producer should be able to send messages even after topic gets reassigned
    assertEquals(topic, producer.send(new ProducerRecord(topic, null, "value".getBytes(StandardCharsets.UTF_8))).get.topic())
  }

  private def topicMetadata(admin: Admin, topic: String): TopicDescription = {
    admin.describeTopics(util.Collections.singletonList(topic)).allTopicNames().get().get(topic)
  }
}
