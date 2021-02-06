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

package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataRequest
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class MetadataRequestWithForwardingTest extends AbstractMetadataRequestTest {

  @BeforeEach
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)
  }

  override def enableForwarding: Boolean = true

  @Test
  def testAutoTopicCreation(): Unit = {
    val topic1 = "t1"
    val topic2 = "t2"
    val topic3 = "t3"
    val topic4 = "t4"
    val topic5 = "t5"
    createTopic(topic1)

    val response1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true).build())
    assertNull(response1.errors.get(topic1))
    checkAutoCreatedTopic(topic2, response1)

    // The default behavior in old versions of the metadata API is to allow topic creation, so
    // protocol downgrades should happen gracefully when auto-creation is explicitly requested.
    val response2 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic3).asJava, true).build(1))
    checkAutoCreatedTopic(topic3, response2)

    // V3 doesn't support a configurable allowAutoTopicCreation, so disabling auto-creation is not supported
    assertThrows(classOf[UnsupportedVersionException], () => sendMetadataRequest(new MetadataRequest(requestData(List(topic4), false), 3.toShort)))

    // V4 and higher support a configurable allowAutoTopicCreation
    val response3 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic4, topic5).asJava, false, 4.toShort).build)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response3.errors.get(topic4))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response3.errors.get(topic5))
    assertEquals(None, zkClient.getTopicPartitionCount(topic5))
  }

  @Test
  def testAutoCreateTopicWithInvalidReplicationFactor(): Unit = {
    // Shutdown all but one broker so that the number of brokers is less than the default replication factor
    servers.tail.foreach(_.shutdown())
    servers.tail.foreach(_.awaitShutdown())

    val topic1 = "testAutoCreateTopic"
    val response1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1).asJava, true).build)
    assertEquals(1, response1.topicMetadata.size)
    val topicMetadata = response1.topicMetadata.asScala.head
    assertEquals(Errors.INVALID_REPLICATION_FACTOR, topicMetadata.error)
    assertEquals(topic1, topicMetadata.topic)
    assertEquals(0, topicMetadata.partitionMetadata.size)
  }

  @Test
  def testAutoCreateOfCollidingTopics(): Unit = {
    val topic1 = "testAutoCreate.Topic"
    val topic2 = "testAutoCreate_Topic"
    val response1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true).build)
    assertEquals(2, response1.topicMetadata.size)

    val responseMap = response1.topicMetadata.asScala.map(metadata => (metadata.topic(), metadata.error)).toMap

    assertEquals(Set(topic1, topic2), responseMap.keySet)
    // The topic creation will be delayed, and the name collision error will be swallowed.
    assertEquals(Set(Errors.LEADER_NOT_AVAILABLE, Errors.INVALID_TOPIC_EXCEPTION), responseMap.values.toSet)

    val topicCreated = responseMap.head._1
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topicCreated, 0)
    TestUtils.waitForPartitionMetadata(servers, topicCreated, 0)

    // retry the metadata for the first auto created topic
    val response2 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topicCreated).asJava, true).build)
    val topicMetadata1 = response2.topicMetadata.asScala.head
    assertEquals(Errors.NONE, topicMetadata1.error)
    assertEquals(Seq(Errors.NONE), topicMetadata1.partitionMetadata.asScala.map(_.error))
    assertEquals(1, topicMetadata1.partitionMetadata.size)
    val partitionMetadata = topicMetadata1.partitionMetadata.asScala.head
    assertEquals(0, partitionMetadata.partition)
    assertEquals(2, partitionMetadata.replicaIds.size)
    assertTrue(partitionMetadata.leaderId.isPresent)
    assertTrue(partitionMetadata.leaderId.get >= 0)
  }
}
