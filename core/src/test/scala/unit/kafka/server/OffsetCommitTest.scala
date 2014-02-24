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

import java.io.File
import kafka.utils._
import junit.framework.Assert._
import java.util.Properties
import kafka.consumer.SimpleConsumer
import org.junit.{After, Before, Test}
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet, Message}
import kafka.zk.ZooKeeperTestHarness
import org.scalatest.junit.JUnit3Suite
import kafka.api.{OffsetCommitRequest, OffsetFetchRequest}
import kafka.utils.TestUtils._
import kafka.common.{ErrorMapping, TopicAndPartition, OffsetMetadataAndError}
import scala.util.Random
import kafka.admin.AdminUtils

class OffsetCommitTest extends JUnit3Suite with ZooKeeperTestHarness {
  val random: Random = new Random()
  var logDir: File = null
  var topicLogDir: File = null
  var server: KafkaServer = null
  var logSize: Int = 100
  val brokerPort: Int = 9099
  var simpleConsumer: SimpleConsumer = null
  var time: Time = new MockTime()

  @Before
  override def setUp() {
    super.setUp()
    val config: Properties = createBrokerConfig(1, brokerPort)
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)
    time = new MockTime()
    server = TestUtils.createServer(new KafkaConfig(config), time)
    simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64*1024, "test-client")
  }

  @After
  override def tearDown() {
    simpleConsumer.close
    server.shutdown
    Utils.rm(logDir)
    super.tearDown()
  }

  @Test
  def testUpdateOffsets() {
    val topic = "topic"

    // Commit an offset
    val topicAndPartition = TopicAndPartition(topic, 0)
    val expectedReplicaAssignment = Map(0  -> List(1))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    val leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)
    assertTrue("Leader should be elected after topic creation", leaderIdOpt.isDefined)
    val commitRequest = OffsetCommitRequest("test-group", Map(topicAndPartition -> OffsetMetadataAndError(offset=42L)))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)

    assertEquals(ErrorMapping.NoError, commitResponse.requestInfo.get(topicAndPartition).get)

    // Fetch it and verify
    val fetchRequest = OffsetFetchRequest("test-group", Seq(topicAndPartition))
    val fetchResponse = simpleConsumer.fetchOffsets(fetchRequest)

    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(topicAndPartition).get.error)
    //assertEquals(OffsetMetadataAndError.NoMetadata, fetchResponse.requestInfo.get(topicAndPartition).get.metadata)
    assertEquals(42L, fetchResponse.requestInfo.get(topicAndPartition).get.offset)

    // Commit a new offset
    val commitRequest1 = OffsetCommitRequest("test-group", Map(topicAndPartition -> OffsetMetadataAndError(
      offset=100L,
      metadata="some metadata"
    )))
    val commitResponse1 = simpleConsumer.commitOffsets(commitRequest1)

    assertEquals(ErrorMapping.NoError, commitResponse1.requestInfo.get(topicAndPartition).get)

    // Fetch it and verify
    val fetchRequest1 = OffsetFetchRequest("test-group", Seq(topicAndPartition))
    val fetchResponse1 = simpleConsumer.fetchOffsets(fetchRequest1)
    
    assertEquals(ErrorMapping.NoError, fetchResponse1.requestInfo.get(topicAndPartition).get.error)
    //assertEquals("some metadata", fetchResponse1.requestInfo.get(topicAndPartition).get.metadata)
    assertEquals(100L, fetchResponse1.requestInfo.get(topicAndPartition).get.offset)

  }

  @Test
  def testCommitAndFetchOffsets() {
    val topic1 = "topic-1"
    val topic2 = "topic-2"
    val topic3 = "topic-3"
    val topic4 = "topic-4"

    val expectedReplicaAssignment = Map(0  -> List(1))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic1, expectedReplicaAssignment)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic2, expectedReplicaAssignment)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic3, expectedReplicaAssignment)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic4, expectedReplicaAssignment)
    var leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic1, 0, 1000)
    assertTrue("Leader should be elected after topic creation", leaderIdOpt.isDefined)
    leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic2, 0, 1000)
    assertTrue("Leader should be elected after topic creation", leaderIdOpt.isDefined)
    leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic3, 0, 1000)
    assertTrue("Leader should be elected after topic creation", leaderIdOpt.isDefined)
    leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic4, 0, 1000)
    assertTrue("Leader should be elected after topic creation", leaderIdOpt.isDefined)

    val commitRequest = OffsetCommitRequest("test-group", Map(
      TopicAndPartition(topic1, 0) -> OffsetMetadataAndError(offset=42L, metadata="metadata one"),
      TopicAndPartition(topic2, 0) -> OffsetMetadataAndError(offset=43L, metadata="metadata two"),
      TopicAndPartition(topic3, 0) -> OffsetMetadataAndError(offset=44L, metadata="metadata three"),
      TopicAndPartition(topic2, 1) -> OffsetMetadataAndError(offset=45L)
    ))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)
    assertEquals(ErrorMapping.NoError, commitResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get)
    assertEquals(ErrorMapping.NoError, commitResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get)
    assertEquals(ErrorMapping.NoError, commitResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get)
    assertEquals(ErrorMapping.NoError, commitResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get)

    val fetchRequest = OffsetFetchRequest("test-group", Seq(
      TopicAndPartition(topic1, 0),
      TopicAndPartition(topic2, 0),
      TopicAndPartition(topic3, 0),
      TopicAndPartition(topic2, 1),
      TopicAndPartition(topic3, 1), // An unknown partition
      TopicAndPartition(topic4, 0)  // An unknown topic
    ))
    val fetchResponse = simpleConsumer.fetchOffsets(fetchRequest)

    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get.error)
    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get.error)
    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get.error)
    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get.error)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get.error)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get.error)

    //assertEquals("metadata one", fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get.metadata)
    //assertEquals("metadata two", fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get.metadata)
    //assertEquals("metadata three", fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get.metadata)
    //assertEquals(OffsetMetadataAndError.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get.metadata)
    //assertEquals(OffsetMetadataAndError.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get.metadata)
    //assertEquals(OffsetMetadataAndError.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get.metadata)

    assertEquals(42L, fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get.offset)
    assertEquals(43L, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get.offset)
    assertEquals(44L, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get.offset)
    assertEquals(45L, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get.offset)
    assertEquals(OffsetMetadataAndError.InvalidOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get.offset)
    assertEquals(OffsetMetadataAndError.InvalidOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get.offset)
  }

  @Test
  def testLargeMetadataPayload() {
    val topicAndPartition = TopicAndPartition("large-metadata", 0)
    val expectedReplicaAssignment = Map(0  -> List(1))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topicAndPartition.topic, expectedReplicaAssignment)
    var leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topicAndPartition.topic, 0, 1000)
    assertTrue("Leader should be elected after topic creation", leaderIdOpt.isDefined)

    val commitRequest = OffsetCommitRequest("test-group", Map(topicAndPartition -> OffsetMetadataAndError(
      offset=42L,
      metadata=random.nextString(server.config.offsetMetadataMaxSize)
    )))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)

    assertEquals(ErrorMapping.NoError, commitResponse.requestInfo.get(topicAndPartition).get)

    val commitRequest1 = OffsetCommitRequest("test-group", Map(topicAndPartition -> OffsetMetadataAndError(
      offset=42L,
      metadata=random.nextString(server.config.offsetMetadataMaxSize + 1)
    )))
    val commitResponse1 = simpleConsumer.commitOffsets(commitRequest1)

    assertEquals(ErrorMapping.OffsetMetadataTooLargeCode, commitResponse1.requestInfo.get(topicAndPartition).get)

  }

  @Test
  def testNullMetadata() {
    val topicAndPartition = TopicAndPartition("null-metadata", 0)
    val expectedReplicaAssignment = Map(0  -> List(1))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topicAndPartition.topic, expectedReplicaAssignment)
    var leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topicAndPartition.topic, 0, 1000)
    assertTrue("Leader should be elected after topic creation", leaderIdOpt.isDefined)
    val commitRequest = OffsetCommitRequest("test-group", Map(topicAndPartition -> OffsetMetadataAndError(
      offset=42L,
      metadata=null
    )))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)
    assertEquals(ErrorMapping.NoError, commitResponse.requestInfo.get(topicAndPartition).get)
  }
}
