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

import kafka.api.{GroupCoordinatorRequest, OffsetCommitRequest, OffsetFetchRequest}
import kafka.consumer.SimpleConsumer
import kafka.common.{OffsetAndMetadata, OffsetMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.utils._
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.{Time, Utils}
import org.junit.{After, Before, Test}
import org.junit.Assert._
import java.util.Properties
import java.io.File

import kafka.admin.AdminUtils

import scala.util.Random
import scala.collection._

class OffsetCommitTest extends ZooKeeperTestHarness {
  val random: Random = new Random()
  val group = "test-group"
  val retentionCheckInterval: Long = 100L
  var logDir: File = null
  var topicLogDir: File = null
  var server: KafkaServer = null
  var logSize: Int = 100
  var simpleConsumer: SimpleConsumer = null

  @Before
  override def setUp() {
    super.setUp()
    val config: Properties = createBrokerConfig(1, zkConnect,  enableDeleteTopic = true)
    config.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    config.setProperty(KafkaConfig.OffsetsRetentionCheckIntervalMsProp, retentionCheckInterval.toString)
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)
    server = TestUtils.createServer(KafkaConfig.fromProps(config), Time.SYSTEM)
    simpleConsumer = new SimpleConsumer("localhost", TestUtils.boundPort(server), 1000000, 64*1024, "test-client")
    val consumerMetadataRequest = GroupCoordinatorRequest(group)
    Stream.continually {
      val consumerMetadataResponse = simpleConsumer.send(consumerMetadataRequest)
      consumerMetadataResponse.coordinatorOpt.isDefined
    }.dropWhile(success => {
      if (!success) Thread.sleep(1000)
      !success
    })
  }

  @After
  override def tearDown() {
    simpleConsumer.close
    server.shutdown
    Utils.delete(logDir)
    super.tearDown()
  }

  @Test
  def testUpdateOffsets() {
    val topic = "topic"

    // Commit an offset
    val topicAndPartition = TopicAndPartition(topic, 0)
    val expectedReplicaAssignment = Map(0  -> List(1))
    // create the topic
    createTopic(zkUtils, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = Seq(server))

    val commitRequest = OffsetCommitRequest(group, immutable.Map(topicAndPartition -> OffsetAndMetadata(offset = 42L)))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)

    assertEquals(Errors.NONE.code, commitResponse.commitStatus.get(topicAndPartition).get)

    // Fetch it and verify
    val fetchRequest = OffsetFetchRequest(group, Seq(topicAndPartition))
    val fetchResponse = simpleConsumer.fetchOffsets(fetchRequest)

    assertEquals(Errors.NONE.code, fetchResponse.requestInfo.get(topicAndPartition).get.error)
    assertEquals(OffsetMetadata.NoMetadata, fetchResponse.requestInfo.get(topicAndPartition).get.metadata)
    assertEquals(42L, fetchResponse.requestInfo.get(topicAndPartition).get.offset)

    // Commit a new offset
    val commitRequest1 = OffsetCommitRequest(group, immutable.Map(topicAndPartition -> OffsetAndMetadata(
      offset=100L,
      metadata="some metadata"
    )))
    val commitResponse1 = simpleConsumer.commitOffsets(commitRequest1)

    assertEquals(Errors.NONE.code, commitResponse1.commitStatus.get(topicAndPartition).get)

    // Fetch it and verify
    val fetchRequest1 = OffsetFetchRequest(group, Seq(topicAndPartition))
    val fetchResponse1 = simpleConsumer.fetchOffsets(fetchRequest1)

    assertEquals(Errors.NONE.code, fetchResponse1.requestInfo.get(topicAndPartition).get.error)
    assertEquals("some metadata", fetchResponse1.requestInfo.get(topicAndPartition).get.metadata)
    assertEquals(100L, fetchResponse1.requestInfo.get(topicAndPartition).get.offset)

    // Fetch an unknown topic and verify
    val unknownTopicAndPartition = TopicAndPartition("unknownTopic", 0)
    val fetchRequest2 = OffsetFetchRequest(group, Seq(unknownTopicAndPartition))
    val fetchResponse2 = simpleConsumer.fetchOffsets(fetchRequest2)

    assertEquals(OffsetMetadataAndError.NoOffset, fetchResponse2.requestInfo.get(unknownTopicAndPartition).get)
    assertEquals(1, fetchResponse2.requestInfo.size)
  }

  @Test
  def testCommitAndFetchOffsets() {
    val topic1 = "topic-1"
    val topic2 = "topic-2"
    val topic3 = "topic-3"
    val topic4 = "topic-4" // Topic that group never consumes
    val topic5 = "topic-5" // Non-existent topic

    createTopic(zkUtils, topic1, servers = Seq(server), numPartitions = 1)
    createTopic(zkUtils, topic2, servers = Seq(server), numPartitions = 2)
    createTopic(zkUtils, topic3, servers = Seq(server), numPartitions = 1)
    createTopic(zkUtils, topic4, servers = Seq(server), numPartitions = 1)

    val commitRequest = OffsetCommitRequest("test-group", immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetAndMetadata(offset=42L, metadata="metadata one"),
      TopicAndPartition(topic2, 0) -> OffsetAndMetadata(offset=43L, metadata="metadata two"),
      TopicAndPartition(topic3, 0) -> OffsetAndMetadata(offset=44L, metadata="metadata three"),
      TopicAndPartition(topic2, 1) -> OffsetAndMetadata(offset=45L)
    ))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)
    assertEquals(Errors.NONE.code, commitResponse.commitStatus.get(TopicAndPartition(topic1, 0)).get)
    assertEquals(Errors.NONE.code, commitResponse.commitStatus.get(TopicAndPartition(topic2, 0)).get)
    assertEquals(Errors.NONE.code, commitResponse.commitStatus.get(TopicAndPartition(topic3, 0)).get)
    assertEquals(Errors.NONE.code, commitResponse.commitStatus.get(TopicAndPartition(topic2, 1)).get)

    val fetchRequest = OffsetFetchRequest(group, Seq(
      TopicAndPartition(topic1, 0),
      TopicAndPartition(topic2, 0),
      TopicAndPartition(topic3, 0),
      TopicAndPartition(topic2, 1),
      TopicAndPartition(topic3, 1), // An unknown partition
      TopicAndPartition(topic4, 0), // An unused topic
      TopicAndPartition(topic5, 0)  // An unknown topic
    ))
    val fetchResponse = simpleConsumer.fetchOffsets(fetchRequest)

    assertEquals(Errors.NONE.code, fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get.error)

    assertEquals(Errors.NONE.code, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get.error)
    assertEquals(Errors.NONE.code, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get.error)

    assertEquals(Errors.NONE.code, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get.error)
    assertEquals(Errors.NONE.code, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get.error)
    assertEquals(OffsetMetadataAndError.NoOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get)

    assertEquals(Errors.NONE.code, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get.error)
    assertEquals(OffsetMetadataAndError.NoOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get)

    assertEquals(Errors.NONE.code, fetchResponse.requestInfo.get(TopicAndPartition(topic5, 0)).get.error)
    assertEquals(OffsetMetadataAndError.NoOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic5, 0)).get)

    assertEquals("metadata one", fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get.metadata)
    assertEquals("metadata two", fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get.metadata)
    assertEquals("metadata three", fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get.metadata)

    assertEquals(OffsetMetadata.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get.metadata)
    assertEquals(OffsetMetadata.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get.metadata)
    assertEquals(OffsetMetadata.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get.metadata)
    assertEquals(OffsetMetadata.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic5, 0)).get.metadata)

    assertEquals(42L, fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get.offset)
    assertEquals(43L, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get.offset)
    assertEquals(44L, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get.offset)
    assertEquals(45L, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get.offset)

    assertEquals(OffsetMetadata.InvalidOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get.offset)
    assertEquals(OffsetMetadata.InvalidOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get.offset)
    assertEquals(OffsetMetadata.InvalidOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic5, 0)).get.offset)
  }

  @Test
  def testLargeMetadataPayload() {
    val topicAndPartition = TopicAndPartition("large-metadata", 0)
    val expectedReplicaAssignment = Map(0  -> List(1))
    createTopic(zkUtils, topicAndPartition.topic, partitionReplicaAssignment = expectedReplicaAssignment,
                servers = Seq(server))

    val commitRequest = OffsetCommitRequest("test-group", immutable.Map(topicAndPartition -> OffsetAndMetadata(
      offset=42L,
      metadata=random.nextString(server.config.offsetMetadataMaxSize)
    )))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)

    assertEquals(Errors.NONE.code, commitResponse.commitStatus.get(topicAndPartition).get)

    val commitRequest1 = OffsetCommitRequest(group, immutable.Map(topicAndPartition -> OffsetAndMetadata(
      offset=42L,
      metadata=random.nextString(server.config.offsetMetadataMaxSize + 1)
    )))
    val commitResponse1 = simpleConsumer.commitOffsets(commitRequest1)

    assertEquals(Errors.OFFSET_METADATA_TOO_LARGE.code, commitResponse1.commitStatus.get(topicAndPartition).get)
  }

  @Test
  def testOffsetExpiration() {
    // set up topic partition
    val topic = "topic"
    val topicPartition = TopicAndPartition(topic, 0)
    createTopic(zkUtils, topic, servers = Seq(server), numPartitions = 1)

    val fetchRequest = OffsetFetchRequest(group, Seq(TopicAndPartition(topic, 0)))

    // v0 version commit request
    // committed offset should not exist with fetch version 1 since it was stored in ZK
    val commitRequest0 = OffsetCommitRequest(
      groupId = group,
      requestInfo = immutable.Map(topicPartition -> OffsetAndMetadata(1L, "metadata")),
      versionId = 0
    )
    assertEquals(Errors.NONE.code, simpleConsumer.commitOffsets(commitRequest0).commitStatus.get(topicPartition).get)
    assertEquals(-1L, simpleConsumer.fetchOffsets(fetchRequest).requestInfo.get(topicPartition).get.offset)

    // committed offset should exist with fetch version 0
    val offsetFetchReq = OffsetFetchRequest(group, Seq(TopicAndPartition(topic, 0)), versionId = 0)
    val offsetFetchResp = simpleConsumer.fetchOffsets(offsetFetchReq)
    assertEquals(1L, offsetFetchResp.requestInfo.get(topicPartition).get.offset)


    // v1 version commit request with commit timestamp set to -1
    // committed offset should not expire
    val commitRequest1 = OffsetCommitRequest(
      groupId = group,
      requestInfo = immutable.Map(topicPartition -> OffsetAndMetadata(2L, "metadata", -1L)),
      versionId = 1
    )
    assertEquals(Errors.NONE.code, simpleConsumer.commitOffsets(commitRequest1).commitStatus.get(topicPartition).get)
    Thread.sleep(retentionCheckInterval * 2)
    assertEquals(2L, simpleConsumer.fetchOffsets(fetchRequest).requestInfo.get(topicPartition).get.offset)

    // v1 version commit request with commit timestamp set to now - two days
    // committed offset should expire
    val commitRequest2 = OffsetCommitRequest(
      groupId = group,
      requestInfo = immutable.Map(topicPartition -> OffsetAndMetadata(3L, "metadata", Time.SYSTEM.milliseconds - 2*24*60*60*1000L)),
      versionId = 1
    )
    assertEquals(Errors.NONE.code, simpleConsumer.commitOffsets(commitRequest2).commitStatus.get(topicPartition).get)
    Thread.sleep(retentionCheckInterval * 2)
    assertEquals(-1L, simpleConsumer.fetchOffsets(fetchRequest).requestInfo.get(topicPartition).get.offset)

    // v2 version commit request with retention time set to 1 hour
    // committed offset should not expire
    val commitRequest3 = OffsetCommitRequest(
      groupId = group,
      requestInfo = immutable.Map(topicPartition -> OffsetAndMetadata(4L, "metadata", -1L)),
      versionId = 2,
      retentionMs = 1000 * 60 * 60L
    )
    assertEquals(Errors.NONE.code, simpleConsumer.commitOffsets(commitRequest3).commitStatus.get(topicPartition).get)
    Thread.sleep(retentionCheckInterval * 2)
    assertEquals(4L, simpleConsumer.fetchOffsets(fetchRequest).requestInfo.get(topicPartition).get.offset)

    // v2 version commit request with retention time set to 0 second
    // committed offset should expire
    val commitRequest4 = OffsetCommitRequest(
      groupId = "test-group",
      requestInfo = immutable.Map(TopicAndPartition(topic, 0) -> OffsetAndMetadata(5L, "metadata", -1L)),
      versionId = 2,
      retentionMs = 0L
    )
    assertEquals(Errors.NONE.code, simpleConsumer.commitOffsets(commitRequest4).commitStatus.get(topicPartition).get)
    Thread.sleep(retentionCheckInterval * 2)
    assertEquals(-1L, simpleConsumer.fetchOffsets(fetchRequest).requestInfo.get(topicPartition).get.offset)

  }

  @Test
  def testNonExistingTopicOffsetCommit() {
    val topic1 = "topicDoesNotExists"
    val topic2 = "topic-2"

    createTopic(zkUtils, topic2, servers = Seq(server), numPartitions = 1)

    // Commit an offset
    val commitRequest = OffsetCommitRequest(group, immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetAndMetadata(offset=42L),
      TopicAndPartition(topic2, 0) -> OffsetAndMetadata(offset=42L)
    ))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)

    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, commitResponse.commitStatus.get(TopicAndPartition(topic1, 0)).get)
    assertEquals(Errors.NONE.code, commitResponse.commitStatus.get(TopicAndPartition(topic2, 0)).get)
  }

  @Test
  def testOffsetsDeleteAfterTopicDeletion() {
    // set up topic partition
    val topic = "topic"
    val topicPartition = TopicAndPartition(topic, 0)
    createTopic(zkUtils, topic, servers = Seq(server), numPartitions = 1)

    val commitRequest = OffsetCommitRequest(group, immutable.Map(topicPartition -> OffsetAndMetadata(offset = 42L)))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)

    assertEquals(Errors.NONE.code, commitResponse.commitStatus.get(topicPartition).get)

    // start topic deletion
    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, Seq(server))
    Thread.sleep(retentionCheckInterval * 2)

    // check if offsets deleted
    val fetchRequest = OffsetFetchRequest(group, Seq(TopicAndPartition(topic, 0)))
    val offsetMetadataAndErrorMap = simpleConsumer.fetchOffsets(fetchRequest)
    val offsetMetadataAndError = offsetMetadataAndErrorMap.requestInfo(topicPartition)
    assertEquals(OffsetMetadataAndError.NoOffset, offsetMetadataAndError)
  }

}
