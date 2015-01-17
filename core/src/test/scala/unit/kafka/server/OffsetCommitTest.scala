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
import kafka.zk.ZooKeeperTestHarness
import org.scalatest.junit.JUnit3Suite
import kafka.api.{ConsumerMetadataRequest, OffsetCommitRequest, OffsetFetchRequest}
import kafka.utils.TestUtils._
import kafka.common.{OffsetMetadataAndError, OffsetAndMetadata, ErrorMapping, TopicAndPartition}
import scala.util.Random
import scala.collection._

class OffsetCommitTest extends JUnit3Suite with ZooKeeperTestHarness {
  val random: Random = new Random()
  var logDir: File = null
  var topicLogDir: File = null
  var server: KafkaServer = null
  var logSize: Int = 100
  val brokerPort: Int = 9099
  val group = "test-group"
  var simpleConsumer: SimpleConsumer = null
  var time: Time = new MockTime()

  @Before
  override def setUp() {
    super.setUp()
    val config: Properties = createBrokerConfig(1, brokerPort)
    // TODO: Currently, when there is no topic in a cluster, the controller doesn't send any UpdateMetadataRequest to
    // the broker. As a result, the live broker list in metadataCache is empty. This causes the ConsumerMetadataRequest
    // to fail since if the number of live brokers is 0, we try to create the offset topic with the default
    // offsets.topic.replication.factor of 3. The creation will fail since there is not enough live brokers. In order
    // for the unit test to pass, overriding offsets.topic.replication.factor to 1 for now. When we fix KAFKA-1867, we
    // need to remove the following config override.
    config.put("offsets.topic.replication.factor", "1")
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)
    time = new MockTime()
    server = TestUtils.createServer(new KafkaConfig(config), time)
    simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64*1024, "test-client")
    val consumerMetadataRequest = ConsumerMetadataRequest(group)
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
    createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = Seq(server))

    val commitRequest = OffsetCommitRequest(group, immutable.Map(topicAndPartition -> OffsetAndMetadata(offset=42L)))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)

    assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(topicAndPartition).get)

    // Fetch it and verify
    val fetchRequest = OffsetFetchRequest(group, Seq(topicAndPartition))
    val fetchResponse = simpleConsumer.fetchOffsets(fetchRequest)

    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(topicAndPartition).get.error)
    assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(topicAndPartition).get.metadata)
    assertEquals(42L, fetchResponse.requestInfo.get(topicAndPartition).get.offset)

    // Commit a new offset
    val commitRequest1 = OffsetCommitRequest(group, immutable.Map(topicAndPartition -> OffsetAndMetadata(
      offset=100L,
      metadata="some metadata"
    )))
    val commitResponse1 = simpleConsumer.commitOffsets(commitRequest1)

    assertEquals(ErrorMapping.NoError, commitResponse1.commitStatus.get(topicAndPartition).get)

    // Fetch it and verify
    val fetchRequest1 = OffsetFetchRequest(group, Seq(topicAndPartition))
    val fetchResponse1 = simpleConsumer.fetchOffsets(fetchRequest1)

    assertEquals(ErrorMapping.NoError, fetchResponse1.requestInfo.get(topicAndPartition).get.error)
    assertEquals("some metadata", fetchResponse1.requestInfo.get(topicAndPartition).get.metadata)
    assertEquals(100L, fetchResponse1.requestInfo.get(topicAndPartition).get.offset)

    // Fetch an unknown topic and verify
    val unknownTopicAndPartition = TopicAndPartition("unknownTopic", 0)
    val fetchRequest2 = OffsetFetchRequest(group, Seq(unknownTopicAndPartition))
    val fetchResponse2 = simpleConsumer.fetchOffsets(fetchRequest2)

    assertEquals(OffsetMetadataAndError.UnknownTopicOrPartition, fetchResponse2.requestInfo.get(unknownTopicAndPartition).get)
    assertEquals(1, fetchResponse2.requestInfo.size)
  }

  @Test
  def testCommitAndFetchOffsets() {
    val topic1 = "topic-1"
    val topic2 = "topic-2"
    val topic3 = "topic-3"
    val topic4 = "topic-4" // Topic that group never consumes
    val topic5 = "topic-5" // Non-existent topic

    createTopic(zkClient, topic1, servers = Seq(server), numPartitions = 1)
    createTopic(zkClient, topic2, servers = Seq(server), numPartitions = 2)
    createTopic(zkClient, topic3, servers = Seq(server), numPartitions = 1)
    createTopic(zkClient, topic4, servers = Seq(server), numPartitions = 1)

    val commitRequest = OffsetCommitRequest("test-group", immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetAndMetadata(offset=42L, metadata="metadata one"),
      TopicAndPartition(topic2, 0) -> OffsetAndMetadata(offset=43L, metadata="metadata two"),
      TopicAndPartition(topic3, 0) -> OffsetAndMetadata(offset=44L, metadata="metadata three"),
      TopicAndPartition(topic2, 1) -> OffsetAndMetadata(offset=45L)
    ))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)
    assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(TopicAndPartition(topic1, 0)).get)
    assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(TopicAndPartition(topic2, 0)).get)
    assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(TopicAndPartition(topic3, 0)).get)
    assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(TopicAndPartition(topic2, 1)).get)

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

    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get.error)
    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get.error)
    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get.error)
    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get.error)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get.error)
    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get.error)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode, fetchResponse.requestInfo.get(TopicAndPartition(topic5, 0)).get.error)
    assertEquals(OffsetMetadataAndError.UnknownTopicOrPartition, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get)
    assertEquals(OffsetMetadataAndError.NoOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get)
    assertEquals(OffsetMetadataAndError.UnknownTopicOrPartition, fetchResponse.requestInfo.get(TopicAndPartition(topic5, 0)).get)

    assertEquals("metadata one", fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get.metadata)
    assertEquals("metadata two", fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get.metadata)
    assertEquals("metadata three", fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get.metadata)
    assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get.metadata)
    assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get.metadata)
    assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get.metadata)
    assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(TopicAndPartition(topic5, 0)).get.metadata)

    assertEquals(42L, fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get.offset)
    assertEquals(43L, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get.offset)
    assertEquals(44L, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get.offset)
    assertEquals(45L, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get.offset)
    assertEquals(OffsetAndMetadata.InvalidOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 1)).get.offset)
    assertEquals(OffsetAndMetadata.InvalidOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic4, 0)).get.offset)
    assertEquals(OffsetAndMetadata.InvalidOffset, fetchResponse.requestInfo.get(TopicAndPartition(topic5, 0)).get.offset)
  }

  @Test
  def testLargeMetadataPayload() {
    val topicAndPartition = TopicAndPartition("large-metadata", 0)
    val expectedReplicaAssignment = Map(0  -> List(1))
    createTopic(zkClient, topicAndPartition.topic, partitionReplicaAssignment = expectedReplicaAssignment,
                servers = Seq(server))

    val commitRequest = OffsetCommitRequest("test-group", immutable.Map(topicAndPartition -> OffsetAndMetadata(
      offset=42L,
      metadata=random.nextString(server.config.offsetMetadataMaxSize)
    )))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)

    assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(topicAndPartition).get)

    val commitRequest1 = OffsetCommitRequest(group, immutable.Map(topicAndPartition -> OffsetAndMetadata(
      offset=42L,
      metadata=random.nextString(server.config.offsetMetadataMaxSize + 1)
    )))
    val commitResponse1 = simpleConsumer.commitOffsets(commitRequest1)

    assertEquals(ErrorMapping.OffsetMetadataTooLargeCode, commitResponse1.commitStatus.get(topicAndPartition).get)

  }
}
