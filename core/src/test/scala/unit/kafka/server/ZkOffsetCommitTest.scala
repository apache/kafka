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

package unit.kafka.server

import java.util.Properties
import kafka.api.{OffsetCommitRequest, OffsetFetchRequest}
import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection._

class ZkOffsetCommitTest extends ZooKeeperTestHarness {
  val group = "test-group"
  val retentionCheckInterval: Long = 100L
  var server: KafkaServer = null
  var simpleConsumer: SimpleConsumer = null

  @Before
  override def setUp() {
    super.setUp()
    val config: Properties = createBrokerConfig(1, zkConnect,  enableDeleteTopic = true)
    config.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    config.setProperty(KafkaConfig.OffsetsRetentionCheckIntervalMsProp, retentionCheckInterval.toString)
    server = TestUtils.createServer(KafkaConfig.fromProps(config), Time.SYSTEM)
    simpleConsumer = new SimpleConsumer("localhost", TestUtils.boundPort(server), 1000000, 64*1024, "test-client")
  }

  @After
  override def tearDown() {
    simpleConsumer.close()
    TestUtils.shutdownServers(Seq(server))
    super.tearDown()
  }

  @Test
  def testCommitAndFetchOffsetsFromZk() {
    val topic1 = "topic-1"
    val topic2 = "topic-2"
    val topic3 = "topic-3"
    val topic4 = "topic-4" // Topic that group never consumes
    val topic5 = "topic-5" // Non-existent topic

    createTopic(zkClient, topic1, servers = Seq(server), numPartitions = 1)
    createTopic(zkClient, topic2, servers = Seq(server), numPartitions = 2)
    createTopic(zkClient, topic3, servers = Seq(server), numPartitions = 1)
    createTopic(zkClient, topic4, servers = Seq(server), numPartitions = 1)

    val commitRequest = OffsetCommitRequest(group, immutable.Map(
      TopicAndPartition(topic1, 0) -> OffsetAndMetadata(offset=42L, metadata="metadata one"),
      TopicAndPartition(topic2, 0) -> OffsetAndMetadata(offset=43L, metadata="metadata two"),
      TopicAndPartition(topic2, 1) -> OffsetAndMetadata(offset=44L),
      TopicAndPartition(topic3, 0) -> OffsetAndMetadata(offset=45L, metadata="metadata three")
    ), versionId = 0)
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)
    assertEquals(Errors.NONE, commitResponse.commitStatus(TopicAndPartition(topic1, 0)))
    assertEquals(Errors.NONE, commitResponse.commitStatus(TopicAndPartition(topic2, 0)))
    assertEquals(Errors.NONE, commitResponse.commitStatus(TopicAndPartition(topic3, 0)))
    assertEquals(Errors.NONE, commitResponse.commitStatus(TopicAndPartition(topic2, 1)))

    val fetchRequest = OffsetFetchRequest(group, Seq(
      TopicAndPartition(topic1, 0),
      TopicAndPartition(topic2, 0),
      TopicAndPartition(topic2, 1),
      TopicAndPartition(topic3, 0),
      TopicAndPartition(topic3, 1), // An unknown partition
      TopicAndPartition(topic4, 0), // An unused topic
      TopicAndPartition(topic5, 0)  // An unknown topic
    ), versionId = 0)
    val fetchResponse = simpleConsumer.fetchOffsets(fetchRequest)

    assertEquals(Errors.NONE, fetchResponse.requestInfo(TopicAndPartition(topic1, 0)).error)

    assertEquals(Errors.NONE, fetchResponse.requestInfo(TopicAndPartition(topic2, 0)).error)
    assertEquals(Errors.NONE, fetchResponse.requestInfo(TopicAndPartition(topic2, 1)).error)

    assertEquals(Errors.NONE, fetchResponse.requestInfo(TopicAndPartition(topic3, 0)).error)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, fetchResponse.requestInfo(TopicAndPartition(topic3, 1)).error)
    assertEquals(OffsetMetadataAndError.UnknownTopicOrPartition, fetchResponse.requestInfo(TopicAndPartition(topic3, 1)))

    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, fetchResponse.requestInfo(TopicAndPartition(topic4, 0)).error)
    assertEquals(OffsetMetadataAndError.UnknownTopicOrPartition, fetchResponse.requestInfo(TopicAndPartition(topic4, 0)))

    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, fetchResponse.requestInfo(TopicAndPartition(topic5, 0)).error)
    assertEquals(OffsetMetadataAndError.UnknownTopicOrPartition, fetchResponse.requestInfo(TopicAndPartition(topic5, 0)))

    assertEquals(42L, fetchResponse.requestInfo(TopicAndPartition(topic1, 0)).offset)
    assertEquals(43L, fetchResponse.requestInfo(TopicAndPartition(topic2, 0)).offset)
    assertEquals(44L, fetchResponse.requestInfo(TopicAndPartition(topic2, 1)).offset)
    assertEquals(45L, fetchResponse.requestInfo(TopicAndPartition(topic3, 0)).offset)
    assertEquals(-1L, fetchResponse.requestInfo(TopicAndPartition(topic3, 1)).offset)
    assertEquals(-1L, fetchResponse.requestInfo(TopicAndPartition(topic4, 0)).offset)
    assertEquals(-1L, fetchResponse.requestInfo(TopicAndPartition(topic5, 0)).offset)

  }

}
