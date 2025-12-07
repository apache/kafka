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

import java.util.Optional
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.test.TestUtils.isValidClusterId
import org.junit.jupiter.api.Assertions._

import scala.collection.Seq
import scala.jdk.CollectionConverters._

@ClusterTestDefaults(
  types = Array(Type.KRAFT, Type.CO_KRAFT),
  brokers = 3,
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "5"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(id = 0, key = "broker.rack", value = "rack/0"),
    new ClusterConfigProperty(id = 1, key = "broker.rack", value = "rack/1"),
    new ClusterConfigProperty(id = 2, key = "broker.rack", value = "rack/2")
  )
)
class MetadataRequestTest(cluster: ClusterInstance) extends AbstractMetadataRequestTest(cluster) {

  @ClusterTest
  def testClusterIdWithRequestVersion1(): Unit = {
    val v1MetadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    val v1ClusterId = v1MetadataResponse.clusterId
    assertNull(v1ClusterId, s"v1 clusterId should be null")
  }

  @ClusterTest
  def testClusterIdIsValid(): Unit = {
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(4.toShort))
    isValidClusterId(metadataResponse.clusterId)
  }

  @ClusterTest
  def testRack(): Unit = {
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(4.toShort))
    // Validate rack matches what's set in generateConfigs() above
    metadataResponse.brokers.forEach { broker =>
      assertEquals(s"rack/${broker.id}", broker.rack, "Rack information should match config")
    }
  }

  @ClusterTest
  def testIsInternal(): Unit = {
    val internalTopic = Topic.GROUP_METADATA_TOPIC_NAME
    val notInternalTopic = "notInternal"
    // create the topics
    cluster.createTopic(internalTopic, 3, 2)
    cluster.createTopic(notInternalTopic, 3, 2)

    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(4.toShort))
    assertTrue(metadataResponse.errors.isEmpty, "Response should have no errors")

    val topicMetadata = metadataResponse.topicMetadata.asScala
    val internalTopicMetadata = topicMetadata.find(_.topic == internalTopic).get
    val notInternalTopicMetadata = topicMetadata.find(_.topic == notInternalTopic).get

    assertTrue(internalTopicMetadata.isInternal, "internalTopic should show isInternal")
    assertFalse(notInternalTopicMetadata.isInternal, "notInternalTopic topic not should show isInternal")

    assertEquals(Set(internalTopic).asJava, metadataResponse.buildCluster().internalTopics)
  }

  @ClusterTest
  def testNoTopicsRequest(): Unit = {
    // create some topics
    cluster.createTopic("t1", 3, 2)
    cluster.createTopic("t2", 3, 2)

    val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List[String]().asJava, true, 4.toShort).build)
    assertTrue(metadataResponse.errors.isEmpty, "Response should have no errors")
    assertTrue(metadataResponse.topicMetadata.isEmpty, "Response should have no topics")
  }

  @ClusterTest
  def testAutoTopicCreation(): Unit = {
    val topic1 = "t1"
    val topic2 = "t2"
    val topic3 = "t3"
    val topic4 = "t4"
    val topic5 = "t5"
    cluster.createTopic(topic1, 1, 1)

    val response1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true).build())
    assertNull(response1.errors.get(topic1))
    checkAutoCreatedTopic(topic2, response1)

    // The default behavior in old versions of the metadata API is to allow topic creation, so
    // protocol downgrades should happen gracefully when auto-creation is explicitly requested.
    val response2 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic3).asJava, true).build(1))
    checkAutoCreatedTopic(topic3, response2)

    // V3 doesn't support a configurable allowAutoTopicCreation, so disabling auto-creation is not supported
    assertThrows(classOf[UnsupportedVersionException], () => sendMetadataRequest(new MetadataRequest(requestData(List(topic4), allowAutoTopicCreation = false), 3.toShort)))

    // V4 and higher support a configurable allowAutoTopicCreation
    val response3 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic4, topic5).asJava, false, 4.toShort).build)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response3.errors.get(topic4))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response3.errors.get(topic5))
  }

  @ClusterTest(brokers = 3, serverProperties = Array(new ClusterConfigProperty(key = ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, value = "3")))
  def testAutoCreateTopicWithInvalidReplicationFactor(): Unit = {
    // Shutdown all but one broker so that the number of brokers is less than the default replication factor
    brokers.tail.foreach(_.shutdown())
    brokers.tail.foreach(_.awaitShutdown())

    val topic1 = "testAutoCreateTopic"
    val response1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1).asJava, true).build)
    assertEquals(1, response1.topicMetadata.size)
    val topicMetadata = response1.topicMetadata.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicMetadata.error)
    assertEquals(topic1, topicMetadata.topic)
    assertEquals(0, topicMetadata.partitionMetadata.size)
  }

  @ClusterTest
  def testAllTopicsRequest(): Unit = {
    // create some topics
    cluster.createTopic("t1", 3, 2)
    cluster.createTopic("t2", 3, 2)

    // v0, Empty list represents all topics
    val metadataResponseV0 = sendMetadataRequest(new MetadataRequest(requestData(List(), allowAutoTopicCreation = true), 0.toShort))
    assertTrue(metadataResponseV0.errors.isEmpty, "V0 Response should have no errors")
    assertEquals(2, metadataResponseV0.topicMetadata.size(), "V0 Response should have 2 (all) topics")

    // v1, Null represents all topics
    val metadataResponseV1 = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    assertTrue(metadataResponseV1.errors.isEmpty, "V1 Response should have no errors")
    assertEquals(2, metadataResponseV1.topicMetadata.size(), "V1 Response should have 2 (all) topics")
  }

  @ClusterTest
  def testTopicIdsInResponse(): Unit = {
    val replicaAssignment = getJavaReplicaAssignment(Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1)))
    val topic1 = "topic1"
    val topic2 = "topic2"
    cluster.createTopicWithAssignment(topic1, replicaAssignment)
    cluster.createTopicWithAssignment(topic2, replicaAssignment)

    // if version < 9, return ZERO_UUID in MetadataResponse
    val resp1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true, 0, 9).build(), Some(anySocketServer))
    assertEquals(2, resp1.topicMetadata.size)
    resp1.topicMetadata.forEach { topicMetadata =>
      assertEquals(Errors.NONE, topicMetadata.error)
      assertEquals(Uuid.ZERO_UUID, topicMetadata.topicId())
    }

    // from version 10, UUID will be included in MetadataResponse
    val resp2 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true, 10, 10).build(), Some(anySocketServer))
    assertEquals(2, resp2.topicMetadata.size)
    resp2.topicMetadata.forEach { topicMetadata =>
      assertEquals(Errors.NONE, topicMetadata.error)
      assertNotEquals(Uuid.ZERO_UUID, topicMetadata.topicId())
      assertNotNull(topicMetadata.topicId())
    }
  }

  /**
    * Preferred replica should be the first item in the replicas list
    */
  @ClusterTest
  def testPreferredReplica(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1))
    val javaReplicaAssignment = getJavaReplicaAssignment(replicaAssignment)
    cluster.createTopicWithAssignment("t1", javaReplicaAssignment)
    // Test metadata on two different brokers to ensure that metadata propagation works correctly
    val responses = Seq(0, 1).map(index =>
      sendMetadataRequest(new MetadataRequest.Builder(Seq("t1").asJava, true).build(),
        Some(brokers(index).socketServer)))
    responses.foreach { response =>
      assertEquals(1, response.topicMetadata.size)
      val topicMetadata = response.topicMetadata.iterator.next()
      assertEquals(Errors.NONE, topicMetadata.error)
      assertEquals("t1", topicMetadata.topic)
      assertEquals(Set(0, 1), topicMetadata.partitionMetadata.asScala.map(_.partition).toSet)
      topicMetadata.partitionMetadata.forEach { partitionMetadata =>
        val assignment = replicaAssignment(partitionMetadata.partition)
        assertEquals(assignment, partitionMetadata.replicaIds.asScala)
        assertEquals(assignment, partitionMetadata.inSyncReplicaIds.asScala)
        assertEquals(Optional.of(assignment.head), partitionMetadata.leaderId)
      }
    }
  }

  @ClusterTest
  def testPartitionInfoPreferredReplica(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 2, 0))
    val javaReplicaAssignment = getJavaReplicaAssignment(replicaAssignment)
    val topic = "testPartitionInfoPreferredReplicaTopic"
    cluster.createTopicWithAssignment(topic, javaReplicaAssignment)

    val response = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic).asJava, true).build())
    val snapshot = response.buildCluster()
    val partitionInfos = snapshot.partitionsForTopic(topic).asScala
    assertEquals(1, partitionInfos.size)

    val partitionInfo = partitionInfos.head
    val preferredReplicaId = replicaAssignment(partitionInfo.partition()).head
    assertEquals(preferredReplicaId, partitionInfo.replicas().head.id())
  }

  @ClusterTest
  def testReplicaDownResponse(): Unit = {
    val replicaDownTopic = "replicaDown"
    val replicaCount = 3.toShort

    // create a topic with 3 replicas
    cluster.createTopic(replicaDownTopic, 1, replicaCount)

    // Kill a replica node that is not the leader
    val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List(replicaDownTopic).asJava, true).build())
    val partitionMetadata = metadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    val downNode = brokers.find { broker =>
      val serverId = broker.dataPlaneRequestProcessor.brokerId
      val leaderId = partitionMetadata.leaderId
      val replicaIds = partitionMetadata.replicaIds.asScala
      leaderId.isPresent && leaderId.get() != serverId && replicaIds.contains(serverId)
    }.get
    downNode.shutdown()

    TestUtils.waitUntilTrue(() => {
      val response = sendMetadataRequest(new MetadataRequest.Builder(List(replicaDownTopic).asJava, true).build())
      !response.brokers.asScala.exists(_.id == downNode.dataPlaneRequestProcessor.brokerId)
    }, "Replica was not found down", 50000)

    // Validate version 0 still filters unavailable replicas and contains error
    val v0MetadataResponse = sendMetadataRequest(new MetadataRequest(requestData(List(replicaDownTopic), allowAutoTopicCreation = true), 0.toShort))
    val v0BrokerIds = v0MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue(v0MetadataResponse.errors.isEmpty, "Response should have no errors")
    assertFalse(v0BrokerIds.contains(downNode.config.brokerId), s"The downed broker should not be in the brokers list")
    assertTrue(v0MetadataResponse.topicMetadata.size == 1, "Response should have one topic")
    val v0PartitionMetadata = v0MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertTrue(v0PartitionMetadata.error == Errors.REPLICA_NOT_AVAILABLE, "PartitionMetadata should have an error")
    assertTrue(v0PartitionMetadata.replicaIds.size == replicaCount - 1, s"Response should have ${replicaCount - 1} replicas")

    // Validate version 1 returns unavailable replicas with no error
    val v1MetadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List(replicaDownTopic).asJava, true).build(1))
    val v1BrokerIds = v1MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue(v1MetadataResponse.errors.isEmpty, "Response should have no errors")
    assertFalse(v1BrokerIds.contains(downNode.config.brokerId), s"The downed broker should not be in the brokers list")
    assertEquals(1, v1MetadataResponse.topicMetadata.size, "Response should have one topic")
    val v1PartitionMetadata = v1MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertEquals(Errors.NONE, v1PartitionMetadata.error, "PartitionMetadata should have no errors")
    assertEquals(replicaCount, v1PartitionMetadata.replicaIds.size, s"Response should have $replicaCount replicas")
  }

  @ClusterTest
  def testIsrAfterBrokerShutDownAndJoinsBack(): Unit = {
    def checkIsr[B <: KafkaBroker](
      brokers: Seq[B],
      topic: String
    ): Unit = {
      val activeBrokers = brokers.filter(_.brokerState != BrokerState.NOT_RUNNING)
      val expectedIsr = activeBrokers.map(_.config.brokerId).toSet

      // Assert that topic metadata at new brokers is updated correctly
      activeBrokers.foreach { broker =>
        var actualIsr = Set.empty[Int]
        TestUtils.waitUntilTrue(() => {
          val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic).asJava, false).build,
            Some(broker.socketServer))
          val firstPartitionMetadata = metadataResponse.topicMetadata.asScala.headOption.flatMap(_.partitionMetadata.asScala.headOption)
          actualIsr = firstPartitionMetadata.map { partitionMetadata =>
            partitionMetadata.inSyncReplicaIds.asScala.map(Int.unbox).toSet
          }.getOrElse(Set.empty)
          expectedIsr == actualIsr
        }, s"Topic metadata not updated correctly in broker $broker\n" +
          s"Expected ISR: $expectedIsr \n" +
          s"Actual ISR : $actualIsr")
      }
    }

    val topic = "isr-after-broker-shutdown"
    val replicaCount = 3.toShort
    cluster.createTopic(topic, 1, replicaCount)

    brokers.last.shutdown()
    brokers.last.awaitShutdown()
    brokers.last.startup()

    checkIsr(brokers, topic)
  }

  @ClusterTest
  def testAliveBrokersWithNoTopics(): Unit = {
    def checkMetadata[B <: KafkaBroker](
      brokers: Seq[B],
      expectedBrokersCount: Int
    ): Unit = {
      var response: Option[MetadataResponse] = None
      TestUtils.waitUntilTrue(() => {
        val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build,
          Some(anySocketServer))
        response = Some(metadataResponse)
        metadataResponse.brokers.size == expectedBrokersCount
      }, s"Expected $expectedBrokersCount brokers, but there are ${response.get.brokers.size}")

      val brokersSorted = response.get.brokers.asScala.toSeq.sortBy(_.id)

      // Assert that metadata is propagated correctly
      brokers.filter(_.brokerState == BrokerState.RUNNING).foreach { broker =>
        TestUtils.waitUntilTrue(() => {
          val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build,
            Some(broker.socketServer))
          val brokers = metadataResponse.brokers.asScala.toSeq.sortBy(_.id)
          val topicMetadata = metadataResponse.topicMetadata.asScala.toSeq.sortBy(_.topic)
          brokersSorted == brokers && metadataResponse.topicMetadata.asScala.toSeq.sortBy(_.topic) == topicMetadata
        }, s"Topic metadata not updated correctly")
      }
    }

    val brokerToShutdown = brokers.last
    brokerToShutdown.shutdown()
    brokerToShutdown.awaitShutdown()
    checkMetadata(brokers, brokers.size - 1)

    brokerToShutdown.startup()
    checkMetadata(brokers, brokers.size)
  }
}
