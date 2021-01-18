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

import java.util.{Optional, Properties}

import kafka.network.SocketServer
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.apache.kafka.test.TestUtils.isValidClusterId
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.jdk.CollectionConverters._
import scala.collection.Seq

class MetadataRequestTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.setProperty(KafkaConfig.DefaultReplicationFactorProp, "2")
    properties.setProperty(KafkaConfig.RackProp, s"rack/${properties.getProperty(KafkaConfig.BrokerIdProp)}")
  }

  @BeforeEach
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)
  }

  @Test
  def testClusterIdWithRequestVersion1(): Unit = {
    val v1MetadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    val v1ClusterId = v1MetadataResponse.clusterId
    assertNull(v1ClusterId, s"v1 clusterId should be null")
  }

  @Test
  def testClusterIdIsValid(): Unit = {
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(2.toShort))
    isValidClusterId(metadataResponse.clusterId)
  }

  @Test
  def testControllerId(): Unit = {
    val controllerServer = servers.find(_.kafkaController.isActive).get
    val controllerId = controllerServer.config.brokerId
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))

    assertEquals(controllerId,
      metadataResponse.controller.id, "Controller id should match the active controller")

    // Fail over the controller
    controllerServer.shutdown()
    controllerServer.startup()

    val controllerServer2 = servers.find(_.kafkaController.isActive).get
    val controllerId2 = controllerServer2.config.brokerId
    assertNotEquals(controllerId, controllerId2, "Controller id should switch to a new broker")
    TestUtils.waitUntilTrue(() => {
      val metadataResponse2 = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
      metadataResponse2.controller != null && controllerServer2.dataPlaneRequestProcessor.brokerId == metadataResponse2.controller.id
    }, "Controller id should match the active controller after failover", 5000)
  }

  @Test
  def testRack(): Unit = {
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    // Validate rack matches what's set in generateConfigs() above
    metadataResponse.brokers.forEach { broker =>
      assertEquals(s"rack/${broker.id}", broker.rack, "Rack information should match config")
    }
  }

  @Test
  def testIsInternal(): Unit = {
    val internalTopic = Topic.GROUP_METADATA_TOPIC_NAME
    val notInternalTopic = "notInternal"
    // create the topics
    createTopic(internalTopic, 3, 2)
    createTopic(notInternalTopic, 3, 2)

    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    assertTrue(metadataResponse.errors.isEmpty, "Response should have no errors")

    val topicMetadata = metadataResponse.topicMetadata.asScala
    val internalTopicMetadata = topicMetadata.find(_.topic == internalTopic).get
    val notInternalTopicMetadata = topicMetadata.find(_.topic == notInternalTopic).get

    assertTrue(internalTopicMetadata.isInternal, "internalTopic should show isInternal")
    assertFalse(notInternalTopicMetadata.isInternal, "notInternalTopic topic not should show isInternal")

    assertEquals(Set(internalTopic).asJava, metadataResponse.cluster.internalTopics)
  }

  @Test
  def testNoTopicsRequest(): Unit = {
    // create some topics
    createTopic("t1", 3, 2)
    createTopic("t2", 3, 2)

    // v0, Doesn't support a "no topics" request
    // v1, Empty list represents "no topics"
    val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List[String]().asJava, true, 1.toShort).build)
    assertTrue(metadataResponse.errors.isEmpty, "Response should have no errors")
    assertTrue(metadataResponse.topicMetadata.isEmpty, "Response should have no topics")
  }

  @Test
  def testAutoTopicCreation(): Unit = {
    def checkAutoCreatedTopic(autoCreatedTopic: String, response: MetadataResponse): Unit = {
      assertEquals(Errors.LEADER_NOT_AVAILABLE, response.errors.get(autoCreatedTopic))
      assertEquals(Some(servers.head.config.numPartitions), zkClient.getTopicPartitionCount(autoCreatedTopic))
      for (i <- 0 until servers.head.config.numPartitions)
        TestUtils.waitUntilMetadataIsPropagated(servers, autoCreatedTopic, i)
    }

    val topic1 = "t1"
    val topic2 = "t2"
    val topic3 = "t3"
    val topic4 = "t4"
    val topic5 = "t5"
    createTopic(topic1, numPartitions = 1, replicationFactor = 1)

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
    val topic1 = "testAutoCreate_Topic"
    val topic2 = "testAutoCreate.Topic"
    val response1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true).build)
    assertEquals(2, response1.topicMetadata.size)
    var topicMetadata1 = response1.topicMetadata.asScala.head
    val topicMetadata2 = response1.topicMetadata.asScala.toSeq(1)
    assertEquals(Errors.LEADER_NOT_AVAILABLE, topicMetadata1.error)
    assertEquals(topic1, topicMetadata1.topic)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION, topicMetadata2.error)
    assertEquals(topic2, topicMetadata2.topic)

    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic1, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic1, 0)

    // retry the metadata for the first auto created topic
    val response2 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1).asJava, true).build)
    topicMetadata1 = response2.topicMetadata.asScala.head
    assertEquals(Errors.NONE, topicMetadata1.error)
    assertEquals(Seq(Errors.NONE), topicMetadata1.partitionMetadata.asScala.map(_.error))
    assertEquals(1, topicMetadata1.partitionMetadata.size)
    val partitionMetadata = topicMetadata1.partitionMetadata.asScala.head
    assertEquals(0, partitionMetadata.partition)
    assertEquals(2, partitionMetadata.replicaIds.size)
    assertTrue(partitionMetadata.leaderId.isPresent)
    assertTrue(partitionMetadata.leaderId.get >= 0)
  }

  @Test
  def testAllTopicsRequest(): Unit = {
    // create some topics
    createTopic("t1", 3, 2)
    createTopic("t2", 3, 2)

    // v0, Empty list represents all topics
    val metadataResponseV0 = sendMetadataRequest(new MetadataRequest(requestData(List(), true), 0.toShort))
    assertTrue(metadataResponseV0.errors.isEmpty, "V0 Response should have no errors")
    assertEquals(2, metadataResponseV0.topicMetadata.size(), "V0 Response should have 2 (all) topics")

    // v1, Null represents all topics
    val metadataResponseV1 = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    assertTrue(metadataResponseV1.errors.isEmpty, "V1 Response should have no errors")
    assertEquals(2, metadataResponseV1.topicMetadata.size(), "V1 Response should have 2 (all) topics")
  }

  @Test
  def testTopicIdsInResponse(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1))
    val topic1 = "topic1"
    val topic2 = "topic2"
    createTopic(topic1, replicaAssignment)
    createTopic(topic2, replicaAssignment)

    // if version < 9, return ZERO_UUID in MetadataResponse
    val resp1 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true, 0, 9).build(), Some(controllerSocketServer))
    assertEquals(2, resp1.topicMetadata.size)
    resp1.topicMetadata.forEach { topicMetadata =>
      assertEquals(Errors.NONE, topicMetadata.error)
      assertEquals(Uuid.ZERO_UUID, topicMetadata.topicId())
    }

    // from version 10, UUID will be included in MetadataResponse
    val resp2 = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic2).asJava, true, 10, 10).build(), Some(notControllerSocketServer))
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
  @Test
  def testPreferredReplica(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1))
    createTopic("t1", replicaAssignment)
    // Call controller and one different broker to ensure that metadata propagation works correctly
    val responses = Seq(
      sendMetadataRequest(new MetadataRequest.Builder(Seq("t1").asJava, true).build(), Some(controllerSocketServer)),
      sendMetadataRequest(new MetadataRequest.Builder(Seq("t1").asJava, true).build(), Some(notControllerSocketServer))
    )
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

  def requestData(topics: List[String], allowAutoTopicCreation: Boolean): MetadataRequestData = {
    val data = new MetadataRequestData
    if (topics == null) data.setTopics(null)
    else topics.foreach(topic => data.topics.add(new MetadataRequestData.MetadataRequestTopic().setName(topic)))

    data.setAllowAutoTopicCreation(allowAutoTopicCreation)
    data
  }

  @Test
  def testReplicaDownResponse(): Unit = {
    val replicaDownTopic = "replicaDown"
    val replicaCount = 3

    // create a topic with 3 replicas
    createTopic(replicaDownTopic, 1, replicaCount)

    // Kill a replica node that is not the leader
    val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List(replicaDownTopic).asJava, true).build())
    val partitionMetadata = metadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    val downNode = servers.find { server =>
      val serverId = server.dataPlaneRequestProcessor.brokerId
      val leaderId = partitionMetadata.leaderId
      val replicaIds = partitionMetadata.replicaIds.asScala
      leaderId.isPresent && leaderId.get() != serverId && replicaIds.contains(serverId)
    }.get
    downNode.shutdown()

    TestUtils.waitUntilTrue(() => {
      val response = sendMetadataRequest(new MetadataRequest.Builder(List(replicaDownTopic).asJava, true).build())
      !response.brokers.asScala.exists(_.id == downNode.dataPlaneRequestProcessor.brokerId)
    }, "Replica was not found down", 5000)


    // Validate version 0 still filters unavailable replicas and contains error
    val v0MetadataResponse = sendMetadataRequest(new MetadataRequest(requestData(List(replicaDownTopic), true), 0.toShort))
    val v0BrokerIds = v0MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue(v0MetadataResponse.errors.isEmpty, "Response should have no errors")
    assertFalse(v0BrokerIds.contains(downNode), s"The downed broker should not be in the brokers list")
    assertTrue(v0MetadataResponse.topicMetadata.size == 1, "Response should have one topic")
    val v0PartitionMetadata = v0MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertTrue(v0PartitionMetadata.error == Errors.REPLICA_NOT_AVAILABLE, "PartitionMetadata should have an error")
    assertTrue(v0PartitionMetadata.replicaIds.size == replicaCount - 1, s"Response should have ${replicaCount - 1} replicas")

    // Validate version 1 returns unavailable replicas with no error
    val v1MetadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List(replicaDownTopic).asJava, true).build(1))
    val v1BrokerIds = v1MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue(v1MetadataResponse.errors.isEmpty, "Response should have no errors")
    assertFalse(v1BrokerIds.contains(downNode), s"The downed broker should not be in the brokers list")
    assertEquals(1, v1MetadataResponse.topicMetadata.size, "Response should have one topic")
    val v1PartitionMetadata = v1MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertEquals(Errors.NONE, v1PartitionMetadata.error, "PartitionMetadata should have no errors")
    assertEquals(replicaCount, v1PartitionMetadata.replicaIds.size, s"Response should have $replicaCount replicas")
  }

  @Test
  def testIsrAfterBrokerShutDownAndJoinsBack(): Unit = {
    def checkIsr(servers: Seq[KafkaServer], topic: String): Unit = {
      val activeBrokers = servers.filter(_.brokerState.currentState != NotRunning.state)
      val expectedIsr = activeBrokers.map(_.config.brokerId).toSet

      // Assert that topic metadata at new brokers is updated correctly
      activeBrokers.foreach { broker =>
        var actualIsr = Set.empty[Int]
        TestUtils.waitUntilTrue(() => {
          val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic).asJava, false).build,
            Some(brokerSocketServer(broker.config.brokerId)))
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
    val replicaCount = 3
    createTopic(topic, 1, replicaCount)

    servers.last.shutdown()
    servers.last.awaitShutdown()
    servers.last.startup()

    checkIsr(servers, topic)
  }

  @Test
  def testAliveBrokersWithNoTopics(): Unit = {
    def checkMetadata(servers: Seq[KafkaServer], expectedBrokersCount: Int): Unit = {
      var controllerMetadataResponse: Option[MetadataResponse] = None
      TestUtils.waitUntilTrue(() => {
        val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build,
          Some(controllerSocketServer))
        controllerMetadataResponse = Some(metadataResponse)
        metadataResponse.brokers.size == expectedBrokersCount
      }, s"Expected $expectedBrokersCount brokers, but there are ${controllerMetadataResponse.get.brokers.size} " +
        "according to the Controller")

      val brokersInController = controllerMetadataResponse.get.brokers.asScala.toSeq.sortBy(_.id)

      // Assert that metadata is propagated correctly
      servers.filter(_.brokerState.currentState != NotRunning.state).foreach { broker =>
        TestUtils.waitUntilTrue(() => {
          val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build,
            Some(brokerSocketServer(broker.config.brokerId)))
          val brokers = metadataResponse.brokers.asScala.toSeq.sortBy(_.id)
          val topicMetadata = metadataResponse.topicMetadata.asScala.toSeq.sortBy(_.topic)
          brokersInController == brokers && metadataResponse.topicMetadata.asScala.toSeq.sortBy(_.topic) == topicMetadata
        }, s"Topic metadata not updated correctly")
      }
    }

    val serverToShutdown = servers.filterNot(_.kafkaController.isActive).last
    serverToShutdown.shutdown()
    serverToShutdown.awaitShutdown()
    checkMetadata(servers, servers.size - 1)

    serverToShutdown.startup()
    checkMetadata(servers, servers.size)
  }

  private def sendMetadataRequest(request: MetadataRequest, destination: Option[SocketServer] = None): MetadataResponse = {
    connectAndReceive[MetadataResponse](request, destination = destination.getOrElse(anySocketServer))
  }

}
