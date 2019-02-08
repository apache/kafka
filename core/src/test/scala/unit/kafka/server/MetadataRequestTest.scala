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

import java.util.Properties

import kafka.network.SocketServer
import kafka.utils.TestUtils
import org.apache.kafka.common.Node
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.junit.Assert._
import org.junit.{Before, Test}
import org.apache.kafka.test.TestUtils.isValidClusterId

import scala.collection.JavaConverters._

class MetadataRequestTest extends BaseRequestTest {

  override def propertyOverrides(properties: Properties) {
    properties.setProperty(KafkaConfig.DefaultReplicationFactorProp, "2")
    properties.setProperty(KafkaConfig.RackProp, s"rack/${properties.getProperty(KafkaConfig.BrokerIdProp)}")
  }

  @Before
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)
  }

  @Test
  def testClusterIdWithRequestVersion1() {
    val v1MetadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    val v1ClusterId = v1MetadataResponse.clusterId
    assertNull(s"v1 clusterId should be null", v1ClusterId)
  }

  @Test
  def testClusterIdIsValid() {
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(2.toShort))
    isValidClusterId(metadataResponse.clusterId)
  }

  @Test
  def testControllerId() {
    val controllerServer = servers.find(_.kafkaController.isActive).get
    val controllerId = controllerServer.config.brokerId
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))

    assertEquals("Controller id should match the active controller",
      controllerId, metadataResponse.controller.id)

    // Fail over the controller
    controllerServer.shutdown()
    controllerServer.startup()

    val controllerServer2 = servers.find(_.kafkaController.isActive).get
    val controllerId2 = controllerServer2.config.brokerId
    assertNotEquals("Controller id should switch to a new broker", controllerId, controllerId2)
    TestUtils.waitUntilTrue(() => {
      val metadataResponse2 = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
      metadataResponse2.controller != null && controllerServer2.dataPlaneRequestProcessor.brokerId == metadataResponse2.controller.id
    }, "Controller id should match the active controller after failover", 5000)
  }

  @Test
  def testRack() {
    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    // Validate rack matches what's set in generateConfigs() above
    metadataResponse.brokers.asScala.foreach { broker =>
      assertEquals("Rack information should match config", s"rack/${broker.id}", broker.rack)
    }
  }

  @Test
  def testIsInternal() {
    val internalTopic = Topic.GROUP_METADATA_TOPIC_NAME
    val notInternalTopic = "notInternal"
    // create the topics
    createTopic(internalTopic, 3, 2)
    createTopic(notInternalTopic, 3, 2)

    val metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    assertTrue("Response should have no errors", metadataResponse.errors.isEmpty)

    val topicMetadata = metadataResponse.topicMetadata.asScala
    val internalTopicMetadata = topicMetadata.find(_.topic == internalTopic).get
    val notInternalTopicMetadata = topicMetadata.find(_.topic == notInternalTopic).get

    assertTrue("internalTopic should show isInternal", internalTopicMetadata.isInternal)
    assertFalse("notInternalTopic topic not should show isInternal", notInternalTopicMetadata.isInternal)

    assertEquals(Set(internalTopic).asJava, metadataResponse.cluster.internalTopics)
  }

  @Test
  def testNoTopicsRequest() {
    // create some topics
    createTopic("t1", 3, 2)
    createTopic("t2", 3, 2)

    // v0, Doesn't support a "no topics" request
    // v1, Empty list represents "no topics"
    val metadataResponse = sendMetadataRequest(new MetadataRequest(List[String]().asJava, true, 1.toShort))
    assertTrue("Response should have no errors", metadataResponse.errors.isEmpty)
    assertTrue("Response should have no topics", metadataResponse.topicMetadata.isEmpty)
  }

  @Test
  def testAutoTopicCreation(): Unit = {
    def checkAutoCreatedTopic(existingTopic: String, autoCreatedTopic: String, response: MetadataResponse): Unit = {
      assertNull(response.errors.get(existingTopic))
      assertEquals(Errors.LEADER_NOT_AVAILABLE, response.errors.get(autoCreatedTopic))
      assertEquals(Some(servers.head.config.numPartitions), zkClient.getTopicPartitionCount(autoCreatedTopic))
      for (i <- 0 until servers.head.config.numPartitions)
        TestUtils.waitUntilMetadataIsPropagated(servers, autoCreatedTopic, i)
    }

    val topic1 = "t1"
    val topic2 = "t2"
    val topic3 = "t3"
    val topic4 = "t4"
    createTopic(topic1, 1, 1)

    val response1 = sendMetadataRequest(new MetadataRequest(Seq(topic1, topic2).asJava, true, ApiKeys.METADATA.latestVersion))
    checkAutoCreatedTopic(topic1, topic2, response1)

    // V3 doesn't support a configurable allowAutoTopicCreation, so the fact that we set it to `false` has no effect
    val response2 = sendMetadataRequest(new MetadataRequest(Seq(topic2, topic3).asJava, false, 3))
    checkAutoCreatedTopic(topic2, topic3, response2)

    // V4 and higher support a configurable allowAutoTopicCreation
    val response3 = sendMetadataRequest(new MetadataRequest(Seq(topic3, topic4).asJava, false, 4))
    assertNull(response3.errors.get(topic3))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response3.errors.get(topic4))
    assertEquals(None, zkClient.getTopicPartitionCount(topic4))
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
    assertEquals(2, partitionMetadata.replicas.size)
    assertNotNull(partitionMetadata.leader)
  }

  @Test
  def testAllTopicsRequest() {
    // create some topics
    createTopic("t1", 3, 2)
    createTopic("t2", 3, 2)

    // v0, Empty list represents all topics
    val metadataResponseV0 = sendMetadataRequest(new MetadataRequest(List[String]().asJava, true, 0.toShort))
    assertTrue("V0 Response should have no errors", metadataResponseV0.errors.isEmpty)
    assertEquals("V0 Response should have 2 (all) topics", 2, metadataResponseV0.topicMetadata.size())

    // v1, Null represents all topics
    val metadataResponseV1 = sendMetadataRequest(MetadataRequest.Builder.allTopics.build(1.toShort))
    assertTrue("V1 Response should have no errors", metadataResponseV1.errors.isEmpty)
    assertEquals("V1 Response should have 2 (all) topics", 2, metadataResponseV1.topicMetadata.size())
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
      topicMetadata.partitionMetadata.asScala.foreach { partitionMetadata =>
        val assignment = replicaAssignment(partitionMetadata.partition)
        assertEquals(assignment, partitionMetadata.replicas.asScala.map(_.id))
        assertEquals(assignment, partitionMetadata.isr.asScala.map(_.id))
        assertEquals(assignment.head, partitionMetadata.leader.id)
      }
    }
  }

  @Test
  def testReplicaDownResponse() {
    val replicaDownTopic = "replicaDown"
    val replicaCount = 3

    // create a topic with 3 replicas
    createTopic(replicaDownTopic, 1, replicaCount)

    // Kill a replica node that is not the leader
    val metadataResponse = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava, true, 1.toShort))
    val partitionMetadata = metadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    val downNode = servers.find { server =>
      val serverId = server.dataPlaneRequestProcessor.brokerId
      val leaderId = partitionMetadata.leader.id
      val replicaIds = partitionMetadata.replicas.asScala.map(_.id)
      serverId != leaderId && replicaIds.contains(serverId)
    }.get
    downNode.shutdown()

    TestUtils.waitUntilTrue(() => {
      val response = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava, true, 1.toShort))
      val metadata = response.topicMetadata.asScala.head.partitionMetadata.asScala.head
      val replica = metadata.replicas.asScala.find(_.id == downNode.dataPlaneRequestProcessor.brokerId).get
      replica.host == "" & replica.port == -1
    }, "Replica was not found down", 5000)

    // Validate version 0 still filters unavailable replicas and contains error
    val v0MetadataResponse = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava, true, 0.toShort))
    val v0BrokerIds = v0MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue("Response should have no errors", v0MetadataResponse.errors.isEmpty)
    assertFalse(s"The downed broker should not be in the brokers list", v0BrokerIds.contains(downNode))
    assertTrue("Response should have one topic", v0MetadataResponse.topicMetadata.size == 1)
    val v0PartitionMetadata = v0MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertTrue("PartitionMetadata should have an error", v0PartitionMetadata.error == Errors.REPLICA_NOT_AVAILABLE)
    assertTrue(s"Response should have ${replicaCount - 1} replicas", v0PartitionMetadata.replicas.size == replicaCount - 1)

    // Validate version 1 returns unavailable replicas with no error
    val v1MetadataResponse = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava, true, 1.toShort))
    val v1BrokerIds = v1MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue("Response should have no errors", v1MetadataResponse.errors.isEmpty)
    assertFalse(s"The downed broker should not be in the brokers list", v1BrokerIds.contains(downNode))
    assertEquals("Response should have one topic", 1, v1MetadataResponse.topicMetadata.size)
    val v1PartitionMetadata = v1MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertEquals("PartitionMetadata should have no errors", Errors.NONE, v1PartitionMetadata.error)
    assertEquals(s"Response should have $replicaCount replicas", replicaCount, v1PartitionMetadata.replicas.size)
  }

  @Test
  def testIsrAfterBrokerShutDownAndJoinsBack(): Unit = {
    def checkIsr(servers: Seq[KafkaServer], topic: String): Unit = {
      val activeBrokers = servers.filter(_.brokerState.currentState != NotRunning.state)
      val expectedIsr = activeBrokers.map { broker =>
        new Node(broker.config.brokerId, "localhost", TestUtils.boundPort(broker), broker.config.rack.orNull)
      }.sortBy(_.id)

      // Assert that topic metadata at new brokers is updated correctly
      activeBrokers.foreach { broker =>
        var actualIsr: Seq[Node] = Seq.empty
        TestUtils.waitUntilTrue(() => {
          val metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic).asJava, false).build,
            Some(brokerSocketServer(broker.config.brokerId)))
          val firstPartitionMetadata = metadataResponse.topicMetadata.asScala.headOption.flatMap(_.partitionMetadata.asScala.headOption)
          actualIsr = firstPartitionMetadata.map { partitionMetadata =>
            partitionMetadata.isr.asScala.sortBy(_.id)
          }.getOrElse(Seq.empty)
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
    val response = connectAndSend(request, ApiKeys.METADATA, destination = destination.getOrElse(anySocketServer))
    MetadataResponse.parse(response, request.version)
  }

}
