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

import java.util.{Properties}

import kafka.common.Topic
import kafka.utils.TestUtils
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.junit.Assert._
import org.junit.Test
import org.apache.kafka.test.TestUtils.isValidClusterId
import scala.collection.JavaConverters._

class MetadataRequestTest extends BaseRequestTest {

  override def propertyOverrides(properties: Properties) {
    properties.setProperty(KafkaConfig.RackProp, s"rack/${properties.getProperty(KafkaConfig.BrokerIdProp)}")
  }

  @Test
  def testClusterIdWithRequestVersion1() {
    val v1MetadataResponse = sendMetadataRequest(MetadataRequest.allTopics, 1)
    val v1ClusterId = v1MetadataResponse.clusterId
    assertNull(s"v1 clusterId should be null", v1ClusterId)
  }

  @Test
  def testClusterIdIsValid() {
    val metadataResponse = sendMetadataRequest(MetadataRequest.allTopics, 2)
    isValidClusterId(metadataResponse.clusterId)
  }

  @Test
  def testControllerId() {
    val controllerServer = servers.find(_.kafkaController.isActive()).get
    val controllerId = controllerServer.config.brokerId
    val metadataResponse = sendMetadataRequest(MetadataRequest.allTopics(), 1)

    assertEquals("Controller id should match the active controller",
      controllerId, metadataResponse.controller.id)

    // Fail over the controller
    controllerServer.shutdown()
    controllerServer.startup()

    val controllerServer2 = servers.find(_.kafkaController.isActive()).get
    val controllerId2 = controllerServer2.config.brokerId
    assertNotEquals("Controller id should switch to a new broker", controllerId, controllerId2)
    TestUtils.waitUntilTrue(() => {
      val metadataResponse2 = sendMetadataRequest(MetadataRequest.allTopics(), 1)
      metadataResponse2.controller != null && controllerServer2.apis.brokerId == metadataResponse2.controller.id
    }, "Controller id should match the active controller after failover", 5000)
  }

  @Test
  def testRack() {
    val metadataResponse = sendMetadataRequest(MetadataRequest.allTopics(), 1)
    // Validate rack matches what's set in generateConfigs() above
    metadataResponse.brokers.asScala.foreach { broker =>
      assertEquals("Rack information should match config", s"rack/${broker.id}", broker.rack)
    }
  }

  @Test
  def testIsInternal() {
    val internalTopic = Topic.GroupMetadataTopicName
    val notInternalTopic = "notInternal"
    // create the topics
    TestUtils.createTopic(zkUtils, internalTopic, 3, 2, servers)
    TestUtils.createTopic(zkUtils, notInternalTopic, 3, 2, servers)

    val metadataResponse = sendMetadataRequest(MetadataRequest.allTopics(), 1)
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
    TestUtils.createTopic(zkUtils, "t1", 3, 2, servers)
    TestUtils.createTopic(zkUtils, "t2", 3, 2, servers)

    // v0, Doesn't support a "no topics" request
    // v1, Empty list represents "no topics"
    val metadataResponse = sendMetadataRequest(new MetadataRequest(List[String]().asJava), 1)
    assertTrue("Response should have no errors", metadataResponse.errors.isEmpty)
    assertTrue("Response should have no topics", metadataResponse.topicMetadata.isEmpty)
  }

  @Test
  def testAllTopicsRequest() {
    // create some topics
    TestUtils.createTopic(zkUtils, "t1", 3, 2, servers)
    TestUtils.createTopic(zkUtils, "t2", 3, 2, servers)

    // v0, Empty list represents all topics
    val metadataResponseV0 = sendMetadataRequest(new MetadataRequest(List[String]().asJava), 0)
    assertTrue("V0 Response should have no errors", metadataResponseV0.errors.isEmpty)
    assertEquals("V0 Response should have 2 (all) topics", 2, metadataResponseV0.topicMetadata.size())

    // v1, Null represents all topics
    val metadataResponseV1 = sendMetadataRequest(MetadataRequest.allTopics(), 1)
    assertTrue("V1 Response should have no errors", metadataResponseV1.errors.isEmpty)
    assertEquals("V1 Response should have 2 (all) topics", 2, metadataResponseV1.topicMetadata.size())
  }

  @Test
  def testReplicaDownResponse() {
    val replicaDownTopic = "replicaDown"
    val replicaCount = 3

    // create a topic with 3 replicas
    TestUtils.createTopic(zkUtils, replicaDownTopic, 1, replicaCount, servers)

    // Kill a replica node that is not the leader
    val metadataResponse = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava), 1)
    val partitionMetadata = metadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    val downNode = servers.find { server =>
      val serverId = server.apis.brokerId
      val leaderId = partitionMetadata.leader.id
      val replicaIds = partitionMetadata.replicas.asScala.map(_.id)
      serverId != leaderId && replicaIds.contains(serverId)
    }.get
    downNode.shutdown()

    TestUtils.waitUntilTrue(() => {
      val response = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava), 1)
      val metadata = response.topicMetadata.asScala.head.partitionMetadata.asScala.head
      val replica = metadata.replicas.asScala.find(_.id == downNode.apis.brokerId).get
      replica.host == "" & replica.port == -1
    }, "Replica was not found down", 5000)

    // Validate version 0 still filters unavailable replicas and contains error
    val v0MetadataResponse = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava), 0)
    val v0BrokerIds = v0MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue("Response should have no errors", v0MetadataResponse.errors.isEmpty)
    assertFalse(s"The downed broker should not be in the brokers list", v0BrokerIds.contains(downNode))
    assertTrue("Response should have one topic", v0MetadataResponse.topicMetadata.size == 1)
    val v0PartitionMetadata = v0MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertTrue("PartitionMetadata should have an error", v0PartitionMetadata.error == Errors.REPLICA_NOT_AVAILABLE)
    assertTrue(s"Response should have ${replicaCount - 1} replicas", v0PartitionMetadata.replicas.size == replicaCount - 1)

    // Validate version 1 returns unavailable replicas with no error
    val v1MetadataResponse = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava), 1)
    val v1BrokerIds = v1MetadataResponse.brokers().asScala.map(_.id).toSeq
    assertTrue("Response should have no errors", v1MetadataResponse.errors.isEmpty)
    assertFalse(s"The downed broker should not be in the brokers list", v1BrokerIds.contains(downNode))
    assertEquals("Response should have one topic", 1, v1MetadataResponse.topicMetadata.size)
    val v1PartitionMetadata = v1MetadataResponse.topicMetadata.asScala.head.partitionMetadata.asScala.head
    assertEquals("PartitionMetadata should have no errors", Errors.NONE, v1PartitionMetadata.error)
    assertEquals(s"Response should have $replicaCount replicas", replicaCount, v1PartitionMetadata.replicas.size)
  }

  private def sendMetadataRequest(request: MetadataRequest, version: Short): MetadataResponse = {
    val response = send(request, ApiKeys.METADATA, Some(version))
    MetadataResponse.parse(response, version)
  }
}
