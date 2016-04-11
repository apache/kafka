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
import org.apache.kafka.common.internals.TopicConstants
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class MetadataRequestTest extends BaseRequestTest {

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, enableControlledShutdown = false)
    props.foreach { p =>
      p.setProperty(KafkaConfig.DeleteTopicEnableProp, "true")
      p.setProperty(KafkaConfig.RackProp, s"rack/${p.getProperty(KafkaConfig.BrokerIdProp)}")
    }
    props.map(KafkaConfig.fromProps)
  }

  private def allMetadataRequest = new MetadataRequest(null, true)
  private def noMetadataRequest = new MetadataRequest(List[String]().asJava)

  @Before
  override def setUp() {
    super.setUp()
  }

  @Test
  def testControllerId() {
    val metadataResponse = sendMetadataRequest(allMetadataRequest, 1)
    val controllerServer = servers.find(_.kafkaController.isActive()).get
    val controllerId = controllerServer.apis.brokerId

    assertEquals("Controller id should match the active controller",
      controllerServer.apis.brokerId, controllerId)

    // Fail over the controller
    controllerServer.shutdown()
    controllerServer.startup()

    val metadataResponse2 = sendMetadataRequest(allMetadataRequest, 1)
    val controllerServer2 = servers.find(_.kafkaController.isActive()).get
    val controllerId2 = controllerServer2.apis.brokerId

    assertNotEquals("Controller id should switch to a new broker", controllerId, controllerId2)
    assertEquals("Controller id should match the active controller after failover",
      controllerServer.apis.brokerId, metadataResponse.controller.id)
  }

  @Test
  def testRack() {
    val metadataResponse = sendMetadataRequest(allMetadataRequest, 1)
    // Validate rack matches whats set in generateConfigs() above
    metadataResponse.brokers().asScala.foreach { broker =>
      assertEquals("Rack information should match config", s"rack/${broker.id}", broker.rack())
    }
  }

  @Test
  def testIsInternal() {
    val internalTopic = TopicConstants.GROUP_METADATA_TOPIC_NAME
    val notInternalTopic = "notInternal"
    // create the topics
    TestUtils.createTopic(zkUtils, internalTopic, 3, 2, servers)
    TestUtils.createTopic(zkUtils, notInternalTopic, 3, 2, servers)

    val metadataResponse = sendMetadataRequest(allMetadataRequest, 1)
    assertTrue("Response should have no errors", metadataResponse.errors().isEmpty)

    val topicMetadata = metadataResponse.topicMetadata().asScala
    val internalTopicMetadata = topicMetadata.find(_.topic == internalTopic).get
    val notInternalTopicMetadata = topicMetadata.find(_.topic == notInternalTopic).get

    assertTrue("internalTopic should show isInternal", internalTopicMetadata.isInternal)
    assertFalse("notInternalTopic topic not should show isInternal", notInternalTopicMetadata.isInternal)
  }

  @Test
  def testNoTopicsRequest() {
    // create some topics
    TestUtils.createTopic(zkUtils, "t1", 3, 2, servers)
    TestUtils.createTopic(zkUtils, "t2", 3, 2, servers)

    val metadataResponse = sendMetadataRequest(noMetadataRequest, 1)
    assertTrue("Response should have no errors", metadataResponse.errors().isEmpty)
    assertTrue("Response should have no topics", metadataResponse.topicMetadata().isEmpty)
  }

  @Test
  def testReplicaDownResponse() {
    val replicaDownTopic = "replicaDown"
    val replicaCount = 3

    // create a topic with 3 replicas
    TestUtils.createTopic(zkUtils, replicaDownTopic, 1, replicaCount, servers)

    // Kill a replica node that is not the leader
    val metadataResponse = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava), 1)
    val partitionMetadata = metadataResponse.topicMetadata().asScala.head.partitionMetadata().asScala.head
    val downNode = servers.find { server =>
      val serverId = server.apis.brokerId
      val leaderId = partitionMetadata.leader.id()
      val replicaIds = partitionMetadata.replicas().asScala.map(_.id())
      serverId != leaderId && replicaIds.contains(serverId)
    }.get
    downNode.shutdown()

    TestUtils.waitUntilTrue({ () =>
      val response = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava), 1)
      val metadata = response.topicMetadata().asScala.head.partitionMetadata().asScala.head
      val replica = metadata.replicas.asScala.find(_.id == downNode.apis.brokerId).get
      replica.host() == "" & replica.port() == -1
    }, "Replica was not found down", 5000)

    // Validate version 0 still filters unavailable replicas and contains error
    val v0MetadataResponse = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava), 0)
    assertTrue("Response should have no errors", v0MetadataResponse.errors().isEmpty)
    assertTrue("Response should have one topic", v0MetadataResponse.topicMetadata().size() == 1)
    val v0PartitionMetadata = v0MetadataResponse.topicMetadata().asScala.head.partitionMetadata().asScala.head
    assertTrue("PartitionMetadata should have an error", v0PartitionMetadata.error() == Errors.REPLICA_NOT_AVAILABLE)
    assertTrue(s"Response should have ${replicaCount - 1} replicas", v0PartitionMetadata.replicas().size() == replicaCount - 1)

    // Validate version 1 returns unavailable replicas with no error
    val v1MetadataResponse = sendMetadataRequest(new MetadataRequest(List(replicaDownTopic).asJava), 1)
    assertTrue("Response should have no errors", v1MetadataResponse.errors().isEmpty)
    assertTrue("Response should have one topic", v1MetadataResponse.topicMetadata().size() == 1)
    val v1PartitionMetadata = v1MetadataResponse.topicMetadata().asScala.head.partitionMetadata().asScala.head
    assertTrue("PartitionMetadata should have no errors", v1PartitionMetadata.error() == Errors.NONE)
    assertTrue(s"Response should have $replicaCount replicas", v1PartitionMetadata.replicas().size() == replicaCount)
  }

  private def sendMetadataRequest(request: MetadataRequest, version: Short): MetadataResponse = {
    val response = send(request, ApiKeys.METADATA, version)
    MetadataResponse.parse(response, version)
  }
}
