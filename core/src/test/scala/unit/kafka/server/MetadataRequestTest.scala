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

import kafka.admin.AdminUtils
import kafka.coordinator.GroupCoordinator
import kafka.utils.TestUtils
import org.apache.kafka.common.internals.TopicConstants
import org.apache.kafka.common.protocol.ApiKeys
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
    val metadataResponse = sendMetadatRequest(allMetadataRequest, 1)
    val controllerServer = servers.find(_.kafkaController.isActive()).get
    val controllerId = controllerServer.apis.brokerId

    assertEquals("Controller id should match the active controller",
      controllerServer.apis.brokerId, controllerId)

    // Fail over the controller
    controllerServer.shutdown()
    controllerServer.startup()

    val metadataResponse2 = sendMetadatRequest(allMetadataRequest, 1)
    val controllerServer2 = servers.find(_.kafkaController.isActive()).get
    val controllerId2 = controllerServer2.apis.brokerId

    assertNotEquals("Controller id should switch to a new broker", controllerId, controllerId2)
    assertEquals("Controller id should match the active controller after failover",
      controllerServer.apis.brokerId, metadataResponse.controller.id)
  }

  @Test
  def testRack() {
    val metadataResponse = sendMetadatRequest(allMetadataRequest, 1)
    // Validate rack matches whats set in generateConfigs() above
    metadataResponse.brokers().asScala.foreach { broker =>
      assertEquals("Rack information should match config", s"rack/${broker.id}", broker.rack())
    }
  }

  @Test
  def testMarkedForDeletion() {
    val deletedTopic = "deleted"
    val notDeletedTopic = "notDeleted"

    TestUtils.createTopic(zkUtils, deletedTopic, 3, 2, servers)
    TestUtils.createTopic(zkUtils, notDeletedTopic, 3, 2, servers)

    // Make sure topics don't get deleted to fast by preventing deletion
    val controllerServer = servers.find(_.kafkaController.isActive()).get
    controllerServer.kafkaController.deleteTopicManager.shutdown()

    AdminUtils.deleteTopic(zkUtils, deletedTopic)

    // Wait until all servers see the delete in the cache
    TestUtils.waitUntilTrue(
      () => servers.forall(_.metadataCache.isMarkedForDeletion(deletedTopic)),
      "Delete event did not update the cache",
      5000
    )

    val metadataResponse = sendMetadatRequest(allMetadataRequest, 1)
    assertTrue("Response should have no errors", metadataResponse.errors().isEmpty)

    val topicMetadata = metadataResponse.topicMetadata().asScala
    val deletedMetadata = topicMetadata.find(_.topic == deletedTopic).get
    val notDeletedMetadata = topicMetadata.find(_.topic == notDeletedTopic).get

    assertTrue("deleted topic should show markedForDeletion", deletedMetadata.markedForDeletion)
    assertFalse("notDeleted topic not should show markedForDeletion", notDeletedMetadata.markedForDeletion)
  }

  @Test
  def testIsInternal() {
    val internalTopic = TopicConstants.GROUP_METADATA_TOPIC_NAME
    val notInternalTopic = "notInternal"
    // create the topics
    TestUtils.createTopic(zkUtils, internalTopic, 3, 2, servers)
    TestUtils.createTopic(zkUtils, notInternalTopic, 3, 2, servers)

    val metadataResponse = sendMetadatRequest(allMetadataRequest, 1)
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

    val metadataResponse = sendMetadatRequest(noMetadataRequest, 1)
    assertTrue("Response should have no errors", metadataResponse.errors().isEmpty)
    assertTrue("Response should have no topics", metadataResponse.topicMetadata().isEmpty)
  }

  private def sendMetadatRequest(request: MetadataRequest, version: Short): MetadataResponse = {
    val response = send(request, ApiKeys.METADATA, version)
    MetadataResponse.parse(response)
  }
}
