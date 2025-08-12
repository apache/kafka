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

import java.util
import java.util.Properties

import kafka.network.SocketServer
import kafka.utils.TestUtils
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.server.config.ServerLogConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertNotNull, assertTrue}

import scala.jdk.CollectionConverters._

abstract class AbstractCreateTopicsRequestTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit =
    properties.put(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, false.toString)

  def topicsReq(topics: Seq[CreatableTopic],
                timeout: Integer = 10000,
                validateOnly: Boolean = false) = {
    val req = new CreateTopicsRequestData()
    req.setTimeoutMs(timeout)
    req.setTopics(new CreatableTopicCollection(topics.asJava.iterator()))
    req.setValidateOnly(validateOnly)
    new CreateTopicsRequest.Builder(req).build()
  }

  def topicReq(name: String,
               numPartitions: Integer = null,
               replicationFactor: Integer = null,
               config: Map[String, String] = null,
               assignment: Map[Int, Seq[Int]] = null): CreatableTopic = {
    val topic = new CreatableTopic()
    topic.setName(name)
    if (numPartitions != null) {
      topic.setNumPartitions(numPartitions)
    } else if (assignment != null) {
      topic.setNumPartitions(-1)
    } else {
      topic.setNumPartitions(1)
    }
    if (replicationFactor != null) {
      topic.setReplicationFactor(replicationFactor.toShort)
    } else if (assignment != null) {
      topic.setReplicationFactor((-1).toShort)
    } else {
      topic.setReplicationFactor(1.toShort)
    }
    if (config != null) {
      val effectiveConfigs = new CreatableTopicConfigCollection()
      config.foreach {
        case (name, value) =>
          effectiveConfigs.add(new CreatableTopicConfig().setName(name).setValue(value))
      }
      topic.setConfigs(effectiveConfigs)
    }
    if (assignment != null) {
      val effectiveAssignments = new CreatableReplicaAssignmentCollection()
      assignment.foreach {
        case (partitionIndex, brokerIdList) => {
          val effectiveAssignment = new CreatableReplicaAssignment()
          effectiveAssignment.setPartitionIndex(partitionIndex)
          val brokerIds = new util.ArrayList[java.lang.Integer]()
          brokerIdList.foreach(brokerId => brokerIds.add(brokerId))
          effectiveAssignment.setBrokerIds(brokerIds)
          effectiveAssignments.add(effectiveAssignment)
        }
      }
      topic.setAssignments(effectiveAssignments)
    }
    topic
  }

  protected def validateValidCreateTopicsRequests(request: CreateTopicsRequest): Unit = {
    val response = sendCreateTopicRequest(request, adminSocketServer)

    assertFalse(response.errorCounts().keySet().asScala.exists(_.code() > 0),
      s"There should be no errors, found ${response.errorCounts().keySet().asScala.mkString(", ")},")

    request.data.topics.forEach { topic =>
      def verifyMetadata(socketServer: SocketServer): Unit = {
        val metadata = sendMetadataRequest(
          new MetadataRequest.Builder(List(topic.name()).asJava, false).build(), socketServer).topicMetadata.asScala
        val metadataForTopic = metadata.filter(_.topic == topic.name()).head

        val partitions = if (!topic.assignments().isEmpty)
          topic.assignments().size
        else
          topic.numPartitions

        val replication = if (!topic.assignments().isEmpty)
          topic.assignments().iterator().next().brokerIds().size()
        else
          topic.replicationFactor

        if (request.data.validateOnly) {
          assertNotNull(metadataForTopic)
          assertNotEquals(Errors.NONE, metadataForTopic.error, s"Topic $topic should not be created")
          assertTrue(metadataForTopic.partitionMetadata.isEmpty, "The topic should have no partitions")
        }
        else {
          assertNotNull(metadataForTopic, "The topic should be created")
          assertEquals(Errors.NONE, metadataForTopic.error)
          if (partitions == -1) {
            assertEquals(configs.head.numPartitions, metadataForTopic.partitionMetadata.size, "The topic should have the default number of partitions")
          } else {
            assertEquals(partitions, metadataForTopic.partitionMetadata.size, "The topic should have the correct number of partitions")
          }

          if (replication == -1) {
            assertEquals(configs.head.defaultReplicationFactor,
              metadataForTopic.partitionMetadata.asScala.head.replicaIds.size, "The topic should have the default replication factor")
          } else {
            assertEquals(replication, metadataForTopic.partitionMetadata.asScala.head.replicaIds.size, "The topic should have the correct replication factor")
          }
        }
      }

      if (!request.data.validateOnly) {
        // Wait until metadata is propagated and validate non-controller broker has the correct metadata
        TestUtils.waitForPartitionMetadata(brokers, topic.name(), 0)
      }
      verifyMetadata(notControllerSocketServer)
    }
  }

  protected def error(error: Errors, errorMessage: Option[String] = None): ApiError =
    new ApiError(error, errorMessage.orNull)

  protected def validateErrorCreateTopicsRequests(request: CreateTopicsRequest,
                                                  expectedResponse: Map[String, ApiError],
                                                  checkErrorMessage: Boolean = true): Unit = {
    val response = sendCreateTopicRequest(request, adminSocketServer)
    assertEquals(expectedResponse.size, response.data().topics().size, "The response size should match")

    expectedResponse.foreach { case (topicName, expectedError) =>
      val expected = expectedResponse(topicName)
      val actual = response.data().topics().find(topicName)
      if (actual == null) {
        throw new RuntimeException(s"No response data found for topic $topicName")
      }
      assertEquals(expected.error.code(), actual.errorCode(), "The response error code should match")
      if (checkErrorMessage) {
        assertEquals(expected.message, actual.errorMessage(), "The response error message should match")
      }
      // If no error validate topic exists
      if (expectedError.isSuccess && !request.data.validateOnly) {
        validateTopicExists(topicName)
      }
    }
  }

  protected def validateTopicExists(topic: String): Unit = {
    TestUtils.waitForPartitionMetadata(brokers, topic, 0)
    val metadata = sendMetadataRequest(
      new MetadataRequest.Builder(List(topic).asJava, true).build()).topicMetadata.asScala
    assertTrue(metadata.exists(p => p.topic.equals(topic) && p.error == Errors.NONE), "The topic should be created")
  }

  protected def sendCreateTopicRequest(
    request: CreateTopicsRequest,
    socketServer: SocketServer = controllerSocketServer
  ): CreateTopicsResponse = {
    connectAndReceive[CreateTopicsResponse](request, socketServer)
  }

  protected def sendMetadataRequest(
    request: MetadataRequest,
    socketServer: SocketServer = anySocketServer
  ): MetadataResponse = {
    connectAndReceive[MetadataResponse](request, socketServer)
  }

}
