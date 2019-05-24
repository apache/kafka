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
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.junit.Assert.{assertEquals, assertFalse, assertNotNull, assertTrue}

import scala.collection.JavaConverters._

class AbstractCreateTopicsRequestTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit =
    properties.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)

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
      val effectiveConfigs = new CreateableTopicConfigCollection()
      config.foreach {
        case (name, value) => {
          effectiveConfigs.add(new CreateableTopicConfig().setName(name).setValue(value))
        }
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
    val response = sendCreateTopicRequest(request)

    assertTrue(s"There should be no errors, found " +
      s"${response.errorCounts().keySet().asScala.mkString(", ")},",
      response.errorCounts().keySet().asScala.find(_.code() > 0).isEmpty)

    request.data().topics().asScala.foreach { case topic =>
      def verifyMetadata(socketServer: SocketServer) = {
        val metadata = sendMetadataRequest(
          new MetadataRequest.Builder(List(topic.name()).asJava, true).build()).topicMetadata.asScala
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
          assertNotNull(s"Topic $topic should be created", metadataForTopic)
          assertFalse(s"Error ${metadataForTopic.error} for topic $topic", metadataForTopic.error == Errors.NONE)
          assertTrue("The topic should have no partitions", metadataForTopic.partitionMetadata.isEmpty)
        }
        else {
          assertNotNull("The topic should be created", metadataForTopic)
          assertEquals(Errors.NONE, metadataForTopic.error)
          assertEquals("The topic should have the correct number of partitions", partitions, metadataForTopic.partitionMetadata.size)
          assertEquals("The topic should have the correct replication factor", replication, metadataForTopic.partitionMetadata.asScala.head.replicas.size)
        }
      }

      // Verify controller broker has the correct metadata
      verifyMetadata(controllerSocketServer)
      if (!request.data.validateOnly) {
        // Wait until metadata is propagated and validate non-controller broker has the correct metadata
        TestUtils.waitUntilMetadataIsPropagated(servers, topic.name(), 0)
      }
      verifyMetadata(notControllerSocketServer)
    }
  }

  protected def error(error: Errors, errorMessage: Option[String] = None): ApiError =
    new ApiError(error, errorMessage.orNull)

  protected def toStructWithDuplicateFirstTopic(request: CreateTopicsRequest): Struct = {
    val struct = request.toStruct
    val topics = struct.getArray("create_topic_requests")
    val firstTopic = topics(0).asInstanceOf[Struct]
    val newTopics = firstTopic :: topics.toList
    struct.set("create_topic_requests", newTopics.toArray)
    struct
  }

  protected def addPartitionsAndReplicationFactorToFirstTopic(request: CreateTopicsRequest) = {
    val struct = request.toStruct
    val topics = struct.getArray("create_topic_requests")
    val firstTopic = topics(0).asInstanceOf[Struct]
    firstTopic.set("num_partitions", 1)
    firstTopic.set("replication_factor", 1.toShort)
    new CreateTopicsRequest(struct, request.version)
  }

  protected def validateErrorCreateTopicsRequests(request: CreateTopicsRequest,
                                                  expectedResponse: Map[String, ApiError],
                                                  checkErrorMessage: Boolean = true,
                                                  requestStruct: Option[Struct] = None): Unit = {
    val response = requestStruct.map(sendCreateTopicRequestStruct(_, request.version)).getOrElse(
      sendCreateTopicRequest(request))
    assertEquals("The response size should match", expectedResponse.size, response.data().topics().size)

    expectedResponse.foreach { case (topicName, expectedError) =>
      val expected = expectedResponse(topicName)
      val actual = response.data().topics().find(topicName)
      if (actual == null) {
        throw new RuntimeException(s"No response data found for topic ${topicName}")
      }
      assertEquals("The response error should match", expected.error.code(), actual.errorCode())
      if (checkErrorMessage) {
        assertEquals(expected.message, actual.errorMessage())
      }
      // If no error validate topic exists
      if (expectedError.isSuccess && !request.data.validateOnly) {
        validateTopicExists(topicName)
      }
    }
  }

  protected def validateTopicExists(topic: String): Unit = {
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    val metadata = sendMetadataRequest(
      new MetadataRequest.Builder(List(topic).asJava, true).build()).topicMetadata.asScala
    assertTrue("The topic should be created", metadata.exists(p => p.topic.equals(topic) && p.error == Errors.NONE))
  }

  protected def sendCreateTopicRequestStruct(requestStruct: Struct, apiVersion: Short,
                                             socketServer: SocketServer = controllerSocketServer): CreateTopicsResponse = {
    val response = connectAndSendStruct(requestStruct, ApiKeys.CREATE_TOPICS, apiVersion, socketServer)
    CreateTopicsResponse.parse(response, apiVersion)
  }

  protected def sendCreateTopicRequest(request: CreateTopicsRequest, socketServer: SocketServer = controllerSocketServer): CreateTopicsResponse = {
    val response = connectAndSend(request, ApiKeys.CREATE_TOPICS, socketServer)
    CreateTopicsResponse.parse(response, request.version)
  }

  protected def sendMetadataRequest(request: MetadataRequest, destination: SocketServer = anySocketServer): MetadataResponse = {
    val response = connectAndSend(request, ApiKeys.METADATA, destination = destination)
    MetadataResponse.parse(response, ApiKeys.METADATA.latestVersion)
  }

}
