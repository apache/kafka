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
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiError, CreateTopicsRequest, CreateTopicsResponse, MetadataRequest, MetadataResponse}
import org.junit.Assert.{assertEquals, assertFalse, assertNotNull, assertTrue}

import scala.collection.JavaConverters._

class AbstractCreateTopicsRequestTest extends BaseRequestTest {

  override def propertyOverrides(properties: Properties): Unit =
    properties.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)

  protected def validateValidCreateTopicsRequests(request: CreateTopicsRequest): Unit = {
    val response = sendCreateTopicRequest(request)

    val error = response.errors.values.asScala.find(_.isFailure)
    assertTrue(s"There should be no errors, found ${response.errors.asScala}", error.isEmpty)

    request.topics.asScala.foreach { case (topic, details) =>

      def verifyMetadata(socketServer: SocketServer) = {
        val metadata = sendMetadataRequest(
          new MetadataRequest.Builder(List(topic).asJava, true).build()).topicMetadata.asScala
        val metadataForTopic = metadata.filter(_.topic == topic).head

        val partitions = if (!details.replicasAssignments.isEmpty)
          details.replicasAssignments.size
        else
          details.numPartitions

        val replication = if (!details.replicasAssignments.isEmpty)
          details.replicasAssignments.asScala.head._2.size
        else
          details.replicationFactor

        if (request.validateOnly) {
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
      if (!request.validateOnly) {
        // Wait until metadata is propagated and validate non-controller broker has the correct metadata
        TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
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
    val errors = response.errors.asScala
    assertEquals("The response size should match", expectedResponse.size, response.errors.size)

    expectedResponse.foreach { case (topic, expectedError) =>
      val expected = expectedResponse(topic)
      val actual = errors(topic)
      assertEquals("The response error should match", expected.error, actual.error)
      if (checkErrorMessage) {
        assertEquals(expected.message, actual.message)
        assertEquals(expected.messageWithFallback, actual.messageWithFallback)
      }
      // If no error validate topic exists
      if (expectedError.isSuccess && !request.validateOnly) {
        validateTopicExists(topic)
      }
    }
  }

  protected def validateTopicExists(topic: String): Unit = {
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    val metadata = sendMetadataRequest(
      new MetadataRequest.Builder(List(topic).asJava, true).build()).topicMetadata.asScala
    assertTrue("The topic should be created", metadata.exists(p => p.topic.equals(topic) && p.error == Errors.NONE))
  }

  protected def replicaAssignmentToJava(assignments: Map[Int, List[Int]]) = {
    assignments.map { case (k, v) => (k: Integer, v.map { i => i: Integer }.asJava) }.asJava
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
