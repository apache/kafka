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

import java.util.{Arrays, Collections}

import kafka.network.SocketServer
import kafka.utils._
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DeleteTopicsRequest, DeleteTopicsResponse, MetadataRequest, MetadataResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class DeleteTopicsRequestTest extends BaseRequestTest {

  @Test
  def testValidDeleteTopicRequests(): Unit = {
    val timeout = 10000
    // Single topic
    createTopic("topic-1", 1, 1)
    validateValidDeleteTopicRequests(new DeleteTopicsRequest.Builder(
        new DeleteTopicsRequestData()
          .setTopicNames(Arrays.asList("topic-1"))
          .setTimeoutMs(timeout)).build())
    // Multi topic
    createTopic("topic-3", 5, 2)
    createTopic("topic-4", 1, 2)
    validateValidDeleteTopicRequests(new DeleteTopicsRequest.Builder(
        new DeleteTopicsRequestData()
          .setTopicNames(Arrays.asList("topic-3", "topic-4"))
          .setTimeoutMs(timeout)).build())

    // Topic Ids
    createTopic("topic-7", 3, 2)
    createTopic("topic-6", 1, 2)
    val ids = getTopicIds()
    validateValidDeleteTopicRequestsWithIds(new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopics(Arrays.asList(new DeleteTopicState().setTopicId(ids("topic-7")),
             new DeleteTopicState().setTopicId(ids("topic-6"))
        )
      ).setTimeoutMs(timeout)).build())
  }

  private def validateValidDeleteTopicRequests(request: DeleteTopicsRequest): Unit = {
    val response = sendDeleteTopicsRequest(request)
    val error = response.errorCounts.asScala.find(_._1 != Errors.NONE)
    assertTrue(error.isEmpty, s"There should be no errors, found ${response.data.responses.asScala}")
    request.data.topicNames.forEach { topic =>
      validateTopicIsDeleted(topic)
    }
  }

  private def validateValidDeleteTopicRequestsWithIds(request: DeleteTopicsRequest): Unit = {
    val response = sendDeleteTopicsRequest(request)
    val error = response.errorCounts.asScala.find(_._1 != Errors.NONE)
    assertTrue(error.isEmpty, s"There should be no errors, found ${response.data.responses.asScala}")
    response.data.responses.forEach { response =>
      validateTopicIsDeleted(response.name())
    }
  }

  @Test
  def testErrorDeleteTopicRequests(): Unit = {
    val timeout = 30000
    val timeoutTopic = "invalid-timeout"

    // Basic
    validateErrorDeleteTopicRequests(new DeleteTopicsRequest.Builder(
        new DeleteTopicsRequestData()
          .setTopicNames(Arrays.asList("invalid-topic"))
          .setTimeoutMs(timeout)).build(),
      Map("invalid-topic" -> Errors.UNKNOWN_TOPIC_OR_PARTITION))

    // Partial
    createTopic("partial-topic-1", 1, 1)
    validateErrorDeleteTopicRequests(new DeleteTopicsRequest.Builder(
        new DeleteTopicsRequestData()
          .setTopicNames(Arrays.asList("partial-topic-1", "partial-invalid-topic"))
          .setTimeoutMs(timeout)).build(),
      Map(
        "partial-topic-1" -> Errors.NONE,
        "partial-invalid-topic" -> Errors.UNKNOWN_TOPIC_OR_PARTITION
      )
    )
    
    // Topic IDs
    createTopic("topic-id-1", 1, 1)
    val validId = getTopicIds()("topic-id-1")
    val invalidId = Uuid.randomUuid
    validateErrorDeleteTopicRequestsWithIds(new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopics(Arrays.asList(new DeleteTopicState().setTopicId(invalidId), 
            new DeleteTopicState().setTopicId(validId)))
        .setTimeoutMs(timeout)).build(),
      Map(
        invalidId -> Errors.UNKNOWN_TOPIC_ID,
        validId -> Errors.NONE
      )
    )

    // Timeout
    createTopic(timeoutTopic, 5, 2)
    // Must be a 0ms timeout to avoid transient test failures. Even a timeout of 1ms has succeeded in the past.
    validateErrorDeleteTopicRequests(new DeleteTopicsRequest.Builder(
        new DeleteTopicsRequestData()
          .setTopicNames(Arrays.asList(timeoutTopic))
          .setTimeoutMs(0)).build(),
      Map(timeoutTopic -> Errors.REQUEST_TIMED_OUT))
    // The topic should still get deleted eventually
    TestUtils.waitUntilTrue(() => !servers.head.metadataCache.contains(timeoutTopic), s"Topic $timeoutTopic is never deleted")
    validateTopicIsDeleted(timeoutTopic)
  }

  private def validateErrorDeleteTopicRequests(request: DeleteTopicsRequest, expectedResponse: Map[String, Errors]): Unit = {
    val response = sendDeleteTopicsRequest(request)
    val errors = response.data.responses

    val errorCount = response.errorCounts().asScala.foldLeft(0)(_+_._2)
    assertEquals(expectedResponse.size, errorCount, "The response size should match")

    expectedResponse.foreach { case (topic, expectedError) =>
      assertEquals(expectedResponse(topic).code, errors.find(topic).errorCode, "The response error should match")
      // If no error validate the topic was deleted
      if (expectedError == Errors.NONE) {
        validateTopicIsDeleted(topic)
      }
    }
  }

  private def validateErrorDeleteTopicRequestsWithIds(request: DeleteTopicsRequest, expectedResponse: Map[Uuid, Errors]): Unit = {
    val response = sendDeleteTopicsRequest(request)
    val responses = response.data.responses
    val errors = responses.asScala.map(result => result.topicId() -> result.errorCode()).toMap
    val names = responses.asScala.map(result => result.topicId() -> result.name()).toMap

    val errorCount = response.errorCounts().asScala.foldLeft(0)(_+_._2)
    assertEquals(expectedResponse.size, errorCount, "The response size should match")

    expectedResponse.foreach { case (topic, expectedError) =>
      assertEquals(expectedResponse(topic).code, errors(topic), "The response error should match")
      // If no error validate the topic was deleted
      if (expectedError == Errors.NONE) {
        validateTopicIsDeleted(names(topic))
      }
    }
  }

  @Test
  def testNotControllerErrorAbsentWhenRequestsForwarded(): Unit = {
    val request = new DeleteTopicsRequest.Builder(
        new DeleteTopicsRequestData()
          .setTopicNames(Collections.singletonList("not-controller"))
          .setTimeoutMs(1000)).build()
    val response = sendDeleteTopicsRequest(request, notControllerSocketServer)

    val error = response.data.responses.find("not-controller").errorCode()
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code,  error, 
      "Expected unknow topic or partition error instead of not controller error now that request forwarding is enabled.")
  }

  private def validateTopicIsDeleted(topic: String): Unit = {
    val metadata = connectAndReceive[MetadataResponse](new MetadataRequest.Builder(
      List(topic).asJava, true).build).topicMetadata.asScala
    TestUtils.waitUntilTrue (() => !metadata.exists(p => p.topic.equals(topic) && p.error == Errors.NONE),
      s"The topic $topic should not exist")
  }

  private def sendDeleteTopicsRequest(request: DeleteTopicsRequest, socketServer: SocketServer = controllerSocketServer): DeleteTopicsResponse = {
    connectAndReceive[DeleteTopicsResponse](request, destination = socketServer)
  }

}
