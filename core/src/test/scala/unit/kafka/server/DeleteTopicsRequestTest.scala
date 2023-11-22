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

import java.util.Arrays
import kafka.network.SocketServer
import kafka.utils._
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DeleteTopicsRequest
import org.apache.kafka.common.requests.DeleteTopicsResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class DeleteTopicsRequestTest extends BaseRequestTest with Logging {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicDeletionClusterHasOfflinePartitions(quorum: String): Unit = {
    // Create a two topics with one partition/replica. Make one of them offline.
    val offlineTopic = "topic-1"
    val onlineTopic = "topic-2"
    createTopicWithAssignment(offlineTopic, Map[Int, Seq[Int]](0 -> Seq(0)))
    createTopicWithAssignment(onlineTopic, Map[Int, Seq[Int]](0 -> Seq(1)))
    killBroker(0)
    ensureConsistentKRaftMetadata()

    // Ensure one topic partition is offline.
    TestUtils.waitUntilTrue(() => {
      aliveBrokers.head.metadataCache.getPartitionInfo(onlineTopic, 0).exists(_.leader() == 1) &&
        aliveBrokers.head.metadataCache.getPartitionInfo(offlineTopic, 0).exists(_.leader() ==
          MetadataResponse.NO_LEADER_ID)
    }, "Topic partition is not offline")

    // Delete the newly created topic and topic with offline partition. See the deletion is
    // successful.
    deleteTopic(onlineTopic)
    deleteTopic(offlineTopic)
    ensureConsistentKRaftMetadata()

    // Restart the dead broker.
    restartDeadBrokers()

    // Make sure the brokers no longer see any deleted topics.
    TestUtils.waitUntilTrue(() =>
      !aliveBrokers.forall(_.metadataCache.contains(onlineTopic)) &&
        !aliveBrokers.forall(_.metadataCache.contains(offlineTopic)),
      "The topics are found in the Broker's cache")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testValidDeleteTopicRequests(quorum: String): Unit = {
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

    ensureConsistentKRaftMetadata()

    request.data.topicNames.forEach { topic =>
      validateTopicIsDeleted(topic)
    }
  }

  private def validateValidDeleteTopicRequestsWithIds(request: DeleteTopicsRequest): Unit = {
    val response = sendDeleteTopicsRequest(request)
    val error = response.errorCounts.asScala.find(_._1 != Errors.NONE)
    assertTrue(error.isEmpty, s"There should be no errors, found ${response.data.responses.asScala}")

    ensureConsistentKRaftMetadata()

    response.data.responses.forEach { response =>
      validateTopicIsDeleted(response.name())
    }
  }

  /*
   * Only run this test against ZK cluster. The KRaft controller doesn't perform operations that have timed out.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testErrorDeleteTopicRequests(quorum: String): Unit = {
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
    TestUtils.waitUntilTrue(() => !brokers.head.metadataCache.contains(timeoutTopic), s"Topic $timeoutTopic is never deleted")
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

  private def validateTopicIsDeleted(topic: String): Unit = {
    val metadata = connectAndReceive[MetadataResponse](new MetadataRequest.Builder(
      List(topic).asJava, true).build).topicMetadata.asScala
    TestUtils.waitUntilTrue (() => !metadata.exists(p => p.topic.equals(topic) && p.error == Errors.NONE),
      s"The topic $topic should not exist")
  }

  private def sendDeleteTopicsRequest(
    request: DeleteTopicsRequest,
    socketServer: SocketServer = adminSocketServer
  ): DeleteTopicsResponse = {
    connectAndReceive[DeleteTopicsResponse](request, destination = socketServer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testDeleteTopicsVersions(quorum: String): Unit = {
    // This test assumes that the current valid versions are 0-6 please adjust the test if there are changes.
    assertEquals(0, DeleteTopicsRequestData.LOWEST_SUPPORTED_VERSION)
    assertEquals(6, DeleteTopicsRequestData.HIGHEST_SUPPORTED_VERSION)

    val timeout = 10000
    (0 until DeleteTopicsRequestData.SCHEMAS.size).foreach { version =>
      info(s"Creating and deleting tests for version $version")

      val topicName = s"topic-$version"

      createTopic(topicName, 1, 1)
      val data = new DeleteTopicsRequestData().setTimeoutMs(timeout)

      if (version < 6) {
        data.setTopicNames(Arrays.asList(topicName))
      } else {
        data.setTopics(Arrays.asList(new DeleteTopicState().setName(topicName)))
      }

      validateValidDeleteTopicRequests(new DeleteTopicsRequest.Builder(data).build(version.toShort))
    }
  }
}
