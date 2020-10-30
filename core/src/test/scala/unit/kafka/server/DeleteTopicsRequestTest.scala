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
import java.util.{Arrays, Properties}

import kafka.network.SocketServer
import kafka.security.authorizer.AclAuthorizer
import kafka.utils._
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{DeleteTopicsRequest, DeleteTopicsResponse, MetadataRequest, MetadataResponse}
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder}
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}
import org.junit.Assert._
import org.junit.rules.TestName
import org.junit.{Rule, Test}

import scala.jdk.CollectionConverters._

class DeleteTopicsRequestTest extends BaseRequestTest {
  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.AuthorizerClassNameProp, classOf[DeleteTopicsTest.TestAuthorizer].getName)
    properties.put(KafkaConfig.PrincipalBuilderClassProp,
      if (testName.getMethodName.contains("NotAuthorized")) {
        classOf[DeleteTopicsTest.TestPrincipalBuilderReturningUnauthorized].getName
      } else {
        classOf[DeleteTopicsTest.TestPrincipalBuilderReturningAuthorized].getName
      })
  }

  private val _testName = new TestName
  @Rule def testName = _testName

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
  }

  private def validateValidDeleteTopicRequests(request: DeleteTopicsRequest): Unit = {
    val response = sendDeleteTopicsRequest(request)
    val error = response.errorCounts.asScala.find(_._1 != Errors.NONE)
    assertTrue(s"There should be no errors, found ${response.data.responses.asScala}", error.isEmpty)
    request.data.topicNames.forEach { topic =>
      validateTopicIsDeleted(topic)
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
    assertEquals("The response size should match", expectedResponse.size, errorCount)

    expectedResponse.foreach { case (topic, expectedError) =>
      assertEquals("The response error should match", expectedResponse(topic).code, errors.find(topic).errorCode)
      // If no error validate the topic was deleted
      if (expectedError == Errors.NONE) {
        validateTopicIsDeleted(topic)
      }
    }
  }

  @Test
  def testNotController(): Unit = {
    val topicExistsTopicName = "topic-exists"
    val topicNotExistsTopicName = "topic-not-exists"
    createTopic(topicExistsTopicName, 1, 1)
    val request = new DeleteTopicsRequest.Builder(
        new DeleteTopicsRequestData()
          .setTopicNames(List(topicExistsTopicName, topicNotExistsTopicName).asJava)
          .setTimeoutMs(1000)).build()
    val response = sendDeleteTopicsRequest(request, notControllerSocketServer)

    val errorExists = response.data.responses().find(topicExistsTopicName).errorCode()
    assertEquals("Expected controller error for existing topic when routed incorrectly",
      Errors.NOT_CONTROLLER.code, errorExists)
    val errorNotExists = response.data.responses().find(topicNotExistsTopicName).errorCode()
    assertEquals("Expected not-exists error for unknown topic when routed incorrectly",
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code, errorNotExists)
  }

  @Test
  def testNotControllerAndNotAuthorized(): Unit = {  // "NotAuthorized" in method name signals authorizer to reject requests
    val topicExistsAndNotAuthorizedTopicName = "topic-exists"
    val topicExistsAndAuthorizedTopicName = DeleteTopicsTest.alwaysAuthorizedTopic1
    createTopic(topicExistsAndNotAuthorizedTopicName, 1, 1)
    createTopic(topicExistsAndAuthorizedTopicName, 1, 1)
    val topicNotExistsAndNotAuthorizedTopicName = "topic-not-exists"
    val topicNotExistsAndAuthorizedTopicName = DeleteTopicsTest.alwaysAuthorizedTopic2
    // make sure we get the authorization error rather than the not-controller error when not authorized
    // and the not-controller error only when authorized
    val request = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopicNames(List(topicExistsAndNotAuthorizedTopicName, topicExistsAndAuthorizedTopicName,
          topicNotExistsAndNotAuthorizedTopicName, topicNotExistsAndAuthorizedTopicName).asJava)
        .setTimeoutMs(1000)).build()
    val response = sendDeleteTopicsRequest(request, notControllerSocketServer)

    val errorTopicExistsAndNotAuthorizedTopicName = response.data.responses().find(topicExistsAndNotAuthorizedTopicName).errorCode()
    assertEquals("Expected authorization error for existing, unauthorized topic when routed incorrectly",
      Errors.TOPIC_AUTHORIZATION_FAILED.code, errorTopicExistsAndNotAuthorizedTopicName)
    val errorTopicExistsAndAuthorizedTopicName = response.data.responses().find(topicExistsAndAuthorizedTopicName).errorCode()
    assertEquals("Expected not-controller error for existing, authorized topic when routed incorrectly",
      Errors.NOT_CONTROLLER.code, errorTopicExistsAndAuthorizedTopicName)
    val errorTopicNotExistsAndNotAuthorizedTopicName = response.data.responses().find(topicNotExistsAndNotAuthorizedTopicName).errorCode()
    assertEquals("Expected authorization error for non-existing, unauthorized topic when routed incorrectly",
      Errors.TOPIC_AUTHORIZATION_FAILED.code, errorTopicNotExistsAndNotAuthorizedTopicName)
    val errorTopicNotExistsAndAuthorizedTopicName = response.data.responses().find(topicNotExistsAndAuthorizedTopicName).errorCode()
    assertEquals("Expected unknown topic error for non-existing, authorized topic when routed incorrectly",
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code, errorTopicNotExistsAndAuthorizedTopicName)
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

object DeleteTopicsTest {
  val UnauthorizedPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Unauthorized")
  val AuthorizedPrincipal = KafkaPrincipal.ANONYMOUS
  val alwaysAuthorizedTopic1 = "always_authorized_topic1"
  val alwaysAuthorizedTopic2 = "always_authorized_topic2"

  class TestAuthorizer extends AclAuthorizer {
    override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
      actions.asScala.map { action =>
        if (requestContext.requestType == ApiKeys.DELETE_TOPICS.id && requestContext.principal == UnauthorizedPrincipal
          && action.resourcePattern().name() != alwaysAuthorizedTopic1
          && action.resourcePattern().name() != alwaysAuthorizedTopic2) {
          AuthorizationResult.DENIED
        } else
          AuthorizationResult.ALLOWED
      }.asJava
    }
  }

  class TestPrincipalBuilderReturningAuthorized extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      AuthorizedPrincipal
    }
  }

  class TestPrincipalBuilderReturningUnauthorized extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      UnauthorizedPrincipal
    }
  }
}
