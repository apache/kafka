/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import kafka.security.authorizer.AclAuthorizer
import org.apache.kafka.common.message.{DescribeUserScramCredentialsRequestData, DescribeUserScramCredentialsResponseData}
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse}
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal}
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

/**
 * Test DescribeUserScramCredentialsRequest/Response API for the cases where no credentials exist
 * or failure is expected due to lack of authorization, sending the request to a non-controller broker, or some other issue.
 * Testing the API for the case where there are actually credentials to describe is performed elsewhere.
 */
class DescribeUserScramCredentialsRequestTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.ControlledShutdownEnableProp, "false")
    properties.put(KafkaConfig.AuthorizerClassNameProp, classOf[DescribeCredentialsTest.TestAuthorizer].getName)
    properties.put(KafkaConfig.PrincipalBuilderClassProp, classOf[DescribeCredentialsTest.TestPrincipalBuilderReturningAuthorized].getName)
  }

  @Test
  def testDescribeNothing(): Unit = {
    val request = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build()
    val response = sendDescribeUserScramCredentialsRequest(request)

    val error = response.data.errorCode
    assertEquals(Errors.NONE.code, error, "Expected no error when describing everything and there are no credentials")
    assertEquals(0, response.data.results.size, "Expected no credentials when describing everything and there are no credentials")
  }

  @Test
  def testDescribeWithNull(): Unit = {
    val request = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(null)).build()
    val response = sendDescribeUserScramCredentialsRequest(request)

    val error = response.data.errorCode
    assertEquals(Errors.NONE.code, error, "Expected no error when describing everything and there are no credentials")
    assertEquals(0, response.data.results.size, "Expected no credentials when describing everything and there are no credentials")
  }

  @Test
  def testDescribeNotController(): Unit = {
    val request = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build()
    val response = sendDescribeUserScramCredentialsRequest(request, notControllerSocketServer)

    val error = response.data.errorCode
    assertEquals(Errors.NONE.code, error, "Did not expect controller error when routed to non-controller")
  }

  @Test
  def testDescribeSameUserTwice(): Unit = {
    val user = "user1"
    val userName = new UserName().setName(user)
    val request = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(List(userName, userName).asJava)).build()
    val response = sendDescribeUserScramCredentialsRequest(request)

    assertEquals(Errors.NONE.code, response.data.errorCode, "Expected no top-level error")
    assertEquals(1, response.data.results.size)
    val result: DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult = response.data.results.get(0)
    assertEquals(Errors.DUPLICATE_RESOURCE.code, result.errorCode, s"Expected duplicate resource error for $user")
    assertEquals(s"Cannot describe SCRAM credentials for the same user twice in a single request: $user", result.errorMessage)
  }

  @Test
  def testUnknownUser(): Unit = {
    val unknownUser = "unknownUser"
    val request = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(List(new UserName().setName(unknownUser)).asJava)).build()
    val response = sendDescribeUserScramCredentialsRequest(request)

    assertEquals(Errors.NONE.code, response.data.errorCode, "Expected no top-level error")
    assertEquals(1, response.data.results.size)
    val result: DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult = response.data.results.get(0)
    assertEquals(Errors.RESOURCE_NOT_FOUND.code, result.errorCode, s"Expected duplicate resource error for $unknownUser")
    assertEquals(s"Attempt to describe a user credential that does not exist: $unknownUser", result.errorMessage)
  }

  private def sendDescribeUserScramCredentialsRequest(request: DescribeUserScramCredentialsRequest, socketServer: SocketServer = controllerSocketServer): DescribeUserScramCredentialsResponse = {
    connectAndReceive[DescribeUserScramCredentialsResponse](request, destination = socketServer)
  }
}

object DescribeCredentialsTest {
  val UnauthorizedPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Unauthorized")
  val AuthorizedPrincipal = KafkaPrincipal.ANONYMOUS

  class TestAuthorizer extends AclAuthorizer {
    override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
      actions.asScala.map { _ =>
        if (requestContext.requestType == ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS.id && requestContext.principal == UnauthorizedPrincipal)
          AuthorizationResult.DENIED
        else
          AuthorizationResult.ALLOWED
      }.asJava
    }
  }

  class TestPrincipalBuilderReturningAuthorized extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      AuthorizedPrincipal
    }
  }

  class TestPrincipalBuilderReturningUnauthorized extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      UnauthorizedPrincipal
    }
  }
}
