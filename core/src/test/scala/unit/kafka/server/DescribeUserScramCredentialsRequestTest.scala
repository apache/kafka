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
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder}
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._

/**
 * Test DescribeUserScramCredentialsRequest/Response API for the cases where either no credentials exist
 * or failure is expected due to lack of authorization or sending the request to a non-controller broker.
 * Testing the API for the case where there are actually credentials to describe is performed elsewhere.
 */
class DescribeUserScramCredentialsRequestTest extends BaseRequestTest {
  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.ControlledShutdownEnableProp, "false")
    properties.put(KafkaConfig.AuthorizerClassNameProp, classOf[DescribeCredentialsTest.TestAuthorizer].getName)
    properties.put(KafkaConfig.PrincipalBuilderClassProp, classOf[DescribeCredentialsTest.TestPrincipalBuilder].getName)
  }

  @Before
  override def setUp(): Unit = {
    DescribeCredentialsTest.principal = KafkaPrincipal.ANONYMOUS // default is to be authorized
    super.setUp()
  }

  @Test
  def testDescribeNothing(): Unit = {
    val request = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build()
    val response = sendDescribeUserScramCredentialsRequest(request)

    val error = response.data.error
    assertEquals("Expected no error when routed correctly", Errors.NONE.code, error)
    assertEquals("Expected no credentials", 0, response.data.userScramCredentials.size)
  }

  @Test
  def testDescribeNotController(): Unit = {
    val request = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build()
    val response = sendDescribeUserScramCredentialsRequest(request, notControllerSocketServer)

    val error = response.data.error
    assertEquals("Expected controller error when routed incorrectly", Errors.NOT_CONTROLLER.code, error)
  }

  @Test
  def testDescribeNotAuthorized(): Unit = {
    DescribeCredentialsTest.principal = DescribeCredentialsTest.UnauthorizedPrincipal

    val request = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build()
    val response = sendDescribeUserScramCredentialsRequest(request)

    val error = response.data.error
    assertEquals("Expected not authorized error", Errors.CLUSTER_AUTHORIZATION_FAILED.code, error)
  }

  private def sendDescribeUserScramCredentialsRequest(request: DescribeUserScramCredentialsRequest, socketServer: SocketServer = controllerSocketServer): DescribeUserScramCredentialsResponse = {
    connectAndReceive[DescribeUserScramCredentialsResponse](request, destination = socketServer)
  }
}

object DescribeCredentialsTest {
  val UnauthorizedPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Unauthorized")
  // Principal used for all client connections. This is modified by tests which
  // check unauthorized code path
  var principal = KafkaPrincipal.ANONYMOUS

  class TestAuthorizer extends AclAuthorizer {
    override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
      // UnauthorizedPrincipal is not authorized for DESCRIBE permission on CLUSTER resource
      actions.asScala.map { action =>
        if (requestContext.principal == UnauthorizedPrincipal && action.operation == AclOperation.DESCRIBE && action.resourcePattern.resourceType == ResourceType.CLUSTER)
          AuthorizationResult.DENIED
        else
          AuthorizationResult.ALLOWED
      }.asJava
    }
  }

  class TestPrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      principal
    }
  }
}
