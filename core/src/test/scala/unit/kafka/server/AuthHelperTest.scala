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

import kafka.network.RequestChannel.Request
import org.apache.kafka.clients.admin.EndpointType

import java.net.InetAddress
import java.util
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.{DescribeClusterRequestData, DescribeClusterResponseData}
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{DescribeClusterRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.argThat
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, verify, when}

import scala.collection.Seq
import scala.jdk.CollectionConverters._

object AuthHelperTest {
  def newMockDescribeClusterRequest(
    data: DescribeClusterRequestData,
    requestVersion: Int
  ): Request = {
    val requestContext = new RequestContext(
      new RequestHeader(ApiKeys.DESCRIBE_CLUSTER, requestVersion.toShort, "", 0),
      "",
      InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS,
      new ListenerName("PLAINTEXT"),
      SecurityProtocol.PLAINTEXT,
      ClientInformation.EMPTY,
      false)
    val request: Request = mock(classOf[Request])
    when(request.body[DescribeClusterRequest]).thenReturn(
      new DescribeClusterRequest(data, requestVersion.toShort))
    when(request.context).thenReturn(requestContext)
    when(request.header).thenReturn(requestContext.header)
    request
  }
}

class AuthHelperTest {
  import AuthHelperTest.newMockDescribeClusterRequest

  private val clientId = ""

  @Test
  def testAuthorize(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val operation = AclOperation.WRITE
    val resourceType = ResourceType.TOPIC
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion, clientId, 0)
    val requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
      SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, true, true)
    )

    when(authorizer.authorize(requestContext, expectedActions.asJava))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val result = new AuthHelper(Some(authorizer)).authorize(
      requestContext, operation, resourceType, resourceName)

    verify(authorizer).authorize(requestContext, expectedActions.asJava)

    assertEquals(true, result)
  }

  @Test
  def testFilterByAuthorized(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val operation = AclOperation.WRITE
    val resourceType = ResourceType.TOPIC
    val resourceName1 = "topic-1"
    val resourceName2 = "topic-2"
    val resourceName3 = "topic-3"
    val requestHeader = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion,
      clientId, 0)
    val requestContext = new RequestContext(requestHeader, "1", InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
      SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, resourceName1, PatternType.LITERAL),
        2, true, true),
      new Action(operation, new ResourcePattern(resourceType, resourceName2, PatternType.LITERAL),
        1, true, true),
      new Action(operation, new ResourcePattern(resourceType, resourceName3, PatternType.LITERAL),
        1, true, true),
    )

    when(authorizer.authorize(
      ArgumentMatchers.eq(requestContext), argThat((t: java.util.List[Action]) => t.containsAll(expectedActions.asJava))
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]].asScala
      actions.map { action =>
        if (Set(resourceName1, resourceName3).contains(action.resourcePattern.name))
          AuthorizationResult.ALLOWED
        else
          AuthorizationResult.DENIED
      }.asJava
    }

    val result = new AuthHelper(Some(authorizer)).filterByAuthorized(
      requestContext,
      operation,
      resourceType,
      // Duplicate resource names should not trigger multiple calls to authorize
      Seq(resourceName1, resourceName2, resourceName1, resourceName3)
    )(identity)

    verify(authorizer).authorize(
      ArgumentMatchers.eq(requestContext), argThat((t: java.util.List[Action]) => t.containsAll(expectedActions.asJava))
    )

    assertEquals(Set(resourceName1, resourceName3), result)
  }

  @Test
  def testComputeDescribeClusterResponseV1WithUnknownEndpointType(): Unit = {
    val authHelper = new AuthHelper(Some(mock(classOf[Authorizer])))
    val request = newMockDescribeClusterRequest(
      new DescribeClusterRequestData().setEndpointType(123.toByte), 1)
    val responseData = authHelper.computeDescribeClusterResponse(request,
      EndpointType.BROKER,
      "ltCWoi9wRhmHSQCIgAznEg",
      () => new DescribeClusterBrokerCollection(),
      () => 1)
    assertEquals(new DescribeClusterResponseData().
      setErrorCode(Errors.UNSUPPORTED_ENDPOINT_TYPE.code()).
      setErrorMessage("Unsupported endpoint type 123"), responseData)
  }

  @Test
  def testComputeDescribeClusterResponseV0WithUnknownEndpointType(): Unit = {
    val authHelper = new AuthHelper(Some(mock(classOf[Authorizer])))
    val request = newMockDescribeClusterRequest(
      new DescribeClusterRequestData().setEndpointType(123.toByte), 0)
    val responseData = authHelper.computeDescribeClusterResponse(request,
      EndpointType.BROKER,
      "ltCWoi9wRhmHSQCIgAznEg",
      () => new DescribeClusterBrokerCollection(),
      () => 1)
    assertEquals(new DescribeClusterResponseData().
      setErrorCode(Errors.INVALID_REQUEST.code()).
      setErrorMessage("Unsupported endpoint type 123"), responseData)
  }

  @Test
  def testComputeDescribeClusterResponseV1WithUnexpectedEndpointType(): Unit = {
    val authHelper = new AuthHelper(Some(mock(classOf[Authorizer])))
    val request = newMockDescribeClusterRequest(
      new DescribeClusterRequestData().setEndpointType(EndpointType.BROKER.id()), 1)
    val responseData = authHelper.computeDescribeClusterResponse(request,
      EndpointType.CONTROLLER,
      "ltCWoi9wRhmHSQCIgAznEg",
      () => new DescribeClusterBrokerCollection(),
      () => 1)
    assertEquals(new DescribeClusterResponseData().
      setErrorCode(Errors.MISMATCHED_ENDPOINT_TYPE.code()).
      setErrorMessage("The request was sent to an endpoint of type CONTROLLER, but we wanted an endpoint of type BROKER"), responseData)
  }

  @Test
  def testComputeDescribeClusterResponseV0WithUnexpectedEndpointType(): Unit = {
    val authHelper = new AuthHelper(Some(mock(classOf[Authorizer])))
    val request = newMockDescribeClusterRequest(
      new DescribeClusterRequestData().setEndpointType(EndpointType.BROKER.id()), 0)
    val responseData = authHelper.computeDescribeClusterResponse(request,
      EndpointType.CONTROLLER,
      "ltCWoi9wRhmHSQCIgAznEg",
      () => new DescribeClusterBrokerCollection(),
      () => 1)
    assertEquals(new DescribeClusterResponseData().
      setErrorCode(Errors.INVALID_REQUEST.code()).
      setErrorMessage("The request was sent to an endpoint of type CONTROLLER, but we wanted an endpoint of type BROKER"), responseData)
  }

  @Test
  def testComputeDescribeClusterResponseWhereControllerIsNotFound(): Unit = {
    val authHelper = new AuthHelper(Some(mock(classOf[Authorizer])))
    val request = newMockDescribeClusterRequest(
      new DescribeClusterRequestData().setEndpointType(EndpointType.CONTROLLER.id()), 1)
    val responseData = authHelper.computeDescribeClusterResponse(request,
      EndpointType.CONTROLLER,
      "ltCWoi9wRhmHSQCIgAznEg",
      () => new DescribeClusterBrokerCollection(),
      () => 1)
    assertEquals(new DescribeClusterResponseData().
      setClusterId("ltCWoi9wRhmHSQCIgAznEg").
      setControllerId(-1).
      setClusterAuthorizedOperations(Int.MinValue).
      setEndpointType(2.toByte), responseData)
  }

  @Test
  def testComputeDescribeClusterResponseSuccess(): Unit = {
    val authHelper = new AuthHelper(Some(mock(classOf[Authorizer])))
    val request = newMockDescribeClusterRequest(
      new DescribeClusterRequestData().setEndpointType(EndpointType.CONTROLLER.id()), 1)
    val nodes = new DescribeClusterBrokerCollection(
      java.util.Arrays.asList[DescribeClusterResponseData.DescribeClusterBroker](
        new DescribeClusterResponseData.DescribeClusterBroker().setBrokerId(1)).iterator())
    val responseData = authHelper.computeDescribeClusterResponse(request,
      EndpointType.CONTROLLER,
      "ltCWoi9wRhmHSQCIgAznEg",
      () => nodes,
      () => 1)
    assertEquals(new DescribeClusterResponseData().
      setClusterId("ltCWoi9wRhmHSQCIgAznEg").
      setControllerId(1).
      setClusterAuthorizedOperations(Int.MinValue).
      setBrokers(nodes).
      setEndpointType(2.toByte), responseData)
  }
}
