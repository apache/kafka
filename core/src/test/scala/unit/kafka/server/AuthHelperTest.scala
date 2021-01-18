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

import java.net.InetAddress
import java.util
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.easymock.EasyMock._
import org.easymock.{EasyMock, IArgumentMatcher}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class AuthHelperTest {
  import AuthHelperTest._

  private val clientId = ""

  @Test
  def testAuthorize(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

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

    EasyMock.expect(authorizer.authorize(requestContext, expectedActions.asJava))
      .andReturn(Seq(AuthorizationResult.ALLOWED).asJava)
      .once()

    EasyMock.replay(authorizer)

    val result = new AuthHelper(Some(authorizer)).authorize(
      requestContext, operation, resourceType, resourceName)

    verify(authorizer)

    assertEquals(true, result)
  }

  @Test
  def testFilterByAuthorized(): Unit = {
    val authorizer: Authorizer = EasyMock.niceMock(classOf[Authorizer])

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

    EasyMock.expect(authorizer.authorize(
      EasyMock.eq(requestContext), matchSameElements(expectedActions.asJava)
    )).andAnswer { () =>
      val actions = EasyMock.getCurrentArguments.apply(1).asInstanceOf[util.List[Action]].asScala
      actions.map { action =>
        if (Set(resourceName1, resourceName3).contains(action.resourcePattern.name))
          AuthorizationResult.ALLOWED
        else
          AuthorizationResult.DENIED
      }.asJava
    }.once()

    EasyMock.replay(authorizer)

    val result = new AuthHelper(Some(authorizer)).filterByAuthorized(
      requestContext,
      operation,
      resourceType,
      // Duplicate resource names should not trigger multiple calls to authorize
      Seq(resourceName1, resourceName2, resourceName1, resourceName3)
    )(identity)

    verify(authorizer)

    assertEquals(Set(resourceName1, resourceName3), result)
  }

}

object AuthHelperTest {

  /**
    * Similar to `EasyMock.eq`, but matches if both lists have the same elements irrespective of ordering.
    */
  def matchSameElements[T](list: java.util.List[T]): java.util.List[T] = {
    EasyMock.reportMatcher(new IArgumentMatcher {
      def matches(argument: Any): Boolean = argument match {
        case l: java.util.List[_] => list.asScala.toSet == l.asScala.toSet
        case _ => false
      }
      def appendTo(buffer: StringBuffer): Unit = buffer.append(s"list($list)")
    })
    null
  }

}
