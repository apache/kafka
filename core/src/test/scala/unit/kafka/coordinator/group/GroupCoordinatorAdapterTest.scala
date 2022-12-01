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
package kafka.coordinator.group

import kafka.coordinator.group.GroupCoordinatorConcurrencyTest.JoinGroupCallback
import kafka.server.RequestLocal
import org.apache.kafka.common.message.{HeartbeatRequestData, HeartbeatResponseData, JoinGroupRequestData, JoinGroupResponseData}
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, verify}

import java.net.InetAddress
import scala.jdk.CollectionConverters._

class GroupCoordinatorAdapterTest {

  private def makeContext(
    apiKey: ApiKeys,
    apiVersion: Short
  ): RequestContext = {
    new RequestContext(
      new RequestHeader(apiKey, apiVersion, "client", 0),
      "1",
      InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS,
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
      SecurityProtocol.PLAINTEXT,
      ClientInformation.EMPTY,
      false
    )
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testJoinGroup(version: Short): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

    val ctx = makeContext(ApiKeys.JOIN_GROUP, version)
    val request = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)
      .setReason("reason")
      .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection(List(
        new JoinGroupRequestProtocol()
          .setName("first")
          .setMetadata("first".getBytes()),
        new JoinGroupRequestProtocol()
          .setName("second")
          .setMetadata("second".getBytes())).iterator.asJava))
    val bufferSupplier = BufferSupplier.create()

    val future = adapter.joinGroup(ctx, request, bufferSupplier)
    assertFalse(future.isDone)

    val capturedProtocols: ArgumentCaptor[List[(String, Array[Byte])]] =
      ArgumentCaptor.forClass(classOf[List[(String, Array[Byte])]])
    val capturedCallback: ArgumentCaptor[JoinGroupCallback] =
      ArgumentCaptor.forClass(classOf[JoinGroupCallback])

    verify(groupCoordinator).handleJoinGroup(
      ArgumentMatchers.eq(request.groupId),
      ArgumentMatchers.eq(request.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(if (version >= 4) true else false),
      ArgumentMatchers.eq(if (version >= 9) true else false),
      ArgumentMatchers.eq(ctx.clientId),
      ArgumentMatchers.eq(InetAddress.getLocalHost.toString),
      ArgumentMatchers.eq(request.rebalanceTimeoutMs),
      ArgumentMatchers.eq(request.sessionTimeoutMs),
      ArgumentMatchers.eq(request.protocolType),
      capturedProtocols.capture(),
      capturedCallback.capture(),
      ArgumentMatchers.eq(Some("reason")),
      ArgumentMatchers.eq(RequestLocal(bufferSupplier))
    )

    assertEquals(List(
      ("first", "first"),
      ("second", "second")
    ), capturedProtocols.getValue.map { case (name, metadata) =>
      (name, new String(metadata))
    })

    capturedCallback.getValue.apply(JoinGroupResult(
      members = List(
        new JoinGroupResponseMember()
          .setMemberId("member")
          .setMetadata("member".getBytes())
          .setGroupInstanceId("instance")
      ),
      memberId = "member",
      generationId = 10,
      protocolType = Some("consumer"),
      protocolName = Some("range"),
      leaderId = "leader",
      skipAssignment = true,
      error = Errors.UNKNOWN_MEMBER_ID
    ))

    val expectedData = new JoinGroupResponseData()
      .setMembers(List(new JoinGroupResponseMember()
        .setMemberId("member")
        .setMetadata("member".getBytes())
        .setGroupInstanceId("instance")).asJava)
      .setMemberId("member")
      .setGenerationId(10)
      .setProtocolType("consumer")
      .setProtocolName("range")
      .setLeader("leader")
      .setSkipAssignment(true)
      .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code)

    assertTrue(future.isDone)
    assertEquals(expectedData, future.get())
  }

  @Test
  def testHeartbeat(): Unit = {
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val adapter = new GroupCoordinatorAdapter(groupCoordinator)

    val ctx = makeContext(ApiKeys.HEARTBEAT, ApiKeys.HEARTBEAT.latestVersion)
    val data = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member1")
      .setGenerationId(0)

    val future = adapter.heartbeat(ctx, data)

    val capturedCallback: ArgumentCaptor[Errors => Unit] =
      ArgumentCaptor.forClass(classOf[Errors => Unit])

    verify(groupCoordinator).handleHeartbeat(
      ArgumentMatchers.eq(data.groupId),
      ArgumentMatchers.eq(data.memberId),
      ArgumentMatchers.eq(None),
      ArgumentMatchers.eq(data.generationId),
      capturedCallback.capture(),
    )

    assertFalse(future.isDone)

    capturedCallback.getValue.apply(Errors.NONE)

    assertTrue(future.isDone)
    assertEquals(new HeartbeatResponseData(), future.get())
  }

}
