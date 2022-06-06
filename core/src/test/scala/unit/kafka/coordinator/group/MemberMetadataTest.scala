/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.group

import java.util.Arrays

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class MemberMetadataTest {
  val groupId = "groupId"
  val groupInstanceId = Some("groupInstanceId")
  val clientId = "clientId"
  val clientHost = "clientHost"
  val memberId = "memberId"
  val protocolType = "consumer"
  val rebalanceTimeoutMs = 60000
  val sessionTimeoutMs = 10000


  @Test
  def testMatchesSupportedProtocols(): Unit = {
    val protocols = List(("range", Array.empty[Byte]))

    val member = new MemberMetadata(memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertTrue(member.matches(protocols))
    assertFalse(member.matches(List(("range", Array[Byte](0)))))
    assertFalse(member.matches(List(("roundrobin", Array.empty[Byte]))))
    assertFalse(member.matches(List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte]))))
  }

  @Test
  def testVoteForPreferredProtocol(): Unit = {
    val protocols = List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte]))

    val member = new MemberMetadata(memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertEquals("range", member.vote(Set("range", "roundrobin")))
    assertEquals("roundrobin", member.vote(Set("blah", "roundrobin")))
  }

  @Test
  def testMetadata(): Unit = {
    val protocols = List(("range", Array[Byte](0)), ("roundrobin", Array[Byte](1)))

    val member = new MemberMetadata(memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertTrue(Arrays.equals(Array[Byte](0), member.metadata("range")))
    assertTrue(Arrays.equals(Array[Byte](1), member.metadata("roundrobin")))
  }

  @Test
  def testMetadataRaisesOnUnsupportedProtocol(): Unit = {
    val protocols = List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte]))

    val member = new MemberMetadata(memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertThrows(classOf[IllegalArgumentException], () => member.metadata("blah"))
  }

  @Test
  def testVoteRaisesOnNoSupportedProtocols(): Unit = {
    val protocols = List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte]))

    val member = new MemberMetadata(memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertThrows(classOf[IllegalArgumentException], () => member.vote(Set("blah")))
  }

  @Test
  def testHasValidGroupInstanceId(): Unit = {
    val protocols = List(("range", Array[Byte](0)), ("roundrobin", Array[Byte](1)))

    val member = new MemberMetadata(memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertTrue(member.isStaticMember)
    assertEquals(groupInstanceId, member.groupInstanceId)
  }
}
