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

import org.junit.Assert._
import org.junit.Test

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
  def testMatchesSupportedProtocols() {
    val protocols = List(("range", Array.empty[Byte]))

    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertTrue(member.matches(protocols))
    assertFalse(member.matches(List(("range", Array[Byte](0)))))
    assertFalse(member.matches(List(("roundrobin", Array.empty[Byte]))))
    assertFalse(member.matches(List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte]))))
  }

  @Test
  def testVoteForPreferredProtocol() {
    val protocols = List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte]))

    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertEquals("range", member.vote(Set("range", "roundrobin")))
    assertEquals("roundrobin", member.vote(Set("blah", "roundrobin")))
  }

  @Test
  def testMetadata() {
    val protocols = List(("range", Array[Byte](0)), ("roundrobin", Array[Byte](1)))

    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertTrue(Arrays.equals(Array[Byte](0), member.metadata("range")))
    assertTrue(Arrays.equals(Array[Byte](1), member.metadata("roundrobin")))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testMetadataRaisesOnUnsupportedProtocol() {
    val protocols = List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte]))

    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    member.metadata("blah")
    fail()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testVoteRaisesOnNoSupportedProtocols() {
    val protocols = List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte]))

    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    member.vote(Set("blah"))
    fail()
  }

  @Test
  def testHasValidGroupInstanceId() {
    val protocols = List(("range", Array[Byte](0)), ("roundrobin", Array[Byte](1)))

    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, protocols)
    assertTrue(member.isStaticMember)
    assertEquals(groupInstanceId, member.groupInstanceId)
  }
}
