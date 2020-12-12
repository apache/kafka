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

package kafka.security.authorizer

import java.net.InetAddress
import java.util.UUID

import org.apache.kafka.common.acl.AclOperation.{ALL, READ, WRITE}
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.acl.{AccessControlEntry, AclOperation}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourceType.{GROUP, TOPIC}
import org.apache.kafka.common.resource.{ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.server.authorizer.Authorizer
import org.junit.Assert.{assertFalse, assertTrue}

class AuthorizerTestFactory(val newRequestContext3: (KafkaPrincipal, InetAddress, ApiKeys) => RequestContext,
                            val addAcls: (Authorizer, Set[AccessControlEntry], ResourcePattern) => Unit,
                            val authorizeByResourceType: (Authorizer, RequestContext, AclOperation, ResourceType) => Boolean,
                            val removeAcls: (Authorizer, Set[AccessControlEntry], ResourcePattern) => Unit) {
  def newRequestContext(kafkaPrincipal: KafkaPrincipal, inetAddress: InetAddress): RequestContext =
    newRequestContext3(kafkaPrincipal, inetAddress, ApiKeys.PRODUCE)

  def testAuthorizeByResourceTypeMultipleAddAndRemove(authorizer: Authorizer): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1")
    val host1 = InetAddress.getByName("192.168.1.1")
    val resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL)
    val denyRead = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, DENY)
    val allowRead = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, ALLOW)
    val u1h1Context = newRequestContext(user1, host1)

    for (_ <- 1 to 10) {
      assertFalse("User1 from host1 should not have READ access to any topic when no ACL exists",
        authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

      addAcls(authorizer, Set(allowRead), resource1)
      assertTrue("User1 from host1 now should have READ access to at least one topic",
        authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

      for (_ <- 1 to 10) {
        addAcls(authorizer, Set(denyRead), resource1)
        assertFalse("User1 from host1 now should not have READ access to any topic",
          authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

        removeAcls(authorizer, Set(denyRead), resource1)
        addAcls(authorizer, Set(allowRead), resource1)
        assertTrue("User1 from host1 now should have READ access to at least one topic",
          authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))
      }

      removeAcls(authorizer, Set(allowRead), resource1)
      assertFalse("User1 from host1 now should not have READ access to any topic",
        authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))
    }
  }

   def testAuthorizeByResourceTypeIsolationUnrelatedDenyWontDominateAllow(authorizer: Authorizer): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1")
    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user2")
    val host1 = InetAddress.getByName("192.168.1.1")
    val host2 = InetAddress.getByName("192.168.1.2")
    val resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL)
    val resource2 = new ResourcePattern(TOPIC, "sb2" + UUID.randomUUID(), LITERAL)
    val resource3 = new ResourcePattern(GROUP, "s", PREFIXED)

    val acl1 = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, DENY)
    val acl2 = new AccessControlEntry(user2.toString, host1.getHostAddress, READ, DENY)
    val acl3 = new AccessControlEntry(user1.toString, host2.getHostAddress, WRITE, DENY)
    val acl4 = new AccessControlEntry(user1.toString, host2.getHostAddress, READ, DENY)
    val acl5 = new AccessControlEntry(user1.toString, host2.getHostAddress, READ, DENY)
    val acl6 = new AccessControlEntry(user2.toString, host2.getHostAddress, READ, DENY)
    val acl7 = new AccessControlEntry(user1.toString, host2.getHostAddress, READ, ALLOW)

    addAcls(authorizer, Set(acl1, acl2, acl3, acl6, acl7), resource1)
    addAcls(authorizer, Set(acl4), resource2)
    addAcls(authorizer, Set(acl5), resource3)

    val u1h1Context = newRequestContext(user1, host1)
    val u1h2Context = newRequestContext(user1, host2)

    assertFalse("User1 from host1 should not have READ access to any topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))
    assertFalse("User1 from host2 should not have READ access to any consumer group",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.GROUP))
    assertFalse("User1 from host2 should not have READ access to any topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TRANSACTIONAL_ID))
    assertFalse("User1 from host2 should not have READ access to any topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.CLUSTER))
    assertTrue("User1 from host2 should have READ access to at least one topic",
      authorizeByResourceType(authorizer, u1h2Context, READ, ResourceType.TOPIC))
  }

   def testAuthorizeByResourceTypeDenyTakesPrecedence(authorizer: Authorizer): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1")
    val host1 = InetAddress.getByName("192.168.1.1")
    val resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL)

    val u1h1Context = newRequestContext(user1, host1)
    val acl1 = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, ALLOW)
    val acl2 = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, DENY)

    addAcls(authorizer, Set(acl1), resource1)
    assertTrue("User1 from host1 should have WRITE access to at least one topic",
      authorizeByResourceType(authorizer, u1h1Context, WRITE, ResourceType.TOPIC))

    addAcls(authorizer, Set(acl2), resource1)
    assertFalse("User1 from host1 should not have WRITE access to any topic",
      authorizeByResourceType(authorizer, u1h1Context, WRITE, ResourceType.TOPIC))
  }

  def testAuthorizeByResourceTypePrefixedResourceDenyDominate(authorizer: Authorizer): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1")
    val host1 = InetAddress.getByName("192.168.1.1")
    val a = new ResourcePattern(GROUP, "a", PREFIXED)
    val ab = new ResourcePattern(GROUP, "ab", PREFIXED)
    val abc = new ResourcePattern(GROUP, "abc", PREFIXED)
    val abcd = new ResourcePattern(GROUP, "abcd", PREFIXED)
    val abcde = new ResourcePattern(GROUP, "abcde", PREFIXED)

    val u1h1Context = newRequestContext(user1, host1)
    val allowAce = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, ALLOW)
    val denyAce = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, DENY)

    addAcls(authorizer, Set(allowAce), abcde)
    assertTrue("User1 from host1 should have READ access to at least one group",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.GROUP))

    addAcls(authorizer, Set(denyAce), abcd)
    assertFalse("User1 from host1 now should not have READ access to any group",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.GROUP))

    addAcls(authorizer, Set(allowAce), abc)
    assertTrue("User1 from host1 now should have READ access to any group",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.GROUP))

    addAcls(authorizer, Set(denyAce), a)
    assertFalse("User1 from host1 now should not have READ access to any group",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.GROUP))

    addAcls(authorizer, Set(allowAce), ab)
    assertFalse("User1 from host1 still should not have READ access to any group",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.GROUP))
  }

  def testAuthorizeByResourceTypeWildcardResourceDenyDominate(authorizer: Authorizer): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1")
    val host1 = InetAddress.getByName("192.168.1.1")
    val wildcard = new ResourcePattern(GROUP, ResourcePattern.WILDCARD_RESOURCE, LITERAL)
    val prefixed = new ResourcePattern(GROUP, "hello", PREFIXED)
    val literal = new ResourcePattern(GROUP, "aloha", LITERAL)

    val u1h1Context = newRequestContext(user1, host1)
    val allowAce = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, ALLOW)
    val denyAce = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, DENY)

    addAcls(authorizer, Set(allowAce), prefixed)
    assertTrue("User1 from host1 should have WRITE access to at least one group",
      authorizeByResourceType(authorizer, u1h1Context, WRITE, ResourceType.GROUP))

    addAcls(authorizer, Set(denyAce), wildcard)
    assertFalse("User1 from host1 now should not have WRITE access to any group",
      authorizeByResourceType(authorizer, u1h1Context, WRITE, ResourceType.GROUP))

    addAcls(authorizer, Set(allowAce), wildcard)
    assertFalse("User1 from host1 still should not have WRITE access to any group",
      authorizeByResourceType(authorizer, u1h1Context, WRITE, ResourceType.GROUP))

    addAcls(authorizer, Set(allowAce), literal)
    assertFalse("User1 from host1 still should not have WRITE access to any group",
      authorizeByResourceType(authorizer, u1h1Context, WRITE, ResourceType.GROUP))
  }

  def testAuthorizeByResourceTypeWithAllOperationAce(authorizer: Authorizer): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1")
    val host1 = InetAddress.getByName("192.168.1.1")
    val resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL)
    val denyAll = new AccessControlEntry(user1.toString, host1.getHostAddress, ALL, DENY)
    val allowAll = new AccessControlEntry(user1.toString, host1.getHostAddress, ALL, ALLOW)
    val denyWrite = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, DENY)
    val u1h1Context = newRequestContext(user1, host1)

    assertFalse("User1 from host1 should not have READ access to any topic when no ACL exists",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(denyWrite, allowAll), resource1)
    assertTrue("User1 from host1 now should have READ access to at least one topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(denyAll), resource1)
    assertFalse("User1 from host1 now should not have READ access to any topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))
  }

  def testAuthorizeByResourceTypeWithAllHostAce(authorizer: Authorizer): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1")
    val host1 = InetAddress.getByName("192.168.1.1")
    val host2 = InetAddress.getByName("192.168.1.2")
    val allHost = AclEntry.WildcardHost
    val resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL)
    val resource2 = new ResourcePattern(TOPIC, "sb2" + UUID.randomUUID(), LITERAL)
    val allowHost1 = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, ALLOW)
    val denyHost1 = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, DENY)
    val denyAllHost = new AccessControlEntry(user1.toString, allHost, READ, DENY)
    val allowAllHost = new AccessControlEntry(user1.toString, allHost, READ, ALLOW)
    val u1h1Context = newRequestContext(user1, host1)
    val u1h2Context = newRequestContext(user1, host2)

    assertFalse("User1 from host1 should not have READ access to any topic when no ACL exists",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(allowHost1), resource1)
    assertTrue("User1 from host1 should now have READ access to at least one topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(denyAllHost), resource1)
    assertFalse("User1 from host1 now shouldn't have READ access to any topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(denyHost1), resource2)
    assertFalse("User1 from host1 still should not have READ access to any topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))
    assertFalse("User1 from host2 should not have READ access to any topic",
      authorizeByResourceType(authorizer, u1h2Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(allowAllHost), resource2)
    assertTrue("User1 from host2 should now have READ access to at least one topic",
      authorizeByResourceType(authorizer, u1h2Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(denyAllHost), resource2)
    assertFalse("User1 from host2 now shouldn't have READ access to any topic",
      authorizeByResourceType(authorizer, u1h2Context, READ, ResourceType.TOPIC))
  }


  def testAuthorizeByResourceTypeWithAllPrincipalAce(authorizer: Authorizer): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1")
    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user2")
    val allUser = AclEntry.WildcardPrincipalString
    val host1 = InetAddress.getByName("192.168.1.1")
    val resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL)
    val resource2 = new ResourcePattern(TOPIC, "sb2" + UUID.randomUUID(), LITERAL)
    val allowUser1 = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, ALLOW)
    val denyUser1 = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, DENY)
    val denyAllUser = new AccessControlEntry(allUser, host1.getHostAddress, READ, DENY)
    val allowAllUser = new AccessControlEntry(allUser, host1.getHostAddress, READ, ALLOW)
    val u1h1Context = newRequestContext(user1, host1)
    val u2h1Context = newRequestContext(user2, host1)

    assertFalse("User1 from host1 should not have READ access to any topic when no ACL exists",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(allowUser1), resource1)
    assertTrue("User1 from host1 should now have READ access to at least one topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(denyAllUser), resource1)
    assertFalse("User1 from host1 now shouldn't have READ access to any topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(denyUser1), resource2)
    assertFalse("User1 from host1 still should not have READ access to any topic",
      authorizeByResourceType(authorizer, u1h1Context, READ, ResourceType.TOPIC))
    assertFalse("User2 from host1 should not have READ access to any topic",
      authorizeByResourceType(authorizer, u2h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(allowAllUser), resource2)
    assertTrue("User2 from host1 should now have READ access to at least one topic",
      authorizeByResourceType(authorizer, u2h1Context, READ, ResourceType.TOPIC))

    addAcls(authorizer, Set(denyAllUser), resource2)
    assertFalse("User2 from host1 now shouldn't have READ access to any topic",
      authorizeByResourceType(authorizer, u2h1Context, READ, ResourceType.TOPIC))
  }

}
