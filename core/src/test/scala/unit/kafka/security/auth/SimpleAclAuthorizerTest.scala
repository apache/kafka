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
package unit.kafka.security.auth

import java.util.UUID

import com.sun.security.auth.UserPrincipal
import kafka.network.RequestChannel.Session
import kafka.security.auth.Acl.WildCardHost
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.Assert._
import org.junit.{Test, Before}

class SimpleAclAuthorizerTest extends ZooKeeperTestHarness {

  val simpleAclAuthorizer = new SimpleAclAuthorizer
  val testPrincipal = Acl.WildCardPrincipal
  val testHostName = "test.host.com"
  var session = new Session(testPrincipal, testHostName)
  var resource: Resource = null
  val superUsers = "User:superuser1, User:superuser2"
  val username = "alice"

  @Before
  override def setUp() {
    super.setUp()

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(SimpleAclAuthorizer.SuperUsersProp, superUsers)

    val config = KafkaConfig.fromProps(props)
    simpleAclAuthorizer.configure(config.originals)
    resource = new Resource(Topic, UUID.randomUUID().toString)
  }

  @Test
  def testTopicAcl(): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val user3 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "batman")
    val host1 = "host1"
    val host2 = "host2"

    //user1 has READ access from host1 and host2.
    val acl1 = new Acl(user1, Allow, host1, Read)
    val acl2 = new Acl(user1, Allow, host2, Read)

    //user1 does not have  READ access from host1.
    val acl3 = new Acl(user1, Deny, host1, Read)

    //user1 has Write access from host1 only.
    val acl4 = new Acl(user1, Allow, host1, Write)

    //user1 has DESCRIBE access from all hosts.
    val acl5 = new Acl(user1, Allow, WildCardHost, Describe)

    //user2 has READ access from all hosts.
    val acl6 = new Acl(user2, Allow, WildCardHost, Read)

    //user3 has WRITE access from all hosts.
    val acl7 = new Acl(user3, Allow, WildCardHost, Write)

    simpleAclAuthorizer.addAcls(Set[Acl](acl1, acl2, acl3, acl4, acl5, acl6, acl7), resource)

    val host1Session = new Session(user1, host1)
    val host2Session = new Session(user1, host2)

    assertTrue("User1 should have READ access from host2", simpleAclAuthorizer.authorize(host2Session, Read, resource))
    assertFalse("User1 should not have READ access from host1 due to denyAcl", simpleAclAuthorizer.authorize(host1Session, Read, resource))
    assertTrue("User1 should have WRITE access from host1", simpleAclAuthorizer.authorize(host1Session, Write, resource))
    assertFalse("User1 should not have WRITE access from host2 as no allow acl is defined", simpleAclAuthorizer.authorize(host2Session, Write, resource))
    assertTrue("User1 should not have DESCRIBE access from host1", simpleAclAuthorizer.authorize(host1Session, Describe, resource))
    assertTrue("User1 should have DESCRIBE access from host2", simpleAclAuthorizer.authorize(host2Session, Describe, resource))
    assertFalse("User1 should not have edit access from host1", simpleAclAuthorizer.authorize(host1Session, Alter, resource))
    assertFalse("User1 should not have edit access from host2", simpleAclAuthorizer.authorize(host2Session, Alter, resource))

    //test if user has READ and write access they also get describe access

    val user2Session = new Session(user2, host1)
    val user3Session = new Session(user3, host1)
    assertTrue("User2 should have DESCRIBE access from host1", simpleAclAuthorizer.authorize(user2Session, Describe, resource))
    assertTrue("User3 should have DESCRIBE access from host2", simpleAclAuthorizer.authorize(user3Session, Describe, resource))
    assertTrue("User2 should have READ access from host1", simpleAclAuthorizer.authorize(user2Session, Read, resource))
    assertTrue("User3 should have WRITE access from host2", simpleAclAuthorizer.authorize(user3Session, Write, resource))
  }

  @Test
  def testDenyTakesPrecedence(): Unit = {
    val user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host = "random-host"
    val session = new Session(user, host)

    val allowAll = Acl.AllowAllAcl
    val denyAcl = new Acl(user, Deny, host, All)
    simpleAclAuthorizer.addAcls(Set[Acl](allowAll, denyAcl), resource)

    assertFalse("deny should take precedence over allow.", simpleAclAuthorizer.authorize(session, Read, resource))
  }

  @Test
  def testAllowAllAccess(): Unit = {
    val allowAllAcl = Acl.AllowAllAcl
    simpleAclAuthorizer.addAcls(Set[Acl](allowAllAcl), resource)

    val session = new Session(new UserPrincipal("random"), "random.host")
    assertTrue("allow all acl should allow access to all.", simpleAclAuthorizer.authorize(session, Read, resource))
  }

  @Test
  def testSuperUserHasAccess(): Unit = {
    val denyAllAcl = new Acl(Acl.WildCardPrincipal, Deny, WildCardHost, All)
    simpleAclAuthorizer.addAcls(Set[Acl](denyAllAcl), resource)

    val session1 = new Session(new UserPrincipal("superuser1"), "random.host")
    val session2 = new Session(new UserPrincipal("superuser2"), "random.host")

    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session1, Read, resource))
    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session2, Read, resource))
  }

  @Test
  def testNoAclFound(): Unit = {
    assertFalse("when acls = [],  authorizer should fail close.", simpleAclAuthorizer.authorize(session, Read, resource))
  }

  @Test
  def testNoAclFoundOverride(): Unit = {
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.put(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp, "true")

    val cfg = KafkaConfig.fromProps(props)
    val testAuthoizer: SimpleAclAuthorizer = new SimpleAclAuthorizer
    testAuthoizer.configure(cfg.originals)
    assertTrue("when acls = null or [],  authorizer should fail open with allow.everyone = true.", testAuthoizer.authorize(session, Read, resource))
  }

  @Test
  def testAclManagementAPIs(): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val host1 = "host1"
    val host2 = "host2"

    val acl1 = new Acl(user1, Allow, host1, Read)
    val acl2 = new Acl(user1, Allow, host1, Write)
    val acl3 = new Acl(user2, Allow, host2, Read)
    val acl4 = new Acl(user2, Allow, host2, Write)

    simpleAclAuthorizer.addAcls(Set[Acl](acl1, acl2, acl3, acl4), resource)
    assertEquals(Set(acl1, acl2, acl3, acl4), simpleAclAuthorizer.getAcls(resource))

    //test addAcl is additive
    val acl5: Acl = new Acl(user2, Allow, WildCardHost, Read)
    simpleAclAuthorizer.addAcls(Set[Acl](acl5), resource)
    assertEquals(Set(acl1, acl2, acl3, acl4, acl5), simpleAclAuthorizer.getAcls(resource))

    assertEquals(Set(acl1, acl2), simpleAclAuthorizer.getAcls(user1))

    //Following assertions fails transiently due to consistency issues.
    //test remove a single acl from existing acls.
    simpleAclAuthorizer.removeAcls(Set(acl1, acl5), resource)
    assertEquals(Set(acl2, acl3, acl4), simpleAclAuthorizer.getAcls(resource))

    //test remove all acls for resource
    simpleAclAuthorizer.removeAcls(resource)
    assertTrue(simpleAclAuthorizer.getAcls(resource).isEmpty)
  }
}