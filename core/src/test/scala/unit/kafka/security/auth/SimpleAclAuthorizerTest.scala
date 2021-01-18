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
package kafka.security.auth

import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import kafka.api.{ApiVersion, KAFKA_2_0_IV0, KAFKA_2_0_IV1}
import kafka.network.RequestChannel.Session
import kafka.security.auth.Acl.{WildCardHost, WildCardResource}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.{ZkAclStore, ZooKeeperTestHarness}
import kafka.zookeeper.{GetChildrenRequest, GetDataRequest, ZooKeeperClient}
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

@deprecated("Use AclAuthorizer", "Since 2.4")
class SimpleAclAuthorizerTest extends ZooKeeperTestHarness {

  private val allowReadAcl = Acl(Acl.WildCardPrincipal, Allow, WildCardHost, Read)
  private val allowWriteAcl = Acl(Acl.WildCardPrincipal, Allow, WildCardHost, Write)
  private val denyReadAcl = Acl(Acl.WildCardPrincipal, Deny, WildCardHost, Read)

  private val wildCardResource = Resource(Topic, WildCardResource, LITERAL)
  private val prefixedResource = Resource(Topic, "foo", PREFIXED)

  private val simpleAclAuthorizer = new SimpleAclAuthorizer
  private val simpleAclAuthorizer2 = new SimpleAclAuthorizer
  private var resource: Resource = _
  private val superUsers = "User:superuser1; User:superuser2"
  private val username = "alice"
  private val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
  private val session = Session(principal, InetAddress.getByName("192.168.0.1"))
  private var config: KafkaConfig = _
  private var zooKeeperClient: ZooKeeperClient = _

  class CustomPrincipal(principalType: String, name: String) extends KafkaPrincipal(principalType, name) {
    override def equals(o: scala.Any): Boolean = false
  }

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    // Increase maxUpdateRetries to avoid transient failures
    simpleAclAuthorizer.maxUpdateRetries = Int.MaxValue
    simpleAclAuthorizer2.maxUpdateRetries = Int.MaxValue

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(SimpleAclAuthorizer.SuperUsersProp, superUsers)

    config = KafkaConfig.fromProps(props)
    simpleAclAuthorizer.configure(config.originals)
    simpleAclAuthorizer2.configure(config.originals)
    resource = Resource(Topic, "foo-" + UUID.randomUUID(), LITERAL)

    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests,
      Time.SYSTEM, "kafka.test", "SimpleAclAuthorizerTest")
  }

  @AfterEach
  override def tearDown(): Unit = {
    simpleAclAuthorizer.close()
    simpleAclAuthorizer2.close()
    zooKeeperClient.close()
    super.tearDown()
  }

  @Test
  def testAuthorizeThrowsOnNonLiteralResource(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => simpleAclAuthorizer.authorize(session, Read, Resource(Topic, "something", PREFIXED)))
  }

  @Test
  def testAuthorizeWithEmptyResourceName(): Unit = {
    assertFalse(simpleAclAuthorizer.authorize(session, Read, Resource(Group, "", LITERAL)))
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl), Resource(Group, WildCardResource, LITERAL))
    assertTrue(simpleAclAuthorizer.authorize(session, Read, Resource(Group, "", LITERAL)))
  }

  // Authorizing the empty resource is not supported because we create a znode with the resource name.
  @Test
  def testEmptyAclThrowsException(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl), Resource(Group, "", LITERAL)))
  }

  @Test
  def testTopicAcl(): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "rob")
    val user3 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "batman")
    val host1 = InetAddress.getByName("192.168.1.1")
    val host2 = InetAddress.getByName("192.168.1.2")

    //user1 has READ access from host1 and host2.
    val acl1 = new Acl(user1, Allow, host1.getHostAddress, Read)
    val acl2 = new Acl(user1, Allow, host2.getHostAddress, Read)

    //user1 does not have  READ access from host1.
    val acl3 = new Acl(user1, Deny, host1.getHostAddress, Read)

    //user1 has Write access from host1 only.
    val acl4 = new Acl(user1, Allow, host1.getHostAddress, Write)

    //user1 has DESCRIBE access from all hosts.
    val acl5 = new Acl(user1, Allow, WildCardHost, Describe)

    //user2 has READ access from all hosts.
    val acl6 = new Acl(user2, Allow, WildCardHost, Read)

    //user3 has WRITE access from all hosts.
    val acl7 = new Acl(user3, Allow, WildCardHost, Write)

    val acls = Set[Acl](acl1, acl2, acl3, acl4, acl5, acl6, acl7)

    changeAclAndVerify(Set.empty[Acl], acls, Set.empty[Acl])

    val host1Session = Session(user1, host1)
    val host2Session = Session(user1, host2)

    assertTrue(simpleAclAuthorizer.authorize(host2Session, Read, resource), "User1 should have READ access from host2")
    assertFalse(simpleAclAuthorizer.authorize(host1Session, Read, resource), "User1 should not have READ access from host1 due to denyAcl")
    assertTrue(simpleAclAuthorizer.authorize(host1Session, Write, resource), "User1 should have WRITE access from host1")
    assertFalse(simpleAclAuthorizer.authorize(host2Session, Write, resource), "User1 should not have WRITE access from host2 as no allow acl is defined")
    assertTrue(simpleAclAuthorizer.authorize(host1Session, Describe, resource), "User1 should not have DESCRIBE access from host1")
    assertTrue(simpleAclAuthorizer.authorize(host2Session, Describe, resource), "User1 should have DESCRIBE access from host2")
    assertFalse(simpleAclAuthorizer.authorize(host1Session, Alter, resource), "User1 should not have edit access from host1")
    assertFalse(simpleAclAuthorizer.authorize(host2Session, Alter, resource), "User1 should not have edit access from host2")

    //test if user has READ and write access they also get describe access
    val user2Session = Session(user2, host1)
    val user3Session = Session(user3, host1)
    assertTrue(simpleAclAuthorizer.authorize(user2Session, Describe, resource), "User2 should have DESCRIBE access from host1")
    assertTrue(simpleAclAuthorizer.authorize(user3Session, Describe, resource), "User3 should have DESCRIBE access from host2")
    assertTrue(simpleAclAuthorizer.authorize(user2Session, Read, resource), "User2 should have READ access from host1")
    assertTrue(simpleAclAuthorizer.authorize(user3Session, Write, resource), "User3 should have WRITE access from host2")
  }

  /**
    CustomPrincipals should be compared with their principal type and name
   */
  @Test
  def testAllowAccessWithCustomPrincipal(): Unit = {
    val user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val customUserPrincipal = new CustomPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.1.1")
    val host2 = InetAddress.getByName("192.168.1.2")

    // user has READ access from host2 but not from host1
    val acl1 = new Acl(user, Deny, host1.getHostAddress, Read)
    val acl2 = new Acl(user, Allow, host2.getHostAddress, Read)
    val acls = Set[Acl](acl1, acl2)
    changeAclAndVerify(Set.empty[Acl], acls, Set.empty[Acl])

    val host1Session = Session(customUserPrincipal, host1)
    val host2Session = Session(customUserPrincipal, host2)

    assertTrue(simpleAclAuthorizer.authorize(host2Session, Read, resource), "User1 should have READ access from host2")
    assertFalse(simpleAclAuthorizer.authorize(host1Session, Read, resource), "User1 should not have READ access from host1 due to denyAcl")
  }

  @Test
  def testDenyTakesPrecedence(): Unit = {
    val user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host = InetAddress.getByName("192.168.2.1")
    val session = Session(user, host)

    val allowAll = Acl.AllowAllAcl
    val denyAcl = new Acl(user, Deny, host.getHostAddress, All)
    val acls = Set[Acl](allowAll, denyAcl)

    changeAclAndVerify(Set.empty[Acl], acls, Set.empty[Acl])

    assertFalse(simpleAclAuthorizer.authorize(session, Read, resource), "deny should take precedence over allow.")
  }

  @Test
  def testAllowAllAccess(): Unit = {
    val allowAllAcl = Acl.AllowAllAcl

    changeAclAndVerify(Set.empty[Acl], Set[Acl](allowAllAcl), Set.empty[Acl])

    val session = Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "random"), InetAddress.getByName("192.0.4.4"))
    assertTrue(simpleAclAuthorizer.authorize(session, Read, resource), "allow all acl should allow access to all.")
  }

  @Test
  def testSuperUserHasAccess(): Unit = {
    val denyAllAcl = new Acl(Acl.WildCardPrincipal, Deny, WildCardHost, All)

    changeAclAndVerify(Set.empty[Acl], Set[Acl](denyAllAcl), Set.empty[Acl])

    val session1 = Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))
    val session2 = Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser2"), InetAddress.getByName("192.0.4.4"))

    assertTrue(simpleAclAuthorizer.authorize(session1, Read, resource), "superuser always has access, no matter what acls.")
    assertTrue(simpleAclAuthorizer.authorize(session2, Read, resource), "superuser always has access, no matter what acls.")
  }

  /**
    CustomPrincipals should be compared with their principal type and name
   */
  @Test
  def testSuperUserWithCustomPrincipalHasAccess(): Unit = {
    val denyAllAcl = new Acl(Acl.WildCardPrincipal, Deny, WildCardHost, All)
    changeAclAndVerify(Set.empty[Acl], Set[Acl](denyAllAcl), Set.empty[Acl])

    val session = Session(new CustomPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))

    assertTrue(simpleAclAuthorizer.authorize(session, Read, resource), "superuser with custom principal always has access, no matter what acls.")
  }

  @Test
  def testWildCardAcls(): Unit = {
    assertFalse(simpleAclAuthorizer.authorize(session, Read, resource), "when acls = [],  authorizer should fail close.")

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.3.1")
    val readAcl = new Acl(user1, Allow, host1.getHostAddress, Read)

    val acls = changeAclAndVerify(Set.empty[Acl], Set[Acl](readAcl), Set.empty[Acl], wildCardResource)

    val host1Session = Session(user1, host1)
    assertTrue(simpleAclAuthorizer.authorize(host1Session, Read, resource), "User1 should have Read access from host1")

    //allow Write to specific topic.
    val writeAcl = new Acl(user1, Allow, host1.getHostAddress, Write)
    changeAclAndVerify(Set.empty[Acl], Set[Acl](writeAcl), Set.empty[Acl])

    //deny Write to wild card topic.
    val denyWriteOnWildCardResourceAcl = new Acl(user1, Deny, host1.getHostAddress, Write)
    changeAclAndVerify(acls, Set[Acl](denyWriteOnWildCardResourceAcl), Set.empty[Acl], wildCardResource)

    assertFalse(simpleAclAuthorizer.authorize(host1Session, Write, resource), "User1 should not have Write access from host1")
  }

  @Test
  def testNoAclFound(): Unit = {
    assertFalse(simpleAclAuthorizer.authorize(session, Read, resource), "when acls = [],  authorizer should fail close.")
  }

  @Test
  def testNoAclFoundOverride(): Unit = {
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.put(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp, "true")

    val cfg = KafkaConfig.fromProps(props)
    val testAuthorizer = new SimpleAclAuthorizer
    try {
      testAuthorizer.configure(cfg.originals)
      assertTrue(testAuthorizer.authorize(session, Read, resource), "when acls = null or [],  authorizer should fail open with allow.everyone = true.")
    } finally {
      testAuthorizer.close()
    }
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

    var acls = changeAclAndVerify(Set.empty[Acl], Set[Acl](acl1, acl2, acl3, acl4), Set.empty[Acl])

    //test addAcl is additive
    val acl5 = new Acl(user2, Allow, WildCardHost, Read)
    acls = changeAclAndVerify(acls, Set[Acl](acl5), Set.empty[Acl])

    //test get by principal name.
    TestUtils.waitUntilTrue(() => Map(resource -> Set(acl1, acl2)) == simpleAclAuthorizer.getAcls(user1), "changes not propagated in timeout period")
    TestUtils.waitUntilTrue(() => Map(resource -> Set(acl3, acl4, acl5)) == simpleAclAuthorizer.getAcls(user2), "changes not propagated in timeout period")

    val resourceToAcls = Map[Resource, Set[Acl]](
      new Resource(Topic, Resource.WildCardResource, LITERAL) -> Set[Acl](new Acl(user2, Allow, WildCardHost, Read)),
      new Resource(Cluster, Resource.WildCardResource, LITERAL) -> Set[Acl](new Acl(user2, Allow, host1, Read)),
      new Resource(Group, Resource.WildCardResource, LITERAL) -> acls,
      new Resource(Group, "test-ConsumerGroup", LITERAL) -> acls
    )

    resourceToAcls foreach { case (key, value) => changeAclAndVerify(Set.empty[Acl], value, Set.empty[Acl], key) }
    TestUtils.waitUntilTrue(() => resourceToAcls + (resource -> acls) == simpleAclAuthorizer.getAcls(), "changes not propagated in timeout period.")

    //test remove acl from existing acls.
    acls = changeAclAndVerify(acls, Set.empty[Acl], Set(acl1, acl5))

    //test remove all acls for resource
    simpleAclAuthorizer.removeAcls(resource)
    TestUtils.waitAndVerifyAcls(Set.empty[Acl], simpleAclAuthorizer, resource)
    assertTrue(!zkClient.resourceExists(resource.toPattern))

    //test removing last acl also deletes ZooKeeper path
    acls = changeAclAndVerify(Set.empty[Acl], Set(acl1), Set.empty[Acl])
    changeAclAndVerify(acls, Set.empty[Acl], acls)
    assertTrue(!zkClient.resourceExists(resource.toPattern))
  }

  @Test
  def testLoadCache(): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new Acl(user1, Allow, "host-1", Read)
    val acls = Set[Acl](acl1)
    simpleAclAuthorizer.addAcls(acls, resource)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val resource1 = Resource(Topic, "test-2", LITERAL)
    val acl2 = new Acl(user2, Deny, "host3", Read)
    val acls1 = Set[Acl](acl2)
    simpleAclAuthorizer.addAcls(acls1, resource1)

    zkClient.deleteAclChangeNotifications()
    val authorizer = new SimpleAclAuthorizer
    try {
      authorizer.configure(config.originals)

      assertEquals(acls, authorizer.getAcls(resource))
      assertEquals(acls1, authorizer.getAcls(resource1))
    } finally {
      authorizer.close()
    }
  }

  @Test
  def testLocalConcurrentModificationOfResourceAcls(): Unit = {
    val commonResource = Resource(Topic, "test", LITERAL)

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new Acl(user1, Allow, WildCardHost, Read)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val acl2 = new Acl(user2, Deny, WildCardHost, Read)

    simpleAclAuthorizer.addAcls(Set(acl1), commonResource)
    simpleAclAuthorizer.addAcls(Set(acl2), commonResource)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer, commonResource)
  }

  @Test
  def testDistributedConcurrentModificationOfResourceAcls(): Unit = {
    val commonResource = Resource(Topic, "test", LITERAL)

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new Acl(user1, Allow, WildCardHost, Read)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val acl2 = new Acl(user2, Deny, WildCardHost, Read)

    // Add on each instance
    simpleAclAuthorizer.addAcls(Set(acl1), commonResource)
    simpleAclAuthorizer2.addAcls(Set(acl2), commonResource)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer, commonResource)
    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer2, commonResource)

    val user3 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "joe")
    val acl3 = new Acl(user3, Deny, WildCardHost, Read)

    // Add on one instance and delete on another
    simpleAclAuthorizer.addAcls(Set(acl3), commonResource)
    val deleted = simpleAclAuthorizer2.removeAcls(Set(acl3), commonResource)

    assertTrue(deleted, "The authorizer should see a value that needs to be deleted")

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer, commonResource)
    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer2, commonResource)
  }

  @Test
  def testHighConcurrencyModificationOfResourceAcls(): Unit = {
    val commonResource = Resource(Topic, "test", LITERAL)

    val acls = (0 to 50).map { i =>
      val useri = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, i.toString)
      new Acl(useri, Allow, WildCardHost, Read)
    }

    // Alternate authorizer, Remove all acls that end in 0
    val concurrentFuctions = acls.map { acl =>
      () => {
        val aclId = acl.principal.getName.toInt
        if (aclId % 2 == 0) {
          simpleAclAuthorizer.addAcls(Set(acl), commonResource)
        } else {
          simpleAclAuthorizer2.addAcls(Set(acl), commonResource)
        }
        if (aclId % 10 == 0) {
          simpleAclAuthorizer2.removeAcls(Set(acl), commonResource)
        }
      }
    }

    val expectedAcls = acls.filter { acl =>
      val aclId = acl.principal.getName.toInt
      aclId % 10 != 0
    }.toSet

    TestUtils.assertConcurrent("Should support many concurrent calls", concurrentFuctions, 30 * 1000)

    TestUtils.waitAndVerifyAcls(expectedAcls, simpleAclAuthorizer, commonResource)
    TestUtils.waitAndVerifyAcls(expectedAcls, simpleAclAuthorizer2, commonResource)
  }

  /**
    * Test ACL inheritance, as described in #{org.apache.kafka.common.acl.AclOperation}
    */
  @Test
  def testAclInheritance(): Unit = {
    testImplicationsOfAllow(All, Set(Read, Write, Create, Delete, Alter, Describe,
      ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite))
    testImplicationsOfDeny(All, Set(Read, Write, Create, Delete, Alter, Describe,
      ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite))
    testImplicationsOfAllow(Read, Set(Describe))
    testImplicationsOfAllow(Write, Set(Describe))
    testImplicationsOfAllow(Delete, Set(Describe))
    testImplicationsOfAllow(Alter, Set(Describe))
    testImplicationsOfDeny(Describe, Set())
    testImplicationsOfAllow(AlterConfigs, Set(DescribeConfigs))
    testImplicationsOfDeny(DescribeConfigs, Set())
  }

  private def testImplicationsOfAllow(parentOp: Operation, allowedOps: Set[Operation]): Unit = {
    val user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host = InetAddress.getByName("192.168.3.1")
    val hostSession = Session(user, host)
    val acl = Acl(user, Allow, WildCardHost, parentOp)
    simpleAclAuthorizer.addAcls(Set(acl), Resource.ClusterResource)
    Operation.values.foreach { op =>
      val authorized = simpleAclAuthorizer.authorize(hostSession, op, Resource.ClusterResource)
      if (allowedOps.contains(op) || op == parentOp)
        assertTrue(authorized, s"ALLOW $parentOp should imply ALLOW $op")
      else
        assertFalse(authorized, s"ALLOW $parentOp should not imply ALLOW $op")
    }
    simpleAclAuthorizer.removeAcls(Set(acl), Resource.ClusterResource)
  }

  private def testImplicationsOfDeny(parentOp: Operation, deniedOps: Set[Operation]): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.3.1")
    val host1Session = Session(user1, host1)
    val acls = Set(Acl(user1, Deny, WildCardHost, parentOp), Acl(user1, Allow, WildCardHost, All))
    simpleAclAuthorizer.addAcls(acls, Resource.ClusterResource)
    Operation.values.foreach { op =>
      val authorized = simpleAclAuthorizer.authorize(host1Session, op, Resource.ClusterResource)
      if (deniedOps.contains(op) || op == parentOp)
        assertFalse(authorized, s"DENY $parentOp should imply DENY $op")
      else
        assertTrue(authorized, s"DENY $parentOp should not imply DENY $op")
    }
    simpleAclAuthorizer.removeAcls(acls, Resource.ClusterResource)
  }

  @Test
  def testHighConcurrencyDeletionOfResourceAcls(): Unit = {
    val acl = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username), Allow, WildCardHost, All)

    // Alternate authorizer to keep adding and removing ZooKeeper path
    val concurrentFuctions = (0 to 50).map { _ =>
      () => {
        simpleAclAuthorizer.addAcls(Set(acl), resource)
        simpleAclAuthorizer2.removeAcls(Set(acl), resource)
      }
    }

    TestUtils.assertConcurrent("Should support many concurrent calls", concurrentFuctions, 30 * 1000)

    TestUtils.waitAndVerifyAcls(Set.empty[Acl], simpleAclAuthorizer, resource)
    TestUtils.waitAndVerifyAcls(Set.empty[Acl], simpleAclAuthorizer2, resource)
  }

  @Test
  def testAccessAllowedIfAllowAclExistsOnWildcardResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl), wildCardResource)

    assertTrue(simpleAclAuthorizer.authorize(session, Read, resource))
  }

  @Test
  def testDeleteAclOnWildcardResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl, allowWriteAcl), wildCardResource)

    simpleAclAuthorizer.removeAcls(Set[Acl](allowReadAcl), wildCardResource)

    assertEquals(Set(allowWriteAcl), simpleAclAuthorizer.getAcls(wildCardResource))
  }

  @Test
  def testDeleteAllAclOnWildcardResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl), wildCardResource)

    simpleAclAuthorizer.removeAcls(wildCardResource)

    assertEquals(Map(), simpleAclAuthorizer.getAcls())
  }

  @Test
  def testAccessAllowedIfAllowAclExistsOnPrefixedResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl), prefixedResource)

    assertTrue(simpleAclAuthorizer.authorize(session, Read, resource))
  }

  @Test
  def testDeleteAclOnPrefixedResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl, allowWriteAcl), prefixedResource)

    simpleAclAuthorizer.removeAcls(Set[Acl](allowReadAcl), prefixedResource)

    assertEquals(Set(allowWriteAcl), simpleAclAuthorizer.getAcls(prefixedResource))
  }

  @Test
  def testDeleteAllAclOnPrefixedResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl, allowWriteAcl), prefixedResource)

    simpleAclAuthorizer.removeAcls(prefixedResource)

    assertEquals(Map(), simpleAclAuthorizer.getAcls())
  }

  @Test
  def testAddAclsOnLiteralResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl, allowWriteAcl), resource)
    simpleAclAuthorizer.addAcls(Set[Acl](allowWriteAcl, denyReadAcl), resource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), simpleAclAuthorizer.getAcls(resource))
    assertEquals(Set(), simpleAclAuthorizer.getAcls(wildCardResource))
    assertEquals(Set(), simpleAclAuthorizer.getAcls(prefixedResource))
  }

  @Test
  def testAddAclsOnWildcardResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl, allowWriteAcl), wildCardResource)
    simpleAclAuthorizer.addAcls(Set[Acl](allowWriteAcl, denyReadAcl), wildCardResource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), simpleAclAuthorizer.getAcls(wildCardResource))
    assertEquals(Set(), simpleAclAuthorizer.getAcls(resource))
    assertEquals(Set(), simpleAclAuthorizer.getAcls(prefixedResource))
  }

  @Test
  def testAddAclsOnPrefiexedResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl, allowWriteAcl), prefixedResource)
    simpleAclAuthorizer.addAcls(Set[Acl](allowWriteAcl, denyReadAcl), prefixedResource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), simpleAclAuthorizer.getAcls(prefixedResource))
    assertEquals(Set(), simpleAclAuthorizer.getAcls(wildCardResource))
    assertEquals(Set(), simpleAclAuthorizer.getAcls(resource))
  }

  @Test
  def testAuthorizeWithPrefixedResource(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "a_other", LITERAL))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "a_other", PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "foo-" + UUID.randomUUID(), PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "foo-" + UUID.randomUUID(), PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "foo-" + UUID.randomUUID() + "-zzz", PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "fooo-" + UUID.randomUUID(), PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "fo-" + UUID.randomUUID(), PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "fop-" + UUID.randomUUID(), PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "fon-" + UUID.randomUUID(), PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "fon-", PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "z_other", PREFIXED))
    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "z_other", LITERAL))

    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl), prefixedResource)

    assertTrue(simpleAclAuthorizer.authorize(session, Read, resource))
  }

  @Test
  def testSingleCharacterResourceAcls(): Unit = {
    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl), Resource(Topic, "f", LITERAL))
    assertTrue(simpleAclAuthorizer.authorize(session, Read, Resource(Topic, "f", LITERAL)))
    assertFalse(simpleAclAuthorizer.authorize(session, Read, Resource(Topic, "foo", LITERAL)))

    simpleAclAuthorizer.addAcls(Set[Acl](allowReadAcl), Resource(Topic, "_", PREFIXED))
    assertTrue(simpleAclAuthorizer.authorize(session, Read, Resource(Topic, "_foo", LITERAL)))
    assertTrue(simpleAclAuthorizer.authorize(session, Read, Resource(Topic, "_", LITERAL)))
    assertFalse(simpleAclAuthorizer.authorize(session, Read, Resource(Topic, "foo_", LITERAL)))
  }

  @Test
  def testGetAclsPrincipal(): Unit = {
    val aclOnSpecificPrincipal = new Acl(principal, Allow, WildCardHost, Write)
    simpleAclAuthorizer.addAcls(Set[Acl](aclOnSpecificPrincipal), resource)

    assertEquals(0, simpleAclAuthorizer.getAcls(Acl.WildCardPrincipal).size,
      "acl on specific should not be returned for wildcard request")
    assertEquals(1, simpleAclAuthorizer.getAcls(principal).size,
      "acl on specific should be returned for specific request")
    assertEquals(1, simpleAclAuthorizer.getAcls(new KafkaPrincipal(principal.getPrincipalType, principal.getName)).size,
      "acl on specific should be returned for different principal instance")

    simpleAclAuthorizer.removeAcls(resource)
    val aclOnWildcardPrincipal = new Acl(Acl.WildCardPrincipal, Allow, WildCardHost, Write)
    simpleAclAuthorizer.addAcls(Set[Acl](aclOnWildcardPrincipal), resource)

    assertEquals(1, simpleAclAuthorizer.getAcls(Acl.WildCardPrincipal).size,
      "acl on wildcard should be returned for wildcard request")
    assertEquals(0, simpleAclAuthorizer.getAcls(principal).size,
      "acl on wildcard should not be returned for specific request")
  }

  @Test
  def testThrowsOnAddPrefixedAclIfInterBrokerProtocolVersionTooLow(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(KAFKA_2_0_IV0))
    assertThrows(classOf[UnsupportedVersionException], () => simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), Resource(Topic, "z_other", PREFIXED)))
  }

  @Test
  def testWritesExtendedAclChangeEventIfInterBrokerProtocolNotSet(): Unit = {
    givenAuthorizerWithProtocolVersion(Option.empty)
    val resource = Resource(Topic, "z_other", PREFIXED)
    val expected = new String(ZkAclStore(PREFIXED).changeStore.createChangeNode(resource.toPattern).bytes, UTF_8)

    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), resource)

    val actual = getAclChangeEventAsString(PREFIXED)

    assertEquals(expected, actual)
  }

  @Test
  def testWritesExtendedAclChangeEventWhenInterBrokerProtocolAtLeastKafkaV2(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(KAFKA_2_0_IV1))
    val resource = Resource(Topic, "z_other", PREFIXED)
    val expected = new String(ZkAclStore(PREFIXED).changeStore.createChangeNode(resource.toPattern).bytes, UTF_8)

    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), resource)

    val actual = getAclChangeEventAsString(PREFIXED)

    assertEquals(expected, actual)
  }

  @Test
  def testWritesLiteralWritesLiteralAclChangeEventWhenInterBrokerProtocolLessThanKafkaV2eralAclChangesForOlderProtocolVersions(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(KAFKA_2_0_IV0))
    val resource = Resource(Topic, "z_other", LITERAL)
    val expected = new String(ZkAclStore(LITERAL).changeStore.createChangeNode(resource.toPattern).bytes, UTF_8)

    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), resource)

    val actual = getAclChangeEventAsString(LITERAL)

    assertEquals(expected, actual)
  }

  @Test
  def testWritesLiteralAclChangeEventWhenInterBrokerProtocolIsKafkaV2(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(KAFKA_2_0_IV1))
    val resource = Resource(Topic, "z_other", LITERAL)
    val expected = new String(ZkAclStore(LITERAL).changeStore.createChangeNode(resource.toPattern).bytes, UTF_8)

    simpleAclAuthorizer.addAcls(Set[Acl](denyReadAcl), resource)

    val actual = getAclChangeEventAsString(LITERAL)

    assertEquals(expected, actual)
  }

  private def givenAuthorizerWithProtocolVersion(protocolVersion: Option[ApiVersion]): Unit = {
    simpleAclAuthorizer.close()

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(SimpleAclAuthorizer.SuperUsersProp, superUsers)
    protocolVersion.foreach(version => props.put(KafkaConfig.InterBrokerProtocolVersionProp, version.toString))

    config = KafkaConfig.fromProps(props)

    simpleAclAuthorizer.configure(config.originals)
  }

  private def getAclChangeEventAsString(patternType: PatternType) = {
    val store = ZkAclStore(patternType)
    val children = zooKeeperClient.handleRequest(GetChildrenRequest(store.changeStore.aclChangePath, registerWatch = true))
    children.maybeThrow()
    assertEquals(1, children.children.size, "Expecting 1 change event")

    val data = zooKeeperClient.handleRequest(GetDataRequest(s"${store.changeStore.aclChangePath}/${children.children.head}"))
    data.maybeThrow()

    new String(data.data, UTF_8)
  }

  private def changeAclAndVerify(originalAcls: Set[Acl], addedAcls: Set[Acl], removedAcls: Set[Acl], resource: Resource = resource): Set[Acl] = {
    var acls = originalAcls

    if(addedAcls.nonEmpty) {
      simpleAclAuthorizer.addAcls(addedAcls, resource)
      acls ++= addedAcls
    }

    if(removedAcls.nonEmpty) {
      simpleAclAuthorizer.removeAcls(removedAcls, resource)
      acls --=removedAcls
    }

    TestUtils.waitAndVerifyAcls(acls, simpleAclAuthorizer, resource)

    acls
  }
}
