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
import java.util.UUID

import kafka.network.RequestChannel.Session
import kafka.security.auth.Acl.WildCardHost
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.Assert._
import org.junit.{After, Before, Test}

class SimpleAclAuthorizerTest extends ZooKeeperTestHarness {

  val simpleAclAuthorizer = new SimpleAclAuthorizer
  val simpleAclAuthorizer2 = new SimpleAclAuthorizer
  val testPrincipal = Acl.WildCardPrincipal
  val testHostName = InetAddress.getByName("192.168.0.1")
  val session = Session(testPrincipal, testHostName)
  var resource: Resource = null
  val superUsers = "User:superuser1; User:superuser2"
  val username = "alice"
  var config: KafkaConfig = null

  @Before
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
    resource = new Resource(Topic, UUID.randomUUID().toString)
  }

  @After
  override def tearDown(): Unit = {
    simpleAclAuthorizer.close()
    simpleAclAuthorizer2.close()
    super.tearDown()
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

    assertTrue("User1 should have READ access from host2", simpleAclAuthorizer.authorize(host2Session, Read, resource))
    assertFalse("User1 should not have READ access from host1 due to denyAcl", simpleAclAuthorizer.authorize(host1Session, Read, resource))
    assertTrue("User1 should have WRITE access from host1", simpleAclAuthorizer.authorize(host1Session, Write, resource))
    assertFalse("User1 should not have WRITE access from host2 as no allow acl is defined", simpleAclAuthorizer.authorize(host2Session, Write, resource))
    assertTrue("User1 should not have DESCRIBE access from host1", simpleAclAuthorizer.authorize(host1Session, Describe, resource))
    assertTrue("User1 should have DESCRIBE access from host2", simpleAclAuthorizer.authorize(host2Session, Describe, resource))
    assertFalse("User1 should not have edit access from host1", simpleAclAuthorizer.authorize(host1Session, Alter, resource))
    assertFalse("User1 should not have edit access from host2", simpleAclAuthorizer.authorize(host2Session, Alter, resource))

    //test if user has READ and write access they also get describe access
    val user2Session = Session(user2, host1)
    val user3Session = Session(user3, host1)
    assertTrue("User2 should have DESCRIBE access from host1", simpleAclAuthorizer.authorize(user2Session, Describe, resource))
    assertTrue("User3 should have DESCRIBE access from host2", simpleAclAuthorizer.authorize(user3Session, Describe, resource))
    assertTrue("User2 should have READ access from host1", simpleAclAuthorizer.authorize(user2Session, Read, resource))
    assertTrue("User3 should have WRITE access from host2", simpleAclAuthorizer.authorize(user3Session, Write, resource))
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

    assertFalse("deny should take precedence over allow.", simpleAclAuthorizer.authorize(session, Read, resource))
  }

  @Test
  def testAllowAllAccess(): Unit = {
    val allowAllAcl = Acl.AllowAllAcl

    changeAclAndVerify(Set.empty[Acl], Set[Acl](allowAllAcl), Set.empty[Acl])

    val session = Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "random"), InetAddress.getByName("192.0.4.4"))
    assertTrue("allow all acl should allow access to all.", simpleAclAuthorizer.authorize(session, Read, resource))
  }

  @Test
  def testSuperUserHasAccess(): Unit = {
    val denyAllAcl = new Acl(Acl.WildCardPrincipal, Deny, WildCardHost, All)

    changeAclAndVerify(Set.empty[Acl], Set[Acl](denyAllAcl), Set.empty[Acl])

    val session1 = Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))
    val session2 = Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser2"), InetAddress.getByName("192.0.4.4"))

    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session1, Read, resource))
    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session2, Read, resource))
  }

  @Test
  def testWildCardAcls(): Unit = {
    assertFalse("when acls = [],  authorizer should fail close.", simpleAclAuthorizer.authorize(session, Read, resource))

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.3.1")
    val readAcl = new Acl(user1, Allow, host1.getHostAddress, Read)
    val wildCardResource = new Resource(resource.resourceType, Resource.WildCardResource)

    val acls = changeAclAndVerify(Set.empty[Acl], Set[Acl](readAcl), Set.empty[Acl], wildCardResource)

    val host1Session = Session(user1, host1)
    assertTrue("User1 should have Read access from host1", simpleAclAuthorizer.authorize(host1Session, Read, resource))

    //allow Write to specific topic.
    val writeAcl = new Acl(user1, Allow, host1.getHostAddress, Write)
    changeAclAndVerify(Set.empty[Acl], Set[Acl](writeAcl), Set.empty[Acl])

    //deny Write to wild card topic.
    val denyWriteOnWildCardResourceAcl = new Acl(user1, Deny, host1.getHostAddress, Write)
    changeAclAndVerify(acls, Set[Acl](denyWriteOnWildCardResourceAcl), Set.empty[Acl], wildCardResource)

    assertFalse("User1 should not have Write access from host1", simpleAclAuthorizer.authorize(host1Session, Write, resource))
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
    val testAuthorizer = new SimpleAclAuthorizer
    try {
      testAuthorizer.configure(cfg.originals)
      assertTrue("when acls = null or [],  authorizer should fail open with allow.everyone = true.", testAuthorizer.authorize(session, Read, resource))
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
      new Resource(Topic, Resource.WildCardResource) -> Set[Acl](new Acl(user2, Allow, WildCardHost, Read)),
      new Resource(Cluster, Resource.WildCardResource) -> Set[Acl](new Acl(user2, Allow, host1, Read)),
      new Resource(Group, Resource.WildCardResource) -> acls,
      new Resource(Group, "test-ConsumerGroup") -> acls
    )

    resourceToAcls foreach { case (key, value) => changeAclAndVerify(Set.empty[Acl], value, Set.empty[Acl], key) }
    TestUtils.waitUntilTrue(() => resourceToAcls + (resource -> acls) == simpleAclAuthorizer.getAcls(), "changes not propagated in timeout period.")

    //test remove acl from existing acls.
    acls = changeAclAndVerify(acls, Set.empty[Acl], Set(acl1, acl5))

    //test remove all acls for resource
    simpleAclAuthorizer.removeAcls(resource)
    TestUtils.waitAndVerifyAcls(Set.empty[Acl], simpleAclAuthorizer, resource)
    assertTrue(!zkClient.resourceExists(resource))

    //test removing last acl also deletes ZooKeeper path
    acls = changeAclAndVerify(Set.empty[Acl], Set(acl1), Set.empty[Acl])
    changeAclAndVerify(acls, Set.empty[Acl], acls)
    assertTrue(!zkClient.resourceExists(resource))
  }

  @Test
  def testLoadCache(): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new Acl(user1, Allow, "host-1", Read)
    val acls = Set[Acl](acl1)
    simpleAclAuthorizer.addAcls(acls, resource)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val resource1 = new Resource(Topic, "test-2")
    val acl2 = new Acl(user2, Deny, "host3", Read)
    val acls1 = Set[Acl](acl2)
    simpleAclAuthorizer.addAcls(acls1, resource1)

    zkClient.deleteAclChangeNotifications
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
    val commonResource = new Resource(Topic, "test")

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
    val commonResource = new Resource(Topic, "test")

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

    assertTrue("The authorizer should see a value that needs to be deleted", deleted)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer, commonResource)
    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer2, commonResource)
  }

  @Test
  def testHighConcurrencyModificationOfResourceAcls(): Unit = {
    val commonResource = new Resource(Topic, "test")

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
        assertTrue(s"ALLOW $parentOp should imply ALLOW $op", authorized)
      else
        assertFalse(s"ALLOW $parentOp should not imply ALLOW $op", authorized)
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
        assertFalse(s"DENY $parentOp should imply DENY $op", authorized)
      else
        assertTrue(s"DENY $parentOp should not imply DENY $op", authorized)
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
