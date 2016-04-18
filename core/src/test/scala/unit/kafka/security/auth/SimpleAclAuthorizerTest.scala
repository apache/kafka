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
import java.util
import java.util.UUID

import kafka.server.KafkaConfig
import kafka.utils.{Json, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.auth._
import org.junit.Assert._
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConversions._

class SimpleAclAuthorizerTest extends ZooKeeperTestHarness {

  val simpleAclAuthorizer = new SimpleAclAuthorizer
  val simpleAclAuthorizer2 = new SimpleAclAuthorizer
  val testPrincipal = Acl.WILDCARD_PRINCIPAL
  val testHostName = InetAddress.getByName("192.168.0.1")
  val session = new Session(testPrincipal, testHostName)
  var resource: Resource = null
  val superUsers = "User:superuser1; User:superuser2"
  val username = "alice"
  var config: KafkaConfig = null

  @Before
  override def setUp() {
    super.setUp()

    // Increase maxUpdateRetries to avoid transient failures
    simpleAclAuthorizer.maxUpdateRetries = Int.MaxValue
    simpleAclAuthorizer2.maxUpdateRetries = Int.MaxValue

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(SimpleAclAuthorizer.SuperUsersProp, superUsers)

    config = KafkaConfig.fromProps(props)
    simpleAclAuthorizer.configure(config.originals)
    simpleAclAuthorizer2.configure(config.originals)
    resource = new Resource(ResourceType.TOPIC, UUID.randomUUID().toString)
  }

  @After
  override def tearDown(): Unit = {
    simpleAclAuthorizer.close()
    simpleAclAuthorizer2.close()
  }

  @Test
  def testTopicAcl() {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "rob")
    val user3 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "batman")
    val host1 = InetAddress.getByName("192.168.1.1")
    val host2 = InetAddress.getByName("192.168.1.2")

    //user1 has READ access from host1 and host2.
    val acl1 = new Acl(user1, PermissionType.ALLOW, host1.getHostAddress, Operation.READ)
    val acl2 = new Acl(user1, PermissionType.ALLOW, host2.getHostAddress, Operation.READ)

    //user1 does not have  READ access from host1.
    val acl3 = new Acl(user1, PermissionType.DENY, host1.getHostAddress, Operation.READ)

    //user1 has Write access from host1 only.
    val acl4 = new Acl(user1, PermissionType.ALLOW, host1.getHostAddress, Operation.WRITE)

    //user1 has DESCRIBE access from all hosts.
    val acl5 = new Acl(user1, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.DESCRIBE)

    //user2 has READ access from all hosts.
    val acl6 = new Acl(user2, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.READ)

    //user3 has WRITE access from all hosts.
    val acl7 = new Acl(user3, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.WRITE)

    val acls = Set[Acl](acl1, acl2, acl3, acl4, acl5, acl6, acl7)

    changeAclAndVerify(Set.empty[Acl], acls, Set.empty[Acl])

    val host1Session = new Session(user1, host1)
    val host2Session = new Session(user1, host2)

    assertTrue("User1 should have READ access from host2", simpleAclAuthorizer.authorize(host2Session, Operation.READ, resource))
    assertFalse("User1 should not have READ access from host1 due to denyAcl", simpleAclAuthorizer.authorize(host1Session, Operation.READ, resource))
    assertTrue("User1 should have WRITE access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.WRITE, resource))
    assertFalse("User1 should not have WRITE access from host2 as no allow acl is defined", simpleAclAuthorizer.authorize(host2Session, Operation.WRITE, resource))
    assertTrue("User1 should not have DESCRIBE access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.DESCRIBE, resource))
    assertTrue("User1 should have DESCRIBE access from host2", simpleAclAuthorizer.authorize(host2Session, Operation.DESCRIBE, resource))
    assertFalse("User1 should not have edit access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.ALTER, resource))
    assertFalse("User1 should not have edit access from host2", simpleAclAuthorizer.authorize(host2Session, Operation.ALTER, resource))

    //test if user has READ and write access they also get describe access
    val user2Session = new Session(user2, host1)
    val user3Session = new Session(user3, host1)
    assertTrue("User2 should have DESCRIBE access from host1", simpleAclAuthorizer.authorize(user2Session, Operation.DESCRIBE, resource))
    assertTrue("User3 should have DESCRIBE access from host2", simpleAclAuthorizer.authorize(user3Session, Operation.DESCRIBE, resource))
    assertTrue("User2 should have READ access from host1", simpleAclAuthorizer.authorize(user2Session, Operation.READ, resource))
    assertTrue("User3 should have WRITE access from host2", simpleAclAuthorizer.authorize(user3Session, Operation.WRITE, resource))
  }

  @Test
  def testDenyTakesPrecedence() {
    val user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host = InetAddress.getByName("192.168.2.1")
    val session = new Session(user, host)

    val allowAll = new Acl(Acl.WILDCARD_PRINCIPAL, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.ALL)
    val denyAcl = new Acl(user, PermissionType.DENY, host.getHostAddress, Operation.ALL)
    val acls = Set[Acl](allowAll, denyAcl)

    changeAclAndVerify(Set.empty[Acl], acls, Set.empty[Acl])

    assertFalse("deny should take precedence over allow.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))
  }

  @Test
  def testAllowAllAccess() {
    val allowAllAcl = new Acl(Acl.WILDCARD_PRINCIPAL, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.ALL)

    changeAclAndVerify(Set.empty[Acl], Set[Acl](allowAllAcl), Set.empty[Acl])

    val session = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "random"), InetAddress.getByName("192.0.4.4"))
    assertTrue("allow all acl should allow access to all.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))
  }

  @Test
  def testSuperUserHasAccess() {
    val denyAllAcl = new Acl(Acl.WILDCARD_PRINCIPAL, PermissionType.DENY, Acl.WILDCARD_HOST, Operation.ALL)

    changeAclAndVerify(Set.empty[Acl], Set[Acl](denyAllAcl), Set.empty[Acl])

    val session1 = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))
    val session2 = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser2"), InetAddress.getByName("192.0.4.4"))

    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session1, Operation.READ, resource))
    assertTrue("superuser always has access, no matter what acls.", simpleAclAuthorizer.authorize(session2, Operation.READ, resource))
  }

  @Test
  def testWildCardAcls(): Unit = {
    assertFalse("when acls = [],  authorizer should fail close.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.3.1")
    val readAcl = new Acl(user1, PermissionType.ALLOW, host1.getHostAddress, Operation.READ)
    val wildCardResource = new Resource(resource.getResourceType, Resource.WILDCARD_RESOURCE)

    val acls = changeAclAndVerify(Set.empty[Acl], Set[Acl](readAcl), Set.empty[Acl], wildCardResource)

    val host1Session = new Session(user1, host1)
    assertTrue("User1 should have read access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.READ, resource))

    //allow Write to specific topic.
    val writeAcl = new Acl(user1, PermissionType.ALLOW, host1.getHostAddress, Operation.WRITE)
    changeAclAndVerify(Set.empty[Acl], Set[Acl](writeAcl), Set.empty[Acl])

    //deny Write to wild card topic.
    val denyWriteOnWildCardResourceAcl = new Acl(user1, PermissionType.DENY, host1.getHostAddress, Operation.WRITE)
    changeAclAndVerify(acls, Set[Acl](denyWriteOnWildCardResourceAcl), Set.empty[Acl], wildCardResource)

    assertFalse("User1 should not have Write access from host1", simpleAclAuthorizer.authorize(host1Session, Operation.WRITE, resource))
  }

  @Test
  def testNoAclFound() {
    assertFalse("when acls = [],  authorizer should fail close.", simpleAclAuthorizer.authorize(session, Operation.READ, resource))
  }

  @Test
  def testNoAclFoundOverride() {
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.put(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp, "true")

    val cfg = KafkaConfig.fromProps(props)
    val testAuthoizer: SimpleAclAuthorizer = new SimpleAclAuthorizer
    testAuthoizer.configure(cfg.originals)
    assertTrue("when acls = null or [],  authorizer should fail open with allow.everyone = true.", testAuthoizer.authorize(session, Operation.READ, resource))
  }

  @Test
  def testAclManagementAPIs() {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val host1 = "host1"
    val host2 = "host2"

    val acl1 = new Acl(user1, PermissionType.ALLOW, host1, Operation.READ)
    val acl2 = new Acl(user1, PermissionType.ALLOW, host1, Operation.WRITE)
    val acl3 = new Acl(user2, PermissionType.ALLOW, host2, Operation.READ)
    val acl4 = new Acl(user2, PermissionType.ALLOW, host2, Operation.WRITE)

    var acls = changeAclAndVerify(Set.empty[Acl], Set[Acl](acl1, acl2, acl3, acl4), Set.empty[Acl])

    //test addAcl is additive
    val acl5 = new Acl(user2, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.READ)
    acls = changeAclAndVerify(acls, Set[Acl](acl5), Set.empty[Acl])

    //test get by principal name.
    TestUtils.waitUntilTrue(() => Map(resource -> Set(acl1, acl2)) == simpleAclAuthorizer.acls(user1), "changes not propagated in timeout period")
    TestUtils.waitUntilTrue(() => Map(resource -> Set(acl3, acl4, acl5)) == simpleAclAuthorizer.acls(user2), "changes not propagated in timeout period")

    val resourceToAcls = Map[Resource, Set[Acl]](
      new Resource(ResourceType.TOPIC, Resource.WILDCARD_RESOURCE) -> Set[Acl](new Acl(user2, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.READ)),
      new Resource(ResourceType.CLUSTER, Resource.WILDCARD_RESOURCE) -> Set[Acl](new Acl(user2, PermissionType.ALLOW, host1, Operation.READ)),
      new Resource(ResourceType.GROUP, Resource.WILDCARD_RESOURCE) -> acls,
      new Resource(ResourceType.GROUP, "test-ConsumerGroup") -> acls
    )

    resourceToAcls foreach { case (key, value) => changeAclAndVerify(Set.empty[Acl], value, Set.empty[Acl], key) }
    TestUtils.waitUntilTrue(() => resourceToAcls + (resource -> acls) == simpleAclAuthorizer.acls(), "changes not propagated in timeout period.")

    //test remove acl from existing acls.
    acls = changeAclAndVerify(acls, Set.empty[Acl], Set(acl1, acl5))

    //test remove all acls for resource
    simpleAclAuthorizer.removeAcls(resource)
    TestUtils.waitAndVerifyAcls(Set.empty[Acl], simpleAclAuthorizer, resource)
    assertTrue(!zkUtils.pathExists(simpleAclAuthorizer.toResourcePath(resource)))

    //test removing last acl also deletes zookeeper path
    acls = changeAclAndVerify(Set.empty[Acl], Set(acl1), Set.empty[Acl])
    changeAclAndVerify(acls, Set.empty[Acl], acls)
    assertTrue(!zkUtils.pathExists(simpleAclAuthorizer.toResourcePath(resource)))
  }

  @Test
  def testLoadCache() {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new Acl(user1, PermissionType.ALLOW, "host-1", Operation.READ)
    val acls = new util.HashSet[Acl]()
    acls.add(acl1)
    simpleAclAuthorizer.addAcls(acls, resource)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val resource1 = new Resource(ResourceType.TOPIC, "test-2")
    val acl2 = new Acl(user2, PermissionType.DENY, "host3", Operation.READ)
    val acls1 = Set[Acl](acl2)
    simpleAclAuthorizer.addAcls(acls1, resource1)

    zkUtils.deletePathRecursive(SimpleAclAuthorizer.AclChangedZkPath)
    val authorizer = new SimpleAclAuthorizer
    authorizer.configure(config.originals)

    assertEquals(acls, authorizer.acls(resource))
    assertEquals(acls1, authorizer.acls(resource1))
  }

  @Test
  def testLocalConcurrentModificationOfResourceAcls() {
    val commonResource = new Resource(ResourceType.TOPIC, "test")

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new Acl(user1, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.READ)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val acl2 = new Acl(user2, PermissionType.DENY, Acl.WILDCARD_HOST, Operation.READ)

    simpleAclAuthorizer.addAcls(Set(acl1), commonResource)
    simpleAclAuthorizer.addAcls(Set(acl2), commonResource)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer, commonResource)
  }

  @Test
  def testDistributedConcurrentModificationOfResourceAcls() {
    val commonResource = new Resource(ResourceType.TOPIC, "test")

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new Acl(user1, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.READ)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val acl2 = new Acl(user2, PermissionType.DENY, Acl.WILDCARD_HOST, Operation.READ)

    // Add on each instance
    simpleAclAuthorizer.addAcls(Set(acl1), commonResource)
    simpleAclAuthorizer2.addAcls(Set(acl2), commonResource)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer, commonResource)
    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer2, commonResource)

    val user3 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "joe")
    val acl3 = new Acl(user3, PermissionType.DENY, Acl.WILDCARD_HOST, Operation.READ)

    // Add on one instance and delete on another
    simpleAclAuthorizer.addAcls(Set(acl3), commonResource)
    val deleted = simpleAclAuthorizer2.removeAcls(Set(acl3), commonResource)

    assertTrue("The authorizer should see a value that needs to be deleted", deleted)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer, commonResource)
    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), simpleAclAuthorizer2, commonResource)
  }

  @Test
  def testHighConcurrencyModificationOfResourceAcls() {
    val commonResource = new Resource(ResourceType.TOPIC, "test")

    val acls = (0 to 50).map { i =>
      val useri = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, i.toString)
      new Acl(useri, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.READ)
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

  @Test
  def testHighConcurrencyDeletionOfResourceAcls() {
    val acl = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username), PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.ALL)

    // Alternate authorizer to keep adding and removing zookeeper path
    val concurrentFuctions = (0 to 50).map { i =>
      () => {
        simpleAclAuthorizer.addAcls(Set(acl), resource)
        simpleAclAuthorizer2.removeAcls(Set(acl), resource)
      }
    }

    TestUtils.assertConcurrent("Should support many concurrent calls", concurrentFuctions, 30 * 1000)

    TestUtils.waitAndVerifyAcls(Set.empty[Acl], simpleAclAuthorizer, resource)
    TestUtils.waitAndVerifyAcls(Set.empty[Acl], simpleAclAuthorizer2, resource)
  }

  val AclJson = "{\"version\": 1, \"acls\": [{\"host\": \"host1\",\"permissionType\": \"Deny\",\"operation\": \"READ\", \"principal\": \"User:alice\"  },  " +
    "{  \"host\":  \"*\" ,  \"permissionType\": \"Allow\",  \"operation\":  \"Read\", \"principal\": \"User:bob\"  },  " +
    "{  \"host\": \"host1\",  \"permissionType\": \"Deny\",  \"operation\":   \"Read\" ,  \"principal\": \"User:bob\"}  ]}"

  @Test
  def testAclJsonConversion(): Unit = {
    val acl1 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"), PermissionType.DENY, "host1" , Operation.READ)
    val acl2 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), PermissionType.ALLOW, "*", Operation.READ)
    val acl3 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), PermissionType.DENY, "host1", Operation.READ)

    val acls = Set[Acl](acl1, acl2, acl3)
    val jsonAcls = Json.encode(simpleAclAuthorizer.aclToJsonCompatibleMap(acls))

    Assert.assertEquals(acls, simpleAclAuthorizer.aclFromJson(jsonAcls))
    Assert.assertEquals(acls, simpleAclAuthorizer.aclFromJson(AclJson))
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
