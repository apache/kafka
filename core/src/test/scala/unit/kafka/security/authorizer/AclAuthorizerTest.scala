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

import java.io.File
import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.{Executors, Semaphore, TimeUnit}
import java.lang.management.ManagementFactory
import javax.management.ObjectName

import kafka.Kafka
import kafka.api.{ApiVersion, KAFKA_2_0_IV0, KAFKA_2_0_IV1}
import kafka.security.authorizer.AclEntry.{WildcardHost, WildcardPrincipalString}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.{ZkAclStore, ZooKeeperTestHarness}
import kafka.zookeeper.{GetChildrenRequest, GetDataRequest, ZooKeeperClient}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.errors.{ApiException, UnsupportedVersionException}
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.PatternType.{LITERAL, MATCH, PREFIXED}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics}
import org.apache.kafka.server.authorizer._
import org.apache.kafka.common.utils.{Time, SecurityUtils => JSecurityUtils}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._
import scala.collection.mutable

class AclAuthorizerTest extends ZooKeeperTestHarness {

  private val allowReadAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, READ, ALLOW)
  private val allowWriteAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, WRITE, ALLOW)
  private val denyReadAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, READ, DENY)

  private val wildCardResource = new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL)
  private val prefixedResource = new ResourcePattern(TOPIC, "foo", PREFIXED)
  private val clusterResource = new ResourcePattern(CLUSTER, CLUSTER_NAME, LITERAL)
  private val wildcardPrincipal = JSecurityUtils.parseKafkaPrincipal(WildcardPrincipalString)

  private val aclAuthorizer = new AclAuthorizer
  private val aclAuthorizer2 = new AclAuthorizer
  private var resource: ResourcePattern = _
  private val superUsers = "User:superuser1; User:superuser2"
  private val username = "alice"
  private val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
  private val requestContext = newRequestContext(principal, InetAddress.getByName("192.168.0.1"))
  private var config: KafkaConfig = _
  private var zooKeeperClient: ZooKeeperClient = _

  class CustomPrincipal(principalType: String, name: String) extends KafkaPrincipal(principalType, name) {
    override def equals(o: scala.Any): Boolean = false
  }

  @Before
  override def setUp(): Unit = {
    super.setUp()

    // Increase maxUpdateRetries to avoid transient failures
    aclAuthorizer.maxUpdateRetries = Int.MaxValue
    aclAuthorizer2.maxUpdateRetries = Int.MaxValue

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(AclAuthorizer.SuperUsersProp, superUsers)

    config = KafkaConfig.fromProps(props)
    aclAuthorizer.configure(config.originals)
    aclAuthorizer2.configure(config.originals)
    resource = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)

    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests,
      Time.SYSTEM, "kafka.test", "AclAuthorizerTest")

    setupAuthorizerMetrics()
  }

  @After
  override def tearDown(): Unit = {
    aclAuthorizer.close()
    aclAuthorizer2.close()
    zooKeeperClient.close()
    super.tearDown()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAuthorizeThrowsOnNonLiteralResource(): Unit = {
    authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(TOPIC, "something", PREFIXED))
  }

  @Test
  def testAuthorizeWithEmptyResourceName(): Unit = {
    assertFalse(authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(GROUP, "", LITERAL)))
    addAcls(aclAuthorizer, Set(allowReadAcl), new ResourcePattern(GROUP, WILDCARD_RESOURCE, LITERAL))
    assertTrue(authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(GROUP, "", LITERAL)))
  }

  // Authorizing the empty resource is not supported because we create a znode with the resource name.
  @Test
  def testEmptyAclThrowsException(): Unit = {
    val e = intercept[ApiException] {
      addAcls(aclAuthorizer, Set(allowReadAcl), new ResourcePattern(GROUP, "", LITERAL))
    }
    assertTrue(s"Unexpected exception $e", e.getCause.isInstanceOf[IllegalArgumentException])
  }

  @Test
  def testTopicAcl(): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "rob")
    val user3 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "batman")
    val host1 = InetAddress.getByName("192.168.1.1")
    val host2 = InetAddress.getByName("192.168.1.2")

    //user1 has READ access from host1 and host2.
    val acl1 = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, ALLOW)
    val acl2 = new AccessControlEntry(user1.toString, host2.getHostAddress, READ, ALLOW)

    //user1 does not have  READ access from host1.
    val acl3 = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, DENY)

    //user1 has WRITE access from host1 only.
    val acl4 = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, ALLOW)

    //user1 has DESCRIBE access from all hosts.
    val acl5 = new AccessControlEntry(user1.toString, WildcardHost, DESCRIBE, ALLOW)

    //user2 has READ access from all hosts.
    val acl6 = new AccessControlEntry(user2.toString, WildcardHost, READ, ALLOW)

    //user3 has WRITE access from all hosts.
    val acl7 = new AccessControlEntry(user3.toString, WildcardHost, WRITE, ALLOW)

    val acls = Set(acl1, acl2, acl3, acl4, acl5, acl6, acl7)

    changeAclAndVerify(Set.empty, acls, Set.empty)

    val host1Context = newRequestContext(user1, host1)
    val host2Context = newRequestContext(user1, host2)

    assertTrue("User1 should have READ access from host2", authorize(aclAuthorizer, host2Context, READ, resource))
    assertFalse("User1 should not have READ access from host1 due to denyAcl", authorize(aclAuthorizer, host1Context, READ, resource))
    assertTrue("User1 should have WRITE access from host1", authorize(aclAuthorizer, host1Context, WRITE, resource))
    assertFalse("User1 should not have WRITE access from host2 as no allow acl is defined", authorize(aclAuthorizer, host2Context, WRITE, resource))
    assertTrue("User1 should not have DESCRIBE access from host1", authorize(aclAuthorizer, host1Context, DESCRIBE, resource))
    assertTrue("User1 should have DESCRIBE access from host2", authorize(aclAuthorizer, host2Context, DESCRIBE, resource))
    assertFalse("User1 should not have edit access from host1", authorize(aclAuthorizer, host1Context, ALTER, resource))
    assertFalse("User1 should not have edit access from host2", authorize(aclAuthorizer, host2Context, ALTER, resource))

    //test if user has READ and write access they also get describe access
    val user2Context = newRequestContext(user2, host1)
    val user3Context = newRequestContext(user3, host1)
    assertTrue("User2 should have DESCRIBE access from host1", authorize(aclAuthorizer, user2Context, DESCRIBE, resource))
    assertTrue("User3 should have DESCRIBE access from host2", authorize(aclAuthorizer, user3Context, DESCRIBE, resource))
    assertTrue("User2 should have READ access from host1", authorize(aclAuthorizer, user2Context, READ, resource))
    assertTrue("User3 should have WRITE access from host2", authorize(aclAuthorizer, user3Context, WRITE, resource))
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
    val acl1 = new AccessControlEntry(user.toString, host1.getHostAddress, READ, DENY)
    val acl2 = new AccessControlEntry(user.toString, host2.getHostAddress, READ, ALLOW)
    val acls = Set(acl1, acl2)
    changeAclAndVerify(Set.empty, acls, Set.empty)

    val host1Context = newRequestContext(customUserPrincipal, host1)
    val host2Context = newRequestContext(customUserPrincipal, host2)

    assertTrue("User1 should have READ access from host2", authorize(aclAuthorizer, host2Context, READ, resource))
    assertFalse("User1 should not have READ access from host1 due to denyAcl", authorize(aclAuthorizer, host1Context, READ, resource))
  }

  @Test
  def testDenyTakesPrecedence(): Unit = {
    val user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host = InetAddress.getByName("192.168.2.1")
    val session = newRequestContext(user, host)

    val allowAll = new AccessControlEntry(WildcardPrincipalString, WildcardHost, AclOperation.ALL, ALLOW)
    val denyAcl = new AccessControlEntry(user.toString, host.getHostAddress, AclOperation.ALL, DENY)
    val acls = Set(allowAll, denyAcl)

    changeAclAndVerify(Set.empty, acls, Set.empty)

    assertFalse("deny should take precedence over allow.", authorize(aclAuthorizer, session, READ, resource))
  }

  @Test
  def testAllowAllAccess(): Unit = {
    val allowAllAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, AclOperation.ALL, ALLOW)

    changeAclAndVerify(Set.empty, Set(allowAllAcl), Set.empty)

    val context = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "random"), InetAddress.getByName("192.0.4.4"))
    assertTrue("allow all acl should allow access to all.", authorize(aclAuthorizer, context, READ, resource))
  }

  @Test
  def testSuperUserHasAccess(): Unit = {
    val denyAllAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, AclOperation.ALL, DENY)

    changeAclAndVerify(Set.empty, Set(denyAllAcl), Set.empty)

    val session1 = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))
    val session2 = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser2"), InetAddress.getByName("192.0.4.4"))

    assertTrue("superuser always has access, no matter what acls.", authorize(aclAuthorizer, session1, READ, resource))
    assertTrue("superuser always has access, no matter what acls.", authorize(aclAuthorizer, session2, READ, resource))
  }

  /**
    CustomPrincipals should be compared with their principal type and name
   */
  @Test
  def testSuperUserWithCustomPrincipalHasAccess(): Unit = {
    val denyAllAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, AclOperation.ALL, DENY)
    changeAclAndVerify(Set.empty, Set(denyAllAcl), Set.empty)

    val session = newRequestContext(new CustomPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))

    assertTrue("superuser with custom principal always has access, no matter what acls.", authorize(aclAuthorizer, session, READ, resource))
  }

  @Test
  def testWildCardAcls(): Unit = {
    assertFalse("when acls = [], authorizer should fail close.", authorize(aclAuthorizer, requestContext, READ, resource))

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.3.1")
    val readAcl = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, ALLOW)

    val acls = changeAclAndVerify(Set.empty, Set(readAcl), Set.empty, wildCardResource)

    val host1Context = newRequestContext(user1, host1)
    assertTrue("User1 should have READ access from host1", authorize(aclAuthorizer, host1Context, READ, resource))

    //allow WRITE to specific topic.
    val writeAcl = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, ALLOW)
    changeAclAndVerify(Set.empty, Set(writeAcl), Set.empty)

    //deny WRITE to wild card topic.
    val denyWriteOnWildCardResourceAcl = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, DENY)
    changeAclAndVerify(acls, Set(denyWriteOnWildCardResourceAcl), Set.empty, wildCardResource)

    assertFalse("User1 should not have WRITE access from host1", authorize(aclAuthorizer, host1Context, WRITE, resource))
  }

  @Test
  def testNoAclFound(): Unit = {
    assertFalse("when acls = [], authorizer should deny op.", authorize(aclAuthorizer, requestContext, READ, resource))
  }

  @Test
  def testNoAclFoundOverride(): Unit = {
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.put(AclAuthorizer.AllowEveryoneIfNoAclIsFoundProp, "true")

    val cfg = KafkaConfig.fromProps(props)
    val testAuthorizer = new AclAuthorizer
    try {
      testAuthorizer.configure(cfg.originals)
      assertTrue("when acls = null or [],  authorizer should allow op with allow.everyone = true.",
        authorize(testAuthorizer, requestContext, READ, resource))
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

    val acl1 = new AccessControlEntry(user1.toString, host1, READ, ALLOW)
    val acl2 = new AccessControlEntry(user1.toString, host1, WRITE, ALLOW)
    val acl3 = new AccessControlEntry(user2.toString, host2, READ, ALLOW)
    val acl4 = new AccessControlEntry(user2.toString, host2, WRITE, ALLOW)

    var acls = changeAclAndVerify(Set.empty, Set(acl1, acl2, acl3, acl4), Set.empty)

    //test addAcl is additive
    val acl5 = new AccessControlEntry(user2.toString, WildcardHost, READ, ALLOW)
    acls = changeAclAndVerify(acls, Set(acl5), Set.empty)

    //test get by principal name.
    TestUtils.waitUntilTrue(() => Set(acl1, acl2).map(acl => new AclBinding(resource, acl)) == getAcls(aclAuthorizer, user1),
      "changes not propagated in timeout period")
    TestUtils.waitUntilTrue(() => Set(acl3, acl4, acl5).map(acl => new AclBinding(resource, acl)) == getAcls(aclAuthorizer, user2),
      "changes not propagated in timeout period")

    val resourceToAcls = Map[ResourcePattern, Set[AccessControlEntry]](
      new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL) -> Set(new AccessControlEntry(user2.toString, WildcardHost, READ, ALLOW)),
      new ResourcePattern(CLUSTER , WILDCARD_RESOURCE, LITERAL) -> Set(new AccessControlEntry(user2.toString, host1, READ, ALLOW)),
      new ResourcePattern(GROUP, WILDCARD_RESOURCE, LITERAL) -> acls,
      new ResourcePattern(GROUP, "test-ConsumerGroup", LITERAL) -> acls
    )

    resourceToAcls foreach { case (key, value) => changeAclAndVerify(Set.empty, value, Set.empty, key) }
    val expectedAcls = (resourceToAcls + (resource -> acls)).flatMap {
      case (res, resAcls) => resAcls.map { acl => new AclBinding(res, acl) }
    }.toSet
    TestUtils.waitUntilTrue(() => expectedAcls == getAcls(aclAuthorizer), "changes not propagated in timeout period.")

    //test remove acl from existing acls.
    acls = changeAclAndVerify(acls, Set.empty, Set(acl1, acl5))

    //test remove all acls for resource
    removeAcls(aclAuthorizer, Set.empty, resource)
    TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], aclAuthorizer, resource)
    assertTrue(!zkClient.resourceExists(resource))

    //test removing last acl also deletes ZooKeeper path
    acls = changeAclAndVerify(Set.empty, Set(acl1), Set.empty)
    changeAclAndVerify(acls, Set.empty, acls)
    assertTrue(!zkClient.resourceExists(resource))
  }

  @Test
  def testLoadCache(): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new AccessControlEntry(user1.toString, "host-1", READ, ALLOW)
    val acls = Set(acl1)
    addAcls(aclAuthorizer, acls, resource)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val resource1 = new ResourcePattern(TOPIC, "test-2", LITERAL)
    val acl2 = new AccessControlEntry(user2.toString, "host3", READ, DENY)
    val acls1 = Set(acl2)
    addAcls(aclAuthorizer, acls1, resource1)

    zkClient.deleteAclChangeNotifications
    val authorizer = new AclAuthorizer
    try {
      authorizer.configure(config.originals)

      assertEquals(acls, getAcls(authorizer, resource))
      assertEquals(acls1, getAcls(authorizer, resource1))
    } finally {
      authorizer.close()
    }
  }

  /**
   * Verify that there is no timing window between loading ACL cache and setting
   * up ZK change listener. Cache must be loaded before creating change listener
   * in the authorizer to avoid the timing window.
   */
  @Test
  def testChangeListenerTiming(): Unit = {
    val configureSemaphore = new Semaphore(0)
    val listenerSemaphore = new Semaphore(0)
    val executor = Executors.newSingleThreadExecutor
    val aclAuthorizer3 = new AclAuthorizer {
      override private[authorizer] def startZkChangeListeners(): Unit = {
        configureSemaphore.release()
        listenerSemaphore.acquireUninterruptibly()
        super.startZkChangeListeners()
      }
    }
    try {
      val future = executor.submit((() => aclAuthorizer3.configure(config.originals)): Runnable)
      configureSemaphore.acquire()
      val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
      val acls = Set(new AccessControlEntry(user1.toString, "host-1", READ, DENY))
      addAcls(aclAuthorizer, acls, resource)

      listenerSemaphore.release()
      future.get(10, TimeUnit.SECONDS)

      assertEquals(acls, getAcls(aclAuthorizer3, resource))
    } finally {
      aclAuthorizer3.close()
      executor.shutdownNow()
    }
  }

  @Test
  def testLocalConcurrentModificationOfResourceAcls(): Unit = {
    val commonResource = new ResourcePattern(TOPIC, "test", LITERAL)

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new AccessControlEntry(user1.toString, WildcardHost, READ, ALLOW)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val acl2 = new AccessControlEntry(user2.toString, WildcardHost, READ, DENY)

    addAcls(aclAuthorizer, Set(acl1), commonResource)
    addAcls(aclAuthorizer, Set(acl2), commonResource)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), aclAuthorizer, commonResource)
  }

  @Test
  def testDistributedConcurrentModificationOfResourceAcls(): Unit = {
    val commonResource = new ResourcePattern(TOPIC, "test", LITERAL)

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new AccessControlEntry(user1.toString, WildcardHost, READ, ALLOW)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val acl2 = new AccessControlEntry(user2.toString, WildcardHost, READ, DENY)

    // Add on each instance
    addAcls(aclAuthorizer, Set(acl1), commonResource)
    addAcls(aclAuthorizer2, Set(acl2), commonResource)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), aclAuthorizer, commonResource)
    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), aclAuthorizer2, commonResource)

    val user3 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "joe")
    val acl3 = new AccessControlEntry(user3.toString, WildcardHost, READ, DENY)

    // Add on one instance and delete on another
    addAcls(aclAuthorizer, Set(acl3), commonResource)
    val deleted = removeAcls(aclAuthorizer2, Set(acl3), commonResource)

    assertTrue("The authorizer should see a value that needs to be deleted", deleted)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), aclAuthorizer, commonResource)
    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), aclAuthorizer2, commonResource)
  }

  @Test
  def testHighConcurrencyModificationOfResourceAcls(): Unit = {
    val commonResource = new ResourcePattern(TOPIC, "test", LITERAL)

    val acls= (0 to 50).map { i =>
      val useri = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, i.toString)
      (new AccessControlEntry(useri.toString, WildcardHost, READ, ALLOW), i)
    }

    // Alternate authorizer, Remove all acls that end in 0
    val concurrentFuctions = acls.map { case (acl, aclId) =>
      () => {
        if (aclId % 2 == 0) {
          addAcls(aclAuthorizer, Set(acl), commonResource)
        } else {
          addAcls(aclAuthorizer2, Set(acl), commonResource)
        }
        if (aclId % 10 == 0) {
          removeAcls(aclAuthorizer2, Set(acl), commonResource)
        }
      }
    }

    val expectedAcls = acls.filter { case (acl, aclId) =>
      aclId % 10 != 0
    }.map(_._1).toSet

    TestUtils.assertConcurrent("Should support many concurrent calls", concurrentFuctions, 30 * 1000)

    TestUtils.waitAndVerifyAcls(expectedAcls, aclAuthorizer, commonResource)
    TestUtils.waitAndVerifyAcls(expectedAcls, aclAuthorizer2, commonResource)
  }

  /**
    * Test ACL inheritance, as described in #{org.apache.kafka.common.acl.AclOperation}
    */
  @Test
  def testAclInheritance(): Unit = {
    testImplicationsOfAllow(AclOperation.ALL, Set(READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE,
      CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE))
    testImplicationsOfDeny(AclOperation.ALL, Set(READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE,
      CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE))
    testImplicationsOfAllow(READ, Set(DESCRIBE))
    testImplicationsOfAllow(WRITE, Set(DESCRIBE))
    testImplicationsOfAllow(DELETE, Set(DESCRIBE))
    testImplicationsOfAllow(ALTER, Set(DESCRIBE))
    testImplicationsOfDeny(DESCRIBE, Set())
    testImplicationsOfAllow(ALTER_CONFIGS, Set(DESCRIBE_CONFIGS))
    testImplicationsOfDeny(DESCRIBE_CONFIGS, Set())
  }

  private def testImplicationsOfAllow(parentOp: AclOperation, allowedOps: Set[AclOperation]): Unit = {
    val user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host = InetAddress.getByName("192.168.3.1")
    val hostContext = newRequestContext(user, host)
    val acl = new AccessControlEntry(user.toString, WildcardHost, parentOp, ALLOW)
    addAcls(aclAuthorizer, Set(acl), clusterResource)
    AclOperation.values.filter(validOp).foreach { op =>
      val authorized = authorize(aclAuthorizer, hostContext, op, clusterResource)
      if (allowedOps.contains(op) || op == parentOp)
        assertTrue(s"ALLOW $parentOp should imply ALLOW $op", authorized)
      else
        assertFalse(s"ALLOW $parentOp should not imply ALLOW $op", authorized)
    }
    removeAcls(aclAuthorizer, Set(acl), clusterResource)
  }

  private def testImplicationsOfDeny(parentOp: AclOperation, deniedOps: Set[AclOperation]): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.3.1")
    val host1Context = newRequestContext(user1, host1)
    val acls = Set(new AccessControlEntry(user1.toString, WildcardHost, parentOp, DENY),
      new AccessControlEntry(user1.toString, WildcardHost, AclOperation.ALL, ALLOW))
    addAcls(aclAuthorizer, acls, clusterResource)
    AclOperation.values.filter(validOp).foreach { op =>
      val authorized = authorize(aclAuthorizer, host1Context, op, clusterResource)
      if (deniedOps.contains(op) || op == parentOp)
        assertFalse(s"DENY $parentOp should imply DENY $op", authorized)
      else
        assertTrue(s"DENY $parentOp should not imply DENY $op", authorized)
    }
    removeAcls(aclAuthorizer, acls, clusterResource)
  }

  @Test
  def testHighConcurrencyDeletionOfResourceAcls(): Unit = {
    val acl = new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username).toString, WildcardHost, AclOperation.ALL, ALLOW)

    // Alternate authorizer to keep adding and removing ZooKeeper path
    val concurrentFuctions = (0 to 50).map { _ =>
      () => {
        addAcls(aclAuthorizer, Set(acl), resource)
        removeAcls(aclAuthorizer2, Set(acl), resource)
      }
    }

    TestUtils.assertConcurrent("Should support many concurrent calls", concurrentFuctions, 30 * 1000)

    TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], aclAuthorizer, resource)
    TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], aclAuthorizer2, resource)
  }

  @Test
  def testAccessAllowedIfAllowAclExistsOnWildcardResource(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl), wildCardResource)

    assertTrue(authorize(aclAuthorizer, requestContext, READ, resource))
  }

  @Test
  def testDeleteAclOnWildcardResource(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl, allowWriteAcl), wildCardResource)

    removeAcls(aclAuthorizer, Set(allowReadAcl), wildCardResource)

    assertEquals(Set(allowWriteAcl), getAcls(aclAuthorizer, wildCardResource))
  }

  @Test
  def testDeleteAllAclOnWildcardResource(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl), wildCardResource)

    removeAcls(aclAuthorizer, Set.empty, wildCardResource)

    assertEquals(Set.empty, getAcls(aclAuthorizer))
  }

  @Test
  def testAccessAllowedIfAllowAclExistsOnPrefixedResource(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl), prefixedResource)

    assertTrue(authorize(aclAuthorizer, requestContext, READ, resource))
  }

  @Test
  def testDeleteAclOnPrefixedResource(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl, allowWriteAcl), prefixedResource)

    removeAcls(aclAuthorizer, Set(allowReadAcl), prefixedResource)

    assertEquals(Set(allowWriteAcl), getAcls(aclAuthorizer, prefixedResource))
  }

  @Test
  def testDeleteAllAclOnPrefixedResource(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl, allowWriteAcl), prefixedResource)

    removeAcls(aclAuthorizer, Set.empty, prefixedResource)

    assertEquals(Set.empty, getAcls(aclAuthorizer))
  }

  @Test
  def testAddAclsOnLiteralResource(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl, allowWriteAcl), resource)
    addAcls(aclAuthorizer, Set(allowWriteAcl, denyReadAcl), resource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(aclAuthorizer, resource))
    assertEquals(Set.empty, getAcls(aclAuthorizer, wildCardResource))
    assertEquals(Set.empty, getAcls(aclAuthorizer, prefixedResource))
  }

  @Test
  def testAddAclsOnWildcardResource(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl, allowWriteAcl), wildCardResource)
    addAcls(aclAuthorizer, Set(allowWriteAcl, denyReadAcl), wildCardResource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(aclAuthorizer, wildCardResource))
    assertEquals(Set.empty, getAcls(aclAuthorizer, resource))
    assertEquals(Set.empty, getAcls(aclAuthorizer, prefixedResource))
  }

  @Test
  def testAddAclsOnPrefixedResource(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl, allowWriteAcl), prefixedResource)
    addAcls(aclAuthorizer, Set(allowWriteAcl, denyReadAcl), prefixedResource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(aclAuthorizer, prefixedResource))
    assertEquals(Set.empty, getAcls(aclAuthorizer, wildCardResource))
    assertEquals(Set.empty, getAcls(aclAuthorizer, resource))
  }

  @Test
  def testAuthorizeWithPrefixedResource(): Unit = {
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "a_other", LITERAL))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "a_other", PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID() + "-zzz", PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fooo-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fo-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fop-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fon-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fon-", PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", LITERAL))

    addAcls(aclAuthorizer, Set(allowReadAcl), prefixedResource)

    assertTrue(authorize(aclAuthorizer, requestContext, READ, resource))
  }

  @Test
  def testSingleCharacterResourceAcls(): Unit = {
    addAcls(aclAuthorizer, Set(allowReadAcl), new ResourcePattern(TOPIC, "f", LITERAL))
    assertTrue(authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(TOPIC, "f", LITERAL)))
    assertFalse(authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(TOPIC, "foo", LITERAL)))

    addAcls(aclAuthorizer, Set(allowReadAcl), new ResourcePattern(TOPIC, "_", PREFIXED))
    assertTrue(authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(TOPIC, "_foo", LITERAL)))
    assertTrue(authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(TOPIC, "_", LITERAL)))
    assertFalse(authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(TOPIC, "foo_", LITERAL)))
  }

  @Test
  def testGetAclsPrincipal(): Unit = {
    val aclOnSpecificPrincipal = new AccessControlEntry(principal.toString, WildcardHost, WRITE, ALLOW)
    addAcls(aclAuthorizer, Set(aclOnSpecificPrincipal), resource)

    assertEquals("acl on specific should not be returned for wildcard request",
      0, getAcls(aclAuthorizer, wildcardPrincipal).size)
    assertEquals("acl on specific should be returned for specific request",
      1, getAcls(aclAuthorizer, principal).size)
    assertEquals("acl on specific should be returned for different principal instance",
      1, getAcls(aclAuthorizer, new KafkaPrincipal(principal.getPrincipalType, principal.getName)).size)

    removeAcls(aclAuthorizer, Set.empty, resource)
    val aclOnWildcardPrincipal = new AccessControlEntry(WildcardPrincipalString, WildcardHost, WRITE, ALLOW)
    addAcls(aclAuthorizer, Set(aclOnWildcardPrincipal), resource)

    assertEquals("acl on wildcard should be returned for wildcard request",
      1, getAcls(aclAuthorizer, wildcardPrincipal).size)
    assertEquals("acl on wildcard should not be returned for specific request",
      0, getAcls(aclAuthorizer, principal).size)
  }

  @Test
  def testAclsFilter(): Unit = {
    val resource1 = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)
    val resource2 = new ResourcePattern(TOPIC, "bar-" + UUID.randomUUID(), LITERAL)
    val prefixedResource = new ResourcePattern(TOPIC, "bar-", PREFIXED)

    val acl1 = new AclBinding(resource1, new AccessControlEntry(principal.toString, WildcardHost, READ, ALLOW))
    val acl2 = new AclBinding(resource1, new AccessControlEntry(principal.toString, "192.168.0.1", WRITE, ALLOW))
    val acl3 = new AclBinding(resource2, new AccessControlEntry(principal.toString, WildcardHost, DESCRIBE, ALLOW))
    val acl4 = new AclBinding(prefixedResource, new AccessControlEntry(wildcardPrincipal.toString, WildcardHost, READ, ALLOW))

    aclAuthorizer.createAcls(requestContext, List(acl1, acl2, acl3, acl4).asJava)
    assertEquals(Set(acl1, acl2, acl3, acl4), aclAuthorizer.acls(AclBindingFilter.ANY).asScala.toSet)
    assertEquals(Set(acl1, acl2), aclAuthorizer.acls(new AclBindingFilter(resource1.toFilter, AccessControlEntryFilter.ANY)).asScala.toSet)
    assertEquals(Set(acl4), aclAuthorizer.acls(new AclBindingFilter(prefixedResource.toFilter, AccessControlEntryFilter.ANY)).asScala.toSet)
    val matchingFilter = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, resource2.name, MATCH), AccessControlEntryFilter.ANY)
    assertEquals(Set(acl3, acl4), aclAuthorizer.acls(matchingFilter).asScala.toSet)

    val filters = List(matchingFilter,
      acl1.toFilter,
      new AclBindingFilter(resource2.toFilter, AccessControlEntryFilter.ANY),
      new AclBindingFilter(new ResourcePatternFilter(TOPIC, "baz", PatternType.ANY), AccessControlEntryFilter.ANY))
    val deleteResults = aclAuthorizer.deleteAcls(requestContext, filters.asJava).asScala.map(_.toCompletableFuture.get)
    assertEquals(List.empty, deleteResults.filter(_.exception.isPresent))
    filters.indices.foreach { i =>
      assertEquals(Set.empty, deleteResults(i).aclBindingDeleteResults.asScala.toSet.filter(_.exception.isPresent))
    }
    assertEquals(Set(acl3, acl4), deleteResults(0).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
    assertEquals(Set(acl1), deleteResults(1).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
    assertEquals(Set.empty, deleteResults(2).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
    assertEquals(Set.empty, deleteResults(3).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
  }

  @Test
  def testThrowsOnAddPrefixedAclIfInterBrokerProtocolVersionTooLow(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(KAFKA_2_0_IV0))
    val e = intercept[ApiException] {
      addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", PREFIXED))
    }
    assertTrue(s"Unexpected exception $e", e.getCause.isInstanceOf[UnsupportedVersionException])
  }

  @Test
  def testWritesExtendedAclChangeEventIfInterBrokerProtocolNotSet(): Unit = {
    givenAuthorizerWithProtocolVersion(Option.empty)
    val resource = new ResourcePattern(TOPIC, "z_other", PREFIXED)
    val expected = new String(ZkAclStore(PREFIXED).changeStore
      .createChangeNode(resource).bytes, UTF_8)

    addAcls(aclAuthorizer, Set(denyReadAcl), resource)

    val actual = getAclChangeEventAsString(PREFIXED)

    assertEquals(expected, actual)
  }

  @Test
  def testWritesExtendedAclChangeEventWhenInterBrokerProtocolAtLeastKafkaV2(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(KAFKA_2_0_IV1))
    val resource = new ResourcePattern(TOPIC, "z_other", PREFIXED)
    val expected = new String(ZkAclStore(PREFIXED).changeStore
      .createChangeNode(resource).bytes, UTF_8)

    addAcls(aclAuthorizer, Set(denyReadAcl), resource)

    val actual = getAclChangeEventAsString(PREFIXED)

    assertEquals(expected, actual)
  }

  @Test
  def testWritesLiteralWritesLiteralAclChangeEventWhenInterBrokerProtocolLessThanKafkaV2eralAclChangesForOlderProtocolVersions(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(KAFKA_2_0_IV0))
    val resource = new ResourcePattern(TOPIC, "z_other", LITERAL)
    val expected = new String(ZkAclStore(LITERAL).changeStore
      .createChangeNode(resource).bytes, UTF_8)

    addAcls(aclAuthorizer, Set(denyReadAcl), resource)

    val actual = getAclChangeEventAsString(LITERAL)

    assertEquals(expected, actual)
  }

  @Test
  def testWritesLiteralAclChangeEventWhenInterBrokerProtocolIsKafkaV2(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(KAFKA_2_0_IV1))
    val resource = new ResourcePattern(TOPIC, "z_other", LITERAL)
    val expected = new String(ZkAclStore(LITERAL).changeStore
      .createChangeNode(resource).bytes, UTF_8)

    addAcls(aclAuthorizer, Set(denyReadAcl), resource)

    val actual = getAclChangeEventAsString(LITERAL)

    assertEquals(expected, actual)
  }

  @Test
  def testAuthorizerNoZkConfig(): Unit = {
    val noTlsProps = Kafka.getPropsFromArgs(Array(prepareDefaultConfig))
    assertEquals(None, AclAuthorizer.zkClientConfigFromKafkaConfigAndMap(
      KafkaConfig.fromProps(noTlsProps),
      mutable.Map(noTlsProps.asInstanceOf[java.util.Map[String, Any]].asScala.toSeq: _*)))
  }

  @Test
  def testAuthorizerZkConfigFromKafkaConfigWithDefaults(): Unit = {
    val props = new java.util.Properties()
    val kafkaValue = "kafkaValue"
    val configs = Map("zookeeper.connect" -> "somewhere", // required, otherwise we would omit it
      KafkaConfig.ZkSslClientEnableProp -> "true",
      KafkaConfig.ZkClientCnxnSocketProp -> kafkaValue,
      KafkaConfig.ZkSslKeyStoreLocationProp -> kafkaValue,
      KafkaConfig.ZkSslKeyStorePasswordProp -> kafkaValue,
      KafkaConfig.ZkSslKeyStoreTypeProp -> kafkaValue,
      KafkaConfig.ZkSslTrustStoreLocationProp -> kafkaValue,
      KafkaConfig.ZkSslTrustStorePasswordProp -> kafkaValue,
      KafkaConfig.ZkSslTrustStoreTypeProp -> kafkaValue,
      KafkaConfig.ZkSslEnabledProtocolsProp -> kafkaValue,
      KafkaConfig.ZkSslCipherSuitesProp -> kafkaValue)
    configs.foreach{case (key, value) => props.put(key, value.toString) }

    val zkClientConfig = AclAuthorizer.zkClientConfigFromKafkaConfigAndMap(
      KafkaConfig.fromProps(props), mutable.Map(configs.toSeq: _*))
    assertTrue(zkClientConfig.isDefined)
    // confirm we get all the values we expect
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(prop => prop match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp =>
        assertEquals("true", KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
      case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp =>
        assertEquals("false", KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
      case KafkaConfig.ZkSslProtocolProp =>
        assertEquals("TLSv1.2", KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
      case _ => assertEquals(kafkaValue, KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
    })
  }

  @Test
  def testAuthorizerZkConfigFromKafkaConfig(): Unit = {
    val props = new java.util.Properties()
    val kafkaValue = "kafkaValue"
    val configs = Map("zookeeper.connect" -> "somewhere", // required, otherwise we would omit it
      KafkaConfig.ZkSslClientEnableProp -> "true",
      KafkaConfig.ZkClientCnxnSocketProp -> kafkaValue,
      KafkaConfig.ZkSslKeyStoreLocationProp -> kafkaValue,
      KafkaConfig.ZkSslKeyStorePasswordProp -> kafkaValue,
      KafkaConfig.ZkSslKeyStoreTypeProp -> kafkaValue,
      KafkaConfig.ZkSslTrustStoreLocationProp -> kafkaValue,
      KafkaConfig.ZkSslTrustStorePasswordProp -> kafkaValue,
      KafkaConfig.ZkSslTrustStoreTypeProp -> kafkaValue,
      KafkaConfig.ZkSslProtocolProp -> kafkaValue,
      KafkaConfig.ZkSslEnabledProtocolsProp -> kafkaValue,
      KafkaConfig.ZkSslCipherSuitesProp -> kafkaValue,
      KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp -> "HTTPS",
      KafkaConfig.ZkSslCrlEnableProp -> "false",
      KafkaConfig.ZkSslOcspEnableProp -> "false")
    configs.foreach{case (key, value) => props.put(key, value.toString) }

    val zkClientConfig = AclAuthorizer.zkClientConfigFromKafkaConfigAndMap(
      KafkaConfig.fromProps(props), mutable.Map(configs.toSeq: _*))
    assertTrue(zkClientConfig.isDefined)
    // confirm we get all the values we expect
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(prop => prop match {
        case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp =>
          assertEquals("true", KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
        case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp =>
          assertEquals("false", KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
        case _ => assertEquals(kafkaValue, KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
      })
  }

  @Test
  def testAuthorizerZkConfigFromPrefixOverrides(): Unit = {
    val props = new java.util.Properties()
    val kafkaValue = "kafkaValue"
    val prefixedValue = "prefixedValue"
    val prefix = "authorizer."
    val configs = Map("zookeeper.connect" -> "somewhere", // required, otherwise we would omit it
      KafkaConfig.ZkSslClientEnableProp -> "false",
      KafkaConfig.ZkClientCnxnSocketProp -> kafkaValue,
      KafkaConfig.ZkSslKeyStoreLocationProp -> kafkaValue,
      KafkaConfig.ZkSslKeyStorePasswordProp -> kafkaValue,
      KafkaConfig.ZkSslKeyStoreTypeProp -> kafkaValue,
      KafkaConfig.ZkSslTrustStoreLocationProp -> kafkaValue,
      KafkaConfig.ZkSslTrustStorePasswordProp -> kafkaValue,
      KafkaConfig.ZkSslTrustStoreTypeProp -> kafkaValue,
      KafkaConfig.ZkSslProtocolProp -> kafkaValue,
      KafkaConfig.ZkSslEnabledProtocolsProp -> kafkaValue,
      KafkaConfig.ZkSslCipherSuitesProp -> kafkaValue,
      KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp -> "HTTPS",
      KafkaConfig.ZkSslCrlEnableProp -> "false",
      KafkaConfig.ZkSslOcspEnableProp -> "false",
      prefix + KafkaConfig.ZkSslClientEnableProp -> "true",
      prefix + KafkaConfig.ZkClientCnxnSocketProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslKeyStoreLocationProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslKeyStorePasswordProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslKeyStoreTypeProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslTrustStoreLocationProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslTrustStorePasswordProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslTrustStoreTypeProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslProtocolProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslEnabledProtocolsProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslCipherSuitesProp -> prefixedValue,
      prefix + KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp -> "",
      prefix + KafkaConfig.ZkSslCrlEnableProp -> "true",
      prefix + KafkaConfig.ZkSslOcspEnableProp -> "true")
    configs.foreach{case (key, value) => props.put(key, value.toString) }

    val zkClientConfig = AclAuthorizer.zkClientConfigFromKafkaConfigAndMap(
      KafkaConfig.fromProps(props), mutable.Map(configs.toSeq: _*))
    assertTrue(zkClientConfig.isDefined)
    // confirm we get all the values we expect
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(prop => prop match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp =>
        assertEquals("true", KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp =>
        assertEquals("false", KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
      case _ => assertEquals(prefixedValue, KafkaConfig.getZooKeeperClientProperty(zkClientConfig.get, prop).getOrElse("<None>"))
    })
  }

  @Test
  def testAuthorizeTotalAcls(): Unit = {
    var totalAcls = aclAuthorizer.totalAcls
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "a_other", LITERAL))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "a_other", PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID() + "-zzz", PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fooo-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fo-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fop-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fon-" + UUID.randomUUID(), PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fon-", PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", PREFIXED))
    addAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", LITERAL))
    addAcls(aclAuthorizer, Set(allowReadAcl), prefixedResource)
    totalAcls = aclAuthorizer.totalAcls
    assertEquals(totalAcls, getAclAuthorizerMetric("acls-total-count"), 0.0)
    removeAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fon-" + UUID.randomUUID(), PREFIXED))
    removeAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "fon-", PREFIXED))
    removeAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", PREFIXED))
    removeAcls(aclAuthorizer, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", LITERAL))
    removeAcls(aclAuthorizer, Set(allowReadAcl), prefixedResource)
    totalAcls = aclAuthorizer.totalAcls
    assertEquals(totalAcls, getAclAuthorizerMetric("acls-total-count"), 0.0)
  }

  @Test
  def testAuthorizationRate(): Unit = {
    val count = 10
    val resource1 = new ResourcePattern(TOPIC, "foo1" + UUID.randomUUID(), LITERAL)
    addAcls(aclAuthorizer, Set(denyReadAcl), resource1)
    assertEquals(0.0, getAclAuthorizerMetric("authorization-denied-rate-per-minute"), 0.0d)
    assertEquals(0.0, getAclAuthorizerMetric("authorization-allowed-rate-per-minute"), 0.0d)
    assertEquals(0.0, getAclAuthorizerMetric("authorization-request-rate-per-minute"), 0.0d)
    for (i <- 1 to count) {
      authorize(aclAuthorizer, requestContext, READ, resource1)
    }
    TimeUnit.SECONDS.sleep(10)
    assertTrue(getAclAuthorizerMetric("authorization-denied-rate-per-minute") >= 10)

    addAcls(aclAuthorizer, Set(allowReadAcl), resource)
    for (i <- 1 to count) {
      authorize(aclAuthorizer, requestContext, READ, resource)
    }
    for (i <- 1 to count) {
      authorize(aclAuthorizer, requestContext, READ, resource1)
    }
    TimeUnit.SECONDS.sleep(30)
    assertTrue(getAclAuthorizerMetric("authorization-allowed-rate-per-minute") >= 10)
    assertTrue(getAclAuthorizerMetric("authorization-request-rate-per-minute") >= 20)
  }

  private def givenAuthorizerWithProtocolVersion(protocolVersion: Option[ApiVersion]): Unit = {
    aclAuthorizer.close()

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(AclAuthorizer.SuperUsersProp, superUsers)
    protocolVersion.foreach(version => props.put(KafkaConfig.InterBrokerProtocolVersionProp, version.toString))

    config = KafkaConfig.fromProps(props)

    aclAuthorizer.configure(config.originals)
  }

  private def getAclChangeEventAsString(patternType: PatternType) = {
    val store = ZkAclStore(patternType)
    val children = zooKeeperClient.handleRequest(GetChildrenRequest(store.changeStore.aclChangePath, registerWatch = true))
    children.maybeThrow()
    assertEquals("Expecting 1 change event", 1, children.children.size)

    val data = zooKeeperClient.handleRequest(GetDataRequest(s"${store.changeStore.aclChangePath}/${children.children.head}"))
    data.maybeThrow()

    new String(data.data, UTF_8)
  }

  private def changeAclAndVerify(originalAcls: Set[AccessControlEntry],
                                 addedAcls: Set[AccessControlEntry],
                                 removedAcls: Set[AccessControlEntry],
                                 resource: ResourcePattern = resource): Set[AccessControlEntry] = {
    var acls = originalAcls

    if(addedAcls.nonEmpty) {
      addAcls(aclAuthorizer, addedAcls, resource)
      acls ++= addedAcls
    }

    if(removedAcls.nonEmpty) {
      removeAcls(aclAuthorizer, removedAcls, resource)
      acls --=removedAcls
    }

    TestUtils.waitAndVerifyAcls(acls, aclAuthorizer, resource)

    acls
  }

  private def newRequestContext(principal: KafkaPrincipal, clientAddress: InetAddress, apiKey: ApiKeys = ApiKeys.PRODUCE): RequestContext = {
    val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
    val header = new RequestHeader(apiKey, 2, "", 1) //ApiKeys apiKey, short version, String clientId, int correlation
    new RequestContext(header, "", clientAddress, principal, ListenerName.forSecurityProtocol(securityProtocol),
      securityProtocol, ClientInformation.EMPTY)
  }

  private def authorize(authorizer: AclAuthorizer, requestContext: RequestContext, operation: AclOperation, resource: ResourcePattern): Boolean = {
    val action = new Action(operation, resource, 1, true, true)
    authorizer.authorize(requestContext, List(action).asJava).asScala.head == AuthorizationResult.ALLOWED
  }

  private def addAcls(authorizer: AclAuthorizer, aces: Set[AccessControlEntry], resourcePattern: ResourcePattern): Unit = {
    val bindings = aces.map { ace => new AclBinding(resourcePattern, ace) }
    authorizer.createAcls(requestContext, bindings.toList.asJava).asScala
      .map(_.toCompletableFuture.get)
      .foreach { result => result.exception.ifPresent { e => throw e } }
  }

  private def removeAcls(authorizer: AclAuthorizer, aces: Set[AccessControlEntry], resourcePattern: ResourcePattern): Boolean = {
    val bindings = if (aces.isEmpty)
      Set(new AclBindingFilter(resourcePattern.toFilter, AccessControlEntryFilter.ANY) )
    else
      aces.map { ace => new AclBinding(resourcePattern, ace).toFilter }
    authorizer.deleteAcls(requestContext, bindings.toList.asJava).asScala
      .map(_.toCompletableFuture.get)
      .forall { result =>
        result.exception.ifPresent { e => throw e }
        result.aclBindingDeleteResults.forEach { r =>
          r.exception.ifPresent { e => throw e }
        }
        !result.aclBindingDeleteResults.isEmpty
      }
  }

  private def getAcls(authorizer: AclAuthorizer, resourcePattern: ResourcePattern): Set[AccessControlEntry] = {
    val acls = authorizer.acls(new AclBindingFilter(resourcePattern.toFilter, AccessControlEntryFilter.ANY)).asScala.toSet
    acls.map(_.entry)
  }

  private def getAcls(authorizer: AclAuthorizer, principal: KafkaPrincipal): Set[AclBinding] = {
    val filter = new AclBindingFilter(ResourcePatternFilter.ANY,
      new AccessControlEntryFilter(principal.toString, null, AclOperation.ANY, AclPermissionType.ANY))
    authorizer.acls(filter).asScala.toSet
  }

  private def getAcls(authorizer: AclAuthorizer): Set[AclBinding] = {
    authorizer.acls(AclBindingFilter.ANY).asScala.toSet
  }

  private def validOp(op: AclOperation): Boolean = {
    op != AclOperation.ANY && op != AclOperation.UNKNOWN
  }

  private def prepareDefaultConfig(): String = {
    prepareConfig(Array("broker.id=1", "zookeeper.connect=somewhere"))
  }

  private def prepareConfig(lines : Array[String]): String = {
    val file = File.createTempFile("kafkatest", ".properties")
    file.deleteOnExit()

    val writer = Files.newOutputStream(file.toPath)
    try {
      lines.foreach { l =>
        writer.write(l.getBytes)
        writer.write("\n".getBytes)
      }
      file.getAbsolutePath
    } finally writer.close()
  }

  private def setupAuthorizerMetrics(): Unit = {
    val mBeanName = "kafka.server:type=kafka.security.authorizer.metrics"
    val server = ManagementFactory.getPlatformMBeanServer()
    val metrics = new Metrics
    aclAuthorizer.setupAuthorizerMetrics(metrics)
    val reporter = new JmxReporter("kafka.server")
    metrics.addReporter(reporter)
    assertTrue(server.isRegistered(new ObjectName(mBeanName)))
  }

  private def getAclAuthorizerMetric(attribute: String): Double = {
    val mBeanName = "kafka.server:type=kafka.security.authorizer.metrics"
    val name = new ObjectName(mBeanName)
    ManagementFactory.getPlatformMBeanServer().getAttribute(name, attribute).asInstanceOf[Double]
  }
}
