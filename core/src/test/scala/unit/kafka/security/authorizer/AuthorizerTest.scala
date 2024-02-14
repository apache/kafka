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

import kafka.Kafka
import kafka.security.authorizer.AclEntry.{WildcardHost, WildcardPrincipalString}
import kafka.server.{KafkaConfig, QuorumTestHarness}
import kafka.utils.{TestInfoUtils, TestUtils}
import kafka.zk.ZkAclStore
import kafka.zookeeper.{GetChildrenRequest, GetDataRequest, ZooKeeperClient}
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.errors.{ApiException, UnsupportedVersionException}
import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.common.resource.PatternType.{LITERAL, MATCH, PREFIXED}
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.{Time, SecurityUtils => JSecurityUtils}
import org.apache.kafka.controller.MockAclMutator
import org.apache.kafka.metadata.authorizer.StandardAuthorizerTest.AuthorizerTestServerInfo
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.server.authorizer._
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_2_0_IV0, IBP_2_0_IV1}
import org.apache.zookeeper.client.ZKClientConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util
import java.util.concurrent.{Executors, Semaphore, TimeUnit}
import java.util.{Collections, Properties, UUID}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class AuthorizerTest extends QuorumTestHarness with BaseAuthorizerTest {

  private final val PLAINTEXT = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9020)
  private final val KRAFT = "kraft"
  private final val ZK = "zk"


  private val allowReadAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, READ, ALLOW)
  private val allowWriteAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, WRITE, ALLOW)
  private val denyReadAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, READ, DENY)

  private val wildCardResource = new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL)
  private val prefixedResource = new ResourcePattern(TOPIC, "foo", PREFIXED)
  private val clusterResource = new ResourcePattern(CLUSTER, CLUSTER_NAME, LITERAL)
  private val wildcardPrincipal = JSecurityUtils.parseKafkaPrincipal(WildcardPrincipalString)

  private var authorizer1: Authorizer = _
  private var authorizer2: Authorizer = _

  private var _testInfo: TestInfo = _

  class CustomPrincipal(principalType: String, name: String) extends KafkaPrincipal(principalType, name) {
    override def equals(o: scala.Any): Boolean = false
  }

  override def authorizer: Authorizer = authorizer1

  def testInfo: TestInfo = _testInfo

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    _testInfo = testInfo

    val props = properties
    config = KafkaConfig.fromProps(props)
    authorizer1 = createAuthorizer(config.originals)
    authorizer2 = createAuthorizer(config.originals)
    resource = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)

    if (!TestInfoUtils.isKRaft(testInfo)) {
      zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests,
        Time.SYSTEM, "kafka.test", "AclAuthorizerTest", new ZKClientConfig, "AclAuthorizerTest")
      // Increase maxUpdateRetries to avoid transient failures
      authorizer1.asInstanceOf[AclAuthorizer].maxUpdateRetries = Int.MaxValue
      authorizer2.asInstanceOf[AclAuthorizer].maxUpdateRetries = Int.MaxValue
    }
  }

  def properties: Properties = {
    val props = TestUtils.createBrokerConfig(0, zkConnectOrNull)
    props.put(AclAuthorizer.SuperUsersProp, superUsers)
    props
  }

  @AfterEach
  override def tearDown(): Unit = {
    authorizer1.close()
    authorizer2.close()
    TestUtils.clearYammerMetrics()
    if (!TestInfoUtils.isKRaft(_testInfo)) {
      zooKeeperClient.close()
    }
    super.tearDown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAuthorizeThrowsOnNonLiteralResource(quorum: String): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => authorize(authorizer1, requestContext, READ,
      new ResourcePattern(TOPIC, "something", PREFIXED)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAuthorizeWithEmptyResourceName(quorum: String): Unit = {
    assertFalse(authorize(authorizer1, requestContext, READ, new ResourcePattern(GROUP, "", LITERAL)))
    addAcls(authorizer1, Set(allowReadAcl), new ResourcePattern(GROUP, WILDCARD_RESOURCE, LITERAL))
    assertTrue(authorize(authorizer1, requestContext, READ, new ResourcePattern(GROUP, "", LITERAL)))
  }

  // Authorizing the empty resource is not supported because we create a znode with the resource name.
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testEmptyAclThrowsException(quorum: String): Unit = {
    val e = assertThrows(classOf[ApiException],
      () => addAcls(authorizer1, Set(allowReadAcl), new ResourcePattern(GROUP, "", LITERAL)))
    if (quorum.equals(ZK))
      assertTrue(e.getCause.isInstanceOf[IllegalArgumentException], s"Unexpected exception $e")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testTopicAcl(quorum: String): Unit = {
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

    assertTrue(authorize(authorizer1, host2Context, READ, resource), "User1 should have READ access from host2")
    assertFalse(authorize(authorizer1, host1Context, READ, resource), "User1 should not have READ access from host1 due to denyAcl")
    assertTrue(authorize(authorizer1, host1Context, WRITE, resource), "User1 should have WRITE access from host1")
    assertFalse(authorize(authorizer1, host2Context, WRITE, resource), "User1 should not have WRITE access from host2 as no allow acl is defined")
    assertTrue(authorize(authorizer1, host1Context, DESCRIBE, resource), "User1 should not have DESCRIBE access from host1")
    assertTrue(authorize(authorizer1, host2Context, DESCRIBE, resource), "User1 should have DESCRIBE access from host2")
    assertFalse(authorize(authorizer1, host1Context, ALTER, resource), "User1 should not have edit access from host1")
    assertFalse(authorize(authorizer1, host2Context, ALTER, resource), "User1 should not have edit access from host2")

    //test if user has READ or WRITE access they also get DESCRIBE access
    val user2Context = newRequestContext(user2, host1)
    val user3Context = newRequestContext(user3, host1)
    assertTrue(authorize(authorizer1, user2Context, DESCRIBE, resource), "User2 should have DESCRIBE access from host1")
    assertTrue(authorize(authorizer1, user3Context, DESCRIBE, resource), "User3 should have DESCRIBE access from host2")
    assertTrue(authorize(authorizer1, user2Context, READ, resource), "User2 should have READ access from host1")
    assertTrue(authorize(authorizer1, user3Context, WRITE, resource), "User3 should have WRITE access from host2")
  }

  /**
    CustomPrincipals should be compared with their principal type and name
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAllowAccessWithCustomPrincipal(quorum: String): Unit = {
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

    assertTrue(authorize(authorizer1, host2Context, READ, resource), "User1 should have READ access from host2")
    assertFalse(authorize(authorizer1, host1Context, READ, resource), "User1 should not have READ access from host1 due to denyAcl")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testDenyTakesPrecedence(quorum: String): Unit = {
    val user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host = InetAddress.getByName("192.168.2.1")
    val session = newRequestContext(user, host)

    val allowAll = new AccessControlEntry(WildcardPrincipalString, WildcardHost, AclOperation.ALL, ALLOW)
    val denyAcl = new AccessControlEntry(user.toString, host.getHostAddress, AclOperation.ALL, DENY)
    val acls = Set(allowAll, denyAcl)

    changeAclAndVerify(Set.empty, acls, Set.empty)

    assertFalse(authorize(authorizer1, session, READ, resource), "deny should take precedence over allow.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAllowAllAccess(quorum: String): Unit = {
    val allowAllAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, AclOperation.ALL, ALLOW)

    changeAclAndVerify(Set.empty, Set(allowAllAcl), Set.empty)

    val context = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "random"), InetAddress.getByName("192.0.4.4"))
    assertTrue(authorize(authorizer1, context, READ, resource), "allow all acl should allow access to all.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testSuperUserHasAccess(quorum: String): Unit = {
    val denyAllAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, AclOperation.ALL, DENY)

    changeAclAndVerify(Set.empty, Set(denyAllAcl), Set.empty)

    val session1 = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))
    val session2 = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser2"), InetAddress.getByName("192.0.4.4"))

    assertTrue(authorize(authorizer1, session1, READ, resource), "superuser always has access, no matter what acls.")
    assertTrue(authorize(authorizer1, session2, READ, resource), "superuser always has access, no matter what acls.")
  }

  /**
    CustomPrincipals should be compared with their principal type and name
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testSuperUserWithCustomPrincipalHasAccess(quorum: String): Unit = {
    val denyAllAcl = new AccessControlEntry(WildcardPrincipalString, WildcardHost, AclOperation.ALL, DENY)
    changeAclAndVerify(Set.empty, Set(denyAllAcl), Set.empty)

    val session = newRequestContext(new CustomPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))

    assertTrue(authorize(authorizer1, session, READ, resource), "superuser with custom principal always has access, no matter what acls.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testWildCardAcls(quorum: String): Unit = {
    assertFalse(authorize(authorizer1, requestContext, READ, resource), "when acls = [], authorizer should fail close.")

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.3.1")
    val readAcl = new AccessControlEntry(user1.toString, host1.getHostAddress, READ, ALLOW)

    val acls = changeAclAndVerify(Set.empty, Set(readAcl), Set.empty, wildCardResource)

    val host1Context = newRequestContext(user1, host1)
    assertTrue(authorize(authorizer1, host1Context, READ, resource), "User1 should have READ access from host1")

    //allow WRITE to specific topic.
    val writeAcl = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, ALLOW)
    changeAclAndVerify(Set.empty, Set(writeAcl), Set.empty)

    //deny WRITE to wild card topic.
    val denyWriteOnWildCardResourceAcl = new AccessControlEntry(user1.toString, host1.getHostAddress, WRITE, DENY)
    changeAclAndVerify(acls, Set(denyWriteOnWildCardResourceAcl), Set.empty, wildCardResource)

    assertFalse(authorize(authorizer1, host1Context, WRITE, resource), "User1 should not have WRITE access from host1")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testNoAclFound(quorum: String): Unit = {
    assertFalse(authorize(authorizer1, requestContext, READ, resource), "when acls = [], authorizer should deny op.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testNoAclFoundOverride(quorum: String): Unit = {
    val props = properties
    props.put(AclAuthorizer.AllowEveryoneIfNoAclIsFoundProp, "true")

    val cfg = KafkaConfig.fromProps(props)
    var testAuthorizer: Authorizer = null
    try {
      testAuthorizer = createAuthorizer(cfg.originals)
      assertTrue(authorize(testAuthorizer, requestContext, READ, resource),
        "when acls = null or [],  authorizer should allow op with allow.everyone = true.")
    } finally {
      testAuthorizer.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAclConfigWithWhitespace(quorum: String): Unit = {
    val props = properties
    props.put(AclAuthorizer.AllowEveryoneIfNoAclIsFoundProp, " true")
    // replace all property values with leading & trailing whitespaces
    props.replaceAll((_,v) => " " + v + " ")
    val cfg = KafkaConfig.fromProps(props)
    var testAuthorizer: Authorizer = null
    try {
      testAuthorizer = createAuthorizer(cfg.originals)
      assertTrue(authorize(testAuthorizer, requestContext, READ, resource),
        "when acls = null or [],  authorizer should allow op with allow.everyone = true.")
    } finally {
      testAuthorizer.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAclManagementAPIs(quorum: String): Unit = {
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
    TestUtils.waitUntilTrue(() => Set(acl1, acl2).map(acl => new AclBinding(resource, acl)) == getAcls(authorizer1, user1),
      "changes not propagated in timeout period")
    TestUtils.waitUntilTrue(() => Set(acl3, acl4, acl5).map(acl => new AclBinding(resource, acl)) == getAcls(authorizer1, user2),
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
    TestUtils.waitUntilTrue(() => expectedAcls == getAcls(authorizer1), "changes not propagated in timeout period.")

    //test remove acl from existing acls.
    acls = changeAclAndVerify(acls, Set.empty, Set(acl1, acl5))

    //test remove all acls for resource
    removeAcls(authorizer1, Set.empty, resource)
    TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer1, resource)
    if (quorum.equals(ZK)) {
      assertFalse(zkClient.resourceExists(resource))
    }

    //test removing last acl also deletes ZooKeeper path
    acls = changeAclAndVerify(Set.empty, Set(acl1), Set.empty)
    changeAclAndVerify(acls, Set.empty, acls)
    if (quorum.equals(ZK)) {
      assertFalse(zkClient.resourceExists(resource))
    }
  }

  @Test
  def testLoadCache(): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new AccessControlEntry(user1.toString, "host-1", READ, ALLOW)
    val acls = Set(acl1)
    addAcls(authorizer1, acls, resource)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val resource1 = new ResourcePattern(TOPIC, "test-2", LITERAL)
    val acl2 = new AccessControlEntry(user2.toString, "host3", READ, DENY)
    val acls1 = Set(acl2)
    addAcls(authorizer1, acls1, resource1)

    zkClient.deleteAclChangeNotifications()
    var authorizer: Authorizer = null
    try {
      authorizer = createAclAuthorizer(config.originals)

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
      addAcls(authorizer1, acls, resource)

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

    addAcls(authorizer1, Set(acl1), commonResource)
    addAcls(authorizer1, Set(acl2), commonResource)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), authorizer1, commonResource)
  }

  @Test
  def testDistributedConcurrentModificationOfResourceAcls(): Unit = {
    val commonResource = new ResourcePattern(TOPIC, "test", LITERAL)

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new AccessControlEntry(user1.toString, WildcardHost, READ, ALLOW)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val acl2 = new AccessControlEntry(user2.toString, WildcardHost, READ, DENY)

    // Add on each instance
    addAcls(authorizer1, Set(acl1), commonResource)
    addAcls(authorizer2, Set(acl2), commonResource)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), authorizer1, commonResource)
    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), authorizer2, commonResource)

    val user3 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "joe")
    val acl3 = new AccessControlEntry(user3.toString, WildcardHost, READ, DENY)

    // Add on one instance and delete on another
    addAcls(authorizer1, Set(acl3), commonResource)
    val deleted = removeAcls(authorizer2, Set(acl3), commonResource)

    assertTrue(deleted, "The authorizer should see a value that needs to be deleted")

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), authorizer1, commonResource)
    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), authorizer2, commonResource)
  }

  @Test
  def testHighConcurrencyModificationOfResourceAcls(): Unit = {
    val commonResource = new ResourcePattern(TOPIC, "test", LITERAL)

    val acls= (0 to 50).map { i =>
      val useri = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, i.toString)
      (new AccessControlEntry(useri.toString, WildcardHost, READ, ALLOW), i)
    }

    // Alternate authorizer, Remove all acls that end in 0
    val concurrentFunctions = acls.map { case (acl, aclId) =>
      () => {
        if (aclId % 2 == 0) {
          addAcls(authorizer1, Set(acl), commonResource)
        } else {
          addAcls(authorizer2, Set(acl), commonResource)
        }
        if (aclId % 10 == 0) {
          removeAcls(authorizer2, Set(acl), commonResource)
        }
      }
    }

    val expectedAcls = acls.filter { case (acl, aclId) =>
      aclId % 10 != 0
    }.map(_._1).toSet

    TestUtils.assertConcurrent("Should support many concurrent calls", concurrentFunctions, 30 * 1000)

    TestUtils.waitAndVerifyAcls(expectedAcls, authorizer1, commonResource)
    TestUtils.waitAndVerifyAcls(expectedAcls, authorizer2, commonResource)
  }

  /**
    * Test ACL inheritance, as described in #{org.apache.kafka.common.acl.AclOperation}
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAclInheritance(quorum: String): Unit = {
    testImplicationsOfAllow(AclOperation.ALL, Set(READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE,
      CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, CREATE_TOKENS, DESCRIBE_TOKENS))
    testImplicationsOfDeny(AclOperation.ALL, Set(READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE,
      CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, CREATE_TOKENS, DESCRIBE_TOKENS))
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
    addAcls(authorizer1, Set(acl), clusterResource)
    AclOperation.values.filter(validOp).foreach { op =>
      val authorized = authorize(authorizer1, hostContext, op, clusterResource)
      if (allowedOps.contains(op) || op == parentOp)
        assertTrue(authorized, s"ALLOW $parentOp should imply ALLOW $op")
      else
        assertFalse(authorized, s"ALLOW $parentOp should not imply ALLOW $op")
    }
    removeAcls(authorizer1, Set(acl), clusterResource)
  }

  private def testImplicationsOfDeny(parentOp: AclOperation, deniedOps: Set[AclOperation]): Unit = {
    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.3.1")
    val host1Context = newRequestContext(user1, host1)
    val acls = Set(new AccessControlEntry(user1.toString, WildcardHost, parentOp, DENY),
      new AccessControlEntry(user1.toString, WildcardHost, AclOperation.ALL, ALLOW))
    addAcls(authorizer1, acls, clusterResource)
    AclOperation.values.filter(validOp).foreach { op =>
      val authorized = authorize(authorizer1, host1Context, op, clusterResource)
      if (deniedOps.contains(op) || op == parentOp)
        assertFalse(authorized, s"DENY $parentOp should imply DENY $op")
      else
        assertTrue(authorized, s"DENY $parentOp should not imply DENY $op")
    }
    removeAcls(authorizer1, acls, clusterResource)
  }

  @Test
  def testHighConcurrencyDeletionOfResourceAcls(): Unit = {
    val acl = new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username).toString, WildcardHost, AclOperation.ALL, ALLOW)

    // Alternate authorizer to keep adding and removing ZooKeeper path
    val concurrentFunctions = (0 to 50).map { _ =>
      () => {
        addAcls(authorizer1, Set(acl), resource)
        removeAcls(authorizer2, Set(acl), resource)
      }
    }

    TestUtils.assertConcurrent("Should support many concurrent calls", concurrentFunctions, 30 * 1000)

    TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer1, resource)
    TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer2, resource)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAccessAllowedIfAllowAclExistsOnWildcardResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl), wildCardResource)

    assertTrue(authorize(authorizer1, requestContext, READ, resource))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testDeleteAclOnWildcardResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), wildCardResource)

    removeAcls(authorizer1, Set(allowReadAcl), wildCardResource)

    assertEquals(Set(allowWriteAcl), getAcls(authorizer1, wildCardResource))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testDeleteAllAclOnWildcardResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl), wildCardResource)

    removeAcls(authorizer1, Set.empty, wildCardResource)

    assertEquals(Set.empty, getAcls(authorizer1))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAccessAllowedIfAllowAclExistsOnPrefixedResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl), prefixedResource)

    assertTrue(authorize(authorizer1, requestContext, READ, resource))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testDeleteAclOnPrefixedResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), prefixedResource)

    removeAcls(authorizer1, Set(allowReadAcl), prefixedResource)

    assertEquals(Set(allowWriteAcl), getAcls(authorizer1, prefixedResource))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testDeleteAllAclOnPrefixedResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), prefixedResource)

    removeAcls(authorizer1, Set.empty, prefixedResource)

    assertEquals(Set.empty, getAcls(authorizer1))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAddAclsOnLiteralResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), resource)
    addAcls(authorizer1, Set(allowWriteAcl, denyReadAcl), resource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(authorizer1, resource))
    assertEquals(Set.empty, getAcls(authorizer1, wildCardResource))
    assertEquals(Set.empty, getAcls(authorizer1, prefixedResource))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAddAclsOnWildcardResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), wildCardResource)
    addAcls(authorizer1, Set(allowWriteAcl, denyReadAcl), wildCardResource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(authorizer1, wildCardResource))
    assertEquals(Set.empty, getAcls(authorizer1, resource))
    assertEquals(Set.empty, getAcls(authorizer1, prefixedResource))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAddAclsOnPrefixedResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), prefixedResource)
    addAcls(authorizer1, Set(allowWriteAcl, denyReadAcl), prefixedResource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(authorizer1, prefixedResource))
    assertEquals(Set.empty, getAcls(authorizer1, wildCardResource))
    assertEquals(Set.empty, getAcls(authorizer1, resource))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAuthorizeWithPrefixedResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "a_other", LITERAL))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "a_other", PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID() + "-zzz", PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "fooo-" + UUID.randomUUID(), PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "fo-" + UUID.randomUUID(), PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "fop-" + UUID.randomUUID(), PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "fon-" + UUID.randomUUID(), PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "fon-", PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", PREFIXED))
    addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", LITERAL))

    addAcls(authorizer1, Set(allowReadAcl), prefixedResource)

    assertTrue(authorize(authorizer1, requestContext, READ, resource))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testSingleCharacterResourceAcls(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl), new ResourcePattern(TOPIC, "f", LITERAL))
    assertTrue(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "f", LITERAL)))
    assertFalse(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "foo", LITERAL)))

    addAcls(authorizer1, Set(allowReadAcl), new ResourcePattern(TOPIC, "_", PREFIXED))
    assertTrue(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "_foo", LITERAL)))
    assertTrue(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "_", LITERAL)))
    assertFalse(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "foo_", LITERAL)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testGetAclsPrincipal(quorum: String): Unit = {
    val aclOnSpecificPrincipal = new AccessControlEntry(principal.toString, WildcardHost, WRITE, ALLOW)
    addAcls(authorizer1, Set(aclOnSpecificPrincipal), resource)

    assertEquals(0,
      getAcls(authorizer1, wildcardPrincipal).size, "acl on specific should not be returned for wildcard request")
    assertEquals(1,
      getAcls(authorizer1, principal).size, "acl on specific should be returned for specific request")
    assertEquals(1,
      getAcls(authorizer1, new KafkaPrincipal(principal.getPrincipalType, principal.getName)).size, "acl on specific should be returned for different principal instance")

    removeAcls(authorizer1, Set.empty, resource)
    val aclOnWildcardPrincipal = new AccessControlEntry(WildcardPrincipalString, WildcardHost, WRITE, ALLOW)
    addAcls(authorizer1, Set(aclOnWildcardPrincipal), resource)

    assertEquals(1, getAcls(authorizer1, wildcardPrincipal).size, "acl on wildcard should be returned for wildcard request")
    assertEquals(0, getAcls(authorizer1, principal).size, "acl on wildcard should not be returned for specific request")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAclsFilter(quorum: String): Unit = {
    val resource1 = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)
    val resource2 = new ResourcePattern(TOPIC, "bar-" + UUID.randomUUID(), LITERAL)
    val prefixedResource = new ResourcePattern(TOPIC, "bar-", PREFIXED)

    val acl1 = new AclBinding(resource1, new AccessControlEntry(principal.toString, WildcardHost, READ, ALLOW))
    val acl2 = new AclBinding(resource1, new AccessControlEntry(principal.toString, "192.168.0.1", WRITE, ALLOW))
    val acl3 = new AclBinding(resource2, new AccessControlEntry(principal.toString, WildcardHost, DESCRIBE, ALLOW))
    val acl4 = new AclBinding(prefixedResource, new AccessControlEntry(wildcardPrincipal.toString, WildcardHost, READ, ALLOW))

    authorizer1.createAcls(requestContext, List(acl1, acl2, acl3, acl4).asJava)
    assertEquals(Set(acl1, acl2, acl3, acl4), authorizer1.acls(AclBindingFilter.ANY).asScala.toSet)
    assertEquals(Set(acl1, acl2), authorizer1.acls(new AclBindingFilter(resource1.toFilter, AccessControlEntryFilter.ANY)).asScala.toSet)
    assertEquals(Set(acl4), authorizer1.acls(new AclBindingFilter(prefixedResource.toFilter, AccessControlEntryFilter.ANY)).asScala.toSet)
    val matchingFilter = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, resource2.name, MATCH), AccessControlEntryFilter.ANY)
    assertEquals(Set(acl3, acl4), authorizer1.acls(matchingFilter).asScala.toSet)

    val filters = List(matchingFilter,
      acl1.toFilter,
      new AclBindingFilter(resource2.toFilter, AccessControlEntryFilter.ANY),
      new AclBindingFilter(new ResourcePatternFilter(TOPIC, "baz", PatternType.ANY), AccessControlEntryFilter.ANY))
    val deleteResults = authorizer1.deleteAcls(requestContext, filters.asJava).asScala.map(_.toCompletableFuture.get)
    assertEquals(List.empty, deleteResults.filter(_.exception.isPresent))
    filters.indices.foreach { i =>
      assertEquals(Set.empty, deleteResults(i).aclBindingDeleteResults.asScala.toSet.filter(_.exception.isPresent))
    }
    assertEquals(Set(acl3, acl4), deleteResults(0).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
    assertEquals(Set(acl1), deleteResults(1).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
    if (quorum.equals(ZK)) {
      assertEquals(Set.empty, deleteResults(2).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
    } else {
      // standard authorizer first finds the acls that match filters and then delete them.
      // So filters[2] will match acl3 even though it is also matching filters[0] and will be deleted by it
      assertEquals(Set(acl3), deleteResults(2).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
    }
    assertEquals(Set.empty, deleteResults(3).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
  }

  @Test
  def testThrowsOnAddPrefixedAclIfInterBrokerProtocolVersionTooLow(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(IBP_2_0_IV0))
    val e = assertThrows(classOf[ApiException],
      () => addAcls(authorizer1, Set(denyReadAcl), new ResourcePattern(TOPIC, "z_other", PREFIXED)))
    assertTrue(e.getCause.isInstanceOf[UnsupportedVersionException], s"Unexpected exception $e")
  }

  @Test
  def testCreateAclWithInvalidResourceName(): Unit = {
    assertThrows(classOf[ApiException],
      () => addAcls(authorizer1, Set(allowReadAcl), new ResourcePattern(TOPIC, "test/1", LITERAL)))
  }

  @Test
  def testWritesExtendedAclChangeEventIfInterBrokerProtocolNotSet(): Unit = {
    givenAuthorizerWithProtocolVersion(Option.empty)
    val resource = new ResourcePattern(TOPIC, "z_other", PREFIXED)
    val expected = new String(ZkAclStore(PREFIXED).changeStore
      .createChangeNode(resource).bytes, UTF_8)

    addAcls(authorizer1, Set(denyReadAcl), resource)

    val actual = getAclChangeEventAsString(PREFIXED)

    assertEquals(expected, actual)
  }

  @Test
  def testWritesExtendedAclChangeEventWhenInterBrokerProtocolAtLeastKafkaV2(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(IBP_2_0_IV1))
    val resource = new ResourcePattern(TOPIC, "z_other", PREFIXED)
    val expected = new String(ZkAclStore(PREFIXED).changeStore
      .createChangeNode(resource).bytes, UTF_8)

    addAcls(authorizer1, Set(denyReadAcl), resource)

    val actual = getAclChangeEventAsString(PREFIXED)

    assertEquals(expected, actual)
  }

  @Test
  def testWritesLiteralWritesLiteralAclChangeEventWhenInterBrokerProtocolLessThanKafkaV2eralAclChangesForOlderProtocolVersions(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(IBP_2_0_IV0))
    val resource = new ResourcePattern(TOPIC, "z_other", LITERAL)
    val expected = new String(ZkAclStore(LITERAL).changeStore
      .createChangeNode(resource).bytes, UTF_8)

    addAcls(authorizer1, Set(denyReadAcl), resource)

    val actual = getAclChangeEventAsString(LITERAL)

    assertEquals(expected, actual)
  }

  @Test
  def testWritesLiteralAclChangeEventWhenInterBrokerProtocolIsKafkaV2(): Unit = {
    givenAuthorizerWithProtocolVersion(Option(IBP_2_0_IV1))
    val resource = new ResourcePattern(TOPIC, "z_other", LITERAL)
    val expected = new String(ZkAclStore(LITERAL).changeStore
      .createChangeNode(resource).bytes, UTF_8)

    addAcls(authorizer1, Set(denyReadAcl), resource)

    val actual = getAclChangeEventAsString(LITERAL)

    assertEquals(expected, actual)
  }

  @Test
  def testAuthorizerNoZkConfig(): Unit = {
    val noTlsProps = Kafka.getPropsFromArgs(Array(prepareDefaultConfig))
    val zkClientConfig = AclAuthorizer.zkClientConfigFromKafkaConfigAndMap(
      KafkaConfig.fromProps(noTlsProps),
      noTlsProps.asInstanceOf[java.util.Map[String, Any]].asScala)
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach { propName =>
      assertNull(zkClientConfig.getProperty(propName))
    }
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
    configs.foreach { case (key, value) => props.put(key, value) }

    val zkClientConfig = AclAuthorizer.zkClientConfigFromKafkaConfigAndMap(
      KafkaConfig.fromProps(props), mutable.Map(configs.toSeq: _*))
    // confirm we get all the values we expect
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(prop => prop match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp =>
        assertEquals("true", KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
      case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp =>
        assertEquals("false", KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
      case KafkaConfig.ZkSslProtocolProp =>
        assertEquals("TLSv1.2", KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
      case _ => assertEquals(kafkaValue, KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
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
    // confirm we get all the values we expect
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(prop => prop match {
        case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp =>
          assertEquals("true", KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
        case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp =>
          assertEquals("false", KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
        case _ => assertEquals(kafkaValue, KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
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
    // confirm we get all the values we expect
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(prop => prop match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp =>
        assertEquals("true", KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp =>
        assertEquals("false", KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
      case _ => assertEquals(prefixedValue, KafkaConfig.zooKeeperClientProperty(zkClientConfig, prop).getOrElse("<None>"))
    })
  }

  @Test
  def testCreateDeleteTiming(): Unit = {
    val literalResource = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)
    val prefixedResource = new ResourcePattern(TOPIC, "bar-", PREFIXED)
    val wildcardResource = new ResourcePattern(TOPIC, "*", LITERAL)
    val ace = new AccessControlEntry(principal.toString, WildcardHost, READ, ALLOW)
    val updateSemaphore = new Semaphore(1)

    def createAcl(createAuthorizer: Authorizer, resource: ResourcePattern): AclBinding = {
      val acl = new AclBinding(resource, ace)
      createAuthorizer.createAcls(requestContext, Collections.singletonList(acl)).asScala
        .foreach(_.toCompletableFuture.get(15, TimeUnit.SECONDS))
      acl
    }

    def deleteAcl(deleteAuthorizer: Authorizer,
                  resource: ResourcePattern,
                  deletePatternType: PatternType): List[AclBinding] = {

      val filter = new AclBindingFilter(
        new ResourcePatternFilter(resource.resourceType(), resource.name(), deletePatternType),
        AccessControlEntryFilter.ANY)
      deleteAuthorizer.deleteAcls(requestContext, Collections.singletonList(filter)).asScala
        .map(_.toCompletableFuture.get(15, TimeUnit.SECONDS))
        .flatMap(_.aclBindingDeleteResults.asScala)
        .map(_.aclBinding)
        .toList
    }

    def listAcls(authorizer: Authorizer): List[AclBinding] = {
      authorizer.acls(AclBindingFilter.ANY).asScala.toList
    }

    def verifyCreateDeleteAcl(deleteAuthorizer: Authorizer,
                              resource: ResourcePattern,
                              deletePatternType: PatternType): Unit = {
      updateSemaphore.acquire()
      assertEquals(List.empty, listAcls(deleteAuthorizer))
      val acl = createAcl(authorizer1, resource)
      val deleted = deleteAcl(deleteAuthorizer, resource, deletePatternType)
      if (deletePatternType != PatternType.MATCH) {
        assertEquals(List(acl), deleted)
      } else {
        assertEquals(List.empty[AclBinding], deleted)
      }
      updateSemaphore.release()
      if (deletePatternType == PatternType.MATCH) {
        TestUtils.waitUntilTrue(() => listAcls(deleteAuthorizer).nonEmpty, "ACL not propagated")
        assertEquals(List(acl), deleteAcl(deleteAuthorizer, resource, deletePatternType))
      }
      TestUtils.waitUntilTrue(() => listAcls(deleteAuthorizer).isEmpty, "ACL delete not propagated")
    }

    val deleteAuthorizer = new AclAuthorizer {
      override def processAclChangeNotification(resource: ResourcePattern): Unit = {
        updateSemaphore.acquire()
        try {
          super.processAclChangeNotification(resource)
        } finally {
          updateSemaphore.release()
        }
      }
    }

    try {
      deleteAuthorizer.configure(config.originals)
      List(literalResource, prefixedResource, wildcardResource).foreach { resource =>
        verifyCreateDeleteAcl(deleteAuthorizer, resource, resource.patternType())
        verifyCreateDeleteAcl(deleteAuthorizer, resource, PatternType.ANY)
        verifyCreateDeleteAcl(deleteAuthorizer, resource, PatternType.MATCH)
      }
    } finally {
      deleteAuthorizer.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array(KRAFT, ZK))
  def testAuthorizeByResourceTypeNoAclFoundOverride(quorum: String): Unit = {
    val props = properties
    props.put(AclAuthorizer.AllowEveryoneIfNoAclIsFoundProp, "true")

    val cfg = KafkaConfig.fromProps(props)
    var authorizer: Authorizer = null
    try {
      authorizer = createAuthorizer(cfg.originals)
      assertTrue(authorizeByResourceType(authorizer, requestContext, READ, resource.resourceType()),
        "If allow.everyone.if.no.acl.found = true, caller should have read access to at least one topic")
      assertTrue(authorizeByResourceType(authorizer, requestContext, WRITE, resource.resourceType()),
        "If allow.everyone.if.no.acl.found = true, caller should have write access to at least one topic")
    } finally {
      authorizer.close()
    }
  }

  private def givenAuthorizerWithProtocolVersion(protocolVersion: Option[MetadataVersion]): Unit = {
    authorizer1.close()

    val props = TestUtils.createBrokerConfig(0, zkConnectOrNull)
    props.put(AclAuthorizer.SuperUsersProp, superUsers)
    protocolVersion.foreach(version => props.put(KafkaConfig.InterBrokerProtocolVersionProp, version.toString))

    config = KafkaConfig.fromProps(props)

    authorizer1.configure(config.originals)
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

  private def changeAclAndVerify(originalAcls: Set[AccessControlEntry],
                                 addedAcls: Set[AccessControlEntry],
                                 removedAcls: Set[AccessControlEntry],
                                 resource: ResourcePattern = resource): Set[AccessControlEntry] = {
    var acls = originalAcls

    if (addedAcls.nonEmpty) {
      addAcls(authorizer1, addedAcls, resource)
      acls ++= addedAcls
    }

    if (removedAcls.nonEmpty) {
      removeAcls(authorizer1, removedAcls, resource)
      acls --=removedAcls
    }

    TestUtils.waitAndVerifyAcls(acls, authorizer1, resource)

    acls
  }

  private def authorize(authorizer: Authorizer, requestContext: RequestContext, operation: AclOperation, resource: ResourcePattern): Boolean = {
    val action = new Action(operation, resource, 1, true, true)
    authorizer.authorize(requestContext, List(action).asJava).asScala.head == AuthorizationResult.ALLOWED
  }

  private def getAcls(authorizer: Authorizer, resourcePattern: ResourcePattern): Set[AccessControlEntry] = {
    val acls = authorizer.acls(new AclBindingFilter(resourcePattern.toFilter, AccessControlEntryFilter.ANY)).asScala.toSet
    acls.map(_.entry)
  }

  private def getAcls(authorizer: Authorizer, principal: KafkaPrincipal): Set[AclBinding] = {
    val filter = new AclBindingFilter(ResourcePatternFilter.ANY,
      new AccessControlEntryFilter(principal.toString, null, AclOperation.ANY, AclPermissionType.ANY))
    authorizer.acls(filter).asScala.toSet
  }

  private def getAcls(authorizer: Authorizer): Set[AclBinding] = {
    authorizer.acls(AclBindingFilter.ANY).asScala.toSet
  }

  private def validOp(op: AclOperation): Boolean = {
    op != AclOperation.ANY && op != AclOperation.UNKNOWN
  }

  private def prepareDefaultConfig: String =
    prepareConfig(Array("broker.id=1", "zookeeper.connect=somewhere"))

  private def prepareConfig(lines : Array[String]): String = {
    val file = TestUtils.tempFile("kafkatest", ".properties")

    val writer = Files.newOutputStream(file.toPath)
    try {
      lines.foreach { l =>
        writer.write(l.getBytes)
        writer.write("\n".getBytes)
      }
      file.getAbsolutePath
    } finally writer.close()
  }

  def createAuthorizer(configs: util.Map[String, AnyRef]): Authorizer = {
    var testAuthorizer: Authorizer = null
    if (TestInfoUtils.isKRaft(_testInfo)) {
      testAuthorizer = createStandardAuthorizer(configs)
    } else {
      testAuthorizer = createAclAuthorizer(configs)
    }
    testAuthorizer
  }

  def createAclAuthorizer(configs: util.Map[String, AnyRef]): AclAuthorizer = {
    val authorizer = new AclAuthorizer
    authorizer.configure(configs)
    authorizer
  }

  def createStandardAuthorizer(configs: util.Map[String, AnyRef]): StandardAuthorizer = {
    val standardAuthorizer = new StandardAuthorizer
    standardAuthorizer.configure(configs)
    initializeStandardAuthorizer(standardAuthorizer, new AuthorizerTestServerInfo(Collections.singletonList(PLAINTEXT)))
    standardAuthorizer
  }

  def initializeStandardAuthorizer(standardAuthorizer: StandardAuthorizer,
                                   serverInfo: AuthorizerServerInfo): Unit = {
    val aclMutator = new MockAclMutator(standardAuthorizer)
    standardAuthorizer.start(serverInfo)
    standardAuthorizer.setAclMutator(aclMutator)
    standardAuthorizer.completeInitialLoad()
  }
}
