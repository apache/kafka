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

import kafka.server.{KafkaConfig, QuorumTestHarness}
import kafka.utils.TestUtils
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.common.resource.PatternType.{LITERAL, MATCH, PREFIXED}
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.{SecurityUtils => JSecurityUtils}
import org.apache.kafka.controller.MockAclMutator
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.metadata.authorizer.StandardAuthorizerTest.AuthorizerTestServerInfo
import org.apache.kafka.security.authorizer.AclEntry.{WILDCARD_HOST, WILDCARD_PRINCIPAL_STRING}
import org.apache.kafka.server.authorizer._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.net.InetAddress
import java.util
import java.util.{Collections, Properties, UUID}
import scala.jdk.CollectionConverters._

class AuthorizerTest extends QuorumTestHarness with BaseAuthorizerTest {

  private final val PLAINTEXT = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9020)
  private final val KRAFT = "kraft"

  private val allowReadAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, READ, ALLOW)
  private val allowWriteAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, WRITE, ALLOW)
  private val denyReadAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, READ, DENY)

  private val wildCardResource = new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL)
  private val prefixedResource = new ResourcePattern(TOPIC, "foo", PREFIXED)
  private val clusterResource = new ResourcePattern(CLUSTER, CLUSTER_NAME, LITERAL)
  private val wildcardPrincipal = JSecurityUtils.parseKafkaPrincipal(WILDCARD_PRINCIPAL_STRING)

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
    authorizer1 = createAuthorizer()
    configureAuthorizer(authorizer1, config.originals)
    authorizer2 = createAuthorizer()
    configureAuthorizer(authorizer2, config.originals)
    resource = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)
  }

  def properties: Properties = {
    val props = TestUtils.createBrokerConfig(0, null)
    props.put(StandardAuthorizer.SUPER_USERS_CONFIG, superUsers)
    props
  }

  @AfterEach
  override def tearDown(): Unit = {
    authorizer1.close()
    authorizer2.close()
    TestUtils.clearYammerMetrics()
    super.tearDown()
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAuthorizeThrowsOnNonLiteralResource(quorum: String): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => authorize(authorizer1, requestContext, READ,
      new ResourcePattern(TOPIC, "something", PREFIXED)))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAuthorizeWithEmptyResourceName(quorum: String): Unit = {
    assertFalse(authorize(authorizer1, requestContext, READ, new ResourcePattern(GROUP, "", LITERAL)))
    addAcls(authorizer1, Set(allowReadAcl), new ResourcePattern(GROUP, WILDCARD_RESOURCE, LITERAL))
    assertTrue(authorize(authorizer1, requestContext, READ, new ResourcePattern(GROUP, "", LITERAL)))
  }

  // Authorizing the empty resource is not supported because empty resource name is invalid.
  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testEmptyAclThrowsException(quorum: String): Unit = {
    assertThrows(classOf[ApiException],
      () => addAcls(authorizer1, Set(allowReadAcl), new ResourcePattern(GROUP, "", LITERAL)))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
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
    val acl5 = new AccessControlEntry(user1.toString, WILDCARD_HOST, DESCRIBE, ALLOW)

    //user2 has READ access from all hosts.
    val acl6 = new AccessControlEntry(user2.toString, WILDCARD_HOST, READ, ALLOW)

    //user3 has WRITE access from all hosts.
    val acl7 = new AccessControlEntry(user3.toString, WILDCARD_HOST, WRITE, ALLOW)

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
   * CustomPrincipals should be compared with their principal type and name
   */
  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
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

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testDenyTakesPrecedence(quorum: String): Unit = {
    val user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host = InetAddress.getByName("192.168.2.1")
    val session = newRequestContext(user, host)

    val allowAll = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, AclOperation.ALL, ALLOW)
    val denyAcl = new AccessControlEntry(user.toString, host.getHostAddress, AclOperation.ALL, DENY)
    val acls = Set(allowAll, denyAcl)

    changeAclAndVerify(Set.empty, acls, Set.empty)

    assertFalse(authorize(authorizer1, session, READ, resource), "deny should take precedence over allow.")
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAllowAllAccess(quorum: String): Unit = {
    val allowAllAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, AclOperation.ALL, ALLOW)

    changeAclAndVerify(Set.empty, Set(allowAllAcl), Set.empty)

    val context = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "random"), InetAddress.getByName("192.0.4.4"))
    assertTrue(authorize(authorizer1, context, READ, resource), "allow all acl should allow access to all.")
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testSuperUserHasAccess(quorum: String): Unit = {
    val denyAllAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, AclOperation.ALL, DENY)

    changeAclAndVerify(Set.empty, Set(denyAllAcl), Set.empty)

    val session1 = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))
    val session2 = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser2"), InetAddress.getByName("192.0.4.4"))

    assertTrue(authorize(authorizer1, session1, READ, resource), "superuser always has access, no matter what acls.")
    assertTrue(authorize(authorizer1, session2, READ, resource), "superuser always has access, no matter what acls.")
  }

  /**
   * CustomPrincipals should be compared with their principal type and name
   */
  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testSuperUserWithCustomPrincipalHasAccess(quorum: String): Unit = {
    val denyAllAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, AclOperation.ALL, DENY)
    changeAclAndVerify(Set.empty, Set(denyAllAcl), Set.empty)

    val session = newRequestContext(new CustomPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"))

    assertTrue(authorize(authorizer1, session, READ, resource), "superuser with custom principal always has access, no matter what acls.")
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
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

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testNoAclFound(quorum: String): Unit = {
    assertFalse(authorize(authorizer1, requestContext, READ, resource), "when acls = [], authorizer should deny op.")
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testNoAclFoundOverride(quorum: String): Unit = {
    val props = properties
    props.put(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true")

    val cfg = KafkaConfig.fromProps(props)
    val testAuthorizer = createAuthorizer()
    try {
      configureAuthorizer(testAuthorizer, cfg.originals)
      assertTrue(authorize(testAuthorizer, requestContext, READ, resource),
        "when acls = null or [],  authorizer should allow op with allow.everyone = true.")
    } finally {
      testAuthorizer.close()
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAclConfigWithWhitespace(quorum: String): Unit = {
    val props = properties
    props.put(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, " true")
    // replace all property values with leading & trailing whitespaces
    props.replaceAll((_, v) => " " + v + " ")
    val cfg = KafkaConfig.fromProps(props)
    val testAuthorizer = createAuthorizer()
    try {
      configureAuthorizer(testAuthorizer, cfg.originals)
      assertTrue(authorize(testAuthorizer, requestContext, READ, resource),
        "when acls = null or [],  authorizer should allow op with allow.everyone = true.")
    } finally {
      testAuthorizer.close()
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
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
    val acl5 = new AccessControlEntry(user2.toString, WILDCARD_HOST, READ, ALLOW)
    acls = changeAclAndVerify(acls, Set(acl5), Set.empty)

    //test get by principal name.
    TestUtils.waitUntilTrue(() => Set(acl1, acl2).map(acl => new AclBinding(resource, acl)) == getAcls(authorizer1, user1),
      "changes not propagated in timeout period")
    TestUtils.waitUntilTrue(() => Set(acl3, acl4, acl5).map(acl => new AclBinding(resource, acl)) == getAcls(authorizer1, user2),
      "changes not propagated in timeout period")

    val resourceToAcls = Map[ResourcePattern, Set[AccessControlEntry]](
      new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL) -> Set(new AccessControlEntry(user2.toString, WILDCARD_HOST, READ, ALLOW)),
      new ResourcePattern(CLUSTER, WILDCARD_RESOURCE, LITERAL) -> Set(new AccessControlEntry(user2.toString, host1, READ, ALLOW)),
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

    acls = changeAclAndVerify(Set.empty, Set(acl1), Set.empty)
    changeAclAndVerify(acls, Set.empty, acls)
  }

  @Test
  def testLocalConcurrentModificationOfResourceAcls(): Unit = {
    val commonResource = new ResourcePattern(TOPIC, "test", LITERAL)

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val acl1 = new AccessControlEntry(user1.toString, WILDCARD_HOST, READ, ALLOW)

    val user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob")
    val acl2 = new AccessControlEntry(user2.toString, WILDCARD_HOST, READ, DENY)

    addAcls(authorizer1, Set(acl1), commonResource)
    addAcls(authorizer1, Set(acl2), commonResource)

    TestUtils.waitAndVerifyAcls(Set(acl1, acl2), authorizer1, commonResource)
  }

  /**
   * Test ACL inheritance, as described in #{org.apache.kafka.common.acl.AclOperation}
   */
  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
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
    val acl = new AccessControlEntry(user.toString, WILDCARD_HOST, parentOp, ALLOW)
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
    val acls = Set(new AccessControlEntry(user1.toString, WILDCARD_HOST, parentOp, DENY),
      new AccessControlEntry(user1.toString, WILDCARD_HOST, AclOperation.ALL, ALLOW))
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

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAccessAllowedIfAllowAclExistsOnWildcardResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl), wildCardResource)

    assertTrue(authorize(authorizer1, requestContext, READ, resource))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testDeleteAclOnWildcardResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), wildCardResource)

    removeAcls(authorizer1, Set(allowReadAcl), wildCardResource)

    assertEquals(Set(allowWriteAcl), getAcls(authorizer1, wildCardResource))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testDeleteAllAclOnWildcardResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl), wildCardResource)

    removeAcls(authorizer1, Set.empty, wildCardResource)

    assertEquals(Set.empty, getAcls(authorizer1))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAccessAllowedIfAllowAclExistsOnPrefixedResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl), prefixedResource)

    assertTrue(authorize(authorizer1, requestContext, READ, resource))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testDeleteAclOnPrefixedResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), prefixedResource)

    removeAcls(authorizer1, Set(allowReadAcl), prefixedResource)

    assertEquals(Set(allowWriteAcl), getAcls(authorizer1, prefixedResource))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testDeleteAllAclOnPrefixedResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), prefixedResource)

    removeAcls(authorizer1, Set.empty, prefixedResource)

    assertEquals(Set.empty, getAcls(authorizer1))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAddAclsOnLiteralResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), resource)
    addAcls(authorizer1, Set(allowWriteAcl, denyReadAcl), resource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(authorizer1, resource))
    assertEquals(Set.empty, getAcls(authorizer1, wildCardResource))
    assertEquals(Set.empty, getAcls(authorizer1, prefixedResource))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAddAclsOnWildcardResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), wildCardResource)
    addAcls(authorizer1, Set(allowWriteAcl, denyReadAcl), wildCardResource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(authorizer1, wildCardResource))
    assertEquals(Set.empty, getAcls(authorizer1, resource))
    assertEquals(Set.empty, getAcls(authorizer1, prefixedResource))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAddAclsOnPrefixedResource(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl, allowWriteAcl), prefixedResource)
    addAcls(authorizer1, Set(allowWriteAcl, denyReadAcl), prefixedResource)

    assertEquals(Set(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(authorizer1, prefixedResource))
    assertEquals(Set.empty, getAcls(authorizer1, wildCardResource))
    assertEquals(Set.empty, getAcls(authorizer1, resource))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
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

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testSingleCharacterResourceAcls(quorum: String): Unit = {
    addAcls(authorizer1, Set(allowReadAcl), new ResourcePattern(TOPIC, "f", LITERAL))
    assertTrue(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "f", LITERAL)))
    assertFalse(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "foo", LITERAL)))

    addAcls(authorizer1, Set(allowReadAcl), new ResourcePattern(TOPIC, "_", PREFIXED))
    assertTrue(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "_foo", LITERAL)))
    assertTrue(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "_", LITERAL)))
    assertFalse(authorize(authorizer1, requestContext, READ, new ResourcePattern(TOPIC, "foo_", LITERAL)))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testGetAclsPrincipal(quorum: String): Unit = {
    val aclOnSpecificPrincipal = new AccessControlEntry(principal.toString, WILDCARD_HOST, WRITE, ALLOW)
    addAcls(authorizer1, Set(aclOnSpecificPrincipal), resource)

    assertEquals(0,
      getAcls(authorizer1, wildcardPrincipal).size, "acl on specific should not be returned for wildcard request")
    assertEquals(1,
      getAcls(authorizer1, principal).size, "acl on specific should be returned for specific request")
    assertEquals(1,
      getAcls(authorizer1, new KafkaPrincipal(principal.getPrincipalType, principal.getName)).size, "acl on specific should be returned for different principal instance")

    removeAcls(authorizer1, Set.empty, resource)
    val aclOnWildcardPrincipal = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, WRITE, ALLOW)
    addAcls(authorizer1, Set(aclOnWildcardPrincipal), resource)

    assertEquals(1, getAcls(authorizer1, wildcardPrincipal).size, "acl on wildcard should be returned for wildcard request")
    assertEquals(0, getAcls(authorizer1, principal).size, "acl on wildcard should not be returned for specific request")
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAclsFilter(quorum: String): Unit = {
    val resource1 = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)
    val resource2 = new ResourcePattern(TOPIC, "bar-" + UUID.randomUUID(), LITERAL)
    val prefixedResource = new ResourcePattern(TOPIC, "bar-", PREFIXED)

    val acl1 = new AclBinding(resource1, new AccessControlEntry(principal.toString, WILDCARD_HOST, READ, ALLOW))
    val acl2 = new AclBinding(resource1, new AccessControlEntry(principal.toString, "192.168.0.1", WRITE, ALLOW))
    val acl3 = new AclBinding(resource2, new AccessControlEntry(principal.toString, WILDCARD_HOST, DESCRIBE, ALLOW))
    val acl4 = new AclBinding(prefixedResource, new AccessControlEntry(wildcardPrincipal.toString, WILDCARD_HOST, READ, ALLOW))

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
    // standard authorizer first finds the acls that match filters and then delete them.
    // So filters[2] will match acl3 even though it is also matching filters[0] and will be deleted by it
    assertEquals(Set(acl3), deleteResults(2).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
    assertEquals(Set.empty, deleteResults(3).aclBindingDeleteResults.asScala.map(_.aclBinding).toSet)
  }

  @ParameterizedTest
  @ValueSource(strings = Array(KRAFT))
  def testAuthorizeByResourceTypeNoAclFoundOverride(quorum: String): Unit = {
    val props = properties
    props.put(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true")

    val cfg = KafkaConfig.fromProps(props)
    val authorizer: Authorizer = createAuthorizer()
    try {
      configureAuthorizer(authorizer, cfg.originals)
      assertTrue(authorizeByResourceType(authorizer, requestContext, READ, resource.resourceType()),
        "If allow.everyone.if.no.acl.found = true, caller should have read access to at least one topic")
      assertTrue(authorizeByResourceType(authorizer, requestContext, WRITE, resource.resourceType()),
        "If allow.everyone.if.no.acl.found = true, caller should have write access to at least one topic")
    } finally {
      authorizer.close()
    }
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
      acls --= removedAcls
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

  def createAuthorizer(): Authorizer = {
    new StandardAuthorizer
  }

  def configureAuthorizer(authorizer: Authorizer,
                          configs: util.Map[String, AnyRef]): Unit = {
    configureStandardAuthorizer(authorizer.asInstanceOf[StandardAuthorizer], configs)
  }

  def configureStandardAuthorizer(standardAuthorizer: StandardAuthorizer,
                                  configs: util.Map[String, AnyRef]): Unit = {
    standardAuthorizer.configure(configs)
    initializeStandardAuthorizer(standardAuthorizer, new AuthorizerTestServerInfo(Collections.singletonList(PLAINTEXT)))
  }

  def initializeStandardAuthorizer(standardAuthorizer: StandardAuthorizer,
                                   serverInfo: AuthorizerServerInfo): Unit = {
    val aclMutator = new MockAclMutator(standardAuthorizer)
    standardAuthorizer.start(serverInfo)
    standardAuthorizer.setAclMutator(aclMutator)
    standardAuthorizer.completeInitialLoad()
  }
}
