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

import kafka.security.auth.SimpleAclAuthorizer
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl._
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.authorizer._
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class AuthorizerWrapperTest extends ZooKeeperTestHarness {
  @nowarn("cat=deprecation")
  private val wrappedSimpleAuthorizer = new AuthorizerWrapper(new SimpleAclAuthorizer)
  @nowarn("cat=deprecation")
  private val wrappedSimpleAuthorizerAllowEveryone = new AuthorizerWrapper(new SimpleAclAuthorizer)
  private var resource: ResourcePattern = _
  private val superUsers = "User:superuser1; User:superuser2"
  private val username = "alice"
  private val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
  private val requestContext = newRequestContext(principal, InetAddress.getByName("192.168.0.1"))
  private var config: KafkaConfig = _
  private var zooKeeperClient: ZooKeeperClient = _

  private val aclAdded: ArrayBuffer[(Authorizer, Set[AccessControlEntry], ResourcePattern)] = ArrayBuffer()
  private val authorizerTestFactory = new AuthorizerTestFactory(
    newRequestContext, addAcls, authorizeByResourceType, removeAcls)

  class CustomPrincipal(principalType: String, name: String) extends KafkaPrincipal(principalType, name) {
    override def equals(o: scala.Any): Boolean = false
  }

  @Before
  @nowarn("cat=deprecation")
  override def setUp(): Unit = {
    super.setUp()

    val props = TestUtils.createBrokerConfig(0, zkConnect)

    props.put(AclAuthorizer.SuperUsersProp, superUsers)
    config = KafkaConfig.fromProps(props)
    wrappedSimpleAuthorizer.configure(config.originals)

    props.put(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp, "true")
    config = KafkaConfig.fromProps(props)
    wrappedSimpleAuthorizerAllowEveryone.configure(config.originals)

    resource = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)
    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests,
      Time.SYSTEM, "kafka.test", "AclAuthorizerTest")
  }

  @After
  override def tearDown(): Unit = {
    val authorizers = Seq(wrappedSimpleAuthorizer, wrappedSimpleAuthorizerAllowEveryone)
    authorizers.foreach(a => {
      a.acls(AclBindingFilter.ANY).forEach(bd => {
        removeAcls(wrappedSimpleAuthorizer, Set(bd.entry), bd.pattern())
      })
    })
    authorizers.foreach(a => {
      a.close()
    })
    zooKeeperClient.close()
    super.tearDown()
  }

  @Test
  def testAuthorizeByResourceTypeMultipleAddAndRemove(): Unit = {
    authorizerTestFactory.testAuthorizeByResourceTypeMultipleAddAndRemove(wrappedSimpleAuthorizer)
  }

  @Test
  def testAuthorizeByResourceTypeIsolationUnrelatedDenyWontDominateAllow(): Unit = {
    authorizerTestFactory.testAuthorizeByResourceTypeIsolationUnrelatedDenyWontDominateAllow(wrappedSimpleAuthorizer)
  }

  @Test
  def testAuthorizeByResourceTypeDenyTakesPrecedence(): Unit = {
    authorizerTestFactory.testAuthorizeByResourceTypeDenyTakesPrecedence(wrappedSimpleAuthorizer)
  }

  @Test
  def testAuthorizeByResourceTypeWildcardResourceDenyDominate(): Unit = {
    authorizerTestFactory.testAuthorizeByResourceTypeWildcardResourceDenyDominate(wrappedSimpleAuthorizer)
  }

  @Test
  def testAuthorizeByResourceTypePrefixedResourceDenyDominate(): Unit = {
    authorizerTestFactory.testAuthorizeByResourceTypePrefixedResourceDenyDominate(wrappedSimpleAuthorizer)
  }

  @Test
  def testAuthorizeByResourceTypeWithAllOperationAce(): Unit = {
    authorizerTestFactory.testAuthorizeByResourceTypeWithAllOperationAce(wrappedSimpleAuthorizer)
  }

  @Test
  def testAuthorizeByResourceTypeWithAllHostAce(): Unit = {
    authorizerTestFactory.testAuthorizeByResourceTypeWithAllHostAce(wrappedSimpleAuthorizer)
  }

  @Test
  def testAuthorizeByResourceTypeWithAllPrincipalAce(): Unit = {
    authorizerTestFactory.testAuthorizeByResourceTypeWithAllPrincipalAce(wrappedSimpleAuthorizer)
  }

  @Test
  def testAuthorizeByResourceTypeEnableAllowEveryOne(): Unit = {
    testAuthorizeByResourceTypeEnableAllowEveryOne(wrappedSimpleAuthorizer)
  }

  private def testAuthorizeByResourceTypeEnableAllowEveryOne(authorizer: Authorizer): Unit = {
    assertTrue("If allow.everyone.if.no.acl.found = true, " +
      "caller should have read access to at least one topic",
      authorizeByResourceType(wrappedSimpleAuthorizerAllowEveryone, requestContext, READ, resource.resourceType()))
    val allUser = AclEntry.WildcardPrincipalString
    val allHost = AclEntry.WildcardHost
    val denyAll = new AccessControlEntry(allUser, allHost, READ, AclPermissionType.DENY)
    val wildcardResource = new ResourcePattern(resource.resourceType(), AclEntry.WildcardResource, LITERAL)

    addAcls(wrappedSimpleAuthorizerAllowEveryone, Set(denyAll), resource)
    assertTrue("Should still allow since the deny only apply on the specific resource",
      authorizeByResourceType(wrappedSimpleAuthorizerAllowEveryone, requestContext, READ, resource.resourceType()))

    addAcls(wrappedSimpleAuthorizerAllowEveryone, Set(denyAll), wildcardResource)
    assertFalse("When an ACL binding which can deny all users and hosts exists, " +
      "even if allow.everyone.if.no.acl.found = true, caller shouldn't have read access any topic",
      authorizeByResourceType(wrappedSimpleAuthorizerAllowEveryone, requestContext, READ, resource.resourceType()))
  }

  @Test
  def testAuthorizeByResourceTypeDisableAllowEveryoneOverride(): Unit = {
    testAuthorizeByResourceTypeDisableAllowEveryoneOverride(wrappedSimpleAuthorizer)
  }

  private def testAuthorizeByResourceTypeDisableAllowEveryoneOverride(authorizer: Authorizer): Unit = {
    assertFalse ("If allow.everyone.if.no.acl.found = false, " +
      "caller shouldn't have read access to any topic",
      authorizeByResourceType(wrappedSimpleAuthorizer, requestContext, READ, resource.resourceType()))
  }

  private def newRequestContext(principal: KafkaPrincipal, clientAddress: InetAddress, apiKey: ApiKeys = ApiKeys.PRODUCE): RequestContext = {
    val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
    val header = new RequestHeader(apiKey, 2, "", 1) //ApiKeys apiKey, short version, String clientId, int correlation
    new RequestContext(header, "", clientAddress, principal, ListenerName.forSecurityProtocol(securityProtocol),
      securityProtocol, ClientInformation.EMPTY, false)
  }

  private def authorizeByResourceType(authorizer: Authorizer, requestContext: RequestContext, operation: AclOperation, resourceType: ResourceType) : Boolean = {
    authorizer.authorizeByResourceType(requestContext, operation, resourceType) == AuthorizationResult.ALLOWED
  }

  private def addAcls(authorizer: Authorizer, aces: Set[AccessControlEntry], resourcePattern: ResourcePattern): Unit = {
    val bindings = aces.map { ace => new AclBinding(resourcePattern, ace) }
    authorizer.createAcls(requestContext, bindings.toList.asJava).asScala
      .map(_.toCompletableFuture.get)
      .foreach { result => result.exception.ifPresent { e => throw e } }
    aclAdded += Tuple3(authorizer, aces, resourcePattern)
  }

  private def removeAcls(authorizer: Authorizer, aces: Set[AccessControlEntry], resourcePattern: ResourcePattern): Boolean = {
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

}
