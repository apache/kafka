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

import java.util.UUID

import kafka.security.auth.SimpleAclAuthorizer
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.authorizer._
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.annotation.nowarn

class AuthorizerWrapperTest extends ZooKeeperTestHarness with BaseAuthorizerTest {
  @nowarn("cat=deprecation")
  private val wrappedSimpleAuthorizer = new AuthorizerWrapper(new SimpleAclAuthorizer)
  @nowarn("cat=deprecation")
  private val wrappedSimpleAuthorizerAllowEveryone = new AuthorizerWrapper(new SimpleAclAuthorizer)

  override def authorizer: Authorizer = wrappedSimpleAuthorizer

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
      Time.SYSTEM, "kafka.test", "AuthorizerWrapperTest")
  }

  @After
  override def tearDown(): Unit = {
    val authorizers = Seq(wrappedSimpleAuthorizer, wrappedSimpleAuthorizerAllowEveryone)
    authorizers.foreach(a => {
      a.close()
    })
    zooKeeperClient.close()
    super.tearDown()
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
    val denyAll = new AccessControlEntry(allUser, allHost, ALL, AclPermissionType.DENY)
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
    assertFalse ("If allow.everyone.if.no.acl.found = false, " +
      "caller shouldn't have read access to any topic",
      authorizeByResourceType(wrappedSimpleAuthorizer, requestContext, READ, resource.resourceType()))
  }
}
