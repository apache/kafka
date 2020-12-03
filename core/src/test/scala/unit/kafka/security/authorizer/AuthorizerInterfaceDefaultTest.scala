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

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.common.acl._
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.resource.{ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.authorizer._
import org.junit.{After, Before, Test}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class AuthorizerInterfaceDefaultTest extends ZooKeeperTestHarness {

  private val interfaceDefaultAuthorizer = new MockAuthorizer
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
  override def setUp(): Unit = {
    super.setUp()

    val authorizers = Seq(interfaceDefaultAuthorizer.authorizer)

    // Increase maxUpdateRetries to avoid transient failures
    authorizers.foreach(a => a.maxUpdateRetries = Int.MaxValue)

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(AclAuthorizer.SuperUsersProp, superUsers)

    config = KafkaConfig.fromProps(props)
    authorizers.foreach(a => a.configure(config.originals))

    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests,
      Time.SYSTEM, "kafka.test", "AclAuthorizerTest")
  }

  @After
  override def tearDown(): Unit = {
    val authorizers = Seq(interfaceDefaultAuthorizer)
    authorizers.foreach(a => {
      a.acls(AclBindingFilter.ANY).forEach(bd => {
        removeAcls(interfaceDefaultAuthorizer, Set(bd.entry), bd.pattern())
      })
    })
    authorizers.foreach(a => {
      a.close()
    })
    zooKeeperClient.close()
    super.tearDown()
  }

  @Test
  def testAuthorizeAnyMultipleAddAndRemoveInterfaceDefault(): Unit = {
    authorizerTestFactory.testAuthorizeAnyMultipleAddAndRemove(interfaceDefaultAuthorizer)
  }

  @Test
  def testAuthorizeAnyUnrelatedDenyWontDominateAllowInterfaceDefault(): Unit = {
    authorizerTestFactory.testAuthorizeAnyIsolationUnrelatedDenyWontDominateAllow(interfaceDefaultAuthorizer)
  }

  @Test
  def testAuthorizeAnyDenyTakesPrecedenceInterfaceDefault(): Unit = {
    authorizerTestFactory.testAuthorizeAnyDenyTakesPrecedence(interfaceDefaultAuthorizer)
  }

  @Test
  def testAuthorizeAnyWildcardResourceDenyDominateInterfaceDefault(): Unit = {
    authorizerTestFactory.testAuthorizeAnyWildcardResourceDenyDominate(interfaceDefaultAuthorizer)
  }

  @Test
  def testAuthorizeAnyPrefixedDenyDominateInterfaceDefault(): Unit = {
    authorizerTestFactory.testAuthorizeAnyPrefixedResourceDenyDominate(interfaceDefaultAuthorizer)
  }

  @Test
  def testAuthorizeAnyWithAllOperationAceInterfaceDefault(): Unit = {
    authorizerTestFactory.testAuthorizeAnyWithAllOperationAce(interfaceDefaultAuthorizer)
  }

  @Test
  def testAuthorizeAnyWithAllHostAceInterfaceDefault(): Unit = {
    authorizerTestFactory.testAuthorizeAnyWithAllHostAce(interfaceDefaultAuthorizer)
  }

  @Test
  def testAuthorizeAnyWithAllPrincipalAceInterfaceDefault(): Unit = {
    authorizerTestFactory.testAuthorizeAnyWithAllPrincipalAce(interfaceDefaultAuthorizer)
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
