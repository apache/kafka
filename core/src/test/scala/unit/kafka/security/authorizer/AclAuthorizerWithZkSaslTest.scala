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
import java.util
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import kafka.api.SaslSetup
import kafka.server.{KafkaConfig, QuorumTestHarness}
import kafka.utils.JaasTestUtils.{JaasModule, JaasSection}
import kafka.utils.{JaasTestUtils, TestUtils}
import kafka.zk.KafkaZkClient
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter}
import org.apache.kafka.common.acl.AclOperation.{READ, WRITE}
import org.apache.kafka.common.acl.AclPermissionType.ALLOW
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType.TOPIC
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST
import org.apache.zookeeper.server.auth.DigestLoginModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import scala.jdk.CollectionConverters._
import scala.collection.Seq

class AclAuthorizerWithZkSaslTest extends QuorumTestHarness with SaslSetup {

  private val aclAuthorizer = new AclAuthorizer
  private val aclAuthorizer2 = new AclAuthorizer
  private val resource: ResourcePattern = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)
  private val username = "alice"
  private val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
  private val requestContext = newRequestContext(principal, InetAddress.getByName("192.168.0.1"))
  private val executor = Executors.newSingleThreadScheduledExecutor
  private var config: KafkaConfig = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    // Allow failed clients to avoid server closing the connection before reporting AuthFailed.
    System.setProperty("zookeeper.allowSaslFailedClients", "true")

    // Configure ZK SASL with TestableDigestLoginModule for clients to inject failures
    TestableDigestLoginModule.reset()
    val jaasSections = JaasTestUtils.zkSections
    val serverJaas = jaasSections.filter(_.contextName == "Server")
    val clientJaas = jaasSections.filter(_.contextName == "Client")
      .map(section => new TestableJaasSection(section.contextName, section.modules))
    startSasl(serverJaas ++ clientJaas)

    // Increase maxUpdateRetries to avoid transient failures
    aclAuthorizer.maxUpdateRetries = Int.MaxValue
    aclAuthorizer2.maxUpdateRetries = Int.MaxValue

    super.setUp(testInfo)
    config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect))

    aclAuthorizer.configure(config.originals)
    aclAuthorizer2.configure(config.originals)
  }

  @AfterEach
  override def tearDown(): Unit = {
    System.clearProperty("zookeeper.allowSaslFailedClients")
    TestableDigestLoginModule.reset()
    executor.shutdownNow()
    aclAuthorizer.close()
    aclAuthorizer2.close()
    super.tearDown()
    closeSasl()
  }

  @Test
  def testAclUpdateWithSessionExpiration(): Unit = {
    zkClient(aclAuthorizer).currentZooKeeper.getTestable.injectSessionExpiration()
    zkClient(aclAuthorizer2).currentZooKeeper.getTestable.injectSessionExpiration()
    verifyAclUpdate()
  }

  @Test
  def testAclUpdateWithAuthFailure(): Unit = {
    injectTransientAuthenticationFailure()
    verifyAclUpdate()
  }

  private def injectTransientAuthenticationFailure(): Unit = {
    TestableDigestLoginModule.injectInvalidCredentials()
    zkClient(aclAuthorizer).currentZooKeeper.getTestable.injectSessionExpiration()
    zkClient(aclAuthorizer2).currentZooKeeper.getTestable.injectSessionExpiration()
    executor.schedule((() => TestableDigestLoginModule.reset()): Runnable,
      ZooKeeperClient.RetryBackoffMs * 2, TimeUnit.MILLISECONDS)
  }

  private def verifyAclUpdate(): Unit = {
    val allowReadAcl = new AccessControlEntry(principal.toString, WILDCARD_HOST, READ, ALLOW)
    val allowWriteAcl = new AccessControlEntry(principal.toString, WILDCARD_HOST, WRITE, ALLOW)
    val acls = Set(allowReadAcl, allowWriteAcl)

    TestUtils.retry(maxWaitMs = 15000) {
      try {
        addAcls(aclAuthorizer, acls, resource)
      } catch {
        case _: Exception => // Ignore error and retry
      }
      assertEquals(acls, getAcls(aclAuthorizer, resource))
    }
    val (acls2, _) = TestUtils.computeUntilTrue(getAcls(aclAuthorizer2, resource)) { _ == acls }
    assertEquals(acls, acls2)
  }

  private def zkClient(authorizer: AclAuthorizer): KafkaZkClient = {
    JTestUtils.fieldValue(authorizer, classOf[AclAuthorizer], "zkClient")
  }

  private def addAcls(authorizer: AclAuthorizer, aces: Set[AccessControlEntry], resourcePattern: ResourcePattern): Unit = {
    val bindings = aces.map { ace => new AclBinding(resourcePattern, ace) }
    authorizer.createAcls(requestContext, bindings.toList.asJava).asScala
      .map(_.toCompletableFuture.get)
      .foreach { result => result.exception.ifPresent { e => throw e } }
  }

  private def getAcls(authorizer: AclAuthorizer, resourcePattern: ResourcePattern): Set[AccessControlEntry] = {
    val acls = authorizer.acls(new AclBindingFilter(resourcePattern.toFilter, AccessControlEntryFilter.ANY)).asScala.toSet
    acls.map(_.entry)
  }

  private def newRequestContext(principal: KafkaPrincipal, clientAddress: InetAddress, apiKey: ApiKeys = ApiKeys.PRODUCE): RequestContext = {
    val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
    val header = new RequestHeader(apiKey, 2, "", 1) //ApiKeys apiKey, short version, String clientId, int correlation
    new RequestContext(header, "", clientAddress, principal, ListenerName.forSecurityProtocol(securityProtocol),
      securityProtocol, ClientInformation.EMPTY, false)
  }
}

object TestableDigestLoginModule {
  @volatile var injectedPassword: Option[String] = None

  def reset(): Unit = {
    injectedPassword = None
  }

  def injectInvalidCredentials(): Unit = {
    injectedPassword = Some("invalidPassword")
  }
}

class TestableDigestLoginModule extends DigestLoginModule {
  override def initialize(subject: Subject, callbackHandler: CallbackHandler, sharedState: util.Map[String, _], options: util.Map[String, _]): Unit = {
    super.initialize(subject, callbackHandler, sharedState, options)
    val injectedPassword = TestableDigestLoginModule.injectedPassword
    injectedPassword.foreach { newPassword =>
      val oldPassword = subject.getPrivateCredentials.asScala.head
      subject.getPrivateCredentials.add(newPassword)
      subject.getPrivateCredentials.remove(oldPassword)
    }
  }
}

class TestableJaasSection(contextName: String, modules: Seq[JaasModule]) extends JaasSection(contextName, modules) {
  override def toString: String = {
    super.toString.replaceFirst(classOf[DigestLoginModule].getName, classOf[TestableDigestLoginModule].getName)
  }
}
