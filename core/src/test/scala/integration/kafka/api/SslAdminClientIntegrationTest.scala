/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.io.File
import java.util
import java.util.Collections
import java.util.concurrent.{CompletionStage, Semaphore}

import kafka.security.authorizer.AclAuthorizer
import kafka.security.authorizer.AuthorizerUtils.{WildcardHost, WildcardPrincipal}
import kafka.security.auth.{Operation, PermissionType}
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, TestUtils}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType._
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.PatternType._
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.server.authorizer._
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._

object SslAdminClientIntegrationTest {
  @volatile var semaphore: Option[Semaphore] = None
  @volatile var lastUpdateRequestContext: Option[AuthorizableRequestContext] = None
  class TestableAclAuthorizer extends AclAuthorizer {
    override def createAcls(requestContext: AuthorizableRequestContext,
                            aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = {
      lastUpdateRequestContext = Some(requestContext)
      semaphore.foreach(_.acquire())
      try {
        super.createAcls(requestContext, aclBindings)
      } finally {
        semaphore.foreach(_.release())
      }
    }

    override def deleteAcls(requestContext: AuthorizableRequestContext,
                            aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = {
      lastUpdateRequestContext = Some(requestContext)
      semaphore.foreach(_.acquire())
      try {
        super.deleteAcls(requestContext, aclBindingFilters)
      } finally {
        semaphore.foreach(_.release())
      }
    }
  }
}

class SslAdminClientIntegrationTest extends SaslSslAdminClientIntegrationTest {
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[SslAdminClientIntegrationTest.TestableAclAuthorizer].getName)

  override protected def securityProtocol = SecurityProtocol.SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  override def configureSecurityBeforeServersStart(): Unit = {
    val authorizer = CoreUtils.createObject[Authorizer](classOf[AclAuthorizer].getName)
    try {
      authorizer.configure(this.configs.head.originals())
      val ace = new AccessControlEntry(WildcardPrincipal, WildcardHost, ALL, ALLOW)
      authorizer.createAcls(null, List(new AclBinding(new ResourcePattern(TOPIC, "*", LITERAL), ace)).asJava)
      authorizer.createAcls(null, List(new AclBinding(new ResourcePattern(GROUP, "*", LITERAL), ace)).asJava)

      authorizer.createAcls(null, List(clusterAcl(ALLOW, CREATE),
                             clusterAcl(ALLOW, DELETE),
                             clusterAcl(ALLOW, CLUSTER_ACTION),
                             clusterAcl(ALLOW, ALTER_CONFIGS),
                             clusterAcl(ALLOW, ALTER))
        .map(ace => new AclBinding(clusterResourcePattern, ace)).asJava)
    } finally {
      authorizer.close()
    }
  }

  override def setUpSasl(): Unit = {
    startSasl(jaasSections(List.empty, None, ZkSasl))
  }

  override def addClusterAcl(permissionType: PermissionType, operation: Operation): Unit = {
    val ace = clusterAcl(permissionType.toJava, operation.toJava)
    val aclBinding = new AclBinding(clusterResourcePattern, ace)
    val authorizer = servers.head.dataPlaneRequestProcessor.authorizer.get
    val prevAcls = authorizer.acls(new AclBindingFilter(clusterResourcePattern.toFilter, AccessControlEntryFilter.ANY))
      .asScala.map(_.entry).toSet
    authorizer.createAcls(null, Collections.singletonList(aclBinding))
    TestUtils.waitAndVerifyAcls(prevAcls ++ Set(ace), authorizer, clusterResourcePattern)
  }

  override def removeClusterAcl(permissionType: PermissionType, operation: Operation): Unit = {
    val ace = clusterAcl(permissionType.toJava, operation.toJava)
    val authorizer = servers.head.dataPlaneRequestProcessor.authorizer.get
    val clusterFilter = new AclBindingFilter(clusterResourcePattern.toFilter, AccessControlEntryFilter.ANY)
    val prevAcls = authorizer.acls(clusterFilter).asScala.map(_.entry).toSet
    val deleteFilter = new AclBindingFilter(clusterResourcePattern.toFilter, ace.toFilter)
    Assert.assertFalse(authorizer.deleteAcls(null, Collections.singletonList(deleteFilter))
      .get(0).toCompletableFuture.get.aclBindingDeleteResults().asScala.head.exception.isPresent)
    TestUtils.waitAndVerifyAcls(prevAcls -- Set(ace), authorizer, clusterResourcePattern)
  }

  private def clusterAcl(permissionType: AclPermissionType, operation: AclOperation): AccessControlEntry = {
    new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*").toString,
      WildcardHost, operation, permissionType)
  }

  @Test
  def testAsyncAclUpdates(): Unit = {
    client = AdminClient.create(createConfig())

    def validateRequestContext(context: AuthorizableRequestContext, apiKey: ApiKeys): Unit = {
      assertEquals(SecurityProtocol.SSL, context.securityProtocol)
      assertEquals("SSL", context.listenerName)
      assertEquals(KafkaPrincipal.ANONYMOUS, context.principal)
      assertEquals(apiKey.id.toInt, context.requestType)
      assertEquals(apiKey.latestVersion.toInt, context.requestVersion)
      assertTrue(s"Invalid correlation id: ${context.correlationId}", context.correlationId > 0)
      assertTrue(s"Invalid client id: ${context.clientId}", context.clientId.startsWith("adminclient"))
      assertTrue(s"Invalid host address: ${context.clientAddress}", context.clientAddress.isLoopbackAddress)
    }

    val testSemaphore = new Semaphore(0)
    SslAdminClientIntegrationTest.semaphore = Some(testSemaphore)
    val results = client.createAcls(List(acl2, acl3).asJava).values
    assertEquals(Set(acl2, acl3), results.keySet().asScala)
    assertFalse(results.values().asScala.exists(_.isDone))
    TestUtils.waitUntilTrue(() => testSemaphore.hasQueuedThreads, "Authorizer not blocked in createAcls")
    testSemaphore.release()
    results.values().asScala.foreach(_.get)
    validateRequestContext(SslAdminClientIntegrationTest.lastUpdateRequestContext.get, ApiKeys.CREATE_ACLS)

    testSemaphore.acquire()
    val results2 = client.deleteAcls(List(ACL1.toFilter, acl2.toFilter, acl3.toFilter).asJava).values
    assertEquals(Set(ACL1.toFilter, acl2.toFilter, acl3.toFilter), results2.keySet.asScala)
    assertFalse(results2.values().asScala.exists(_.isDone))
    TestUtils.waitUntilTrue(() => testSemaphore.hasQueuedThreads, "Authorizer not blocked in deleteAcls")
    testSemaphore.release()
    results.values().asScala.foreach(_.get)
    assertEquals(0, results2.get(ACL1.toFilter).get.values.size())
    assertEquals(Set(acl2), results2.get(acl2.toFilter).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl3), results2.get(acl3.toFilter).get.values.asScala.map(_.binding).toSet)
    validateRequestContext(SslAdminClientIntegrationTest.lastUpdateRequestContext.get, ApiKeys.DELETE_ACLS)
  }
}
