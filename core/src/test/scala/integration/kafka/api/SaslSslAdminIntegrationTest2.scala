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
import kafka.security.authorizer.AclAuthorizer
import kafka.security.authorizer.AclEntry.{WildcardHost, WildcardPrincipalString}
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, JaasTestUtils, TestUtils}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation.{ALL, ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, DELETE}
import org.apache.kafka.common.acl.AclPermissionType.ALLOW
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourceType.{GROUP, TOPIC}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.server.authorizer.Authorizer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.collection.Seq

class SaslSslAdminIntegrationTest2 extends BaseAdminIntegrationTest2 with SaslSetup {
  val clusterResourcePattern = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)

  val authorizationAdmin = new AclAuthorizationAdmin(classOf[AclAuthorizer], classOf[AclAuthorizer])

  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  override def generateConfigs: Seq[KafkaConfig] = {
    this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, authorizationAdmin.authorizerClassName)
    super.generateConfigs
  }

  override def configureSecurityBeforeServersStart(): Unit = {
    authorizationAdmin.initializeAcls()
  }

  @BeforeEach
  override def setUp(): Unit = {
    setUpSasl()
    super.setUp()
  }

  def setUpSasl(): Unit = {
    startSasl(jaasSections(Seq("GSSAPI"), Some("GSSAPI"), Both, JaasTestUtils.KafkaServerContextName))
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  val acl2 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))

  override def configuredClusterPermissions: Set[AclOperation] = {
    Set(AclOperation.ALTER, AclOperation.CREATE, AclOperation.CLUSTER_ACTION, AclOperation.ALTER_CONFIGS,
      AclOperation.DESCRIBE, AclOperation.DESCRIBE_CONFIGS)
  }

  class AclAuthorizationAdmin(authorizerClass: Class[_ <: AclAuthorizer], authorizerForInitClass: Class[_ <: AclAuthorizer]) {

    def authorizerClassName: String = authorizerClass.getName

    def initializeAcls(): Unit = {
      val authorizer = CoreUtils.createObject[Authorizer](authorizerForInitClass.getName)
      try {
        authorizer.configure(configs.head.originals())
        val ace = new AccessControlEntry(WildcardPrincipalString, WildcardHost, ALL, ALLOW)
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

    def addClusterAcl(permissionType: AclPermissionType, operation: AclOperation): Unit = {
      val ace = clusterAcl(permissionType, operation)
      val aclBinding = new AclBinding(clusterResourcePattern, ace)
      val authorizer = servers.head.dataPlaneRequestProcessor.authorizer.get
      val prevAcls = authorizer.acls(new AclBindingFilter(clusterResourcePattern.toFilter, AccessControlEntryFilter.ANY))
        .asScala.map(_.entry).toSet
      authorizer.createAcls(null, Collections.singletonList(aclBinding))
      TestUtils.waitAndVerifyAcls(prevAcls ++ Set(ace), authorizer, clusterResourcePattern)
    }

    def removeClusterAcl(permissionType: AclPermissionType, operation: AclOperation): Unit = {
      val ace = clusterAcl(permissionType, operation)
      val authorizer = servers.head.dataPlaneRequestProcessor.authorizer.get
      val clusterFilter = new AclBindingFilter(clusterResourcePattern.toFilter, AccessControlEntryFilter.ANY)
      val prevAcls = authorizer.acls(clusterFilter).asScala.map(_.entry).toSet
      val deleteFilter = new AclBindingFilter(clusterResourcePattern.toFilter, ace.toFilter)
      assertFalse(authorizer.deleteAcls(null, Collections.singletonList(deleteFilter))
        .get(0).toCompletableFuture.get.aclBindingDeleteResults().asScala.head.exception.isPresent)
      TestUtils.waitAndVerifyAcls(prevAcls -- Set(ace), authorizer, clusterResourcePattern)
    }

    private def clusterAcl(permissionType: AclPermissionType, operation: AclOperation): AccessControlEntry = {
      new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*").toString,
        WildcardHost, operation, permissionType)
    }
  }
}
