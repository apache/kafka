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
import kafka.security.authorizer.AclAuthorizer
import kafka.security.authorizer.AclEntry.{WildcardHost, WildcardPrincipalString}
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation.{ALL, ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, DELETE}
import org.apache.kafka.common.acl.AclPermissionType.ALLOW
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourceType.{GROUP, TOPIC}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.server.authorizer.Authorizer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

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

  val anyAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
    new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))
  val acl2 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  val fooAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foobar", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val prefixAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.PREFIXED),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))

  override def configuredClusterPermissions: Set[AclOperation] = {
    Set(AclOperation.ALTER, AclOperation.CREATE, AclOperation.CLUSTER_ACTION, AclOperation.ALTER_CONFIGS,
      AclOperation.DESCRIBE, AclOperation.DESCRIBE_CONFIGS)
  }

  @Test
  def testAclDelete(): Unit = {
    client = Admin.create(createConfig)
    ensureAcls(Set(anyAcl, acl2, fooAcl, prefixAcl))

    val allTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.MATCH), AccessControlEntryFilter.ANY)
    val allLiteralTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val allPrefixedTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.PREFIXED), AccessControlEntryFilter.ANY)

    // Delete only ACLs on literal 'mytopic2' topic
    var deleted = client.deleteAcls(List(acl2.toFilter).asJava).all().get().asScala.toSet
    assertEquals(Set(acl2), deleted)
    assertEquals(Set(anyAcl, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete only ACLs on literal '*' topic
    deleted = client.deleteAcls(List(anyAcl.toFilter).asJava).all().get().asScala.toSet
    assertEquals(Set(anyAcl), deleted)
    assertEquals(Set(acl2, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete only ACLs on specific prefixed 'mytopic' topics:
    deleted = client.deleteAcls(List(prefixAcl.toFilter).asJava).all().get().asScala.toSet
    assertEquals(Set(prefixAcl), deleted)
    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all literal ACLs:
    deleted = client.deleteAcls(List(allLiteralTopicAcls).asJava).all().get().asScala.toSet
    assertEquals(Set(anyAcl, acl2, fooAcl), deleted)
    assertEquals(Set(prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all prefixed ACLs:
    deleted = client.deleteAcls(List(allPrefixedTopicAcls).asJava).all().get().asScala.toSet
    assertEquals(Set(prefixAcl), deleted)
    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all topic ACLs:
    deleted = client.deleteAcls(List(allTopicAcls).asJava).all().get().asScala.toSet
    assertEquals(Set(), getAcls(allTopicAcls))
  }

  //noinspection ScalaDeprecation - test explicitly covers clients using legacy / deprecated constructors
  @Test
  def testLegacyAclOpsNeverAffectOrReturnPrefixed(): Unit = {
    client = Admin.create(createConfig)
    ensureAcls(Set(anyAcl, acl2, fooAcl, prefixAcl))  // <-- prefixed exists, but should never be returned.

    val allTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.MATCH), AccessControlEntryFilter.ANY)
    val legacyAllTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val legacyMyTopic2Acls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val legacyAnyTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "*", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val legacyFooTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "foobar", PatternType.LITERAL), AccessControlEntryFilter.ANY)

    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(legacyAllTopicAcls))
    assertEquals(Set(acl2), getAcls(legacyMyTopic2Acls))
    assertEquals(Set(anyAcl), getAcls(legacyAnyTopicAcls))
    assertEquals(Set(fooAcl), getAcls(legacyFooTopicAcls))

    // Delete only (legacy) ACLs on 'mytopic2' topic
    var deleted = client.deleteAcls(List(legacyMyTopic2Acls).asJava).all().get().asScala.toSet
    assertEquals(Set(acl2), deleted)
    assertEquals(Set(anyAcl, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete only (legacy) ACLs on '*' topic
    deleted = client.deleteAcls(List(legacyAnyTopicAcls).asJava).all().get().asScala.toSet
    assertEquals(Set(anyAcl), deleted)
    assertEquals(Set(acl2, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all (legacy) topic ACLs:
    deleted = client.deleteAcls(List(legacyAllTopicAcls).asJava).all().get().asScala.toSet
    assertEquals(Set(anyAcl, acl2, fooAcl), deleted)
    assertEquals(Set(), getAcls(legacyAllTopicAcls))
    assertEquals(Set(prefixAcl), getAcls(allTopicAcls))
  }

  private def waitForDescribeAcls(client: Admin, filter: AclBindingFilter, acls: Set[AclBinding]): Unit = {
    var lastResults: util.Collection[AclBinding] = null
    TestUtils.waitUntilTrue(() => {
      lastResults = client.describeAcls(filter).values.get()
      acls == lastResults.asScala.toSet
    }, s"timed out waiting for ACLs $acls.\nActual $lastResults")
  }

  private def ensureAcls(bindings: Set[AclBinding]): Unit = {
    client.createAcls(bindings.asJava).all().get()

    bindings.foreach(binding => waitForDescribeAcls(client, binding.toFilter, Set(binding)))
  }

  private def getAcls(allTopicAcls: AclBindingFilter) = {
    client.describeAcls(allTopicAcls).values.get().asScala.toSet
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
