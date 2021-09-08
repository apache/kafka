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
import kafka.log.LogConfig
import kafka.security.authorizer.AclAuthorizer
import kafka.security.authorizer.AclEntry.{WildcardHost, WildcardPrincipalString}
import kafka.server.{Defaults, KafkaConfig}
import kafka.utils.{CoreUtils, JaasTestUtils, TestUtils}
import kafka.utils.TestUtils._
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation.{ALL, ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, DELETE, DESCRIBE}
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{ClusterAuthorizationException, InvalidRequestException, TopicAuthorizationException, UnknownTopicOrPartitionException}
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
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionException
import scala.util.{Failure, Success, Try}

class SaslSslAdminIntegrationTest extends BaseAdminIntegrationTest with SaslSetup {
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
  val acl3 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val fooAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foobar", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val prefixAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.PREFIXED),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val transactionalIdAcl = new AclBinding(new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "transactional_id", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  val groupAcl = new AclBinding(new ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL),
    new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))

  @Test
  def testAclOperations(): Unit = {
    client = Admin.create(createConfig)
    val acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
    assertEquals(7, getAcls(AclBindingFilter.ANY).size)
    val results = client.createAcls(List(acl2, acl3).asJava)
    assertEquals(Set(acl2, acl3), results.values.keySet().asScala)
    results.values.values.forEach(value => value.get)
    val aclUnknown = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.UNKNOWN, AclPermissionType.ALLOW))
    val results2 = client.createAcls(List(aclUnknown).asJava)
    assertEquals(Set(aclUnknown), results2.values.keySet().asScala)
    assertFutureExceptionTypeEquals(results2.all, classOf[InvalidRequestException])
    val results3 = client.deleteAcls(List(acl.toFilter, acl2.toFilter, acl3.toFilter).asJava).values
    assertEquals(Set(acl.toFilter, acl2.toFilter, acl3.toFilter), results3.keySet.asScala)
    assertEquals(0, results3.get(acl.toFilter).get.values.size())
    assertEquals(Set(acl2), results3.get(acl2.toFilter).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl3), results3.get(acl3.toFilter).get.values.asScala.map(_.binding).toSet)
  }

  @Test
  def testAclOperations2(): Unit = {
    client = Admin.create(createConfig)
    val results = client.createAcls(List(acl2, acl2, transactionalIdAcl).asJava)
    assertEquals(Set(acl2, acl2, transactionalIdAcl), results.values.keySet.asScala)
    results.all.get()
    waitForDescribeAcls(client, acl2.toFilter, Set(acl2))
    waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set(transactionalIdAcl))

    val filterA = new AclBindingFilter(new ResourcePatternFilter(ResourceType.GROUP, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val filterB = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val filterC = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TRANSACTIONAL_ID, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)

    waitForDescribeAcls(client, filterA, Set(groupAcl))
    waitForDescribeAcls(client, filterC, Set(transactionalIdAcl))

    val results2 = client.deleteAcls(List(filterA, filterB, filterC).asJava, new DeleteAclsOptions())
    assertEquals(Set(filterA, filterB, filterC), results2.values.keySet.asScala)
    assertEquals(Set(groupAcl), results2.values.get(filterA).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(transactionalIdAcl), results2.values.get(filterC).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl2), results2.values.get(filterB).get.values.asScala.map(_.binding).toSet)

    waitForDescribeAcls(client, filterB, Set())
    waitForDescribeAcls(client, filterC, Set())
  }

  @Test
  def testAclDescribe(): Unit = {
    client = Admin.create(createConfig)
    ensureAcls(Set(anyAcl, acl2, fooAcl, prefixAcl))

    val allTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.ANY), AccessControlEntryFilter.ANY)
    val allLiteralTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val allPrefixedTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.PREFIXED), AccessControlEntryFilter.ANY)
    val literalMyTopic2Acls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val prefixedMyTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic", PatternType.PREFIXED), AccessControlEntryFilter.ANY)
    val allMyTopic2Acls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.MATCH), AccessControlEntryFilter.ANY)
    val allFooTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "foobar", PatternType.MATCH), AccessControlEntryFilter.ANY)

    assertEquals(Set(anyAcl), getAcls(anyAcl.toFilter))
    assertEquals(Set(prefixAcl), getAcls(prefixAcl.toFilter))
    assertEquals(Set(acl2), getAcls(acl2.toFilter))
    assertEquals(Set(fooAcl), getAcls(fooAcl.toFilter))

    assertEquals(Set(acl2), getAcls(literalMyTopic2Acls))
    assertEquals(Set(prefixAcl), getAcls(prefixedMyTopicAcls))
    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(allLiteralTopicAcls))
    assertEquals(Set(prefixAcl), getAcls(allPrefixedTopicAcls))
    assertEquals(Set(anyAcl, acl2, prefixAcl), getAcls(allMyTopic2Acls))
    assertEquals(Set(anyAcl, fooAcl), getAcls(allFooTopicAcls))
    assertEquals(Set(anyAcl, acl2, fooAcl, prefixAcl), getAcls(allTopicAcls))
  }

  @Test
  def testAttemptToCreateInvalidAcls(): Unit = {
    client = Admin.create(createConfig)
    val clusterAcl = new AclBinding(new ResourcePattern(ResourceType.CLUSTER, "foobar", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val emptyResourceNameAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val results = client.createAcls(List(clusterAcl, emptyResourceNameAcl).asJava, new CreateAclsOptions())
    assertEquals(Set(clusterAcl, emptyResourceNameAcl), results.values.keySet().asScala)
    assertFutureExceptionTypeEquals(results.values.get(clusterAcl), classOf[InvalidRequestException])
    assertFutureExceptionTypeEquals(results.values.get(emptyResourceNameAcl), classOf[InvalidRequestException])
  }

  override def configuredClusterPermissions: Set[AclOperation] = {
    Set(AclOperation.ALTER, AclOperation.CREATE, AclOperation.CLUSTER_ACTION, AclOperation.ALTER_CONFIGS,
      AclOperation.DESCRIBE, AclOperation.DESCRIBE_CONFIGS)
  }

  private def verifyCauseIsClusterAuth(e: Throwable): Unit = assertEquals(classOf[ClusterAuthorizationException], e.getCause.getClass)

  private def testAclCreateGetDelete(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val result = client.createAcls(List(fooAcl, transactionalIdAcl).asJava, new CreateAclsOptions)
      if (expectAuth) {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(_) => true
        }
      } else {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) => false
        }
      }
    }, "timed out waiting for createAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, fooAcl.toFilter, Set(fooAcl))
      waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set(transactionalIdAcl))
    }
    TestUtils.waitUntilTrue(() => {
      val result = client.deleteAcls(List(fooAcl.toFilter, transactionalIdAcl.toFilter).asJava, new DeleteAclsOptions)
      if (expectAuth) {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(_) => true
        }
      } else {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) =>
            assertEquals(Set(fooAcl, transactionalIdAcl), result.values.keySet)
            assertEquals(Set(fooAcl), result.values.get(fooAcl.toFilter).get.values.asScala.map(_.binding).toSet)
            assertEquals(Set(transactionalIdAcl),
              result.values.get(transactionalIdAcl.toFilter).get.values.asScala.map(_.binding).toSet)
            true
        }
      }
    }, "timed out waiting for deleteAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, fooAcl.toFilter, Set.empty)
      waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set.empty)
    }
  }

  private def testAclGet(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val userAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
        new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))
      val results = client.describeAcls(userAcl.toFilter)
      if (expectAuth) {
        Try(results.values.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(acls) => Set(userAcl).equals(acls.asScala.toSet)
        }
      } else {
        Try(results.values.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) => false
        }
      }
    }, "timed out waiting for describeAcls to " + (if (expectAuth) "succeed" else "fail"))
  }

  @Test
  def testAclAuthorizationDenied(): Unit = {
    client = Admin.create(createConfig)

    // Test that we cannot create or delete ACLs when ALTER is denied.
    authorizationAdmin.addClusterAcl(DENY, ALTER)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we cannot do anything with ACLs when DESCRIBE and ALTER are denied.
    authorizationAdmin.addClusterAcl(DENY, DESCRIBE)
    testAclGet(expectAuth = false)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we can create, delete, and get ACLs with the default ACLs.
    authorizationAdmin.removeClusterAcl(DENY, DESCRIBE)
    authorizationAdmin.removeClusterAcl(DENY, ALTER)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = true)

    // Test that we can't do anything with ACLs without the ALLOW ALTER ACL in place.
    authorizationAdmin.removeClusterAcl(ALLOW, ALTER)
    authorizationAdmin.removeClusterAcl(ALLOW, DELETE)
    testAclGet(expectAuth = false)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we can describe, but not alter ACLs, with only the ALLOW DESCRIBE ACL in place.
    authorizationAdmin.addClusterAcl(ALLOW, DESCRIBE)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = false)
  }

  @Test
  def testCreateTopicsResponseMetadataAndConfig(): Unit = {
    val topic1 = "mytopic1"
    val topic2 = "mytopic2"
    val denyAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic2, PatternType.LITERAL),
      new AccessControlEntry("User:*", "*", AclOperation.DESCRIBE_CONFIGS, AclPermissionType.DENY))

    client = Admin.create(createConfig)
    client.createAcls(List(denyAcl).asJava, new CreateAclsOptions()).all().get()

    val topics = Seq(topic1, topic2)
    val configsOverride = Map(LogConfig.SegmentBytesProp -> "100000").asJava
    val newTopics = Seq(
      new NewTopic(topic1, 2, 3.toShort).configs(configsOverride),
      new NewTopic(topic2, Option.empty[Integer].asJava, Option.empty[java.lang.Short].asJava).configs(configsOverride))
    val validateResult = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true))
    validateResult.all.get()
    waitForTopics(client, List(), topics)

    def validateMetadataAndConfigs(result: CreateTopicsResult): Unit = {
      assertEquals(2, result.numPartitions(topic1).get())
      assertEquals(3, result.replicationFactor(topic1).get())
      val topicConfigs = result.config(topic1).get().entries.asScala
      assertTrue(topicConfigs.nonEmpty)
      val segmentBytesConfig = topicConfigs.find(_.name == LogConfig.SegmentBytesProp).get
      assertEquals(100000, segmentBytesConfig.value.toLong)
      assertEquals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, segmentBytesConfig.source)
      val compressionConfig = topicConfigs.find(_.name == LogConfig.CompressionTypeProp).get
      assertEquals(Defaults.CompressionType, compressionConfig.value)
      assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, compressionConfig.source)

      assertFutureExceptionTypeEquals(result.numPartitions(topic2), classOf[TopicAuthorizationException])
      assertFutureExceptionTypeEquals(result.replicationFactor(topic2), classOf[TopicAuthorizationException])
      assertFutureExceptionTypeEquals(result.config(topic2), classOf[TopicAuthorizationException])
    }
    validateMetadataAndConfigs(validateResult)

    val createResult = client.createTopics(newTopics.asJava, new CreateTopicsOptions())
    createResult.all.get()
    waitForTopics(client, topics, List())
    validateMetadataAndConfigs(createResult)
    val topicIds = getTopicIds()
    assertNotEquals(Uuid.ZERO_UUID, createResult.topicId(topic1).get())
    assertEquals(topicIds(topic1), createResult.topicId(topic1).get())
    assertFutureExceptionTypeEquals(createResult.topicId(topic2), classOf[TopicAuthorizationException])
    
    val createResponseConfig = createResult.config(topic1).get().entries.asScala

    val describeResponseConfig = describeConfigs(topic1)
    assertEquals(describeResponseConfig.map(_.name).toSet, createResponseConfig.map(_.name).toSet)
    describeResponseConfig.foreach { describeEntry =>
      val name = describeEntry.name
      val createEntry = createResponseConfig.find(_.name == name).get
      assertEquals(describeEntry.value, createEntry.value, s"Value mismatch for $name")
      assertEquals(describeEntry.isReadOnly, createEntry.isReadOnly, s"isReadOnly mismatch for $name")
      assertEquals(describeEntry.isSensitive, createEntry.isSensitive, s"isSensitive mismatch for $name")
      assertEquals(describeEntry.source, createEntry.source, s"Source mismatch for $name")
    }
  }

  private def describeConfigs(topic: String): Iterable[ConfigEntry] = {
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    var configEntries: Iterable[ConfigEntry] = null

    TestUtils.waitUntilTrue(() => {
      try {
        val topicResponse = client.describeConfigs(List(topicResource).asJava).all.get.get(topicResource)
        configEntries = topicResponse.entries.asScala
        true
      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false
      }
    }, "Timed out waiting for describeConfigs")

    configEntries
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
