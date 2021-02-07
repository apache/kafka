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
import java.util.Properties

import kafka.security.authorizer.{AclAuthorizer, AclEntry}
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.acl.AclOperation.{ALL, ALTER, CLUSTER_ACTION, DELETE, DESCRIBE}
import org.apache.kafka.common.acl.AclPermissionType.ALLOW
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.authorizer.Authorizer
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNull}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.jdk.CollectionConverters._

object DescribeAuthorizedOperationsTest {
  val Group1 = "group1"
  val Group2 = "group2"
  val Group3 = "group3"
  val Topic1 = "topic1"
  val Topic2 = "topic2"

  val Group1Acl = new AclBinding(
    new ResourcePattern(ResourceType.GROUP, Group1, PatternType.LITERAL),
    accessControlEntry(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, ALL))

  val Group2Acl = new AclBinding(
    new ResourcePattern(ResourceType.GROUP, Group2, PatternType.LITERAL),
    accessControlEntry(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, DESCRIBE))

  val Group3Acl = new AclBinding(
    new ResourcePattern(ResourceType.GROUP, Group3, PatternType.LITERAL),
    accessControlEntry(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, DELETE))

  val ClusterAllAcl = new AclBinding(
    new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
    accessControlEntry(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, ALL))

  val Topic1Acl = new AclBinding(
    new ResourcePattern(ResourceType.TOPIC, Topic1, PatternType.LITERAL),
    accessControlEntry(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, ALL))

  val Topic2All = new AclBinding(
    new ResourcePattern(ResourceType.TOPIC, Topic2, PatternType.LITERAL),
    accessControlEntry(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, DELETE))

  private def accessControlEntry(
    userName: String,
    operation: AclOperation
  ): AccessControlEntry = {
    new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName).toString,
      AclEntry.WildcardHost, operation, ALLOW)
  }
}

class DescribeAuthorizedOperationsTest extends IntegrationTestHarness with SaslSetup {
  import DescribeAuthorizedOperationsTest._

  override val brokerCount = 1
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[AclAuthorizer].getName)

  var client: Admin = _

  override protected def securityProtocol = SecurityProtocol.SASL_SSL

  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  override def configureSecurityBeforeServersStart(): Unit = {
    val authorizer = CoreUtils.createObject[Authorizer](classOf[AclAuthorizer].getName)
    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val topicResource = new ResourcePattern(ResourceType.TOPIC, AclEntry.WildcardResource, PatternType.LITERAL)

    try {
      authorizer.configure(this.configs.head.originals())
      val result = authorizer.createAcls(null, List(
        new AclBinding(clusterResource, accessControlEntry(
          JaasTestUtils.KafkaServerPrincipalUnqualifiedName, CLUSTER_ACTION)),
        new AclBinding(clusterResource, accessControlEntry(
          JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, ALTER)),
        new AclBinding(topicResource, accessControlEntry(
          JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, DESCRIBE))
      ).asJava)
      result.asScala.map(_.toCompletableFuture.get).foreach(result => assertFalse(result.exception.isPresent))
    } finally {
      authorizer.close()
    }
  }

  @BeforeEach
  override def setUp(): Unit = {
    startSasl(jaasSections(Seq("GSSAPI"), Some("GSSAPI"), Both, JaasTestUtils.KafkaServerContextName))
    super.setUp()
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
    client = Admin.create(createConfig())
  }

  @AfterEach
  override def tearDown(): Unit = {
    Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
    closeSasl()
  }

  private def createConfig(): Properties = {
    val adminClientConfig = new Properties()
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    adminClientConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    adminClientConfig.putAll(securityProps)
    adminClientConfig
  }

  @Test
  def testConsumerGroupAuthorizedOperations(): Unit = {
    val results = client.createAcls(List(Group1Acl, Group2Acl, Group3Acl).asJava)
    assertEquals(Set(Group1Acl, Group2Acl, Group3Acl), results.values.keySet.asScala)
    results.all.get

    val describeConsumerGroupsResult = client.describeConsumerGroups(Seq(Group1, Group2, Group3).asJava,
      new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
    assertEquals(3, describeConsumerGroupsResult.describedGroups().size())
    val expectedOperations = AclEntry.supportedOperations(ResourceType.GROUP).asJava

    val group1Description = describeConsumerGroupsResult.describedGroups().get(Group1).get
    assertEquals(expectedOperations, group1Description.authorizedOperations())

    val group2Description = describeConsumerGroupsResult.describedGroups().get(Group2).get
    assertEquals(Set(AclOperation.DESCRIBE), group2Description.authorizedOperations().asScala.toSet)

    val group3Description = describeConsumerGroupsResult.describedGroups().get(Group3).get
    assertEquals(Set(AclOperation.DESCRIBE, AclOperation.DELETE), group3Description.authorizedOperations().asScala.toSet)
  }

  @Test
  def testClusterAuthorizedOperations(): Unit = {
    // test without includeAuthorizedOperations flag
    var clusterDescribeResult = client.describeCluster()
    assertNull(clusterDescribeResult.authorizedOperations.get())

    // test with includeAuthorizedOperations flag, we have give Alter permission
    // in configureSecurityBeforeServersStart()
    clusterDescribeResult = client.describeCluster(new DescribeClusterOptions().
      includeAuthorizedOperations(true))
    assertEquals(Set(AclOperation.DESCRIBE, AclOperation.ALTER),
      clusterDescribeResult.authorizedOperations().get().asScala.toSet)

    // enable all operations for cluster resource
    val results = client.createAcls(List(ClusterAllAcl).asJava)
    assertEquals(Set(ClusterAllAcl), results.values.keySet.asScala)
    results.all.get

    val expectedOperations = AclEntry.supportedOperations(ResourceType.CLUSTER).asJava

    clusterDescribeResult = client.describeCluster(new DescribeClusterOptions().
      includeAuthorizedOperations(true))
    assertEquals(expectedOperations, clusterDescribeResult.authorizedOperations().get())
  }

  @Test
  def testTopicAuthorizedOperations(): Unit = {
    createTopic(Topic1)
    createTopic(Topic2)

    // test without includeAuthorizedOperations flag
    var describeTopicsResult = client.describeTopics(Set(Topic1, Topic2).asJava).all.get()
    assertNull(describeTopicsResult.get(Topic1).authorizedOperations)
    assertNull(describeTopicsResult.get(Topic2).authorizedOperations)

    // test with includeAuthorizedOperations flag
    describeTopicsResult = client.describeTopics(Set(Topic1, Topic2).asJava,
      new DescribeTopicsOptions().includeAuthorizedOperations(true)).all.get()
    assertEquals(Set(AclOperation.DESCRIBE), describeTopicsResult.get(Topic1).authorizedOperations().asScala.toSet)
    assertEquals(Set(AclOperation.DESCRIBE), describeTopicsResult.get(Topic2).authorizedOperations().asScala.toSet)

    // add few permissions
    val results = client.createAcls(List(Topic1Acl, Topic2All).asJava)
    assertEquals(Set(Topic1Acl, Topic2All), results.values.keySet.asScala)
    results.all.get

    val expectedOperations = AclEntry.supportedOperations(ResourceType.TOPIC).asJava

    describeTopicsResult = client.describeTopics(Set(Topic1, Topic2).asJava,
      new DescribeTopicsOptions().includeAuthorizedOperations(true)).all.get()
    assertEquals(expectedOperations, describeTopicsResult.get(Topic1).authorizedOperations())
    assertEquals(Set(AclOperation.DESCRIBE, AclOperation.DELETE),
      describeTopicsResult.get(Topic2).authorizedOperations().asScala.toSet)
  }
}
