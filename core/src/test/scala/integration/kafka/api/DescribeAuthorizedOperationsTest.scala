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
import org.apache.kafka.common.acl.AclOperation.{ALTER, CLUSTER_ACTION, DESCRIBE}
import org.apache.kafka.common.acl.AclPermissionType.ALLOW
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.authorizer.Authorizer
import org.junit.Assert.{assertEquals, assertFalse, assertNull}
import org.junit.{After, Before, Test}

import scala.jdk.CollectionConverters._

class DescribeAuthorizedOperationsTest extends IntegrationTestHarness with SaslSetup {
  override val brokerCount = 1
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[AclAuthorizer].getName)

  var client: Admin = _
  val group1 = "group1"
  val group2 = "group2"
  val group3 = "group3"
  val topic1 = "topic1"
  val topic2 = "topic2"

  override protected def securityProtocol = SecurityProtocol.SASL_SSL

  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  override def configureSecurityBeforeServersStart(): Unit = {
    val authorizer = CoreUtils.createObject[Authorizer](classOf[AclAuthorizer].getName)
    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val topicResource = new ResourcePattern(ResourceType.TOPIC, AclEntry.WildcardResource, PatternType.LITERAL)

    try {
      authorizer.configure(this.configs.head.originals())
      val result = authorizer.createAcls(null, List(
        new AclBinding(clusterResource, accessControlEntry(JaasTestUtils.KafkaServerPrincipalUnqualifiedName.toString, ALLOW, CLUSTER_ACTION)),
        new AclBinding(clusterResource, accessControlEntry(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2.toString, ALLOW, ALTER)),
        new AclBinding(topicResource, accessControlEntry(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2.toString, ALLOW, DESCRIBE))).asJava)
      result.asScala.map(_.toCompletableFuture.get).foreach { result => assertFalse(result.exception.isPresent) }

    } finally {
      authorizer.close()
    }
  }

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(Seq("GSSAPI"), Some("GSSAPI"), Both, JaasTestUtils.KafkaServerContextName))
    super.setUp()
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
  }

  private def accessControlEntry(userName: String, permissionType: AclPermissionType, operation: AclOperation): AccessControlEntry = {
    new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName).toString,
      AclEntry.WildcardHost, operation, permissionType)
  }

  @After
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
    closeSasl()
  }

  val group1Acl = new AclBinding(new ResourcePattern(ResourceType.GROUP, group1, PatternType.LITERAL),
    new AccessControlEntry("User:" + JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, "*", AclOperation.ALL, AclPermissionType.ALLOW))

  val group2Acl = new AclBinding(new ResourcePattern(ResourceType.GROUP, group2, PatternType.LITERAL),
    new AccessControlEntry("User:" + JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))

  val group3Acl = new AclBinding(new ResourcePattern(ResourceType.GROUP, group3, PatternType.LITERAL),
    new AccessControlEntry("User:" + JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, "*", AclOperation.DELETE, AclPermissionType.ALLOW))

  val clusterAllAcl = new AclBinding(new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
    new AccessControlEntry("User:" + JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, "*", AclOperation.ALL, AclPermissionType.ALLOW))

  val topic1Acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic1, PatternType.LITERAL),
    new AccessControlEntry("User:" + JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, "*", AclOperation.ALL, AclPermissionType.ALLOW))

  val topic2All = new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic2, PatternType.LITERAL),
    new AccessControlEntry("User:" + JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, "*", AclOperation.DELETE, AclPermissionType.ALLOW))

  def createConfig(): Properties = {
    val adminClientConfig = new Properties()
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    adminClientConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.forEach { (key, value) => adminClientConfig.put(key.asInstanceOf[String], value) }
    adminClientConfig
  }

  @Test
  def testConsumerGroupAuthorizedOperations(): Unit = {
    client = Admin.create(createConfig())

    val results = client.createAcls(List(group1Acl, group2Acl, group3Acl).asJava)
    assertEquals(Set(group1Acl, group2Acl, group3Acl), results.values.keySet.asScala)
    results.all.get

    val describeConsumerGroupsResult = client.describeConsumerGroups(Seq(group1, group2, group3).asJava,
      new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
    assertEquals(3, describeConsumerGroupsResult.describedGroups().size())
    val expectedOperations = AclEntry.supportedOperations(ResourceType.GROUP).asJava

    val group1Description = describeConsumerGroupsResult.describedGroups().get(group1).get
    assertEquals(expectedOperations, group1Description.authorizedOperations())

    val group2Description = describeConsumerGroupsResult.describedGroups().get(group2).get
    assertEquals(Set(AclOperation.DESCRIBE), group2Description.authorizedOperations().asScala.toSet)

    val group3Description = describeConsumerGroupsResult.describedGroups().get(group3).get
    assertEquals(Set(AclOperation.DESCRIBE, AclOperation.DELETE), group3Description.authorizedOperations().asScala.toSet)
  }

  @Test
  def testClusterAuthorizedOperations(): Unit = {
    client = Admin.create(createConfig())

    // test without includeAuthorizedOperations flag
    var clusterDescribeResult = client.describeCluster()
    assertNull(clusterDescribeResult.authorizedOperations.get())

    //test with includeAuthorizedOperations flag, we have give Alter permission
    // in configureSecurityBeforeServersStart()
    clusterDescribeResult = client.describeCluster(new DescribeClusterOptions().
      includeAuthorizedOperations(true))
    assertEquals(Set(AclOperation.DESCRIBE, AclOperation.ALTER),
      clusterDescribeResult.authorizedOperations().get().asScala.toSet)

    // enable all operations for cluster resource
    val results = client.createAcls(List(clusterAllAcl).asJava)
    assertEquals(Set(clusterAllAcl), results.values.keySet.asScala)
    results.all.get

    val expectedOperations = AclEntry.supportedOperations(ResourceType.CLUSTER).asJava

    clusterDescribeResult = client.describeCluster(new DescribeClusterOptions().
      includeAuthorizedOperations(true))
    assertEquals(expectedOperations, clusterDescribeResult.authorizedOperations().get())
  }

  @Test
  def testTopicAuthorizedOperations(): Unit = {
    client = Admin.create(createConfig())
    createTopic(topic1)
    createTopic(topic2)

    // test without includeAuthorizedOperations flag
    var describeTopicsResult = client.describeTopics(Set(topic1, topic2).asJava).all.get()
    assertNull(describeTopicsResult.get(topic1).authorizedOperations)
    assertNull(describeTopicsResult.get(topic2).authorizedOperations)

    //test with includeAuthorizedOperations flag
    describeTopicsResult = client.describeTopics(Set(topic1, topic2).asJava,
      new DescribeTopicsOptions().includeAuthorizedOperations(true)).all.get()
    assertEquals(Set(AclOperation.DESCRIBE), describeTopicsResult.get(topic1).authorizedOperations().asScala.toSet)
    assertEquals(Set(AclOperation.DESCRIBE), describeTopicsResult.get(topic2).authorizedOperations().asScala.toSet)

    //add few permissions
    val results = client.createAcls(List(topic1Acl, topic2All).asJava)
    assertEquals(Set(topic1Acl, topic2All), results.values.keySet.asScala)
    results.all.get

    val expectedOperations = AclEntry.supportedOperations(ResourceType.TOPIC).asJava

    describeTopicsResult = client.describeTopics(Set(topic1, topic2).asJava,
      new DescribeTopicsOptions().includeAuthorizedOperations(true)).all.get()
    assertEquals(expectedOperations, describeTopicsResult.get(topic1).authorizedOperations())
    assertEquals(Set(AclOperation.DESCRIBE, AclOperation.DELETE),
      describeTopicsResult.get(topic2).authorizedOperations().asScala.toSet)
  }
}
