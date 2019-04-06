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

import kafka.security.auth.{Allow, Alter, Authorizer, Cluster, ClusterAction, Describe, Group, Operation, PermissionType, Resource, SimpleAclAuthorizer, Topic, Acl => AuthAcl}
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Utils
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class DescribeAuthorizedOperationsTest extends IntegrationTestHarness with SaslSetup {
  override val brokerCount = 1
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName)

  var client: AdminClient = _
  val group1 = "group1"
  val group2 = "group2"
  val group3 = "group3"
  val topic1 = "topic1"
  val topic2 = "topic2"

  override protected def securityProtocol = SecurityProtocol.SASL_SSL

  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  override def configureSecurityBeforeServersStart() {
    val authorizer = CoreUtils.createObject[Authorizer](classOf[SimpleAclAuthorizer].getName)
    val topicResource = Resource(Topic, Resource.WildCardResource, PatternType.LITERAL)

    try {
      authorizer.configure(this.configs.head.originals())
      authorizer.addAcls(Set(clusterAcl(JaasTestUtils.KafkaServerPrincipalUnqualifiedName, Allow, ClusterAction),
        clusterAcl(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, Allow, Alter)), Resource.ClusterResource)
      authorizer.addAcls(Set(clusterAcl(JaasTestUtils.KafkaClientPrincipalUnqualifiedName2, Allow, Describe)), topicResource)
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

  private def clusterAcl(userName: String, permissionType: PermissionType, operation: Operation): AuthAcl = {
    new AuthAcl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName), permissionType,
      AuthAcl.WildCardHost, operation)
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

  val clusteAllAcl = new AclBinding(Resource.ClusterResource.toPattern,
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
    securityProps.asScala.foreach { case (key, value) => adminClientConfig.put(key.asInstanceOf[String], value) }
    adminClientConfig
  }

  @Test
  def testConsumerGroupAuthorizedOperations(): Unit = {
    client = AdminClient.create(createConfig())

    val results = client.createAcls(List(group1Acl, group2Acl, group3Acl).asJava)
    assertEquals(Set(group1Acl, group2Acl, group3Acl), results.values.keySet.asScala)
    results.all.get

    val describeConsumerGroupsResult = client.describeConsumerGroups(Seq(group1, group2, group3).asJava,
      new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
    assertEquals(3, describeConsumerGroupsResult.describedGroups().size())
    val expectedOperations =  Group.supportedOperations
      .map(operation => operation.toJava).asJava

    val group1Description = describeConsumerGroupsResult.describedGroups().get(group1).get
    assertEquals(expectedOperations, group1Description.authorizedOperations())

    val group2Description = describeConsumerGroupsResult.describedGroups().get(group2).get
    assertEquals(Set(AclOperation.DESCRIBE), group2Description.authorizedOperations().asScala.toSet)

    val group3Description = describeConsumerGroupsResult.describedGroups().get(group3).get
    assertEquals(Set(AclOperation.DESCRIBE, AclOperation.DELETE), group3Description.authorizedOperations().asScala.toSet)
  }

  @Test
  def testClusterAuthorizedOperations(): Unit = {
    client = AdminClient.create(createConfig())

    // test without includeAuthorizedOperations flag
    var clusterDescribeResult = client.describeCluster()
    assertEquals(Set(), clusterDescribeResult.authorizedOperations().get().asScala.toSet)

    //test with includeAuthorizedOperations flag, we have give Alter permission
    // in configureSecurityBeforeServersStart()
    clusterDescribeResult = client.describeCluster(new DescribeClusterOptions().
      includeAuthorizedOperations(true))
    assertEquals(Set(AclOperation.DESCRIBE, AclOperation.ALTER),
      clusterDescribeResult.authorizedOperations().get().asScala.toSet)

    // enable all operations for cluster resource
    val results = client.createAcls(List(clusteAllAcl).asJava)
    assertEquals(Set(clusteAllAcl), results.values.keySet.asScala)
    results.all.get

    val expectedOperations = Cluster.supportedOperations
      .map(operation => operation.toJava).asJava

    clusterDescribeResult = client.describeCluster(new DescribeClusterOptions().
      includeAuthorizedOperations(true))
    assertEquals(expectedOperations, clusterDescribeResult.authorizedOperations().get())
  }

  @Test
  def testTopicAuthorizedOperations(): Unit = {
    client = AdminClient.create(createConfig())
    createTopic(topic1)
    createTopic(topic2)

    // test without includeAuthorizedOperations flag
    var describeTopicsResult = client.describeTopics(Set(topic1, topic2).asJava).all.get()
    assertEquals(Set(), describeTopicsResult.get(topic1).authorizedOperations().asScala.toSet)
    assertEquals(Set(), describeTopicsResult.get(topic2).authorizedOperations().asScala.toSet)

    //test with includeAuthorizedOperations flag
    describeTopicsResult = client.describeTopics(Set(topic1, topic2).asJava,
      new DescribeTopicsOptions().includeAuthorizedOperations(true)).all.get()
    assertEquals(Set(AclOperation.DESCRIBE), describeTopicsResult.get(topic1).authorizedOperations().asScala.toSet)
    assertEquals(Set(AclOperation.DESCRIBE), describeTopicsResult.get(topic2).authorizedOperations().asScala.toSet)

    //add few permissions
    val results = client.createAcls(List(topic1Acl, topic2All).asJava)
    assertEquals(Set(topic1Acl, topic2All), results.values.keySet.asScala)
    results.all.get

    val expectedOperations = Topic.supportedOperations
      .map(operation => operation.toJava).asJava

    describeTopicsResult = client.describeTopics(Set(topic1, topic2).asJava,
      new DescribeTopicsOptions().includeAuthorizedOperations(true)).all.get()
    assertEquals(expectedOperations, describeTopicsResult.get(topic1).authorizedOperations())
    assertEquals(Set(AclOperation.DESCRIBE, AclOperation.DELETE),
      describeTopicsResult.get(topic2).authorizedOperations().asScala.toSet)
  }
}
