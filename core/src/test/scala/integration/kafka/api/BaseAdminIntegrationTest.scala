/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package integration.kafka.api

import java.util
import java.util.Collections
import java.util.concurrent.ExecutionException

import kafka.api.IntegrationTestHarness
import kafka.security.auth.{Cluster, Topic}
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.admin.{Admin, AdminClient, AdminClientConfig, DescribeClusterOptions, DescribeTopicsOptions, NewTopic, TopicDescription}
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.errors.{SecurityDisabledException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.rules.Timeout
import org.junit.{After, Before, Rule, Test}

import scala.collection.JavaConverters._

abstract class BaseAdminIntegrationTest extends IntegrationTestHarness with Logging {
  val brokerCount = 3
  val consumerCount = 1
  val producerCount = 1

  var client: Admin = _

  @Rule
  def globalTimeout: Timeout = Timeout.millis(120000)

  @Before
  override def setUp(): Unit = {
    super.setUp()
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
  }

  @After
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  /**
   * Test that ACL operations are not possible when the authorizer is disabled.
   * Also see {@link kafka.api.SaslSslAdminClientIntegrationTest} for tests of ACL operations
   * when the authorizer is enabled.
   */
  @Test
  def testAclOperations(): Unit = {
    val acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
    client = AdminClient.create(createConfig())
    assertFutureExceptionTypeEquals(client.describeAcls(AclBindingFilter.ANY).values(), classOf[SecurityDisabledException])
    assertFutureExceptionTypeEquals(client.createAcls(Collections.singleton(acl)).all(),
      classOf[SecurityDisabledException])
    assertFutureExceptionTypeEquals(client.deleteAcls(Collections.singleton(acl.toFilter())).all(),
      classOf[SecurityDisabledException])
  }

  @Test
  def testAuthorizedOperations(): Unit = {
    client = AdminClient.create(createConfig())

    // without includeAuthorizedOperations flag
    var result = client.describeCluster
    assertEquals(Set().asJava, result.authorizedOperations().get())

    //with includeAuthorizedOperations flag
    result = client.describeCluster(new DescribeClusterOptions().includeAuthorizedOperations(true))
    var expectedOperations = configuredClusterPermissions.asJava
    assertEquals(expectedOperations, result.authorizedOperations().get())

    val topic = "mytopic"
    val newTopics = Seq(new NewTopic(topic, 3, 3.toShort))
    client.createTopics(newTopics.asJava).all.get()
    waitForTopics(client, expectedPresent = Seq(topic), expectedMissing = List())

    // without includeAuthorizedOperations flag
    var topicResult = getTopicMetadata(client, topic)
    assertEquals(Set().asJava, topicResult.authorizedOperations)

    //with includeAuthorizedOperations flag
    topicResult = getTopicMetadata(client, topic, new DescribeTopicsOptions().includeAuthorizedOperations(true))
    expectedOperations = Topic.supportedOperations
      .map(operation => operation.toJava).asJava
    assertEquals(expectedOperations, topicResult.authorizedOperations)
  }

  def configuredClusterPermissions() : Set[AclOperation] = {
    Cluster.supportedOperations.map(operation => operation.toJava)
  }

  override def generateConfigs: Seq[KafkaConfig] = {
    val cfgs = TestUtils.createBrokerConfigs(brokerCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties, logDirCount = 2)
    cfgs.foreach { config =>
      config.setProperty(KafkaConfig.ListenersProp, s"${listenerName.value}://localhost:${TestUtils.RandomPort}")
      config.remove(KafkaConfig.InterBrokerSecurityProtocolProp)
      config.setProperty(KafkaConfig.InterBrokerListenerNameProp, listenerName.value)
      config.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, s"${listenerName.value}:${securityProtocol.name}")
      config.setProperty(KafkaConfig.DeleteTopicEnableProp, "true")
      config.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
      config.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false")
      // We set this in order to test that we don't expose sensitive data via describe configs. This will already be
      // set for subclasses with security enabled and we don't want to overwrite it.
      if (!config.containsKey(KafkaConfig.SslTruststorePasswordProp))
        config.setProperty(KafkaConfig.SslTruststorePasswordProp, "some.invalid.pass")
    }
    cfgs.foreach(_ ++= serverConfig)
    cfgs.map(KafkaConfig.fromProps)
  }

  def createConfig(): util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  def waitForTopics(client: Admin, expectedPresent: Seq[String], expectedMissing: Seq[String]): Unit = {
    TestUtils.waitUntilTrue(() => {
      val topics = client.listTopics.names.get()
      expectedPresent.forall(topicName => topics.contains(topicName)) &&
        expectedMissing.forall(topicName => !topics.contains(topicName))
    }, "timed out waiting for topics")
  }

  def getTopicMetadata(client: Admin,
                       topic: String,
                       describeOptions: DescribeTopicsOptions = new DescribeTopicsOptions,
                       expectedNumPartitionsOpt: Option[Int] = None): TopicDescription = {
    var result: TopicDescription = null
    TestUtils.waitUntilTrue(() => {
      val topicResult = client.describeTopics(Set(topic).asJava, describeOptions).values.get(topic)
      try {
        result = topicResult.get
        expectedNumPartitionsOpt.map(_ == result.partitions.size).getOrElse(true)
      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false  // metadata may not have propagated yet, so retry
      }
    }, s"Timed out waiting for metadata for $topic")
    result
  }

}
