/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.api

import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.ExecutionException

import org.apache.kafka.common.utils.Utils
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.server.{Defaults, KafkaConfig}
import org.apache.kafka.clients.admin._
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.{SecurityDisabledException, TopicExistsException}
import org.apache.kafka.common.protocol.ApiKeys
import org.junit.{After, Before, Rule, Test}
import org.apache.kafka.common.requests.MetadataResponse
import org.junit.rules.Timeout
import org.junit.Assert._

import scala.collection.JavaConverters._

/**
 * An integration test of the KafkaAdminClient.
 *
 * Also see {@link org.apache.kafka.clients.admin.KafkaAdminClientTest} for a unit test of the admin client.
 */
class KafkaAdminClientIntegrationTest extends KafkaServerTestHarness with Logging {

  @Rule
  def globalTimeout = Timeout.millis(120000)

  var client: AdminClient = null

  @Before
  override def setUp(): Unit = {
    super.setUp
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
  }

  @After
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  val brokerCount = 3
  lazy val serverConfig = new Properties

  def createConfig(): util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  def waitForTopics(client: AdminClient, expectedPresent: Seq[String], expectedMissing: Seq[String]): Unit = {
    TestUtils.waitUntilTrue(() => {
        val topics = client.listTopics().names().get()
        expectedPresent.forall(topicName => topics.contains(topicName)) &&
          expectedMissing.forall(topicName => !topics.contains(topicName))
      }, "timed out waiting for topics")
  }

  def assertFutureExceptionTypeEquals(future: KafkaFuture[_], clazz: Class[_ <: Throwable]): Unit = {
    try {
      future.get()
      fail("Expected CompletableFuture.get to return an exception")
    } catch {
      case e: ExecutionException =>
        val cause = e.getCause()
        assertTrue("Expected an exception of type " + clazz.getName + "; got type " +
            cause.getClass().getName, clazz.isInstance(cause))
    }
  }

  @Test
  def testClose(): Unit = {
    val client = AdminClient.create(createConfig())
    client.close()
    client.close() // double close has no effect
  }

  @Test
  def testListNodes(): Unit = {
    client = AdminClient.create(createConfig())
    val brokerStrs = brokerList.split(",").toList.sorted
    var nodeStrs: List[String] = null
    do {
      val nodes = client.describeCluster().nodes().get().asScala
      nodeStrs = nodes.map ( node => s"${node.host}:${node.port}" ).toList.sorted
    } while (nodeStrs.size < brokerStrs.size)
    assertEquals(brokerStrs.mkString(","), nodeStrs.mkString(","))
  }

  @Test
  def testCreateDeleteTopics(): Unit = {
    client = AdminClient.create(createConfig())
    val topics = Seq("mytopic", "mytopic2")
    val newTopics = topics.map(new NewTopic(_, 1, 1))
    client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all.get()
    waitForTopics(client, List(), List("mytopic", "mytopic2"))

    client.createTopics(newTopics.asJava).all.get()
    waitForTopics(client, List("mytopic", "mytopic2"), List())

    val results = client.createTopics(newTopics.asJava).results()
    assertTrue(results.containsKey("mytopic"))
    assertFutureExceptionTypeEquals(results.get("mytopic"), classOf[TopicExistsException])
    assertTrue(results.containsKey("mytopic2"))
    assertFutureExceptionTypeEquals(results.get("mytopic2"), classOf[TopicExistsException])
    val topicsFromDescribe = client.describeTopics(Seq("mytopic", "mytopic2").asJava).all.get().asScala.keys
    assertEquals(topics.toSet, topicsFromDescribe)

    client.deleteTopics(topics.asJava).all.get()
    waitForTopics(client, List(), List("mytopic", "mytopic2"))
  }

  @Test
  def testGetAllBrokerVersionsAndDescribeCluster(): Unit = {
    client = AdminClient.create(createConfig())
    val nodes = client.describeCluster().nodes().get()
    val clusterId = client.describeCluster().clusterId().get()
    assertEquals(servers.head.apis.clusterId, clusterId)
    val controller = client.describeCluster().controller().get()
    assertEquals(servers.head.apis.metadataCache.getControllerId.
      getOrElse(MetadataResponse.NO_CONTROLLER_ID), controller.id())
    val nodesToVersions = client.apiVersions(nodes).all().get()
    val brokers = brokerList.split(",")
    assert(brokers.size == nodesToVersions.size())
    for ((node, brokerVersionInfo) <- nodesToVersions.asScala) {
      val hostStr = s"${node.host}:${node.port}"
      assertTrue(s"Unknown host:port pair $hostStr in brokerVersionInfos", brokers.contains(hostStr))
      assertEquals(1, brokerVersionInfo.usableVersion(ApiKeys.API_VERSIONS))
    }
  }

  @Test
  def testDescribeAndAlterConfigs(): Unit = {
    client = AdminClient.create(createConfig)
    val topic1 = "describe-alter-configs-topic-1"
    val configResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    val topicConfig1 = new Properties
    topicConfig1.setProperty(LogConfig.MaxMessageBytesProp, "500000")
    topicConfig1.setProperty(LogConfig.RetentionMsProp, "60000000")
    TestUtils.createTopic(zkUtils, topic1, 1, 1, servers, topicConfig1)

    val topic2 = "describe-alter-configs-topic-2"
    val configResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    TestUtils.createTopic(zkUtils, topic2, 1, 1, servers, new Properties)

    val configResource3 = new ConfigResource(ConfigResource.Type.BROKER, servers(1).config.brokerId.toString)
    val configResource4 = new ConfigResource(ConfigResource.Type.BROKER, servers(2).config.brokerId.toString)
    val configResources = Seq(configResource1, configResource2, configResource3, configResource4)
    var describeResult = client.describeConfigs(configResources.asJava)
    var configs = describeResult.all.get

    assertEquals(4, configs.size)

    assertEquals(topicConfig1.get(LogConfig.MaxMessageBytesProp),
      configs.get(configResource1).get(LogConfig.MaxMessageBytesProp).value)
    assertEquals(topicConfig1.get(LogConfig.RetentionMsProp),
      configs.get(configResource1).get(LogConfig.RetentionMsProp).value)

    assertEquals(Defaults.MessageMaxBytes.toString,
      configs.get(configResource2).get(LogConfig.MaxMessageBytesProp).value)

    assertEquals(servers(1).config.values.size, configs.get(configResource3).entries.size)
    assertEquals(servers(1).config.brokerId.toString, configs.get(configResource3).get(KafkaConfig.BrokerIdProp).value)
    assertEquals(servers(1).config.controllerSocketTimeoutMs.toString,
      configs.get(configResource3).get(KafkaConfig.ControllerSocketTimeoutMsProp).value)

    assertEquals(servers(2).config.values.size, configs.get(configResource4).entries.size)
    assertEquals(servers(2).config.brokerId.toString, configs.get(configResource4).get(KafkaConfig.BrokerIdProp).value)
    assertEquals(servers(2).config.logCleanerThreads.toString,
      configs.get(configResource4).get(KafkaConfig.LogCleanerThreadsProp).value)

    val topicConfigEntries1 = Seq(
      new ConfigEntry(LogConfig.FlushMsProp, "1000")
    ).asJava

    val topicConfigEntries2 = Seq(
      new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.9"),
      new ConfigEntry(LogConfig.CompressionTypeProp, "lz4")
    ).asJava

    val alterResult = client.alterConfigs(Map(
      configResource1 -> new Config(topicConfigEntries1),
      configResource2 -> new Config(topicConfigEntries2)
    ).asJava)

    assertEquals(Set(configResource1, configResource2).asJava, alterResult.results.keySet)
    alterResult.all.get

    describeResult = client.describeConfigs(Seq(configResource1, configResource2).asJava)
    configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("1000", configs.get(configResource1).get(LogConfig.FlushMsProp).value)
    assertEquals(Defaults.MessageMaxBytes.toString,
      configs.get(configResource1).get(LogConfig.MaxMessageBytesProp).value)
    assertEquals((Defaults.LogRetentionHours * 60 * 60 * 1000).toString,
      configs.get(configResource1).get(LogConfig.RetentionMsProp).value)

    assertEquals("0.9", configs.get(configResource2).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals("lz4", configs.get(configResource2).get(LogConfig.CompressionTypeProp).value)
  }

  val ACL1 = new AclBinding(new Resource(ResourceType.TOPIC, "mytopic3"),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));

  /**
   * Test that ACL operations are not possible when the authorizer is disabled.
   * Also see {@link kafka.api.SaslSslAdminClientIntegrationTest} for tests of ACL operations
   * when the authorizer is enabled.
   */
  @Test
  def testAclOperations(): Unit = {
    client = AdminClient.create(createConfig())
    assertFutureExceptionTypeEquals(client.describeAcls(AclBindingFilter.ANY).all(), classOf[SecurityDisabledException])
    assertFutureExceptionTypeEquals(client.createAcls(Collections.singleton(ACL1)).all(),
        classOf[SecurityDisabledException])
    assertFutureExceptionTypeEquals(client.deleteAcls(Collections.singleton(ACL1.toFilter())).all(),
      classOf[SecurityDisabledException])
    client.close()
  }

  override def generateConfigs() = {
    val cfgs = TestUtils.createBrokerConfigs(brokerCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties)
    cfgs.foreach { config =>
      config.setProperty(KafkaConfig.ListenersProp, s"${listenerName.value}://localhost:${TestUtils.RandomPort}")
      config.remove(KafkaConfig.InterBrokerSecurityProtocolProp)
      config.setProperty(KafkaConfig.InterBrokerListenerNameProp, listenerName.value)
      config.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, s"${listenerName.value}:${securityProtocol.name}")
      config.setProperty(KafkaConfig.DeleteTopicEnableProp, "true");
    }
    cfgs.foreach(_.putAll(serverConfig))
    cfgs.map(KafkaConfig.fromProps)
  }
}
