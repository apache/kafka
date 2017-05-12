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
import java.util.Properties
import java.util.concurrent.ExecutionException

import org.apache.kafka.common.utils.Utils
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import org.apache.kafka.clients.admin._
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.TopicExistsException
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
    var nodeStrs : List[String] = null
    do {
      var nodes = client.describeCluster().nodes().get().asScala
      nodeStrs = nodes.map ( node => s"${node.host}:${node.port}" ).toList.sorted
    } while (nodeStrs.size < brokerStrs.size)
    assertEquals(brokerStrs.mkString(","), nodeStrs.mkString(","))
    client.close()
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
