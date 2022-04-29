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

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.server.{Defaults, KafkaConfig}
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AlterConfigsOptions, Config, ConfigEntry}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{InvalidRequestException, PolicyViolationException}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.policy.AlterConfigPolicy
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo, Timeout}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

/**
  * Tests AdminClient calls when the broker is configured with policies like AlterConfigPolicy, CreateTopicPolicy, etc.
  */
@Timeout(120)
class AdminClientWithPoliciesIntegrationTest extends KafkaServerTestHarness with Logging {

  import AdminClientWithPoliciesIntegrationTest._

  var client: Admin = null
  val brokerCount = 3

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  def createConfig: util.Map[String, Object] =
    Map[String, Object](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers()).asJava

  override def generateConfigs = {
    val configs = TestUtils.createBrokerConfigs(brokerCount, zkConnect)
    configs.foreach(props => props.put(KafkaConfig.AlterConfigPolicyClassNameProp, classOf[Policy]))
    configs.map(KafkaConfig.fromProps)
  }

  @Test
  def testDescribeConfigs(): Unit = {
    client = Admin.create(createConfig)
    // Create three topics, one with custom settings and two with default settings
    val maxMessageBytes = "500000"
    val logRetentionMs = "60000000"
    val topicPropertyNames = List(LogConfig.MaxMessageBytesProp, LogConfig.RetentionMsProp)

    val topic1 = "describe-configs-topic-1"
    val topic1Config = new Properties
    topic1Config.setProperty(LogConfig.MaxMessageBytesProp, maxMessageBytes)
    topic1Config.setProperty(LogConfig.RetentionMsProp, logRetentionMs)
    createTopic(topic1, 1, 1, topic1Config)

    val topic2 = "describe-configs-topic-2"
    createTopic(topic2, 1, 1)

    val topic3 = "describe-configs-topic-3"
    createTopic(topic3, 1, 1)

    // Wait for topic metadata to sync up on all broker nodes
    TestUtils.waitForAllPartitionsMetadata(servers, topic1, 1)
    TestUtils.waitForAllPartitionsMetadata(servers, topic2, 1)
    TestUtils.waitForAllPartitionsMetadata(servers, topic3, 1)

    // Now fetch topic properties and confirm that we can fetch
    // all of the properties as well as ones we are interested in
    val topic1Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1, topicPropertyNames.asJava)
    val topic2Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic2, topicPropertyNames.asJava)
    val topic3Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic3)

    val topicConfigs = client.describeConfigs(List(topic1Resource, topic2Resource, topic3Resource).asJava).all().get()
    // Check for topic1 and topic2 we got only 2 properties, whereas for topic3 all of them
    // were fetched
    val topic1ConfigEntries = topicConfigs.get(topic1Resource).entries()
    assertEquals(2, topic1ConfigEntries.size())
    assertEquals(topicPropertyNames.toSet, topic1ConfigEntries.asScala.map(_.name).toSet)
    assertEquals(Set(maxMessageBytes, logRetentionMs), topic1ConfigEntries.asScala.map(_.value()).toSet)

    val topic2ConfigEntries = topicConfigs.get(topic2Resource).entries()
    assertEquals(2, topic2ConfigEntries.size())
    assertEquals(topicPropertyNames.toSet, topic2ConfigEntries.asScala.map(_.name).toSet)
    assertEquals(Set(Defaults.MessageMaxBytes.toString, (Defaults.LogRetentionHours * 60 * 60 * 1000L).toString),
      topic2ConfigEntries.asScala.map(_.value()).toSet)

    val topic3ConfigEntries = topicConfigs.get(topic3Resource).entries()
    assertTrue(topic3ConfigEntries.size() > 2, "Only 2 topic configuration fetched.")
    val configKeys = topic3ConfigEntries.asScala.map(_.name).toSet.asJava
    assertTrue(configKeys.containsAll(topicPropertyNames.asJava), configKeys.toString)
    assertEquals(Set(Defaults.MessageMaxBytes.toString, (Defaults.LogRetentionHours * 60 * 60 * 1000L).toString),
      topic3ConfigEntries.asScala.filter(configEntry => topicPropertyNames.contains(configEntry.name))
        .map(_.value()).toSet)
  }

  @Test
  def testValidAlterConfigs(): Unit = {
    client = Admin.create(createConfig)
    // Create topics
    val topic1 = "describe-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    val topicConfig1 = new Properties
    topicConfig1.setProperty(LogConfig.MaxMessageBytesProp, "500000")
    topicConfig1.setProperty(LogConfig.RetentionMsProp, "60000000")
    createTopic(topic1, 1, 1, topicConfig1)

    val topic2 = "describe-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2, 1, 1)

    PlaintextAdminIntegrationTest.checkValidAlterConfigs(client, topicResource1, topicResource2)
  }

  @Test
  def testInvalidAlterConfigs(): Unit = {
    client = Admin.create(createConfig)
    PlaintextAdminIntegrationTest.checkInvalidAlterConfigs(zkClient, servers, client)
  }

  @nowarn("cat=deprecation")
  @Test
  def testInvalidAlterConfigsDueToPolicy(): Unit = {
    client = Admin.create(createConfig)

    // Create topics
    val topic1 = "invalid-alter-configs-due-to-policy-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    createTopic(topic1, 1, 1)

    val topic2 = "invalid-alter-configs-due-to-policy-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2, 1, 1)

    val topic3 = "invalid-alter-configs-due-to-policy-topic-3"
    val topicResource3 = new ConfigResource(ConfigResource.Type.TOPIC, topic3)
    createTopic(topic3, 1, 1)

    val topicConfigEntries1 = Seq(
      new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.9"),
      new ConfigEntry(LogConfig.MinInSyncReplicasProp, "2") // policy doesn't allow this
    ).asJava

    var topicConfigEntries2 = Seq(new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.8")).asJava

    val topicConfigEntries3 = Seq(new ConfigEntry(LogConfig.MinInSyncReplicasProp, "-1")).asJava

    val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, servers.head.config.brokerId.toString)
    val brokerConfigEntries = Seq(new ConfigEntry(KafkaConfig.SslTruststorePasswordProp, "12313")).asJava

    // Alter configs: second is valid, the others are invalid
    var alterResult = client.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      topicResource3 -> new Config(topicConfigEntries3),
      brokerResource -> new Config(brokerConfigEntries)
    ).asJava)

    assertEquals(Set(topicResource1, topicResource2, topicResource3, brokerResource).asJava, alterResult.values.keySet)
    assertTrue(assertThrows(classOf[ExecutionException], () => alterResult.values.get(topicResource1).get).getCause.isInstanceOf[PolicyViolationException])
    alterResult.values.get(topicResource2).get
    assertTrue(assertThrows(classOf[ExecutionException], () => alterResult.values.get(topicResource3).get).getCause.isInstanceOf[InvalidRequestException])
    assertTrue(assertThrows(classOf[ExecutionException], () => alterResult.values.get(brokerResource).get).getCause.isInstanceOf[InvalidRequestException])

    // Verify that the second resource was updated and the others were not
    var describeResult = client.describeConfigs(Seq(topicResource1, topicResource2, topicResource3, brokerResource).asJava)
    var configs = describeResult.all.get
    assertEquals(4, configs.size)

    assertEquals(Defaults.LogCleanerMinCleanRatio.toString, configs.get(topicResource1).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals(Defaults.MinInSyncReplicas.toString, configs.get(topicResource1).get(LogConfig.MinInSyncReplicasProp).value)

    assertEquals("0.8", configs.get(topicResource2).get(LogConfig.MinCleanableDirtyRatioProp).value)

    assertNull(configs.get(brokerResource).get(KafkaConfig.SslTruststorePasswordProp).value)

    // Alter configs with validateOnly = true: only second is valid
    topicConfigEntries2 = Seq(new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.7")).asJava

    alterResult = client.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      brokerResource -> new Config(brokerConfigEntries),
      topicResource3 -> new Config(topicConfigEntries3)
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    assertEquals(Set(topicResource1, topicResource2, topicResource3, brokerResource).asJava, alterResult.values.keySet)
    assertTrue(assertThrows(classOf[ExecutionException], () => alterResult.values.get(topicResource1).get).getCause.isInstanceOf[PolicyViolationException])
    alterResult.values.get(topicResource2).get
    assertTrue(assertThrows(classOf[ExecutionException], () => alterResult.values.get(topicResource3).get).getCause.isInstanceOf[InvalidRequestException])
    assertTrue(assertThrows(classOf[ExecutionException], () => alterResult.values.get(brokerResource).get).getCause.isInstanceOf[InvalidRequestException])

    // Verify that no resources are updated since validate_only = true
    describeResult = client.describeConfigs(Seq(topicResource1, topicResource2, topicResource3, brokerResource).asJava)
    configs = describeResult.all.get
    assertEquals(4, configs.size)

    assertEquals(Defaults.LogCleanerMinCleanRatio.toString, configs.get(topicResource1).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals(Defaults.MinInSyncReplicas.toString, configs.get(topicResource1).get(LogConfig.MinInSyncReplicasProp).value)

    assertEquals("0.8", configs.get(topicResource2).get(LogConfig.MinCleanableDirtyRatioProp).value)

    assertNull(configs.get(brokerResource).get(KafkaConfig.SslTruststorePasswordProp).value)
  }


}

object AdminClientWithPoliciesIntegrationTest {

  class Policy extends AlterConfigPolicy {

    var configs: Map[String, _] = _
    var closed = false

    def configure(configs: util.Map[String, _]): Unit = {
      this.configs = configs.asScala.toMap
    }

    def validate(requestMetadata: AlterConfigPolicy.RequestMetadata): Unit = {
      require(!closed, "Policy should not be closed")
      require(!configs.isEmpty, "configure should have been called with non empty configs")
      require(!requestMetadata.configs.isEmpty, "request configs should not be empty")
      require(requestMetadata.resource.name.nonEmpty, "resource name should not be empty")
      require(requestMetadata.resource.name.contains("topic"))
      if (requestMetadata.configs.containsKey(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG))
        throw new PolicyViolationException("Min in sync replicas cannot be updated")
    }

    def close(): Unit = closed = true

  }
}
