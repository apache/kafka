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
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils.assertFutureExceptionTypeEquals
import kafka.utils.{Logging, TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AlterConfigOp, AlterConfigsOptions, Config, ConfigEntry}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidRequestException, PolicyViolationException}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.policy.AlterConfigPolicy
import org.apache.kafka.storage.internals.log.LogConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * Tests AdminClient calls when the broker is configured with policies like AlterConfigPolicy, CreateTopicPolicy, etc.
  */
@Timeout(120)
class AdminClientWithPoliciesIntegrationTest extends KafkaServerTestHarness with Logging {

  import AdminClientWithPoliciesIntegrationTest._

  var client: Admin = _
  val brokerCount = 3

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    TestUtils.waitUntilBrokerMetadataIsPropagated(brokers)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  def createConfig: util.Map[String, Object] =
    Map[String, Object](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers()).asJava

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    val configs = TestUtils.createBrokerConfigs(brokerCount, zkConnectOrNull)
    configs.foreach(overrideNodeConfigs)
    configs.map(KafkaConfig.fromProps)
  }

  override def kraftControllerConfigs(): Seq[Properties] = {
    val props = new Properties()
    overrideNodeConfigs(props)
    Seq(props)
  }

  private def overrideNodeConfigs(props: Properties): Unit = {
    props.put(KafkaConfig.AlterConfigPolicyClassNameProp, classOf[Policy])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testValidAlterConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)
    // Create topics
    val topic1 = "describe-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    val topicConfig1 = new Properties
    topicConfig1.setProperty(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "500000")
    topicConfig1.setProperty(TopicConfig.RETENTION_MS_CONFIG, "60000000")
    createTopic(topic1, 1, 1, topicConfig1)

    val topic2 = "describe-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2, 1, 1)

    PlaintextAdminIntegrationTest.checkValidAlterConfigs(client, this, topicResource1, topicResource2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInvalidAlterConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)
    PlaintextAdminIntegrationTest.checkInvalidAlterConfigs(this, client)
  }

  @nowarn("cat=deprecation")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInvalidAlterConfigsDueToPolicy(quorum: String): Unit = {
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

    // Set a mutable broker config
    val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, brokers.head.config.brokerId.toString)
    val brokerConfigs = Seq(new ConfigEntry(KafkaConfig.MessageMaxBytesProp, "50000")).asJava
    val alterResult1 = client.alterConfigs(Map(brokerResource -> new Config(brokerConfigs)).asJava)
    alterResult1.all.get
    assertEquals(Set(KafkaConfig.MessageMaxBytesProp), validationsForResource(brokerResource).head.configs().keySet().asScala)
    validations.clear()

    val topicConfigEntries1 = Seq(
      new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"),
      new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2") // policy doesn't allow this
    ).asJava

    var topicConfigEntries2 = Seq(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.8")).asJava

    val topicConfigEntries3 = Seq(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "-1")).asJava

    val brokerConfigEntries = Seq(new ConfigEntry(KafkaConfig.SslTruststorePasswordProp, "12313")).asJava

    // Alter configs: second is valid, the others are invalid
    var alterResult = client.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      topicResource3 -> new Config(topicConfigEntries3),
      brokerResource -> new Config(brokerConfigEntries)
    ).asJava)

    assertEquals(Set(topicResource1, topicResource2, topicResource3, brokerResource).asJava, alterResult.values.keySet)
    assertFutureExceptionTypeEquals(alterResult.values.get(topicResource1), classOf[PolicyViolationException])
    alterResult.values.get(topicResource2).get
    assertFutureExceptionTypeEquals(alterResult.values.get(topicResource3), classOf[InvalidConfigurationException])
    assertFutureExceptionTypeEquals(alterResult.values.get(brokerResource), classOf[InvalidRequestException])
    assertTrue(validationsForResource(brokerResource).isEmpty,
      "Should not see the broker resource in the AlterConfig policy when the broker configs are not being updated.")
    validations.clear()

    // Verify that the second resource was updated and the others were not
    ensureConsistentKRaftMetadata()
    var describeResult = client.describeConfigs(Seq(topicResource1, topicResource2, topicResource3, brokerResource).asJava)
    var configs = describeResult.all.get
    assertEquals(4, configs.size)

    assertEquals(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO.toString, configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals(LogConfig.DEFAULT_MIN_IN_SYNC_REPLICAS.toString, configs.get(topicResource1).get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value)

    assertEquals("0.8", configs.get(topicResource2).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)

    assertNull(configs.get(brokerResource).get(KafkaConfig.SslTruststorePasswordProp).value)

    // Alter configs with validateOnly = true: only second is valid
    topicConfigEntries2 = Seq(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.7")).asJava

    alterResult = client.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      brokerResource -> new Config(brokerConfigEntries),
      topicResource3 -> new Config(topicConfigEntries3)
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    assertEquals(Set(topicResource1, topicResource2, topicResource3, brokerResource).asJava, alterResult.values.keySet)
    assertFutureExceptionTypeEquals(alterResult.values.get(topicResource1), classOf[PolicyViolationException])
    alterResult.values.get(topicResource2).get
    assertFutureExceptionTypeEquals(alterResult.values.get(topicResource3), classOf[InvalidConfigurationException])
    assertFutureExceptionTypeEquals(alterResult.values.get(brokerResource), classOf[InvalidRequestException])
    assertTrue(validationsForResource(brokerResource).isEmpty,
      "Should not see the broker resource in the AlterConfig policy when the broker configs are not being updated.")
    validations.clear()

    // Verify that no resources are updated since validate_only = true
    ensureConsistentKRaftMetadata()
    describeResult = client.describeConfigs(Seq(topicResource1, topicResource2, topicResource3, brokerResource).asJava)
    configs = describeResult.all.get
    assertEquals(4, configs.size)

    assertEquals(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO.toString, configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals(LogConfig.DEFAULT_MIN_IN_SYNC_REPLICAS.toString, configs.get(topicResource1).get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value)

    assertEquals("0.8", configs.get(topicResource2).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)

    assertNull(configs.get(brokerResource).get(KafkaConfig.SslTruststorePasswordProp).value)

    // Do an incremental alter config on the broker, ensure we don't see the broker config we set earlier in the policy
    alterResult = client.incrementalAlterConfigs(Map(
      brokerResource ->
        Seq(new AlterConfigOp(
          new ConfigEntry(KafkaConfig.MaxConnectionsProp, "9999"), OpType.SET)
        ).asJavaCollection
    ).asJava)
    alterResult.all.get
    assertEquals(Set(KafkaConfig.MaxConnectionsProp), validationsForResource(brokerResource).head.configs().keySet().asScala)
  }

}

object AdminClientWithPoliciesIntegrationTest {

  val validations = new mutable.ListBuffer[AlterConfigPolicy.RequestMetadata]()

  def validationsForResource(resource: ConfigResource): Seq[AlterConfigPolicy.RequestMetadata] = {
    validations.filter { req => req.resource().equals(resource) }.toSeq
  }

  class Policy extends AlterConfigPolicy {

    var configs: Map[String, _] = _
    var closed = false

    def configure(configs: util.Map[String, _]): Unit = {
      validations.clear()
      this.configs = configs.asScala.toMap
    }

    def validate(requestMetadata: AlterConfigPolicy.RequestMetadata): Unit = {
      validations.append(requestMetadata)
      require(!closed, "Policy should not be closed")
      require(configs.nonEmpty, "configure should have been called with non empty configs")
      require(!requestMetadata.configs.isEmpty, "request configs should not be empty")
      require(requestMetadata.resource.name.nonEmpty, "resource name should not be empty")
      if (requestMetadata.configs.containsKey(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG))
        throw new PolicyViolationException("Min in sync replicas cannot be updated")
    }

    def close(): Unit = closed = true

  }
}
