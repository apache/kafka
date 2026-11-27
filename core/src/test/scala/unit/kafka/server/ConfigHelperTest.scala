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
package kafka.server

import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResourceResult
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigSource
import org.apache.kafka.metadata.{MetadataCache, MockConfigRepository}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{mock, when}

import scala.jdk.CollectionConverters._

class ConfigHelperTest {

  private val topic = "test-topic"

  private def props(kvs: (String, String)*): Properties = {
    val p = new Properties()
    kvs.foreach { case (k, v) => p.put(k, v) }
    p
  }

  private def defaultBrokerConfig(): KafkaConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(0))

  private def layeredBrokerConfig(): KafkaConfig = {
    val staticProps = TestUtils.createBrokerConfig(0)
    staticProps.put("log.roll.hours", "2")
    staticProps.put("log.retention.minutes", "30")
    staticProps.put("log.segment.bytes", "1048576")
    staticProps.put("log.cleanup.policy", "compact")
    staticProps.put("min.insync.replicas", "2")
    val config = KafkaConfig.fromProps(staticProps)
    config.dynamicConfig.initialize(None)
    config.dynamicConfig.updateDefaultConfig(props(
      "log.retention.ms" -> "7200000",
      "log.roll.ms" -> "1800000"))
    config.dynamicConfig.updateBrokerConfig(0, props(
      "log.retention.ms" -> "3600000",
      "log.segment.bytes" -> "4194304"))
    config
  }

  private def staticBrokerConfig(staticConfigs: (String, String)*): KafkaConfig = {
    val staticProps = TestUtils.createBrokerConfig(0)
    staticConfigs.foreach { case (k, v) => staticProps.put(k, v) }
    KafkaConfig.fromProps(staticProps)
  }

  private def describeTopic(config: KafkaConfig, topicConfigs: (String, String)*): Map[String, DescribeConfigsResourceResult] = {
    val repository = new MockConfigRepository()
    topicConfigs.foreach { case (k, v) => repository.setTopicConfig(topic, k, v) }
    val metadataCache = mock(classOf[MetadataCache])
    when(metadataCache.contains(anyString())).thenReturn(true)
    val resource = new DescribeConfigsResource().setResourceType(ConfigResource.Type.TOPIC.id).setResourceName(topic)
    val result = new ConfigHelper(metadataCache, config, repository)
      .describeConfigs(List(resource), includeSynonyms = true, includeDocumentation = true).head
    assertEquals(Errors.NONE.code, result.errorCode)
    result.configs.asScala.map(c => c.name -> c).toMap
  }

  private def assertConfig(configs: Map[String, DescribeConfigsResourceResult],
                           name: String,
                           value: String,
                           source: ConfigSource,
                           synonyms: (String, String, ConfigSource)*): Unit = {
    val config = configs(name)
    assertEquals(value, config.value, s"value of $name")
    assertEquals(source.id, config.configSource, s"source of $name")
    assertEquals(synonyms.map { case (n, v, s) => (n, v, s.id) }.toList,
      config.synonyms.asScala.map(s => (s.name, s.value, s.source)).toList, s"synonyms of $name")
    assertNotNull(config.documentation, s"documentation of $name")
  }

  @Test
  def testDescribeTopicConfigsWithDefaultBrokerConfig(): Unit = {
    val configs = describeTopic(defaultBrokerConfig())
    assertConfig(configs, "retention.ms", "604800000", ConfigSource.DEFAULT_CONFIG,
      ("log.retention.hours", "168", ConfigSource.DEFAULT_CONFIG))
    assertConfig(configs, "segment.bytes", "1073741824", ConfigSource.DEFAULT_CONFIG,
      ("log.segment.bytes", "1073741824", ConfigSource.DEFAULT_CONFIG))
    assertConfig(configs, "min.insync.replicas", "1", ConfigSource.DEFAULT_CONFIG,
      ("min.insync.replicas", "1", ConfigSource.DEFAULT_CONFIG))
  }

  @Test
  def testDescribeTopicConfigsResolvesBrokerLayers(): Unit = {
    val configs = describeTopic(layeredBrokerConfig())
    assertConfig(configs, "retention.ms", "3600000", ConfigSource.DYNAMIC_BROKER_CONFIG,
      ("log.retention.ms", "3600000", ConfigSource.DYNAMIC_BROKER_CONFIG),
      ("log.retention.ms", "7200000", ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
      ("log.retention.minutes", "30", ConfigSource.STATIC_BROKER_CONFIG),
      ("log.retention.hours", "168", ConfigSource.DEFAULT_CONFIG))
    assertConfig(configs, "segment.ms", "1800000", ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
      ("log.roll.ms", "1800000", ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
      ("log.roll.hours", "2", ConfigSource.STATIC_BROKER_CONFIG),
      ("log.roll.hours", "168", ConfigSource.DEFAULT_CONFIG))
    assertConfig(configs, "segment.bytes", "4194304", ConfigSource.DYNAMIC_BROKER_CONFIG,
      ("log.segment.bytes", "4194304", ConfigSource.DYNAMIC_BROKER_CONFIG),
      ("log.segment.bytes", "1048576", ConfigSource.STATIC_BROKER_CONFIG),
      ("log.segment.bytes", "1073741824", ConfigSource.DEFAULT_CONFIG))
    assertConfig(configs, "min.insync.replicas", "2", ConfigSource.STATIC_BROKER_CONFIG,
      ("min.insync.replicas", "2", ConfigSource.STATIC_BROKER_CONFIG),
      ("min.insync.replicas", "1", ConfigSource.DEFAULT_CONFIG))
  }

  @Test
  def testDescribeTopicConfigsWithTopicOverrides(): Unit = {
    val configs = describeTopic(layeredBrokerConfig(),
      "cleanup.policy" -> "delete, compact",
      "retention.ms" -> "0100",
      "unclean.leader.election.enable" -> "TRUE")
    assertConfig(configs, "cleanup.policy", "delete,compact", ConfigSource.TOPIC_CONFIG,
      ("cleanup.policy", "delete,compact", ConfigSource.TOPIC_CONFIG),
      ("log.cleanup.policy", "compact", ConfigSource.STATIC_BROKER_CONFIG),
      ("log.cleanup.policy", "delete", ConfigSource.DEFAULT_CONFIG))
    assertConfig(configs, "retention.ms", "100", ConfigSource.TOPIC_CONFIG,
      ("retention.ms", "100", ConfigSource.TOPIC_CONFIG),
      ("log.retention.ms", "3600000", ConfigSource.DYNAMIC_BROKER_CONFIG),
      ("log.retention.ms", "7200000", ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
      ("log.retention.minutes", "30", ConfigSource.STATIC_BROKER_CONFIG),
      ("log.retention.hours", "168", ConfigSource.DEFAULT_CONFIG))
    assertConfig(configs, "unclean.leader.election.enable", "true", ConfigSource.TOPIC_CONFIG,
      ("unclean.leader.election.enable", "true", ConfigSource.TOPIC_CONFIG),
      ("unclean.leader.election.enable", "false", ConfigSource.DEFAULT_CONFIG))
  }

  @Test
  def testDescribeTopicConfigsKeepsSynonymsWhenHighestPrioritySynonymIsUnset(): Unit = {
    val configs = describeTopic(staticBrokerConfig(
      "log.retention.minutes" -> "30",
      "log.roll.hours" -> "2"))
    assertConfig(configs, "retention.ms", "1800000", ConfigSource.STATIC_BROKER_CONFIG,
      ("log.retention.minutes", "30", ConfigSource.STATIC_BROKER_CONFIG),
      ("log.retention.hours", "168", ConfigSource.DEFAULT_CONFIG))
    assertConfig(configs, "segment.ms", "7200000", ConfigSource.STATIC_BROKER_CONFIG,
      ("log.roll.hours", "2", ConfigSource.STATIC_BROKER_CONFIG),
      ("log.roll.hours", "168", ConfigSource.DEFAULT_CONFIG))
  }

  @Test
  def testDescribeTopicConfigsFirstSynonymMatchesConfigSource(): Unit = {
    val brokerConfigs = Seq(
      defaultBrokerConfig(),
      layeredBrokerConfig(),
      staticBrokerConfig("log.retention.minutes" -> "30", "log.roll.hours" -> "2"))
    brokerConfigs.foreach { brokerConfig =>
      Seq(describeTopic(brokerConfig), describeTopic(brokerConfig, "retention.ms" -> "100")).foreach { configs =>
        configs.values.foreach { config =>
          val expectedSource = config.synonyms.asScala.headOption.map(_.source)
            .getOrElse(ConfigSource.DEFAULT_CONFIG.id)
          assertEquals(expectedSource, config.configSource, s"source of ${config.name}")
        }
      }
    }
  }

  @Test
  def testDescribeTopicConfigsIncludesInternalConfigOnlyWhenSet(): Unit = {
    assertFalse(describeTopic(defaultBrokerConfig()).contains("internal.segment.bytes"))

    val configs = describeTopic(defaultBrokerConfig(), "internal.segment.bytes" -> "1048576")
    assertConfig(configs, "internal.segment.bytes", "1048576", ConfigSource.TOPIC_CONFIG,
      ("internal.segment.bytes", "1048576", ConfigSource.TOPIC_CONFIG))
  }
}
