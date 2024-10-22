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

import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER, CLIENT_METRICS, GROUP, TOPIC}
import org.apache.kafka.common.config.TopicConfig.{REMOTE_LOG_STORAGE_ENABLE_CONFIG, SEGMENT_BYTES_CONFIG, SEGMENT_JITTER_MS_CONFIG, SEGMENT_MS_CONFIG}
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidRequestException, InvalidTopicException}
import org.apache.kafka.coordinator.group.GroupConfig
import org.apache.kafka.server.metrics.ClientMetricsConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.Collections.emptyMap

class ControllerConfigurationValidatorTest {
  val config = new KafkaConfig(TestUtils.createDummyBrokerConfig())
  val validator = new ControllerConfigurationValidator(config)

  @Test
  def testDefaultTopicResourceIsRejected(): Unit = {
    assertEquals("Default topic resources are not allowed.",
        assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(TOPIC, ""), emptyMap(), emptyMap())). getMessage)
  }

  @Test
  def testInvalidTopicNameRejected(): Unit = {
    assertEquals("Topic name is invalid: '(<-invalid->)' contains " +
      "one or more characters other than ASCII alphanumerics, '.', '_' and '-'",
        assertThrows(classOf[InvalidTopicException], () => validator.validate(
          new ConfigResource(TOPIC, "(<-invalid->)"), emptyMap(), emptyMap())). getMessage)
  }

  @Test
  def testUnknownResourceType(): Unit = {
    assertEquals("Unknown resource type BROKER_LOGGER",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER_LOGGER, "foo"), emptyMap(), emptyMap())). getMessage)
  }

  @Test
  def testNullTopicConfigValue(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "10")
    config.put(SEGMENT_BYTES_CONFIG, null)
    config.put(SEGMENT_MS_CONFIG, null)
    assertEquals("Null value not supported for topic configs: segment.bytes,segment.ms",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(TOPIC, "foo"), config, emptyMap())). getMessage)
  }

  @Test
  def testValidTopicConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    config.put(SEGMENT_BYTES_CONFIG, "67108864")
    validator.validate(new ConfigResource(TOPIC, "foo"), config, emptyMap())
  }

  @Test
  def testInvalidTopicConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    config.put(SEGMENT_BYTES_CONFIG, "67108864")
    config.put("foobar", "abc")
    assertEquals("Unknown topic config name: foobar",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(TOPIC, "foo"), config, emptyMap())). getMessage)
  }

  @ParameterizedTest(name = "testDisablingRemoteStorageTopicConfig with wasRemoteStorageEnabled: {0}")
  @ValueSource(booleans = Array(true, false))
  def testDisablingRemoteStorageTopicConfig(wasRemoteStorageEnabled: Boolean): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false")
    if (wasRemoteStorageEnabled) {
      assertEquals("It is invalid to disable remote storage without deleting remote data. " +
        "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
        "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.",
        assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
          new ConfigResource(TOPIC, "foo"), config, util.Collections.singletonMap(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"))).getMessage)
    } else {
      validator.validate(
        new ConfigResource(TOPIC, "foo"), config, util.Collections.emptyMap())
      validator.validate(
        new ConfigResource(TOPIC, "foo"), config, util.Collections.singletonMap(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"))
    }
  }

  @Test
  def testInvalidBrokerEntity(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    assertEquals("Unable to parse broker name as a base 10 number.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER, "blah"), config, emptyMap())). getMessage)
  }

  @Test
  def testInvalidNegativeBrokerId(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    assertEquals("Invalid negative broker ID.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER, "-1"), config, emptyMap())). getMessage)
  }

  @Test
  def testValidClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(ClientMetricsConfigs.INTERVAL_MS_CONFIG, "2000")
    config.put(ClientMetricsConfigs.METRICS_CONFIG, "org.apache.kafka.client.producer.partition.queue.,org.apache.kafka.client.producer.partition.latency")
    config.put(ClientMetricsConfigs.MATCH_CONFIG, "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538,client_id=1" +
      ",client_software_name=apache-kafka-java,client_software_version=2.8.0-SNAPSHOT,client_source_address=127.0.0.1," +
      "client_source_port=1234")
    validator.validate(new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())
  }

  @Test
  def testInvalidSubscriptionNameClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    assertEquals("Subscription name can't be empty",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, ""), config, emptyMap())). getMessage)
  }

  @Test
  def testInvalidIntervalClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(ClientMetricsConfigs.INTERVAL_MS_CONFIG, "10")
    assertEquals("Invalid value 10 for interval.ms, interval must be between 100 and 3600000 (1 hour)",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())). getMessage)

    config.put(ClientMetricsConfigs.INTERVAL_MS_CONFIG, "3600001")
    assertEquals("Invalid value 3600001 for interval.ms, interval must be between 100 and 3600000 (1 hour)",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())). getMessage)
  }

  @Test
  def testUndefinedConfigClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put("random", "10")
    assertEquals("Unknown client metrics configuration: random",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())). getMessage)
  }

  @Test
  def testInvalidMatchClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(ClientMetricsConfigs.MATCH_CONFIG, "10")
    assertEquals("Illegal client matching pattern: 10",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())). getMessage)
  }

  @Test
  def testValidGroupConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000")
    config.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "5000")
    validator.validate(new ConfigResource(GROUP, "group"), config, emptyMap())
  }

  @Test
  def testInvalidGroupNameGroupConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    assertEquals("Default group resources are not allowed.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(GROUP, ""), config, emptyMap())).getMessage)
  }

  @Test
  def testNullGroupConfigValue(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000")
    config.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, null)
    assertEquals("Null value not supported for group configs: consumer.heartbeat.interval.ms",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(GROUP, "group"), config, emptyMap())).getMessage)
  }

  @Test
  def testInvalidGroupConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000")
    config.put("foobar", "abc")
    assertEquals("Unknown group config name: foobar",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(GROUP, "group"), config, emptyMap())).getMessage)
  }
}
