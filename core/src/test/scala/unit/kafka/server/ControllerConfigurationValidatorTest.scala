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

import kafka.metrics.clientmetrics.ClientMetricsConfig

import java.util.TreeMap
import java.util.Collections.emptyMap
import org.junit.jupiter.api.Test
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER, CLIENT_METRICS, TOPIC}
import org.apache.kafka.common.config.TopicConfig.{SEGMENT_BYTES_CONFIG, SEGMENT_JITTER_MS_CONFIG, SEGMENT_MS_CONFIG}
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidRequestException, InvalidTopicException}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}

class ControllerConfigurationValidatorTest {
  val validator = new ControllerConfigurationValidator()

  @Test
  def testDefaultTopicResourceIsRejected(): Unit = {
    assertEquals("Default topic resources are not allowed.",
        assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(TOPIC, ""), emptyMap())). getMessage())
  }

  @Test
  def testInvalidTopicNameRejected(): Unit = {
    assertEquals("Topic name \"(<-invalid->)\" is illegal, it contains a character " +
      "other than ASCII alphanumerics, '.', '_' and '-'",
        assertThrows(classOf[InvalidTopicException], () => validator.validate(
          new ConfigResource(TOPIC, "(<-invalid->)"), emptyMap())). getMessage())
  }

  @Test
  def testUnknownResourceType(): Unit = {
    assertEquals("Unknown resource type BROKER_LOGGER",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER_LOGGER, "foo"), emptyMap())). getMessage())
  }

  @Test
  def testNullTopicConfigValue(): Unit = {
    val config = new TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "10")
    config.put(SEGMENT_BYTES_CONFIG, null)
    config.put(SEGMENT_MS_CONFIG, null)
    assertEquals("Null value not supported for topic configs: segment.bytes,segment.ms",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(TOPIC, "foo"), config)). getMessage())
  }

  @Test
  def testValidTopicConfig(): Unit = {
    val config = new TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    config.put(SEGMENT_BYTES_CONFIG, "67108864")
    validator.validate(new ConfigResource(TOPIC, "foo"), config)
  }

  @Test
  def testInvalidTopicConfig(): Unit = {
    val config = new TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    config.put(SEGMENT_BYTES_CONFIG, "67108864")
    config.put("foobar", "abc")
    assertEquals("Unknown topic config name: foobar",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(TOPIC, "foo"), config)). getMessage())
  }

  @Test
  def testInvalidBrokerEntity(): Unit = {
    val config = new TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    assertEquals("Unable to parse broker name as a base 10 number.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER, "blah"), config)). getMessage())
  }

  @Test
  def testInvalidNegativeBrokerId(): Unit = {
    val config = new TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    assertEquals("Invalid negative broker ID.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER, "-1"), config)). getMessage())
  }

  @Test
  def testClientMetricSubscription() = {
    val groupName: String = "subscription-1"
    val metrics = "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
    val clientMatchingPattern = "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538"

    val validator = new ControllerConfigurationValidator()
    val props = new TreeMap[String, String]
    val resource = new ConfigResource(CLIENT_METRICS, groupName)

    // Test-1: test the missing parameters,
    // add one after one until all the required params are added
    assertThrows(classOf[IllegalArgumentException], () => validator.validate(resource, props))

    val interval = -1
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, interval.toString)
    assertThrows(classOf[IllegalArgumentException], () =>
      validator.validate(resource, props))

    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, 2000.toString)
    assertThrows(classOf[IllegalArgumentException], () => validator.validate(resource, props))

    props.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics)
    validator.validate(resource, props)

    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, "client_software_name=*")
    assertThrows(classOf[InvalidConfigurationException], () => validator.validate(resource, props))

    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchingPattern)
    validator.validate(resource, props)

    // TEST-2 Add an invalid parameter
    props.put("INVALID_PARAMETER", "INVALID_ARGUMENT")
    assertThrows(classOf[IllegalArgumentException], () => validator.validate(resource, props))
    props.remove("INVALID_PARAMETER")
    validator.validate(resource, props)

    // TEST-3: Delete the metric subscription
    props.clear()
    props.put(ClientMetricsConfig.ClientMetrics.DeleteSubscription, "true")
    validator.validate(resource, props)

    // TEST-4: subscription with all metrics flag
    props.clear()
    props.put(ClientMetricsConfig.ClientMetrics.AllMetricsFlag, "true")
    assertThrows(classOf[IllegalArgumentException], () => validator.validate(resource, props))

    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchingPattern)
    assertThrows(classOf[IllegalArgumentException], () => validator.validate(resource, props))

    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, 2000.toString)
    validator.validate(resource, props)
  }

}

