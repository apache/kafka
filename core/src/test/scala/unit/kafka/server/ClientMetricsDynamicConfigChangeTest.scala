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

package unit.kafka.server

import java.util.Properties
import kafka.integration.KafkaServerTestHarness

import kafka.metrics.ClientMetricsTestUtils
import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMatchingParams.{CLIENT_SOFTWARE_NAME, CLIENT_SOFTWARE_VERSION}
import kafka.metrics.clientmetrics.ClientMetricsConfig

import kafka.utils._
import kafka.server.KafkaConfig

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.config.ConfigResource

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.mutable.ListBuffer
import scala.collection.Map
import scala.jdk.CollectionConverters._

class ClientMetricsDynamicConfigChangeTest extends KafkaServerTestHarness {
  def generateConfigs = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnectOrNull)))

  private def createAdminClient(): Admin = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    Admin.create(props)
  }

  private def updateClientSubscription(subscriptionId :String,
                                       properties: Properties,
                                       waitingCondition: () => Boolean): Unit = {
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionId)
      val entries = new ListBuffer[AlterConfigOp]
      properties.forEach((k, v) => {
        entries.append(new AlterConfigOp(new ConfigEntry(k.toString, v.toString), AlterConfigOp.OpType.SET))
      })
      admin.incrementalAlterConfigs(Map(resource -> entries.asJavaCollection).asJava).all.get
    } finally {
      admin.close()
    }

    // Wait until notification is delivered and processed
    val maxWaitTime = 60 * 1000 // 1 minute wait time
    TestUtils.waitUntilTrue(waitingCondition, "Failed to update the client metrics subscription", maxWaitTime)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testClientMetricsConfigChange(quorum: String): Unit = {
    val configEntityName: String = "subscription-2"
    val metrics =
      "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
    val pushInterval = 30 * 1000 // 30 seconds
    val pattern1 = CLIENT_SOFTWARE_NAME + " = Java       "
    val pattern2 = CLIENT_SOFTWARE_VERSION + " =     11.*   "
    val patternsList = List(pattern1, pattern2)

    val props = ClientMetricsTestUtils.getDefaultProperties()

    // ********  Test Create the new client subscription with multiple client matching patterns *********
    updateClientSubscription(configEntityName, props,
      () =>  ClientMetricsConfig.getClientSubscriptionInfo(configEntityName) != null)
    val cmSubscription = ClientMetricsConfig.getClientSubscriptionInfo(configEntityName)
    assertTrue(cmSubscription.getPushIntervalMs == pushInterval)
    assertTrue(cmSubscription.getSubscribedMetrics.size == 2 &&
      cmSubscription.getSubscribedMetrics.mkString(",").equals(metrics))
    assertTrue(cmSubscription.getClientMatchingPatterns.size == patternsList.size)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testClientMetricsConfigMultipleUpdates(quorum: String): Unit = {
    val configEntityName: String = "subscription-1"
    val metrics =
      "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
    val clientMatchingPattern = "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538"
    val pushInterval = 30 * 1000 // 30 seconds

    val props = ClientMetricsTestUtils.getDefaultProperties()
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchingPattern)

    // ********  Test-1 Create the new client subscription *********
    updateClientSubscription(configEntityName, props,
      () => ClientMetricsConfig.getClientSubscriptionInfo(configEntityName) != null)
    val cmSubscription = ClientMetricsConfig.getClientSubscriptionInfo(configEntityName)
    assertTrue(cmSubscription.getPushIntervalMs == pushInterval)
    assertTrue(cmSubscription.getSubscribedMetrics.size == 2 &&
      cmSubscription.getSubscribedMetrics.mkString(",").equals(metrics))
    val res = cmSubscription.getClientMatchingPatterns.mkString(",").replace(" -> ", "=")
    assertTrue(cmSubscription.getClientMatchingPatterns.size == 1 && res.equals(clientMatchingPattern))

    // *******  Test-2 Update the existing metric subscriptions  *********
    val updatedMetrics = "org.apache.kafka/client.producer.partition.latency"
    props.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, updatedMetrics)
    updateClientSubscription(configEntityName, props,
      () => ClientMetricsConfig.getClientSubscriptionInfo(configEntityName).getSubscribedMetrics.size == 1)
    assertTrue(ClientMetricsConfig.getClientSubscriptionInfo(configEntityName)
      .getSubscribedMetrics.mkString(",").equals(updatedMetrics))

    // *******  Test-3 Delete the metric subscriptions  *********
    props.put(ClientMetricsConfig.ClientMetrics.DeleteSubscription, "true")
    updateClientSubscription(configEntityName, props,
      () => ClientMetricsConfig.getClientSubscriptionInfo(configEntityName) == null)
    assertTrue(ClientMetricsConfig.getClientSubscriptionInfo(configEntityName) == null)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk"))
  // TODO: needs to figure out how to run the same test in kraft mode.
  def testClientMetricsRestartServer(quorum: String): Unit = {
    val configEntityName: String = "subscription-1"
    val metrics =
      "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
    val clientMatchingPattern = "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538"
    val pushInterval = 30 * 1000 // 30 seconds

    val props = ClientMetricsTestUtils.getDefaultProperties()
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchingPattern)

    // Create the new client subscription
    updateClientSubscription(configEntityName, props,
      () => ClientMetricsConfig.getClientSubscriptionInfo(configEntityName) != null)
    val cmSubscription = ClientMetricsConfig.getClientSubscriptionInfo(configEntityName)
    assertTrue(cmSubscription.getPushIntervalMs == pushInterval)
    assertTrue(cmSubscription.getSubscribedMetrics.size == 2 &&
      cmSubscription.getSubscribedMetrics.mkString(",").equals(metrics))
    val res = cmSubscription.getClientMatchingPatterns.mkString(",").replace(" -> ", "=")
    assertTrue(cmSubscription.getClientMatchingPatterns.size == 1 && res.equals(clientMatchingPattern))

    // Restart the server and make sure that client metric subscription data is intact
    ClientMetricsConfig.clearClientSubscriptions()
    assertTrue(ClientMetricsConfig.getSubscriptionsCount == 0)
    val server = servers.head
    server.shutdown()
    server.startup()
    assertTrue(ClientMetricsConfig.getSubscriptionsCount != 0)
    assertTrue(ClientMetricsConfig.getClientSubscriptionInfo(configEntityName) != null)
    assertTrue(ClientMetricsConfig.getClientSubscriptionInfo(configEntityName)
      .getSubscribedMetrics.mkString(",").equals(metrics))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  // TODO: needs to figure out how to run the same test in kraft mode.
  def testAllMetrics(quorum: String): Unit = {
    val configEntityName: String = "subscription-4"
    val pushInterval = 5 * 1000 // 5 seconds

    val props = new Properties()
    props.put(ClientMetricsConfig.ClientMetrics.AllMetricsFlag, "true")
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, pushInterval.toString)

    // ********  Test Create the new client subscription with all metrics *********
    updateClientSubscription(configEntityName, props,
      () =>  ClientMetricsConfig.getClientSubscriptionInfo(configEntityName) != null)
    val cmSubscription = ClientMetricsConfig.getClientSubscriptionInfo(configEntityName)
    assertTrue(cmSubscription.getPushIntervalMs == pushInterval)
    assertTrue(cmSubscription.getAllMetricsSubscribed)
    assertTrue(cmSubscription.getSubscribedMetrics.size == 1)
    assertTrue(cmSubscription.getSubscribedMetrics.mkString(",").equals(""))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  // TODO: needs to figure out how to run the same test in kraft mode.
  def testClientMetricsDefaults(quorum: String): Unit = {
    val configEntityName: String = "subscription-5"

    // We create a mostly blank properties object. This should work and will assume all metrics,
    // all clients (no filtering), and a default push interval.
    val props = new Properties()
    props.put(ClientMetricsConfig.ClientMetrics.AllMetricsFlag, "true")

    // ********  Test Create the new client subscription with all metrics *********
    updateClientSubscription(configEntityName, props,
      () =>  ClientMetricsConfig.getClientSubscriptionInfo(configEntityName) != null)
    val cmSubscription = ClientMetricsConfig.getClientSubscriptionInfo(configEntityName)
    assertTrue(cmSubscription.getClientMatchingPatterns.isEmpty)
    assertTrue(cmSubscription.getPushIntervalMs == ClientMetricsConfig.ClientMetrics.DEFAULT_PUSH_INTERVAL)
    assertTrue(cmSubscription.getAllMetricsSubscribed)
    assertTrue(cmSubscription.getSubscribedMetrics.size == 1)
    assertTrue(cmSubscription.getSubscribedMetrics.mkString(",").equals(""))
  }

}
