/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig.FlushMessagesProp
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}

import java.util.Properties
import scala.collection.Map
import kafka.utils.{Json, TestUtils}
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.nio.charset.StandardCharsets
import scala.collection.Seq
import scala.jdk.CollectionConverters._

/**
 * This test is testing DynamicConfigManager. For DynamicConfig change tests,
 * Please see [[kafka.server.DynamicConfigChangeTest]]
 */
class DynamicConfigManagerTest extends KafkaServerTestHarness {
  private val changeExpirationMs = 1000
  private val retryWaitingMs = 10000
  private val mockTime = new MockTime()
  // the 1st change notification node will be created
  private val notificationNode = "/config/changes/config_change_0000000000"
  private var dynamicConfigManager: DynamicConfigManager = _

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    assertFalse(zkClient.pathExists(notificationNode), "The first change notification node should not exist")
    assertTrue(this.servers.head.dynamicConfigHandlers.contains(ConfigType.Topic),
      "Should contain a ConfigHandler for topics")

    dynamicConfigManager = new DynamicConfigManager(zkClient,
      Map(ConfigType.Topic -> this.servers.head.dynamicConfigHandlers.get(ConfigType.Topic).get), changeExpirationMs, mockTime)

    dynamicConfigManager.startup()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (dynamicConfigManager != null) {
      dynamicConfigManager.shutdown()
    }
    super.tearDown()
  }

  @Test
  def shouldPurgeNodeWhenChangeExpired(): Unit = {
    mockTime.setCurrentTimeMs(System.currentTimeMillis())

    val oldVal = 100000L
    val newVal = 200000L
    val tp = new TopicPartition("test", 0)
    val logProps = new Properties()
    logProps.put(FlushMessagesProp, oldVal.toString)
    createTopic(tp.topic, 1, 1, logProps)

    // make sure the topic config is set into zookeeper
    TestUtils.retry(retryWaitingMs) {
      val logOpt = this.servers.head.logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldVal, logOpt.get.config.flushInterval)
    }

    logProps.put(FlushMessagesProp, newVal.toString)
    // update the topic config
    adminZkClient.changeTopicConfig(tp.topic, logProps)

    // make sure the topic config is updated and the change notification node is created
    TestUtils.retry(retryWaitingMs) {
      assertEquals(newVal, this.servers.head.logManager.getLog(tp).get.config.flushInterval)
      assertTrue(zkClient.pathExists(notificationNode), "The first change notification node should be created")
    }

    // advance half of changeExpirationMs
    mockTime.sleep(changeExpirationMs / 2)

    logProps.put(FlushMessagesProp, oldVal.toString)
    // update the topic config 2nd time
    adminZkClient.changeTopicConfig(tp.topic, logProps)
    // make sure the previous change notification node still existed and new one is created
    TestUtils.retry(retryWaitingMs) {
      assertEquals(oldVal, this.servers.head.logManager.getLog(tp).get.config.flushInterval)
      assertTrue(zkClient.pathExists(notificationNode), "The first change notification node should still exist")
    }

    // advance the time to exceed changeExpirationMs
    mockTime.sleep(changeExpirationMs)

    logProps.put(FlushMessagesProp, newVal.toString)
    // update the topic config again
    adminZkClient.changeTopicConfig(tp.topic, logProps)
    // make sure the previous change notification nodes are purged due to expiration
    TestUtils.retry(retryWaitingMs) {
      assertEquals(newVal, this.servers.head.logManager.getLog(tp).get.config.flushInterval)
      assertFalse(zkClient.pathExists(notificationNode), "The first change notification node should be purged")
    }
  }

  @Test
  def shouldProcessNotification(): Unit = {
    // Ignore non-json notifications because they can be from the deprecated TopicConfigManager
    dynamicConfigManager.ConfigChangedNotificationHandler.processNotification("not json".getBytes(StandardCharsets.UTF_8))

    // Incorrect Map. No version
    var jsonMap: Map[String, Any] = Map("v" -> 1, "x" -> 2)

    assertThrows(classOf[Throwable], () =>
      dynamicConfigManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))

    // Version is provided with unsupported version
    jsonMap = Map("version" -> 3, "entity_type" -> "garbage", "entity_name" -> "x")
    assertThrows(classOf[Throwable], () =>
      dynamicConfigManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))

    // Version is provided. EntityType is incorrect
    jsonMap = Map("version" -> 1, "entity_type" -> "garbage", "entity_name" -> "x")
    assertThrows(classOf[Throwable], () =>
      dynamicConfigManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))

    // EntityName isn't provided
    jsonMap = Map("version" -> 1, "entity_type" -> ConfigType.Topic)
    assertThrows(classOf[Throwable], () =>
      dynamicConfigManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))

    // Everything is provided for version 1
    jsonMap = Map("version" -> 1, "entity_type" -> ConfigType.Topic, "entity_name" -> "x")
    dynamicConfigManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava))

    // EntityPath isn't provided for version 2
    jsonMap = Map("version" -> 2, "entity_type" -> ConfigType.Topic, "entity_name" -> "x")
    assertThrows(classOf[Throwable], () =>
      dynamicConfigManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))

    // EntityPath is incorrect for version 2
    jsonMap = Map("version" -> 2, "entity_type" -> ConfigType.Topic, "entity_name" -> "x", "entity_path" -> "topics/")
    assertThrows(classOf[Throwable], () =>
      dynamicConfigManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))

    // EntityPath is incorrect for version 2
    jsonMap = Map("version" -> 2, "entity_type" -> ConfigType.Topic, "entity_name" -> "x", "entity_path" -> "/")
    assertThrows(classOf[Throwable], () =>
      dynamicConfigManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))

    // Everything is provided version 2
    jsonMap = Map("version" -> 2, "entity_type" -> ConfigType.Topic, "entity_name" -> "x", "entity_path" -> "topics/test")
    dynamicConfigManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava))
  }

  override def generateConfigs: Seq[KafkaConfig] =
    List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))
}