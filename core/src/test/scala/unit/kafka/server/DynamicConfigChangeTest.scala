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

import org.apache.kafka.common.protocol.ApiKeys
import org.junit.Assert._
import org.apache.kafka.common.metrics.Quota
import org.easymock.{Capture, EasyMock}
import org.junit.Test
import kafka.integration.KafkaServerTestHarness
import kafka.utils._
import kafka.common._
import kafka.log.LogConfig
import kafka.admin.{AdminOperationException, AdminUtils}

import scala.collection.Map

class DynamicConfigChangeTest extends KafkaServerTestHarness {
  def generateConfigs() = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))

  @Test
  def testConfigChange() {
    assertTrue("Should contain a ConfigHandler for topics",
               this.servers(0).dynamicConfigHandlers.contains(ConfigType.Topic))
    val oldVal: java.lang.Long = 100000L
    val newVal: java.lang.Long = 200000L
    val tp = TopicAndPartition("test", 0)
    val logProps = new Properties()
    logProps.put(LogConfig.FlushMessagesProp, oldVal.toString)
    AdminUtils.createTopic(zkUtils, tp.topic, 1, 1, logProps)
    TestUtils.retry(10000) {
      val logOpt = this.servers(0).logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldVal, logOpt.get.config.flushInterval)
    }
    logProps.put(LogConfig.FlushMessagesProp, newVal.toString)
    AdminUtils.changeTopicConfig(zkUtils, tp.topic, logProps)
    TestUtils.retry(10000) {
      assertEquals(newVal, this.servers(0).logManager.getLog(tp).get.config.flushInterval)
    }
  }

  @Test
  def testClientQuotaConfigChange() {
    assertTrue("Should contain a ConfigHandler for topics",
               this.servers(0).dynamicConfigHandlers.contains(ConfigType.Client))
    val clientId = "testClient"
    val props = new Properties()
    props.put(ClientConfigOverride.ProducerOverride, "1000")
    props.put(ClientConfigOverride.ConsumerOverride, "2000")
    AdminUtils.changeClientIdConfig(zkUtils, clientId, props)

    TestUtils.retry(10000) {
      val configHandler = this.servers(0).dynamicConfigHandlers(ConfigType.Client).asInstanceOf[ClientIdConfigHandler]
      val quotaManagers: Map[Short, ClientQuotaManager] = servers(0).apis.quotaManagers
      val overrideProducerQuota = quotaManagers.get(ApiKeys.PRODUCE.id).get.quota(clientId)
      val overrideConsumerQuota = quotaManagers.get(ApiKeys.FETCH.id).get.quota(clientId)

      assertEquals(s"ClientId $clientId must have overridden producer quota of 1000",
        Quota.upperBound(1000), overrideProducerQuota)
        assertEquals(s"ClientId $clientId must have overridden consumer quota of 2000",
        Quota.upperBound(2000), overrideConsumerQuota)
    }
  }

  @Test
  def testConfigChangeOnNonExistingTopic() {
    val topic = TestUtils.tempTopic
    try {
      val logProps = new Properties()
      logProps.put(LogConfig.FlushMessagesProp, 10000: java.lang.Integer)
      AdminUtils.changeTopicConfig(zkUtils, topic, logProps)
      fail("Should fail with AdminOperationException for topic doesn't exist")
    } catch {
      case e: AdminOperationException => // expected
    }
  }

  @Test
  def testProcessNotification {
    val props = new Properties()
    props.put("a.b", "10")

    // Create a mock ConfigHandler to record config changes it is asked to process
    val entityArgument = EasyMock.newCapture[String]
    val propertiesArgument = EasyMock.newCapture[Properties]
    val handler = EasyMock.createNiceMock(classOf[ConfigHandler])
    handler.processConfigChanges(
      EasyMock.and(EasyMock.capture(entityArgument), EasyMock.isA(classOf[String])),
      EasyMock.and(EasyMock.capture(propertiesArgument), EasyMock.isA(classOf[Properties])))
    EasyMock.expectLastCall().once()
    EasyMock.replay(handler)

    val configManager = new DynamicConfigManager(zkUtils, Map(ConfigType.Topic -> handler))
    // Notifications created using the old TopicConfigManager are ignored.
    configManager.ConfigChangedNotificationHandler.processNotification("not json")

    // Incorrect Map. No version
    try {
      val jsonMap = Map("v" -> 1, "x" -> 2)
      configManager.ConfigChangedNotificationHandler.processNotification(Json.encode(jsonMap))
      fail("Should have thrown an Exception while parsing incorrect notification " + jsonMap)
    }
    catch {
      case t: Throwable =>
    }
    // Version is provided. EntityType is incorrect
    try {
      val jsonMap = Map("version" -> 1, "entity_type" -> "garbage", "entity_name" -> "x")
      configManager.ConfigChangedNotificationHandler.processNotification(Json.encode(jsonMap))
      fail("Should have thrown an Exception while parsing incorrect notification " + jsonMap)
    }
    catch {
      case t: Throwable =>
    }

    // EntityName isn't provided
    try {
      val jsonMap = Map("version" -> 1, "entity_type" -> ConfigType.Topic)
      configManager.ConfigChangedNotificationHandler.processNotification(Json.encode(jsonMap))
      fail("Should have thrown an Exception while parsing incorrect notification " + jsonMap)
    }
    catch {
      case t: Throwable =>
    }

    // Everything is provided
    val jsonMap = Map("version" -> 1, "entity_type" -> ConfigType.Topic, "entity_name" -> "x")
    configManager.ConfigChangedNotificationHandler.processNotification(Json.encode(jsonMap))

    // Verify that processConfigChanges was only called once
    EasyMock.verify(handler)
  }
}
