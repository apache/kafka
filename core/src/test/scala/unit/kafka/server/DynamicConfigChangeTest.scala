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

import java.nio.charset.StandardCharsets
import java.util.Properties

import kafka.log.LogConfig._
import kafka.server.Constants._
import org.junit.Assert._
import org.apache.kafka.common.metrics.Quota
import org.easymock.EasyMock
import org.junit.Test
import kafka.integration.KafkaServerTestHarness
import kafka.utils._
import kafka.admin.AdminOperationException
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.common.TopicPartition

import scala.collection.Map
import scala.collection.JavaConverters._

class DynamicConfigChangeTest extends KafkaServerTestHarness {
  def generateConfigs = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))

  @Test
  def testConfigChange(): Unit = {
    assertTrue("Should contain a ConfigHandler for topics",
      this.servers.head.dynamicConfigHandlers.contains(ConfigType.Topic))
    val oldVal: java.lang.Long = 100000L
    val newVal: java.lang.Long = 200000L
    val tp = new TopicPartition("test", 0)
    val logProps = new Properties()
    logProps.put(FlushMessagesProp, oldVal.toString)
    adminZkClient.createTopic(tp.topic, 1, 1, logProps)
    TestUtils.retry(10000) {
      val logOpt = this.servers.head.logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldVal, logOpt.get.config.flushInterval)
    }
    logProps.put(FlushMessagesProp, newVal.toString)
    adminZkClient.changeTopicConfig(tp.topic, logProps)
    TestUtils.retry(10000) {
      assertEquals(newVal, this.servers.head.logManager.getLog(tp).get.config.flushInterval)
    }
  }

  private def testQuotaConfigChange(user: String, clientId: String, rootEntityType: String, configEntityName: String): Unit = {
    assertTrue("Should contain a ConfigHandler for " + rootEntityType ,
               this.servers.head.dynamicConfigHandlers.contains(rootEntityType))
    val props = new Properties()
    props.put(DynamicConfig.Client.ProducerByteRateOverrideProp, "1000")
    props.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, "2000")

    val quotaManagers = servers.head.apis.quotas
    rootEntityType match {
      case ConfigType.Client => adminZkClient.changeClientIdConfig(configEntityName, props)
      case _ => adminZkClient.changeUserOrUserClientIdConfig(configEntityName, props)
    }

    TestUtils.retry(10000) {
      val overrideProducerQuota = quotaManagers.produce.quota(user, clientId)
      val overrideConsumerQuota = quotaManagers.fetch.quota(user, clientId)

      assertEquals(s"User $user clientId $clientId must have overridden producer quota of 1000",
        Quota.upperBound(1000), overrideProducerQuota)
      assertEquals(s"User $user clientId $clientId must have overridden consumer quota of 2000",
        Quota.upperBound(2000), overrideConsumerQuota)
    }

    val defaultProducerQuota = Long.MaxValue.asInstanceOf[Double]
    val defaultConsumerQuota = Long.MaxValue.asInstanceOf[Double]

    val emptyProps = new Properties()
    rootEntityType match {
      case ConfigType.Client => adminZkClient.changeClientIdConfig(configEntityName, emptyProps)
      case _ => adminZkClient.changeUserOrUserClientIdConfig(configEntityName, emptyProps)
    }
    TestUtils.retry(10000) {
      val producerQuota = quotaManagers.produce.quota(user, clientId)
      val consumerQuota = quotaManagers.fetch.quota(user, clientId)

      assertEquals(s"User $user clientId $clientId must have reset producer quota to " + defaultProducerQuota,
        Quota.upperBound(defaultProducerQuota), producerQuota)
      assertEquals(s"User $user clientId $clientId must have reset consumer quota to " + defaultConsumerQuota,
        Quota.upperBound(defaultConsumerQuota), consumerQuota)
    }
  }

  @Test
  def testClientIdQuotaConfigChange(): Unit = {
    testQuotaConfigChange("ANONYMOUS", "testClient", ConfigType.Client, "testClient")
  }

  @Test
  def testUserQuotaConfigChange(): Unit = {
    testQuotaConfigChange("ANONYMOUS", "testClient", ConfigType.User, "ANONYMOUS")
  }

  @Test
  def testUserClientIdQuotaChange(): Unit = {
    testQuotaConfigChange("ANONYMOUS", "testClient", ConfigType.User, "ANONYMOUS/clients/testClient")
  }

  @Test
  def testDefaultClientIdQuotaConfigChange(): Unit = {
    testQuotaConfigChange("ANONYMOUS", "testClient", ConfigType.Client, "<default>")
  }

  @Test
  def testDefaultUserQuotaConfigChange(): Unit = {
    testQuotaConfigChange("ANONYMOUS", "testClient", ConfigType.User, "<default>")
  }

  @Test
  def testDefaultUserClientIdQuotaConfigChange(): Unit = {
    testQuotaConfigChange("ANONYMOUS", "testClient", ConfigType.User, "<default>/clients/<default>")
  }

  @Test
  def testQuotaInitialization(): Unit = {
    val server = servers.head
    val clientIdProps = new Properties()
    server.shutdown()
    clientIdProps.put(DynamicConfig.Client.ProducerByteRateOverrideProp, "1000")
    clientIdProps.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, "2000")
    val userProps = new Properties()
    userProps.put(DynamicConfig.Client.ProducerByteRateOverrideProp, "10000")
    userProps.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, "20000")
    val userClientIdProps = new Properties()
    userClientIdProps.put(DynamicConfig.Client.ProducerByteRateOverrideProp, "100000")
    userClientIdProps.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, "200000")

    adminZkClient.changeClientIdConfig("overriddenClientId", clientIdProps)
    adminZkClient.changeUserOrUserClientIdConfig("overriddenUser", userProps)
    adminZkClient.changeUserOrUserClientIdConfig("ANONYMOUS/clients/overriddenUserClientId", userClientIdProps)

    // Remove config change znodes to force quota initialization only through loading of user/client quotas
    zkClient.getChildren(ConfigEntityChangeNotificationZNode.path).foreach { p => zkClient.deletePath(ConfigEntityChangeNotificationZNode.path + "/" + p) }
    server.startup()
    val quotaManagers = server.apis.quotas

    assertEquals(Quota.upperBound(1000),  quotaManagers.produce.quota("someuser", "overriddenClientId"))
    assertEquals(Quota.upperBound(2000),  quotaManagers.fetch.quota("someuser", "overriddenClientId"))
    assertEquals(Quota.upperBound(10000),  quotaManagers.produce.quota("overriddenUser", "someclientId"))
    assertEquals(Quota.upperBound(20000),  quotaManagers.fetch.quota("overriddenUser", "someclientId"))
    assertEquals(Quota.upperBound(100000),  quotaManagers.produce.quota("ANONYMOUS", "overriddenUserClientId"))
    assertEquals(Quota.upperBound(200000),  quotaManagers.fetch.quota("ANONYMOUS", "overriddenUserClientId"))
  }

  @Test
  def testConfigChangeOnNonExistingTopic(): Unit = {
    val topic = TestUtils.tempTopic
    try {
      val logProps = new Properties()
      logProps.put(FlushMessagesProp, 10000: java.lang.Integer)
      adminZkClient.changeTopicConfig(topic, logProps)
      fail("Should fail with AdminOperationException for topic doesn't exist")
    } catch {
      case _: AdminOperationException => // expected
    }
  }

  @Test
  def testProcessNotification(): Unit = {
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

    val configManager = new DynamicConfigManager(zkClient, Map(ConfigType.Topic -> handler))
    // Notifications created using the old TopicConfigManager are ignored.
    configManager.ConfigChangedNotificationHandler.processNotification("not json".getBytes(StandardCharsets.UTF_8))

    // Incorrect Map. No version
    try {
      val jsonMap = Map("v" -> 1, "x" -> 2)
      configManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava))
      fail("Should have thrown an Exception while parsing incorrect notification " + jsonMap)
    }
    catch {
      case _: Throwable =>
    }
    // Version is provided. EntityType is incorrect
    try {
      val jsonMap = Map("version" -> 1, "entity_type" -> "garbage", "entity_name" -> "x")
      configManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava))
      fail("Should have thrown an Exception while parsing incorrect notification " + jsonMap)
    }
    catch {
      case _: Throwable =>
    }

    // EntityName isn't provided
    try {
      val jsonMap = Map("version" -> 1, "entity_type" -> ConfigType.Topic)
      configManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava))
      fail("Should have thrown an Exception while parsing incorrect notification " + jsonMap)
    }
    catch {
      case _: Throwable =>
    }

    // Everything is provided
    val jsonMap = Map("version" -> 1, "entity_type" -> ConfigType.Topic, "entity_name" -> "x")
    configManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava))

    // Verify that processConfigChanges was only called once
    EasyMock.verify(handler)
  }

  @Test
  def shouldParseReplicationQuotaProperties(): Unit = {
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null)
    val props: Properties = new Properties()

    //Given
    props.put(LeaderReplicationThrottledReplicasProp, "0:101,0:102,1:101,1:102")

    //When/Then
    assertEquals(Seq(0,1), configHandler.parseThrottledPartitions(props, 102, LeaderReplicationThrottledReplicasProp))
    assertEquals(Seq(), configHandler.parseThrottledPartitions(props, 103, LeaderReplicationThrottledReplicasProp))
  }

  @Test
  def shouldParseWildcardReplicationQuotaProperties(): Unit = {
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null)
    val props: Properties = new Properties()

    //Given
    props.put(LeaderReplicationThrottledReplicasProp, "*")

    //When
    val result = configHandler.parseThrottledPartitions(props, 102, LeaderReplicationThrottledReplicasProp)

    //Then
    assertEquals(AllReplicas, result)
  }

  @Test
  def shouldParseReplicationQuotaReset(): Unit = {
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null)
    val props: Properties = new Properties()

    //Given
    props.put(FollowerReplicationThrottledReplicasProp, "")

    //When
    val result = configHandler.parseThrottledPartitions(props, 102, FollowerReplicationThrottledReplicasProp)

    //Then
    assertEquals(Seq(), result)
  }

  @Test
  def shouldParseRegardlessOfWhitespaceAroundValues(): Unit = {
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null)
    assertEquals(AllReplicas, parse(configHandler, "* "))
    assertEquals(Seq(), parse(configHandler, " "))
    assertEquals(Seq(6), parse(configHandler, "6:102"))
    assertEquals(Seq(6), parse(configHandler, "6:102 "))
    assertEquals(Seq(6), parse(configHandler, " 6:102"))
  }

  def parse(configHandler: TopicConfigHandler, value: String): Seq[Int] = {
    configHandler.parseThrottledPartitions(CoreUtils.propsWith(LeaderReplicationThrottledReplicasProp, value), 102, LeaderReplicationThrottledReplicasProp)
  }
}
