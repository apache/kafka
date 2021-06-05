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

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.ExecutionException
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig._
import kafka.utils._
import kafka.server.Constants._
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.metrics.Quota
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class DynamicConfigChangeTest extends KafkaServerTestHarness {
  def generateConfigs = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))

  @Test
  def testConfigChange(): Unit = {
    assertTrue(this.servers.head.dynamicConfigHandlers.contains(ConfigType.Topic),
      "Should contain a ConfigHandler for topics")
    val oldVal: java.lang.Long = 100000L
    val newVal: java.lang.Long = 200000L
    val tp = new TopicPartition("test", 0)
    val logProps = new Properties()
    logProps.put(FlushMessagesProp, oldVal.toString)
    createTopic(tp.topic, 1, 1, logProps)
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

  @Test
  def testDynamicTopicConfigChange(): Unit = {
    val tp = new TopicPartition("test", 0)
    val oldSegmentSize = 1000
    val logProps = new Properties()
    logProps.put(SegmentBytesProp, oldSegmentSize.toString)
    createTopic(tp.topic, 1, 1, logProps)
    TestUtils.retry(10000) {
      val logOpt = this.servers.head.logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldSegmentSize, logOpt.get.config.segmentSize)
    }

    val log = servers.head.logManager.getLog(tp).get

    val newSegmentSize = 2000
    logProps.put(SegmentBytesProp, newSegmentSize.toString)
    adminZkClient.changeTopicConfig(tp.topic, logProps)
    TestUtils.retry(10000) {
      assertEquals(newSegmentSize, log.config.segmentSize)
    }

    (1 to 50).foreach(i => TestUtils.produceMessage(servers, tp.topic, i.toString))
    // Verify that the new config is used for all segments
    assertTrue(log.logSegments.forall(_.size > 1000), "Log segment size change not applied")
  }

  private def testQuotaConfigChange(user: String, clientId: String, rootEntityType: String, configEntityName: String): Unit = {
    assertTrue(this.servers.head.dynamicConfigHandlers.contains(rootEntityType), "Should contain a ConfigHandler for " + rootEntityType)
    val props = new Properties()
    props.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, "1000")
    props.put(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, "2000")

    val quotaManagers = servers.head.dataPlaneRequestProcessor.quotas
    rootEntityType match {
      case ConfigType.Client => adminZkClient.changeClientIdConfig(configEntityName, props)
      case _ => adminZkClient.changeUserOrUserClientIdConfig(configEntityName, props)
    }

    TestUtils.retry(10000) {
      val overrideProducerQuota = quotaManagers.produce.quota(user, clientId)
      val overrideConsumerQuota = quotaManagers.fetch.quota(user, clientId)

      assertEquals(Quota.upperBound(1000),
        overrideProducerQuota, s"User $user clientId $clientId must have overridden producer quota of 1000")
      assertEquals(Quota.upperBound(2000),
        overrideConsumerQuota, s"User $user clientId $clientId must have overridden consumer quota of 2000")
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

      assertEquals(Quota.upperBound(defaultProducerQuota),
        producerQuota, s"User $user clientId $clientId must have reset producer quota to " + defaultProducerQuota)
      assertEquals(Quota.upperBound(defaultConsumerQuota),
        consumerQuota, s"User $user clientId $clientId must have reset consumer quota to " + defaultConsumerQuota)
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
    clientIdProps.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, "1000")
    clientIdProps.put(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, "2000")
    val userProps = new Properties()
    userProps.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, "10000")
    userProps.put(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, "20000")
    val userClientIdProps = new Properties()
    userClientIdProps.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, "100000")
    userClientIdProps.put(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, "200000")

    adminZkClient.changeClientIdConfig("overriddenClientId", clientIdProps)
    adminZkClient.changeUserOrUserClientIdConfig("overriddenUser", userProps)
    adminZkClient.changeUserOrUserClientIdConfig("ANONYMOUS/clients/overriddenUserClientId", userClientIdProps)

    // Remove config change znodes to force quota initialization only through loading of user/client quotas
    zkClient.getChildren(ConfigEntityChangeNotificationZNode.path).foreach { p => zkClient.deletePath(ConfigEntityChangeNotificationZNode.path + "/" + p) }
    server.startup()
    val quotaManagers = server.dataPlaneRequestProcessor.quotas

    assertEquals(Quota.upperBound(1000),  quotaManagers.produce.quota("someuser", "overriddenClientId"))
    assertEquals(Quota.upperBound(2000),  quotaManagers.fetch.quota("someuser", "overriddenClientId"))
    assertEquals(Quota.upperBound(10000),  quotaManagers.produce.quota("overriddenUser", "someclientId"))
    assertEquals(Quota.upperBound(20000),  quotaManagers.fetch.quota("overriddenUser", "someclientId"))
    assertEquals(Quota.upperBound(100000),  quotaManagers.produce.quota("ANONYMOUS", "overriddenUserClientId"))
    assertEquals(Quota.upperBound(200000),  quotaManagers.fetch.quota("ANONYMOUS", "overriddenUserClientId"))
  }

  @Test
  def testIpHandlerUnresolvableAddress(): Unit = {
    val configHandler = new IpConfigHandler(null)
    val props: Properties = new Properties()
    props.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "1")

    assertThrows(classOf[IllegalArgumentException], () => configHandler.processConfigChanges("illegal-hostname", props))
  }

  @Test
  def testIpQuotaInitialization(): Unit = {
    val server = servers.head
    val ipOverrideProps = new Properties()
    ipOverrideProps.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "10")
    val ipDefaultProps = new Properties()
    ipDefaultProps.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "20")
    server.shutdown()

    adminZkClient.changeIpConfig(ConfigEntityName.Default, ipDefaultProps)
    adminZkClient.changeIpConfig("1.2.3.4", ipOverrideProps)

    // Remove config change znodes to force quota initialization only through loading of ip quotas
    zkClient.getChildren(ConfigEntityChangeNotificationZNode.path).foreach { p =>
      zkClient.deletePath(ConfigEntityChangeNotificationZNode.path + "/" + p)
    }
    server.startup()

    val connectionQuotas = server.socketServer.connectionQuotas
    assertEquals(10L, connectionQuotas.connectionRateForIp(InetAddress.getByName("1.2.3.4")))
    assertEquals(20L, connectionQuotas.connectionRateForIp(InetAddress.getByName("2.4.6.8")))
  }

  @Test
  def testIpQuotaConfigChange(): Unit = {
    val ipOverrideProps = new Properties()
    ipOverrideProps.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "10")
    val ipDefaultProps = new Properties()
    ipDefaultProps.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "20")

    val overrideQuotaIp = InetAddress.getByName("1.2.3.4")
    val defaultQuotaIp = InetAddress.getByName("2.3.4.5")
    adminZkClient.changeIpConfig(ConfigEntityName.Default, ipDefaultProps)
    adminZkClient.changeIpConfig(overrideQuotaIp.getHostAddress, ipOverrideProps)

    val connectionQuotas = servers.head.socketServer.connectionQuotas

    def verifyConnectionQuota(ip: InetAddress, expectedQuota: Integer) = {
      TestUtils.retry(10000) {
        val quota = connectionQuotas.connectionRateForIp(ip)
        assertEquals(expectedQuota, quota, s"Unexpected quota for IP $ip")
      }
    }

    verifyConnectionQuota(overrideQuotaIp, 10)
    verifyConnectionQuota(defaultQuotaIp, 20)

    val emptyProps = new Properties()
    adminZkClient.changeIpConfig(overrideQuotaIp.getHostAddress, emptyProps)
    verifyConnectionQuota(overrideQuotaIp, 20)

    adminZkClient.changeIpConfig(ConfigEntityName.Default, emptyProps)
    verifyConnectionQuota(overrideQuotaIp, QuotaConfigs.IP_CONNECTION_RATE_DEFAULT)
  }

  @Test
  def testConfigChangeOnNonExistingTopic(): Unit = {
    val topic = TestUtils.tempTopic()
    val logProps = new Properties()
    logProps.put(FlushMessagesProp, 10000: java.lang.Integer)
    assertThrows(classOf[UnknownTopicOrPartitionException], () => adminZkClient.changeTopicConfig(topic, logProps))
  }

  @Test
  def testConfigChangeOnNonExistingTopicWithAdminClient(): Unit = {
    val topic = TestUtils.tempTopic()
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val op = new AlterConfigOp(new ConfigEntry(FlushMessagesProp, "10000"), AlterConfigOp.OpType.SET)
      admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all.get
      fail("Should fail with UnknownTopicOrPartitionException for topic doesn't exist")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[UnknownTopicOrPartitionException])
    } finally {
      admin.close()
    }
  }

  @Test
  def testProcessNotification(): Unit = {
    val props = new Properties()
    props.put("a.b", "10")

    // Create a mock ConfigHandler to record config changes it is asked to process
    val entityArgument = EasyMock.newCapture[String]
    val propertiesArgument = EasyMock.newCapture[Properties]
    val handler: ConfigHandler = EasyMock.createNiceMock(classOf[ConfigHandler])
    handler.processConfigChanges(
      EasyMock.and(EasyMock.capture(entityArgument), EasyMock.isA(classOf[String])),
      EasyMock.and(EasyMock.capture(propertiesArgument), EasyMock.isA(classOf[Properties])))
    EasyMock.expectLastCall().once()
    EasyMock.replay(handler)

    val configManager = new DynamicConfigManager(zkClient, Map(ConfigType.Topic -> handler))
    // Notifications created using the old TopicConfigManager are ignored.
    configManager.ConfigChangedNotificationHandler.processNotification("not json".getBytes(StandardCharsets.UTF_8))

    // Incorrect Map. No version
    var jsonMap: Map[String, Any] = Map("v" -> 1, "x" -> 2)

    assertThrows(classOf[Throwable], () => configManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))
    // Version is provided. EntityType is incorrect
    jsonMap = Map("version" -> 1, "entity_type" -> "garbage", "entity_name" -> "x")
    assertThrows(classOf[Throwable], () => configManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))

    // EntityName isn't provided
    jsonMap = Map("version" -> 1, "entity_type" -> ConfigType.Topic)
    assertThrows(classOf[Throwable], () => configManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava)))

    // Everything is provided
    jsonMap = Map("version" -> 1, "entity_type" -> ConfigType.Topic, "entity_name" -> "x")
    configManager.ConfigChangedNotificationHandler.processNotification(Json.encodeAsBytes(jsonMap.asJava))

    // Verify that processConfigChanges was only called once
    EasyMock.verify(handler)
  }

  @Test
  def shouldParseReplicationQuotaProperties(): Unit = {
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null, null)
    val props: Properties = new Properties()

    //Given
    props.put(LeaderReplicationThrottledReplicasProp, "0:101,0:102,1:101,1:102")

    //When/Then
    assertEquals(Seq(0,1), configHandler.parseThrottledPartitions(props, 102, LeaderReplicationThrottledReplicasProp))
    assertEquals(Seq(), configHandler.parseThrottledPartitions(props, 103, LeaderReplicationThrottledReplicasProp))
  }

  @Test
  def shouldParseWildcardReplicationQuotaProperties(): Unit = {
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null, null)
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
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null, null)
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
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null, null)
    assertEquals(AllReplicas, parse(configHandler, "* "))
    assertEquals(Seq(), parse(configHandler, " "))
    assertEquals(Seq(6), parse(configHandler, "6:102"))
    assertEquals(Seq(6), parse(configHandler, "6:102 "))
    assertEquals(Seq(6), parse(configHandler, " 6:102"))
  }

  def parse(configHandler: TopicConfigHandler, value: String): Seq[Int] = {
    configHandler.parseThrottledPartitions(CoreUtils.propsWith(LeaderReplicationThrottledReplicasProp, value), 102, LeaderReplicationThrottledReplicasProp)
  }

  private def createAdminClient(): Admin = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    Admin.create(props)
  }

}
