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
import java.util
import java.util.Collections.{singletonList, singletonMap}
import java.util.{Collections, Properties}
import java.util.concurrent.ExecutionException
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig._
import kafka.utils._
import kafka.server.Constants._
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, Config, ConfigEntry}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.errors.{InvalidRequestException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.quota.ClientQuotaAlteration.Op
import org.apache.kafka.common.quota.ClientQuotaEntity.{CLIENT_ID, IP, USER}
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.record.{CompressionType, RecordVersion}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.server.common.MetadataVersion.IBP_3_0_IV1
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, verify}

import scala.annotation.nowarn
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

@Timeout(100)
class DynamicConfigChangeTest extends KafkaServerTestHarness {
  def generateConfigs = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnectOrNull)))

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testConfigChange(quorum: String): Unit = {
    if (!isKRaftTest()) {
      assertTrue(this.servers.head.dynamicConfigHandlers.contains(ConfigType.Topic),
        "Should contain a ConfigHandler for topics")
    }
    val oldVal: java.lang.Long = 100000L
    val newVal: java.lang.Long = 200000L
    val tp = new TopicPartition("test", 0)
    val logProps = new Properties()
    logProps.put(FlushMessagesProp, oldVal.toString)
    createTopic(tp.topic, 1, 1, logProps)
    TestUtils.retry(10000) {
      val logOpt = this.brokers.head.logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldVal, logOpt.get.config.flushInterval)
    }
    if (isKRaftTest()) {
      val admin = createAdminClient()
      try {
        val resource = new ConfigResource(ConfigResource.Type.TOPIC, tp.topic())
        val op = new AlterConfigOp(new ConfigEntry(FlushMessagesProp, newVal.toString()),
          SET)
        val resource2 = new ConfigResource(ConfigResource.Type.BROKER, "")
        val op2 = new AlterConfigOp(new ConfigEntry(KafkaConfig.LogFlushIntervalMsProp, newVal.toString()),
          SET)
        admin.incrementalAlterConfigs(Map(
          resource -> List(op).asJavaCollection,
          resource2 -> List(op2).asJavaCollection,
        ).asJava).all.get
      } finally {
        admin.close()
      }
    } else {
      val newProps = new Properties()
      newProps.setProperty(FlushMessagesProp, newVal.toString())
      adminZkClient.changeTopicConfig(tp.topic, newProps)
    }
    TestUtils.retry(10000) {
      assertEquals(newVal, this.brokers.head.logManager.getLog(tp).get.config.flushInterval)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDynamicTopicConfigChange(quorum: String): Unit = {
    val tp = new TopicPartition("test", 0)
    val oldSegmentSize = 1000
    val logProps = new Properties()
    logProps.put(SegmentBytesProp, oldSegmentSize.toString)
    createTopic(tp.topic, 1, 1, logProps)
    TestUtils.retry(10000) {
      val logOpt = this.brokers.head.logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldSegmentSize, logOpt.get.config.segmentSize)
    }

    val newSegmentSize = 2000
    if (isKRaftTest()) {
      val admin = createAdminClient()
      try {
        val resource = new ConfigResource(ConfigResource.Type.TOPIC, tp.topic())
        val op = new AlterConfigOp(new ConfigEntry(SegmentBytesProp, newSegmentSize.toString()),
          SET)
        admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all.get
      } finally {
        admin.close()
      }
    } else {
      val newProps = new Properties()
      newProps.put(SegmentBytesProp, newSegmentSize.toString)
      adminZkClient.changeTopicConfig(tp.topic, newProps)
    }

    val log = brokers.head.logManager.getLog(tp).get
    TestUtils.retry(10000) {
      assertEquals(newSegmentSize, log.config.segmentSize)
    }

    (1 to 50).foreach(i => TestUtils.produceMessage(brokers, tp.topic, i.toString))
    // Verify that the new config is used for all segments
    assertTrue(log.logSegments.forall(_.size > 1000), "Log segment size change not applied")
  }

  @nowarn("cat=deprecation")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testMessageFormatVersionChange(quorum: String): Unit = {
    val tp = new TopicPartition("test", 0)
    val logProps = new Properties()
    logProps.put(MessageFormatVersionProp, "0.10.2")
    createTopic(tp.topic, 1, 1, logProps)
    val server = servers.head
    TestUtils.waitUntilTrue(() => server.logManager.getLog(tp).isDefined,
      "Topic metadata propagation failed")
    val log = server.logManager.getLog(tp).get
    // message format version should always be 3.0 if inter-broker protocol is 3.0 or higher
    assertEquals(IBP_3_0_IV1, log.config.messageFormatVersion)
    assertEquals(RecordVersion.V2, log.config.recordVersion)

    val compressionType = CompressionType.LZ4.name
    logProps.put(MessageFormatVersionProp, "0.11.0")
    // set compression type so that we can detect when the config change has propagated
    logProps.put(CompressionTypeProp, compressionType)
    adminZkClient.changeTopicConfig(tp.topic, logProps)
    TestUtils.waitUntilTrue(() =>
      server.logManager.getLog(tp).get.config.compressionType == compressionType,
      "Topic config change propagation failed")
    assertEquals(IBP_3_0_IV1, log.config.messageFormatVersion)
    assertEquals(RecordVersion.V2, log.config.recordVersion)
  }

  private def testQuotaConfigChange(entity: ClientQuotaEntity,
                                    user: KafkaPrincipal,
                                    clientId: String): Unit = {
    val admin = createAdminClient()
    try {
      val alterations = util.Arrays.asList(
        new ClientQuotaAlteration(entity, util.Arrays.asList(
          new Op(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 1000),
          new Op(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 2000))))
      admin.alterClientQuotas(alterations).all().get()

      val quotaManagers = brokers.head.dataPlaneRequestProcessor.quotas
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

      val removals = util.Arrays.asList(
        new ClientQuotaAlteration(entity, util.Arrays.asList(
          new Op(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, null),
          new Op(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, null))))
      admin.alterClientQuotas(removals).all().get()
      TestUtils.retry(10000) {
        val producerQuota = quotaManagers.produce.quota(user, clientId)
        val consumerQuota = quotaManagers.fetch.quota(user, clientId)

        assertEquals(Quota.upperBound(defaultProducerQuota),
          producerQuota, s"User $user clientId $clientId must have reset producer quota to " + defaultProducerQuota)
        assertEquals(Quota.upperBound(defaultConsumerQuota),
          consumerQuota, s"User $user clientId $clientId must have reset consumer quota to " + defaultConsumerQuota)
      }
    } finally {
      admin.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testClientIdQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(CLIENT_ID, "testClient")
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testUserQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(USER, "ANONYMOUS")
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testUserClientIdQuotaChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(USER, "ANONYMOUS")
    m.put(CLIENT_ID, "testClient")
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDefaultClientIdQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(CLIENT_ID, null)
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDefaultUserQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(USER, null)
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDefaultUserClientIdQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(USER, null)
    m.put(CLIENT_ID, null)
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testQuotaInitialization(quorum: String): Unit = {
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testIpQuotaInitialization(quorum: String): Unit = {
    val broker = brokers.head
    if (isKRaftTest()) {
      val admin = createAdminClient()
      try {
        val alterations = util.Arrays.asList(
          new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, null)),
            singletonList(new Op(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 20))),
          new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, "1.2.3.4")),
            singletonList(new Op(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 10))))
        admin.alterClientQuotas(alterations).all().get()
      } finally {
        admin.close()
      }
    } else {
      broker.shutdown()

      val ipDefaultProps = new Properties()
      ipDefaultProps.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "20")
      adminZkClient.changeIpConfig(ConfigEntityName.Default, ipDefaultProps)

      val ipOverrideProps = new Properties()
      ipOverrideProps.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "10")
      adminZkClient.changeIpConfig("1.2.3.4", ipOverrideProps)

      // Remove config change znodes to force quota initialization only through loading of ip quotas
      zkClient.getChildren(ConfigEntityChangeNotificationZNode.path).foreach { p =>
        zkClient.deletePath(ConfigEntityChangeNotificationZNode.path + "/" + p)
      }
      broker.startup()
    }
    TestUtils.retry(10000) {
      val connectionQuotas = broker.socketServer.connectionQuotas
      assertEquals(10L, connectionQuotas.connectionRateForIp(InetAddress.getByName("1.2.3.4")))
      assertEquals(20L, connectionQuotas.connectionRateForIp(InetAddress.getByName("2.4.6.8")))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testIpQuotaConfigChange(quorum: String): Unit = {
    val admin = createAdminClient()
    try {
      val alterations = util.Arrays.asList(
        new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, null)),
          singletonList(new Op(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 20))),
        new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, "1.2.3.4")),
          singletonList(new Op(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 10))))
      admin.alterClientQuotas(alterations).all().get()

      def verifyConnectionQuota(ip: InetAddress, expectedQuota: Integer) = {
        val connectionQuotas = brokers.head.socketServer.connectionQuotas
        TestUtils.retry(10000) {
          val quota = connectionQuotas.connectionRateForIp(ip)
          assertEquals(expectedQuota, quota, s"Unexpected quota for IP $ip")
        }
      }

      val overrideQuotaIp = InetAddress.getByName("1.2.3.4")
      verifyConnectionQuota(overrideQuotaIp, 10)

      val defaultQuotaIp = InetAddress.getByName("2.3.4.5")
      verifyConnectionQuota(defaultQuotaIp, 20)

      val deletions1 = util.Arrays.asList(
        new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, "1.2.3.4")),
          singletonList(new Op(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, null))))
      admin.alterClientQuotas(deletions1).all().get()
      verifyConnectionQuota(overrideQuotaIp, 20)

      val deletions2 = util.Arrays.asList(
        new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, null)),
          singletonList(new Op(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, null))))
      admin.alterClientQuotas(deletions2).all().get()
      verifyConnectionQuota(overrideQuotaIp, QuotaConfigs.IP_CONNECTION_RATE_DEFAULT)
    } finally {
      admin.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testConfigChangeOnNonExistingTopic(quorum: String): Unit = {
    val topic = TestUtils.tempTopic()
    val logProps = new Properties()
    logProps.put(FlushMessagesProp, 10000: java.lang.Integer)
    assertThrows(classOf[UnknownTopicOrPartitionException], () => adminZkClient.changeTopicConfig(topic, logProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testConfigChangeOnNonExistingTopicWithAdminClient(quorum: String): Unit = {
    val topic = TestUtils.tempTopic()
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val op = new AlterConfigOp(new ConfigEntry(FlushMessagesProp, "10000"), SET)
      admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all.get
      fail("Should fail with UnknownTopicOrPartitionException for topic doesn't exist")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[UnknownTopicOrPartitionException])
    } finally {
      admin.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testProcessNotification(quorum: String): Unit = {
    val props = new Properties()
    props.put("a.b", "10")

    // Create a mock ConfigHandler to record config changes it is asked to process
    val handler: ConfigHandler = mock(classOf[ConfigHandler])

    val configManager = new ZkConfigManager(zkClient, Map(ConfigType.Topic -> handler))
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
    verify(handler).processConfigChanges(anyString, any[Properties])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testIncrementalAlterDefaultTopicConfig(quorum: String): Unit = {
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, "")
      val op = new AlterConfigOp(new ConfigEntry(FlushMessagesProp, "200000"), SET)
      val future = admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all
      TestUtils.assertFutureExceptionTypeEquals(future, classOf[InvalidRequestException])
    } finally {
      admin.close()
    }
  }

  @nowarn("cat=deprecation")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterDefaultTopicConfig(quorum: String): Unit = {
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, "")
      val config = new Config(Collections.singleton(new ConfigEntry(FlushMessagesProp, "200000")))
      val future = admin.alterConfigs(Map(resource -> config).asJava).all
      TestUtils.assertFutureExceptionTypeEquals(future, classOf[InvalidRequestException])
    } finally {
      admin.close()
    }
  }

  private def createAdminClient(): Admin = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    Admin.create(props)
  }
}

class DynamicConfigChangeUnitTest {
  @Test
  def testIpHandlerUnresolvableAddress(): Unit = {
    val configHandler = new IpConfigHandler(null)
    val props: Properties = new Properties()
    props.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "1")

    assertThrows(classOf[IllegalArgumentException], () => configHandler.processConfigChanges("illegal-hostname", props))
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
  def shouldParseRegardlessOfWhitespaceAroundValues(): Unit = {
    def parse(configHandler: TopicConfigHandler, value: String): Seq[Int] = {
      configHandler.parseThrottledPartitions(
        CoreUtils.propsWith(LeaderReplicationThrottledReplicasProp, value),
        102, LeaderReplicationThrottledReplicasProp)
    }
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null, null)
    assertEquals(AllReplicas, parse(configHandler, "* "))
    assertEquals(Seq(), parse(configHandler, " "))
    assertEquals(Seq(6), parse(configHandler, "6:102"))
    assertEquals(Seq(6), parse(configHandler, "6:102 "))
    assertEquals(Seq(6), parse(configHandler, " 6:102"))
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
}
