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

import kafka.cluster.Partition
import kafka.integration.KafkaServerTestHarness
import kafka.log.UnifiedLog
import kafka.log.remote.RemoteLogManager
import kafka.utils.TestUtils.random
import kafka.utils._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{Admin, AlterClientQuotasOptions, AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{InvalidRequestException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.quota.ClientQuotaAlteration.Op
import org.apache.kafka.common.quota.ClientQuotaEntity.{CLIENT_ID, IP, USER}
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.coordinator.group.GroupConfig
import org.apache.kafka.server.config.{QuotaConfig, ServerLogConfigs}
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import java.net.InetAddress
import java.util
import java.util.Collections.{singletonList, singletonMap}
import java.util.concurrent.ExecutionException
import java.util.{Collections, Properties}
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

@Timeout(100)
class DynamicConfigChangeTest extends KafkaServerTestHarness {
  override def generateConfigs: Seq[KafkaConfig] = {
    val cfg = TestUtils.createBrokerConfig(0, null)
    List(KafkaConfig.fromProps(cfg))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testConfigChange(quorum: String): Unit = {
    val oldVal: java.lang.Long = 100000L
    val newVal: java.lang.Long = 200000L
    val tp = new TopicPartition("test", 0)
    val logProps = new Properties()
    logProps.put(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, oldVal.toString)
    createTopic(tp.topic, 1, 1, logProps)
    TestUtils.retry(10000) {
      val logOpt = this.brokers.head.logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldVal, logOpt.get.config.flushInterval)
    }
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, tp.topic())
      val op = new AlterConfigOp(new ConfigEntry(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, newVal.toString),
        OpType.SET)
      val resource2 = new ConfigResource(ConfigResource.Type.BROKER, "")
      val op2 = new AlterConfigOp(new ConfigEntry(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, newVal.toString),
        OpType.SET)
      admin.incrementalAlterConfigs(Map(
        resource -> List(op).asJavaCollection,
        resource2 -> List(op2).asJavaCollection,
      ).asJava).all.get
    } finally {
      admin.close()
    }
    TestUtils.retry(10000) {
      assertEquals(newVal, this.brokers.head.logManager.getLog(tp).get.config.flushInterval)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDynamicTopicConfigChange(quorum: String): Unit = {
    val tp = new TopicPartition("test", 0)
    val oldSegmentSize = 1000
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, oldSegmentSize.toString)
    createTopic(tp.topic, 1, 1, logProps)
    TestUtils.retry(10000) {
      val logOpt = this.brokers.head.logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldSegmentSize, logOpt.get.config.segmentSize)
    }

    val newSegmentSize = 2000
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, tp.topic())
      val op = new AlterConfigOp(new ConfigEntry(TopicConfig.SEGMENT_BYTES_CONFIG, newSegmentSize.toString),
        OpType.SET)
      admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all.get
    } finally {
      admin.close()
    }
    val log = brokers.head.logManager.getLog(tp).get
    TestUtils.retry(10000) {
      assertEquals(newSegmentSize, log.config.segmentSize)
    }

    (1 to 50).foreach(i => TestUtils.produceMessage(brokers, tp.topic, i.toString))
    // Verify that the new config is used for all segments
    assertTrue(log.logSegments.stream.allMatch(_.size > 1000), "Log segment size change not applied")
  }

  private def testQuotaConfigChange(entity: ClientQuotaEntity,
                                    user: KafkaPrincipal,
                                    clientId: String): Unit = {
    val admin = createAdminClient()
    try {
      val alterations = util.Arrays.asList(
        new ClientQuotaAlteration(entity, util.Arrays.asList(
          new Op(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 1000),
          new Op(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 2000))))
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
          new Op(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, null),
          new Op(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, null))))

      // validate only
      admin.alterClientQuotas(removals, new AlterClientQuotasOptions().validateOnly(true)).all().get()
      assertEquals(Quota.upperBound(1000),
        quotaManagers.produce.quota(user, clientId), s"User $user clientId $clientId must have same producer quota of 1000")
      assertEquals(Quota.upperBound(2000),
        quotaManagers.fetch.quota(user, clientId), s"User $user clientId $clientId must have same consumer quota of 2000")

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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testClientIdQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(CLIENT_ID, "testClient")
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUserQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(USER, "ANONYMOUS")
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUserClientIdQuotaChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(USER, "ANONYMOUS")
    m.put(CLIENT_ID, "testClient")
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDefaultClientIdQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(CLIENT_ID, null)
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDefaultUserQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(USER, null)
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDefaultUserClientIdQuotaConfigChange(quorum: String): Unit = {
    val m = new util.HashMap[String, String]
    m.put(USER, null)
    m.put(CLIENT_ID, null)
    testQuotaConfigChange(new ClientQuotaEntity(m), KafkaPrincipal.ANONYMOUS, "testClient")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIpQuotaInitialization(quorum: String): Unit = {
    val broker = brokers.head
    val admin = createAdminClient()
    try {
      val alterations = util.Arrays.asList(
        new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, null)),
          singletonList(new Op(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 20))),
        new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, "1.2.3.4")),
          singletonList(new Op(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 10))))
      admin.alterClientQuotas(alterations).all().get()
    } finally {
      admin.close()
    }
    TestUtils.retry(10000) {
      val connectionQuotas = broker.socketServer.connectionQuotas
      assertEquals(10L, connectionQuotas.connectionRateForIp(InetAddress.getByName("1.2.3.4")))
      assertEquals(20L, connectionQuotas.connectionRateForIp(InetAddress.getByName("2.4.6.8")))
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIpQuotaConfigChange(quorum: String): Unit = {
    val admin = createAdminClient()
    try {
      val alterations = util.Arrays.asList(
        new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, null)),
          singletonList(new Op(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 20))),
        new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, "1.2.3.4")),
          singletonList(new Op(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 10))))
      admin.alterClientQuotas(alterations).all().get()

      def verifyConnectionQuota(ip: InetAddress, expectedQuota: Integer): Unit = {
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
          singletonList(new Op(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, null))))
      admin.alterClientQuotas(deletions1).all().get()
      verifyConnectionQuota(overrideQuotaIp, 20)

      val deletions2 = util.Arrays.asList(
        new ClientQuotaAlteration(new ClientQuotaEntity(singletonMap(IP, null)),
          singletonList(new Op(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, null))))
      admin.alterClientQuotas(deletions2).all().get()
      verifyConnectionQuota(overrideQuotaIp, QuotaConfig.IP_CONNECTION_RATE_DEFAULT)
    } finally {
      admin.close()
    }
  }

  private def tempTopic() : String = "testTopic" + random.nextInt(1000000)

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testConfigChangeOnNonExistingTopicWithAdminClient(quorum: String): Unit = {
    val topic = tempTopic()
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val op = new AlterConfigOp(new ConfigEntry(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, "10000"), OpType.SET)
      admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all.get
      fail("Should fail with UnknownTopicOrPartitionException for topic doesn't exist")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[UnknownTopicOrPartitionException])
    } finally {
      admin.close()
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIncrementalAlterDefaultTopicConfig(quorum: String): Unit = {
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, "")
      val op = new AlterConfigOp(new ConfigEntry(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, "200000"), OpType.SET)
      val future = admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all
      assertFutureThrows(future, classOf[InvalidRequestException])
    } finally {
      admin.close()
    }
  }

  private def setBrokerConfigs(brokerId: String, newValue: Long): Unit = alterBrokerConfigs(brokerId, newValue, OpType.SET)
  private def deleteBrokerConfigs(brokerId: String): Unit = alterBrokerConfigs(brokerId, 0, OpType.DELETE)
  private def alterBrokerConfigs(brokerId: String, newValue: Long, op: OpType): Unit = {
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.BROKER, brokerId)
      val configOp = new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, newValue.toString), op)
      val configOp2 = new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, newValue.toString), op)
      val configOp3 = new AlterConfigOp(new ConfigEntry(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, newValue.toString), op)
      val configOps = List(configOp, configOp2, configOp3).asJavaCollection
      admin.incrementalAlterConfigs(Map(
        resource -> configOps,
      ).asJava).all.get
    } finally {
      admin.close()
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testBrokerIdConfigChangeAndDelete(quorum: String): Unit = {
    val newValue: Long = 100000L
    val brokerId: String = this.brokers.head.config.brokerId.toString
    setBrokerConfigs(brokerId, newValue)
    for (b <- this.brokers) {
      val value = if (b.config.brokerId.toString == brokerId) newValue else QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT
      TestUtils.retry(10000) {
        assertEquals(value, b.quotaManagers.leader.upperBound)
        assertEquals(value, b.quotaManagers.follower.upperBound)
        assertEquals(value, b.quotaManagers.alterLogDirs.upperBound)
      }
    }
    deleteBrokerConfigs(brokerId)
    for (b <- this.brokers) {
      TestUtils.retry(10000) {
        assertEquals(QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT, b.quotaManagers.leader.upperBound)
        assertEquals(QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT, b.quotaManagers.follower.upperBound)
        assertEquals(QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT, b.quotaManagers.alterLogDirs.upperBound)
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDefaultBrokerIdConfigChangeAndDelete(quorum: String): Unit = {
    val newValue: Long = 100000L
    val brokerId: String = ""
    setBrokerConfigs(brokerId, newValue)
    for (b <- this.brokers) {
      TestUtils.retry(10000) {
        assertEquals(newValue, b.quotaManagers.leader.upperBound)
        assertEquals(newValue, b.quotaManagers.follower.upperBound)
        assertEquals(newValue, b.quotaManagers.alterLogDirs.upperBound)
      }
    }
    deleteBrokerConfigs(brokerId)
    for (b <- this.brokers) {
      TestUtils.retry(10000) {
        assertEquals(QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT, b.quotaManagers.leader.upperBound)
        assertEquals(QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT, b.quotaManagers.follower.upperBound)
        assertEquals(QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT, b.quotaManagers.alterLogDirs.upperBound)
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDefaultAndBrokerIdConfigChange(quorum: String): Unit = {
    val newValue: Long = 100000L
    val brokerId: String = this.brokers.head.config.brokerId.toString
    setBrokerConfigs(brokerId, newValue)
    val newDefaultValue: Long = 200000L
    setBrokerConfigs("", newDefaultValue)
    for (b <- this.brokers) {
      val value = if (b.config.brokerId.toString == brokerId) newValue else newDefaultValue
      TestUtils.retry(10000) {
        assertEquals(value, b.quotaManagers.leader.upperBound)
        assertEquals(value, b.quotaManagers.follower.upperBound)
        assertEquals(value, b.quotaManagers.alterLogDirs.upperBound)
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDynamicGroupConfigChange(quorum: String): Unit = {
    val newSessionTimeoutMs = 50000
    val consumerGroupId = "group-foo"
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.GROUP, consumerGroupId)
      val op = new AlterConfigOp(
        new ConfigEntry(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, newSessionTimeoutMs.toString),
        OpType.SET
      )
      admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all.get
    } finally {
      admin.close()
    }

    TestUtils.retry(10000) {
      brokers.head.groupCoordinator.groupMetadataTopicConfigs()
      val configOpt = brokerServers.head.groupCoordinator.groupConfig(consumerGroupId)
      assertTrue(configOpt.isPresent)
    }

    val groupConfig = brokerServers.head.groupCoordinator.groupConfig(consumerGroupId).get()
    assertEquals(newSessionTimeoutMs, groupConfig.consumerSessionTimeoutMs())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft+kip848"))
  def testDynamicShareGroupConfigChange(quorum: String): Unit = {
    val newRecordLockDurationMs = 50000
    val shareGroupId = "group-foo"
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.GROUP, shareGroupId)
      val op = new AlterConfigOp(
        new ConfigEntry(GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, newRecordLockDurationMs.toString),
        OpType.SET
      )
      admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all.get
    } finally {
      admin.close()
    }

    TestUtils.retry(10000) {
      brokers.head.groupCoordinator.groupMetadataTopicConfigs()
      val configOpt = brokerServers.head.groupCoordinator.groupConfig(shareGroupId)
      assertTrue(configOpt.isPresent)
    }

    val groupConfig = brokerServers.head.groupCoordinator.groupConfig(shareGroupId).get()
    assertEquals(newRecordLockDurationMs, groupConfig.shareRecordLockDurationMs)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIncrementalAlterDefaultGroupConfig(quorum: String): Unit = {
    val admin = createAdminClient()
    try {
      val resource = new ConfigResource(ConfigResource.Type.GROUP, "")
      val op = new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "200000"), OpType.SET)
      val future = admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all
      assertFutureThrows(future, classOf[InvalidRequestException])
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
    props.put(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "1")

    assertThrows(classOf[IllegalArgumentException], () => configHandler.processConfigChanges("illegal-hostname", props))
  }

  @Test
  def shouldParseReplicationQuotaProperties(): Unit = {
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null, null)
    val props: Properties = new Properties()

    //Given
    props.put(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "0:101,0:102,1:101,1:102")

    //When/Then
    assertEquals(Seq(0,1), configHandler.parseThrottledPartitions(props, 102, QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG))
    assertEquals(Seq(), configHandler.parseThrottledPartitions(props, 103, QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG))
  }

  @Test
  def shouldParseWildcardReplicationQuotaProperties(): Unit = {
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null, null)
    val props: Properties = new Properties()

    //Given
    props.put(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "*")

    //When
    val result = configHandler.parseThrottledPartitions(props, 102, QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG)

    //Then
    assertEquals(ReplicationQuotaManager.ALL_REPLICAS.asScala.map(_.toInt).toSeq, result)
  }

  @Test
  def shouldParseRegardlessOfWhitespaceAroundValues(): Unit = {
    def parse(configHandler: TopicConfigHandler, value: String): Seq[Int] = {
      configHandler.parseThrottledPartitions(
        CoreUtils.propsWith(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, value),
        102, QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
    }
    val configHandler: TopicConfigHandler = new TopicConfigHandler(null, null, null, null)
    assertEquals(ReplicationQuotaManager.ALL_REPLICAS.asScala.map(_.toInt).toSeq, parse(configHandler, "* "))
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
    props.put(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "")

    //When
    val result = configHandler.parseThrottledPartitions(props, 102, QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)

    //Then
    assertEquals(Seq(), result)
  }

  @Test
  def testEnableRemoteLogStorageOnTopic(): Unit = {
    val topic = "test-topic"
    val topicUuid = Uuid.randomUuid()
    val rlm: RemoteLogManager = mock(classOf[RemoteLogManager])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val metadataCache = mock(classOf[MetadataCache])
    when(replicaManager.remoteLogManager).thenReturn(Some(rlm))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(metadataCache.getTopicId(topic)).thenReturn(topicUuid)

    val tp0 = new TopicPartition(topic, 0)
    val log0: UnifiedLog = mock(classOf[UnifiedLog])
    val partition0: Partition = mock(classOf[Partition])
    when(log0.topicPartition).thenReturn(tp0)
    when(log0.remoteLogEnabled()).thenReturn(true)
    when(partition0.isLeader).thenReturn(true)
    when(replicaManager.onlinePartition(tp0)).thenReturn(Some(partition0))
    when(log0.config).thenReturn(new LogConfig(Collections.emptyMap()))

    val tp1 = new TopicPartition(topic, 1)
    val log1: UnifiedLog = mock(classOf[UnifiedLog])
    val partition1: Partition = mock(classOf[Partition])
    when(log1.topicPartition).thenReturn(tp1)
    when(log1.remoteLogEnabled()).thenReturn(true)
    when(partition1.isLeader).thenReturn(false)
    when(replicaManager.onlinePartition(tp1)).thenReturn(Some(partition1))
    when(log1.config).thenReturn(new LogConfig(Collections.emptyMap()))

    val leaderPartitionsArg: ArgumentCaptor[util.Set[Partition]] = ArgumentCaptor.forClass(classOf[util.Set[Partition]])
    val followerPartitionsArg: ArgumentCaptor[util.Set[Partition]] = ArgumentCaptor.forClass(classOf[util.Set[Partition]])
    doNothing().when(rlm).onLeadershipChange(leaderPartitionsArg.capture(), followerPartitionsArg.capture(), any())

    val isRemoteLogEnabledBeforeUpdate = false
    val configHandler: TopicConfigHandler = new TopicConfigHandler(replicaManager, null, null, None)
    configHandler.maybeUpdateRemoteLogComponents(topic, Seq(log0, log1), isRemoteLogEnabledBeforeUpdate, false)
    assertEquals(Collections.singleton(partition0), leaderPartitionsArg.getValue)
    assertEquals(Collections.singleton(partition1), followerPartitionsArg.getValue)
  }

  @Test
  def testEnableRemoteLogStorageOnTopicOnAlreadyEnabledTopic(): Unit = {
    val topic = "test-topic"
    val tp0 = new TopicPartition(topic, 0)
    val rlm: RemoteLogManager = mock(classOf[RemoteLogManager])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val partition: Partition = mock(classOf[Partition])
    when(replicaManager.remoteLogManager).thenReturn(Some(rlm))
    when(replicaManager.onlinePartition(tp0)).thenReturn(Some(partition))

    val log0: UnifiedLog = mock(classOf[UnifiedLog])
    when(log0.remoteLogEnabled()).thenReturn(true)
    doNothing().when(rlm).onLeadershipChange(any(), any(), any())
    when(log0.config).thenReturn(new LogConfig(Collections.emptyMap()))
    when(log0.topicPartition).thenReturn(tp0)
    when(partition.isLeader).thenReturn(true)

    val isRemoteLogEnabledBeforeUpdate = true
    val configHandler: TopicConfigHandler = new TopicConfigHandler(replicaManager, null, null, None)
    configHandler.maybeUpdateRemoteLogComponents(topic, Seq(log0), isRemoteLogEnabledBeforeUpdate, false)
    verify(rlm, never()).onLeadershipChange(any(), any(), any())
  }
}
