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
package kafka.zk

import kafka.server.KRaftCachedControllerId
import kafka.test.{ClusterConfig, ClusterGenerator, ClusterInstance}
import kafka.test.annotation.{AutoStart, ClusterConfigProperty, ClusterTemplate, ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import kafka.utils.TestUtils
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.acl.AclOperation.{DESCRIBE, READ, WRITE}
import org.apache.kafka.common.acl.AclPermissionType.ALLOW
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{TimeoutException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.message.AllocateProducerIdsRequestData
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.requests.{AllocateProducerIdsRequest, AllocateProducerIdsResponse}
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType.TOPIC
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.common.utils.{Sanitizer, SecurityUtils}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.authorizer.StandardAcl
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.security.authorizer.AclEntry.{WILDCARD_HOST, WILDCARD_PRINCIPAL_STRING}
import org.apache.kafka.security.PasswordEncoder
import org.apache.kafka.server.ControllerRequestCompletionHandler
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion, ProducerIdsBlock}
import org.apache.kafka.server.config.{ConfigType, KRaftConfigs, ServerLogConfigs, ZkConfigs}
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertFalse, assertNotEquals, assertNotNull, assertTrue, fail}
import org.junit.jupiter.api.{Assumptions, Timeout}
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.function.Executable
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit}
import java.util.{Collections, Optional, UUID}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

object ZkMigrationIntegrationTest {

  def zkClustersForAllMigrationVersions(clusterGenerator: ClusterGenerator): Unit = {
    Seq(
      MetadataVersion.IBP_3_4_IV0,
      MetadataVersion.IBP_3_5_IV2,
      MetadataVersion.IBP_3_6_IV2,
      MetadataVersion.IBP_3_7_IV0,
      MetadataVersion.IBP_3_7_IV1,
      MetadataVersion.IBP_3_7_IV2,
      MetadataVersion.IBP_3_7_IV4,
      MetadataVersion.IBP_3_8_IV0
    ).foreach { mv =>
      val serverProperties = new util.HashMap[String, String]()
      serverProperties.put("inter.broker.listener.name", "EXTERNAL")
      serverProperties.put("listeners", "PLAINTEXT://localhost:0,EXTERNAL://localhost:0")
      serverProperties.put("advertised.listeners", "PLAINTEXT://localhost:0,EXTERNAL://localhost:0")
      serverProperties.put("listener.security.protocol.map", "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      clusterGenerator.accept(ClusterConfig.defaultBuilder()
        .setMetadataVersion(mv)
        .setBrokers(3)
        .setServerProperties(serverProperties)
        .setTypes(Set(Type.ZK).asJava)
        .build())
    }
  }
}

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@Timeout(300)
class ZkMigrationIntegrationTest {

  val log: Logger = LoggerFactory.getLogger(classOf[ZkMigrationIntegrationTest])

  class MetadataDeltaVerifier {
    val metadataDelta = new MetadataDelta(MetadataImage.EMPTY)
    var offset = 0
    def accept(batch: java.util.List[ApiMessageAndVersion]): Unit = {
      batch.forEach(message => {
        metadataDelta.replay(message.message())
        offset += 1
      })
    }

    def verify(verifier: MetadataImage => Unit): Unit = {
      val image = metadataDelta.apply(new MetadataProvenance(offset, 0, 0))
      verifier.apply(image)
    }
  }

  @ClusterTest(
    brokers = 3, types = Array(Type.ZK), autoStart = AutoStart.YES,
    metadataVersion = MetadataVersion.IBP_3_4_IV0,
    serverProperties = Array(
      new ClusterConfigProperty(key="authorizer.class.name", value="kafka.security.authorizer.AclAuthorizer"),
      new ClusterConfigProperty(key="super.users", value="User:ANONYMOUS")
    )
  )
  def testMigrateAcls(clusterInstance: ClusterInstance): Unit = {
    val admin = clusterInstance.createAdminClient()

    val resource1 = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL)
    val resource2 = new ResourcePattern(TOPIC, "bar-" + UUID.randomUUID(), LITERAL)
    val prefixedResource = new ResourcePattern(TOPIC, "bar-", PREFIXED)
    val username = "alice"
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val wildcardPrincipal = SecurityUtils.parseKafkaPrincipal(WILDCARD_PRINCIPAL_STRING)

    val acl1 = new AclBinding(resource1, new AccessControlEntry(principal.toString, WILDCARD_HOST, READ, ALLOW))
    val acl2 = new AclBinding(resource1, new AccessControlEntry(principal.toString, "192.168.0.1", WRITE, ALLOW))
    val acl3 = new AclBinding(resource2, new AccessControlEntry(principal.toString, WILDCARD_HOST, DESCRIBE, ALLOW))
    val acl4 = new AclBinding(prefixedResource, new AccessControlEntry(wildcardPrincipal.toString, WILDCARD_HOST, READ, ALLOW))

    val result = admin.createAcls(List(acl1, acl2, acl3, acl4).asJava)
    result.all().get

    val underlying = clusterInstance.asInstanceOf[ZkClusterInstance].getUnderlying()
    val zkClient = underlying.zkClient
    val migrationClient = ZkMigrationClient(zkClient, PasswordEncoder.NOOP)
    val verifier = new MetadataDeltaVerifier()
    migrationClient.readAllMetadata(batch => verifier.accept(batch), _ => { })
    verifier.verify { image =>
      val aclMap = image.acls().acls()
      assertEquals(4, aclMap.size())
      assertTrue(aclMap.values().containsAll(Seq(
        StandardAcl.fromAclBinding(acl1),
        StandardAcl.fromAclBinding(acl2),
        StandardAcl.fromAclBinding(acl3),
        StandardAcl.fromAclBinding(acl4)
      ).asJava))
    }
  }

  @ClusterTest(
    brokers = 3, types = Array(Type.ZK), autoStart = AutoStart.YES,
    metadataVersion = MetadataVersion.IBP_3_4_IV0,
    serverProperties = Array(
      new ClusterConfigProperty(key = "authorizer.class.name", value = "kafka.security.authorizer.AclAuthorizer"),
      new ClusterConfigProperty(key = "super.users", value = "User:ANONYMOUS"),
      new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
      new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
      new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
      new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
    )
  )
  def testStartZkBrokerWithAuthorizer(zkCluster: ClusterInstance): Unit = {
    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_4_IV0).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      val serverProperties = new util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig)) // This would throw if authorizers weren't allowed
      zkCluster.waitForReadyBrokers()
      readyFuture.get(30, TimeUnit.SECONDS)

      val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
      TestUtils.waitUntilTrue(
        () => zkClient.getControllerId.contains(3000),
        "Timed out waiting for KRaft controller to take over",
        30000)

      def inDualWrite(): Boolean = {
        val migrationState = kraftCluster.controllers().get(3000).migrationSupport.get.migrationDriver.migrationState().get(10, TimeUnit.SECONDS)
        migrationState.allowDualWrite()
      }
      TestUtils.waitUntilTrue(() => inDualWrite(), "Timed out waiting for dual-write mode")
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  /**
   * Test ZkMigrationClient against a real ZooKeeper-backed Kafka cluster. This test creates a ZK cluster
   * and modifies data using AdminClient. The ZkMigrationClient is then used to read the metadata from ZK
   * as would happen during a migration. The generated records are then verified.
   */
  @ClusterTest(brokers = 3, types = Array(Type.ZK), metadataVersion = MetadataVersion.IBP_3_4_IV0)
  def testMigrate(clusterInstance: ClusterInstance): Unit = {
    val admin = clusterInstance.createAdminClient()
    val newTopics = new util.ArrayList[NewTopic]()
    newTopics.add(new NewTopic("test-topic-1", 2, 3.toShort)
      .configs(Map(TopicConfig.SEGMENT_BYTES_CONFIG -> "102400", TopicConfig.SEGMENT_MS_CONFIG -> "300000").asJava))
    newTopics.add(new NewTopic("test-topic-2", 1, 3.toShort))
    newTopics.add(new NewTopic("test-topic-3", 10, 3.toShort))
    val createTopicResult = admin.createTopics(newTopics)
    createTopicResult.all().get(60, TimeUnit.SECONDS)

    val quotas = new util.ArrayList[ClientQuotaAlteration]()
    val defaultUserEntity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, null))
    quotas.add(new ClientQuotaAlteration(defaultUserEntity, List(new ClientQuotaAlteration.Op("consumer_byte_rate", 900.0)).asJava))
    val defaultClientIdEntity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.CLIENT_ID, null))
    quotas.add(new ClientQuotaAlteration(defaultClientIdEntity, List(new ClientQuotaAlteration.Op("consumer_byte_rate", 900.0)).asJava))
    val defaultIpEntity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.IP, null))
    quotas.add(new ClientQuotaAlteration(defaultIpEntity, List(new ClientQuotaAlteration.Op("connection_creation_rate", 9.0)).asJava))
    val userEntity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> "user/1@prod").asJava)
    quotas.add(new ClientQuotaAlteration(userEntity, List(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)).asJava))
    val userClientEntity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> "user/1@prod", ClientQuotaEntity.CLIENT_ID -> "client/1@domain").asJava)
    quotas.add(new ClientQuotaAlteration(userClientEntity,
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 800.0), new ClientQuotaAlteration.Op("producer_byte_rate", 100.0)).asJava))
    val ipEntity = new ClientQuotaEntity(Map(ClientQuotaEntity.IP -> "8.8.8.8").asJava)
    quotas.add(new ClientQuotaAlteration(ipEntity, List(new ClientQuotaAlteration.Op("connection_creation_rate", 10.0)).asJava))
    admin.alterClientQuotas(quotas).all().get(60, TimeUnit.SECONDS)

    val zkClient = clusterInstance.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
    val kafkaConfig = clusterInstance.asInstanceOf[ZkClusterInstance].getUnderlying.servers.head.config
    val zkConfigEncoder = kafkaConfig.passwordEncoderSecret match {
      case Some(secret) =>
        PasswordEncoder.encrypting(secret,
          kafkaConfig.passwordEncoderKeyFactoryAlgorithm,
          kafkaConfig.passwordEncoderCipherAlgorithm,
          kafkaConfig.passwordEncoderKeyLength,
          kafkaConfig.passwordEncoderIterations)
      case None => PasswordEncoder.NOOP
    }

    val migrationClient = ZkMigrationClient(zkClient, zkConfigEncoder)
    var migrationState = migrationClient.getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState.EMPTY)
    migrationState = migrationState.withNewKRaftController(3000, 42)
    migrationState = migrationClient.claimControllerLeadership(migrationState)

    val brokers = new java.util.HashSet[Integer]()
    val verifier = new MetadataDeltaVerifier()
    migrationClient.readAllMetadata(batch => verifier.accept(batch), brokerId => brokers.add(brokerId))
    assertEquals(Seq(0, 1, 2), brokers.asScala.toSeq)

    verifier.verify { image =>
      assertNotNull(image.topics().getTopic("test-topic-1"))
      assertEquals(2, image.topics().getTopic("test-topic-1").partitions().size())

      assertNotNull(image.topics().getTopic("test-topic-2"))
      assertEquals(1, image.topics().getTopic("test-topic-2").partitions().size())

      assertNotNull(image.topics().getTopic("test-topic-3"))
      assertEquals(10, image.topics().getTopic("test-topic-3").partitions().size())

      val clientQuotas = image.clientQuotas().entities()
      assertEquals(new java.util.HashSet[ClientQuotaEntity](java.util.Arrays.asList(
        defaultUserEntity,
        defaultClientIdEntity,
        defaultIpEntity,
        userEntity,
        userClientEntity,
        ipEntity
      )), clientQuotas.keySet())
    }
    migrationState = migrationClient.releaseControllerLeadership(migrationState)
  }

  @ClusterTemplate("zkClustersForAllMigrationVersions")
  def testMigrateTopicDeletions(zkCluster: ClusterInstance): Unit = {
    // Create some topics in ZK mode
    var admin = zkCluster.createAdminClient()
    val newTopics = new util.ArrayList[NewTopic]()
    newTopics.add(new NewTopic("test-topic-1", 10, 3.toShort))
    newTopics.add(new NewTopic("test-topic-2", 10, 3.toShort))
    newTopics.add(new NewTopic("test-topic-3", 10, 3.toShort))
    val createTopicResult = admin.createTopics(newTopics)
    createTopicResult.all().get(300, TimeUnit.SECONDS)
    admin.close()
    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient

    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(zkCluster.config().metadataVersion()).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      val serverProperties = new util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig))

      // Emulate a ZK topic deletion
      zkClient.createDeleteTopicPath("test-topic-1")
      zkClient.createDeleteTopicPath("test-topic-2")
      zkClient.createDeleteTopicPath("test-topic-3")

      zkCluster.waitForReadyBrokers()
      readyFuture.get(60, TimeUnit.SECONDS)

      // Only continue with the test if there are some pending deletions to verify. If there are not any pending
      // deletions, this will mark the test as "skipped" instead of failed.
      val topicDeletions = zkClient.getTopicDeletions
      Assumptions.assumeTrue(topicDeletions.nonEmpty,
        "This test needs pending topic deletions after a migration in order to verify the behavior")

      // Wait for migration to begin
      log.info("Waiting for ZK migration to complete")
      TestUtils.waitUntilTrue(
        () => zkClient.getOrCreateMigrationState(ZkMigrationLeadershipState.EMPTY).initialZkMigrationComplete(),
        "Timed out waiting for migration to complete",
        30000)

      // At this point, some of the topics may have been deleted by ZK controller and the rest will be
      // implicitly deleted by the KRaft controller and remove from the ZK brokers as stray partitions
      def topicsAllDeleted(admin: Admin): Boolean = {
        val topics = admin.listTopics().names().get(60, TimeUnit.SECONDS)
        topics.retainAll(util.Arrays.asList(
          "test-topic-1", "test-topic-2", "test-topic-3"
        ))
        topics.isEmpty
      }

      admin = zkCluster.createAdminClient()
      log.info("Waiting for topics to be deleted")
      TestUtils.waitUntilTrue(
        () => topicsAllDeleted(admin),
        "Timed out waiting for topics to be deleted",
        30000,
        1000)

      val newTopics = new util.ArrayList[NewTopic]()
      newTopics.add(new NewTopic("test-topic-1", 2, 3.toShort))
      newTopics.add(new NewTopic("test-topic-2", 1, 3.toShort))
      newTopics.add(new NewTopic("test-topic-3", 10, 3.toShort))
      val createTopicResult = admin.createTopics(newTopics)
      createTopicResult.all().get(60, TimeUnit.SECONDS)

      def topicsAllRecreated(admin: Admin): Boolean = {
        val topics = admin.listTopics().names().get(60, TimeUnit.SECONDS)
        topics.retainAll(util.Arrays.asList(
          "test-topic-1", "test-topic-2", "test-topic-3"
        ))
        topics.size() == 3
      }

      log.info("Waiting for topics to be re-created")
      TestUtils.waitUntilTrue(
        () => topicsAllRecreated(admin),
        "Timed out waiting for topics to be created",
        30000,
        1000)

      TestUtils.retry(300000) {
        // Need a retry here since topic metadata may be inconsistent between brokers
        val topicDescriptions = try {
          admin.describeTopics(util.Arrays.asList(
            "test-topic-1", "test-topic-2", "test-topic-3"
          )).topicNameValues().asScala.map { case (name, description) =>
            name -> description.get(60, TimeUnit.SECONDS)
          }.toMap
        } catch {
          case e: ExecutionException if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => Map.empty[String, TopicDescription]
          case t: Throwable => fail("Error describing topics", t.getCause)
        }

        assertEquals(2, topicDescriptions("test-topic-1").partitions().size())
        assertEquals(1, topicDescriptions("test-topic-2").partitions().size())
        assertEquals(10, topicDescriptions("test-topic-3").partitions().size())
        topicDescriptions.foreach { case (topic, description) =>
          description.partitions().forEach(partition => {
            assertEquals(3, partition.replicas().size(), s"Unexpected number of replicas for $topic-${partition.partition()}")
            assertEquals(3, partition.isr().size(), s"Unexpected ISR for $topic-${partition.partition()}")
          })
        }

        val absentTopics = admin.listTopics().names().get(60, TimeUnit.SECONDS).asScala
        assertTrue(absentTopics.contains("test-topic-1"))
        assertTrue(absentTopics.contains("test-topic-2"))
        assertTrue(absentTopics.contains("test-topic-3"))
      }

      admin.close()
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  // SCRAM and Quota are intermixed. Test SCRAM Only here
  @ClusterTest(types = Array(Type.ZK), brokers = 3, metadataVersion = MetadataVersion.IBP_3_5_IV2, serverProperties = Array(
    new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
    new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
  ))
  def testDualWriteScram(zkCluster: ClusterInstance): Unit = {
    var admin = zkCluster.createAdminClient()
    createUserScramCredentials(admin).all().get(60, TimeUnit.SECONDS)
    admin.close()

    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient

    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_5_IV2).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      val serverProperties = new util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig))
      zkCluster.waitForReadyBrokers()
      readyFuture.get(30, TimeUnit.SECONDS)

      // Wait for migration to begin
      log.info("Waiting for ZK migration to begin")
      TestUtils.waitUntilTrue(
        () => zkClient.getControllerId.contains(3000),
        "Timed out waiting for KRaft controller to take over",
        30000)

      // Alter the metadata
      log.info("Updating metadata with AdminClient")
      admin = zkCluster.createAdminClient()
      alterUserScramCredentials(admin).all().get(60, TimeUnit.SECONDS)

      // Verify the changes made to KRaft are seen in ZK
      log.info("Verifying metadata changes with ZK")
      verifyUserScramCredentials(zkClient)
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  @ClusterTest(types = Array(Type.ZK), brokers = 3, metadataVersion = MetadataVersion.IBP_3_8_IV0, serverProperties = Array(
    new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
    new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
  ))
  def testDeleteLogOnStartup(zkCluster: ClusterInstance): Unit = {
    var admin = zkCluster.createAdminClient()
    try {
      val newTopics = new util.ArrayList[NewTopic]()
      newTopics.add(new NewTopic("testDeleteLogOnStartup", 2, 3.toShort)
        .configs(Map(TopicConfig.SEGMENT_BYTES_CONFIG -> "102400", TopicConfig.SEGMENT_MS_CONFIG -> "300000").asJava))
      val createTopicResult = admin.createTopics(newTopics)
      createTopicResult.all().get(60, TimeUnit.SECONDS)
    } finally {
      admin.close()
    }

    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_8_IV0).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      val serverProperties = new util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig)) // This would throw if authorizers weren't allowed
      zkCluster.waitForReadyBrokers()
      readyFuture.get(30, TimeUnit.SECONDS)

      val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
      TestUtils.waitUntilTrue(
        () => zkClient.getControllerId.contains(3000),
        "Timed out waiting for KRaft controller to take over",
        30000)

      def hasKRaftController: Boolean = {
        zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().brokers.forall(
          broker => broker.metadataCache.getControllerId match {
            case Some(_: KRaftCachedControllerId) => true
            case _ => false
          }
        )
      }
      TestUtils.waitUntilTrue(() => hasKRaftController, "Timed out waiting for ZK brokers to see a KRaft controller")

      log.info("Restart brokers again")
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.empty())
      zkCluster.waitForReadyBrokers()

      admin = zkCluster.createAdminClient()
      try {
        // List topics is served from local MetadataCache on brokers. For ZK brokers this cache is populated by UMR
        // which won't be sent until the broker has been unfenced by the KRaft controller. So, seeing the topic in
        // the brokers cache tells us it has recreated and re-replicated the metadata log
        TestUtils.waitUntilTrue(
          () => admin.listTopics().names().get(30, TimeUnit.SECONDS).asScala.contains("testDeleteLogOnStartup"),
          "Timed out listing topics",
          30000)
      } finally {
        admin.close()
      }
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  // SCRAM and Quota are intermixed. Test Quota Only here
  @ClusterTemplate("zkClustersForAllMigrationVersions")
  def testDualWrite(zkCluster: ClusterInstance): Unit = {
    // Create a topic in ZK mode
    val topicName = "test"
    var admin = zkCluster.createAdminClient()
    val newTopics = new util.ArrayList[NewTopic]()
    newTopics.add(new NewTopic(topicName, 2, 3.toShort)
      .configs(Map(TopicConfig.SEGMENT_BYTES_CONFIG -> "102400", TopicConfig.SEGMENT_MS_CONFIG -> "300000").asJava))
    val createTopicResult = admin.createTopics(newTopics)
    createTopicResult.all().get(60, TimeUnit.SECONDS)
    admin.close()

    // Verify the configs exist in ZK
    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
    val propsBefore = zkClient.getEntityConfigs(ConfigType.TOPIC, topicName)
    assertEquals("102400", propsBefore.getProperty(TopicConfig.SEGMENT_BYTES_CONFIG))
    assertEquals("300000", propsBefore.getProperty(TopicConfig.SEGMENT_MS_CONFIG))

    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(zkCluster.config().metadataVersion()).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Allocate a block of producer IDs while in ZK mode
      var nextProducerId = -1L

      TestUtils.retry(60000) {
        assertDoesNotThrow((() => nextProducerId = sendAllocateProducerIds(zkCluster.asInstanceOf[ZkClusterInstance]).get(20, TimeUnit.SECONDS)): Executable)
      }
      assertEquals(0, nextProducerId)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      val serverProperties = new util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig))
      zkCluster.waitForReadyBrokers()
      readyFuture.get(30, TimeUnit.SECONDS)

      // Wait for migration to begin
      log.info("Waiting for ZK migration to begin")
      TestUtils.waitUntilTrue(
        () => zkClient.getControllerId.contains(3000),
        "Timed out waiting for KRaft controller to take over",
        30000)

      // Alter the metadata
      log.info("Updating metadata with AdminClient")
      admin = zkCluster.createAdminClient()
      alterTopicConfig(admin)
      alterClientQuotas(admin)
      alterBrokerConfigs(admin)

      // Verify the changes made to KRaft are seen in ZK
      log.info("Verifying metadata changes with ZK")
      verifyTopicConfigs(zkClient)
      verifyClientQuotas(zkClient)
      verifyBrokerConfigs(zkClient)
      var nextKRaftProducerId = -1L

      TestUtils.retry(60000) {
        assertDoesNotThrow((() => nextKRaftProducerId = sendAllocateProducerIds(zkCluster.asInstanceOf[ZkClusterInstance]).get(20, TimeUnit.SECONDS)): Executable)
      }
      assertNotEquals(nextProducerId, nextKRaftProducerId)
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  // SCRAM and Quota are intermixed. Test both here
  @ClusterTest(types = Array(Type.ZK), brokers = 3, metadataVersion = MetadataVersion.IBP_3_5_IV2, serverProperties = Array(
    new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
    new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
  ))
  def testDualWriteQuotaAndScram(zkCluster: ClusterInstance): Unit = {
    var admin = zkCluster.createAdminClient()
    createUserScramCredentials(admin).all().get(60, TimeUnit.SECONDS)
    admin.close()

    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient

    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_5_IV2).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      val serverProperties = new util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig))
      zkCluster.waitForReadyBrokers()
      readyFuture.get(30, TimeUnit.SECONDS)

      // Wait for migration to begin
      log.info("Waiting for ZK migration to begin")
      TestUtils.waitUntilTrue(
        () => zkClient.getControllerId.contains(3000),
        "Timed out waiting for KRaft controller to take over",
        30000)

      // Alter the metadata
      log.info("Updating metadata with AdminClient")
      admin = zkCluster.createAdminClient()
      alterUserScramCredentials(admin).all().get(60, TimeUnit.SECONDS)
      alterClientQuotas(admin)

      // Verify the changes made to KRaft are seen in ZK
      log.info("Verifying metadata changes with ZK")
      verifyUserScramCredentials(zkClient)
      verifyClientQuotas(zkClient)
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  @ClusterTest(types = Array(Type.ZK), brokers = 3, metadataVersion = MetadataVersion.IBP_3_4_IV0, serverProperties = Array(
    new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
    new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
  ))
  def testNewAndChangedTopicsInDualWrite(zkCluster: ClusterInstance): Unit = {
    val topic1 = "test1"
    val topic2 = "test2"
    var admin = zkCluster.createAdminClient()
    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient

    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_4_IV0).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      val serverProperties = new util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig))
      zkCluster.waitForReadyBrokers()
      readyFuture.get(30, TimeUnit.SECONDS)

      // Wait for migration to begin
      log.info("Waiting for ZK migration to begin")
      TestUtils.waitUntilTrue(
        () => zkClient.getControllerId.contains(3000),
        "Timed out waiting for KRaft controller to take over",
        30000)

      // Alter the metadata
      admin = zkCluster.createAdminClient()
      log.info(s"Create new topic $topic1 with AdminClient with some configs")
      val topicConfigs = util.Collections.singletonMap("cleanup.policy", "compact")
      createTopic(topic1, 2, 3.toShort, topicConfigs, admin)
      verifyTopic(topic1, 2, 3.toShort, topicConfigs, admin, zkClient)

      log.info(s"Create new topic $topic2 with AdminClient without configs")
      val emptyTopicConfigs: util.Map[String, String] = util.Collections.emptyMap[String, String]
      createTopic(topic2, 2, 3.toShort, emptyTopicConfigs, admin)
      verifyTopic(topic2, 2, 3.toShort, emptyTopicConfigs, admin, zkClient)

      val newPartitionCount = 3
      log.info(s"Create new partitions with AdminClient to $topic1")
      admin.createPartitions(Map(topic1 -> NewPartitions.increaseTo(newPartitionCount)).asJava).all().get(60, TimeUnit.SECONDS)
      val (topicDescOpt, _) = TestUtils.computeUntilTrue(topicDesc(topic1, admin))(td => {
        td.isDefined && td.get.partitions().asScala.size == newPartitionCount
      })
      assertTrue(topicDescOpt.isDefined)
      val partitions = topicDescOpt.get.partitions().asScala
      assertEquals(newPartitionCount, partitions.size)

      // Verify the changes
      verifyZKTopicPartitionMetadata(topic1, newPartitionCount, 3.toShort, zkClient)
      verifyKRaftTopicPartitionMetadata(topic1, newPartitionCount, 3.toShort, admin)
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  @ClusterTest(types = Array(Type.ZK), brokers = 4, metadataVersion = MetadataVersion.IBP_3_7_IV0, serverProperties = Array(
    new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
    new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
  ))
  def testPartitionReassignmentInHybridMode(zkCluster: ClusterInstance): Unit = {
    // Create a topic in ZK mode
    val topicName = "test"
    var admin = zkCluster.createAdminClient()
    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient

    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_7_IV0).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      .setConfigProp(ZkConfigs.ZK_CONNECT_CONFIG, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      val serverProperties = new util.HashMap[String, String](zkCluster.config().serverProperties())
      serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      val clusterConfig = ClusterConfig.builder(zkCluster.config())
        .setServerProperties(serverProperties)
        .build()
      zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig))
      zkCluster.waitForReadyBrokers()
      readyFuture.get(30, TimeUnit.SECONDS)

      // Wait for migration to begin
      log.info("Waiting for ZK migration to begin")
      TestUtils.waitUntilTrue(
        () => zkClient.getControllerId.contains(3000),
        "Timed out waiting for KRaft controller to take over",
        30000)

      // Create a topic with replicas on brokers 0, 1, 2
      log.info("Create new topic with AdminClient")
      admin = zkCluster.createAdminClient()
      val newTopics = new util.ArrayList[NewTopic]()
      val replicaAssignment = Collections.singletonMap(Integer.valueOf(0), Seq(0, 1, 2).map(int2Integer).asJava)
      newTopics.add(new NewTopic(topicName, replicaAssignment))
      val createTopicResult = admin.createTopics(newTopics)
      createTopicResult.all().get(60, TimeUnit.SECONDS)

      val topicPartition = new TopicPartition(topicName, 0)

      // Verify the changes made to KRaft are seen in ZK
      verifyZKTopicPartitionMetadata(topicName, 1, 3.toShort, zkClient)

      // Reassign replicas to brokers 1, 2, 3 and wait for reassignment to complete
      admin.alterPartitionReassignments(Collections.singletonMap(topicPartition,
        Optional.of(new NewPartitionReassignment(Seq(1, 2, 3).map(int2Integer).asJava)))).all().get()

      TestUtils.waitUntilTrue(() => {
        val listPartitionReassignmentsResult = admin.listPartitionReassignments().reassignments().get()
        listPartitionReassignmentsResult.isEmpty
      }, "Timed out waiting for reassignments to complete.")

      // Verify that the partition is removed from broker 0
      TestUtils.waitUntilTrue(() => {
        val brokers = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.brokers
        assertTrue(brokers.size == 4)
        assertTrue(brokers.head.config.brokerId == 0)
        brokers.head.replicaManager.onlinePartition(topicPartition).isEmpty
      }, "Timed out waiting for removed replica reassignment to be marked offline")
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  @ClusterTest(types = Array(Type.ZK), brokers = 3, metadataVersion = MetadataVersion.IBP_3_4_IV0, serverProperties = Array(
    new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
    new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
  ))
  def testIncrementalAlterConfigsPreMigration(zkCluster: ClusterInstance): Unit = {
    // Enable migration configs and restart brokers without KRaft quorum ready
    val serverProperties = new util.HashMap[String, String](zkCluster.config().serverProperties())
    serverProperties.put(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
    serverProperties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9999")
    serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
    val clusterConfig = ClusterConfig.builder(zkCluster.config())
      .setServerProperties(serverProperties)
      .build()
    zkCluster.asInstanceOf[ZkClusterInstance].rollingBrokerRestart(Optional.of(clusterConfig))
    zkCluster.waitForReadyBrokers()

    val admin = zkCluster.createAdminClient()
    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
    try {
      alterBrokerConfigs(admin)
      verifyBrokerConfigs(zkClient)
    } finally {
      admin.close()
      zkClient.close()
      zkCluster.stop()
    }
  }

  def createTopic(topicName: String, numPartitions: Int, replicationFactor: Short, configs: util.Map[String, String], admin: Admin): Unit = {
    val newTopic = new NewTopic(topicName, numPartitions, replicationFactor).configs(configs)
    val createTopicResult = admin.createTopics(util.Collections.singletonList(newTopic))
    createTopicResult.all.get(60, TimeUnit.SECONDS)

    TestUtils.waitUntilTrue(() => {
      admin.listTopics.names.get.contains(topicName)
    }, s"Unable to find topic $topicName")
  }

  def verifyTopic(topicName: String, numPartitions: Int, replicationFactor: Short, configs: util.Map[String, String], admin: Admin, zkClient: KafkaZkClient): Unit = {
    // Verify the changes are in ZK
    verifyZKTopicPartitionMetadata(topicName, numPartitions, replicationFactor, zkClient)
    verifyZKTopicConfigs(topicName, configs, zkClient)
    // Verify the changes are in KRaft
    verifyKRaftTopicPartitionMetadata(topicName, numPartitions, replicationFactor, admin)
    verifyKRaftTopicConfigs(topicName, configs, admin)
  }

  def verifyKRaftTopicPartitionMetadata(topicName: String, numPartitions: Int, replicationFactor: Short, admin: Admin): Unit = {
    val description = topicDesc(topicName, admin).get
    assertEquals(numPartitions, description.partitions.size)
    description.partitions.forEach { p =>
      assertEquals(replicationFactor, p.isr.size)
    }
  }

  def verifyKRaftTopicConfigs(topicName: String, configs: util.Map[String, String], admin: Admin): Unit = {
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName)
    val entries = admin.describeConfigs(util.Collections.singletonList(configResource)).values.get(configResource).get.entries
    val dynamicConfigs = entries.asScala.filter(configEntry => configEntry.source == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG).toList
    assertEquals(configs.size, dynamicConfigs.size)
    dynamicConfigs.foreach(c => assertEquals(configs.get(c.name), c.value))
  }

  def verifyZKTopicConfigs(topicName: String, configs: util.Map[String, String], zkClient: KafkaZkClient): Unit = {
    TestUtils.waitUntilTrue(() => {
      zkClient.pathExists(ConfigEntityZNode.path(ConfigType.TOPIC, topicName))
    }, s"Unable to find ${ConfigEntityZNode.path(ConfigType.TOPIC, topicName)} in ZooKeeper")
    val props = zkClient.getEntityConfigs(ConfigType.TOPIC, topicName)
    assertEquals(configs.size, props.size)
    configs.forEach { case (k, v) =>
      assertEquals(v, props.get(k))
    }
  }

  def verifyZKTopicPartitionMetadata(topicName: String, numPartitions: Int, replicationFactor: Short, zkClient: KafkaZkClient): Unit = {
    val partitions = (0 until numPartitions).map(pId => new TopicPartition(topicName, pId))
    val (topicIdReplicaAssignment, success) = TestUtils.computeUntilTrue(
      zkClient.getReplicaAssignmentAndTopicIdForTopics(Set(topicName)).headOption) {
      x => x.exists(_.assignment.size == partitions.size)
    }
    assertTrue(success, "Unable to find topic metadata in Zk")
    TestUtils.waitUntilTrue(() => {
      val lisrMap = zkClient.getTopicPartitionStates(partitions)
      lisrMap.size == partitions.size &&
        lisrMap.forall { case (tp, lisr) =>
          lisr.leaderAndIsr.isr.size == replicationFactor &&
          lisr.leaderAndIsr.leader >= 0 &&
            topicIdReplicaAssignment.exists(_.assignment(tp).replicas == lisr.leaderAndIsr.isr)
        }
    }, "Unable to find topic partition metadata")
  }

  def topicDesc(topic: String, admin: Admin): Option[TopicDescription] = {
    try {
      admin.describeTopics(util.Collections.singleton(topic)).allTopicNames.get.asScala.get(topic)
    } catch {
      case _: Throwable => None
    }
  }

  def sendAllocateProducerIds(zkClusterInstance: ZkClusterInstance): CompletableFuture[Long] = {
    val channel = zkClusterInstance.getUnderlying.brokers.head.clientToControllerChannelManager
    val brokerId = zkClusterInstance.getUnderlying.brokers.head.config.brokerId
    val brokerEpoch = zkClusterInstance.getUnderlying.brokers.head.replicaManager.brokerEpochSupplier.apply()
    val request = new AllocateProducerIdsRequest.Builder(new AllocateProducerIdsRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpoch))

    val producerIdStart = new CompletableFuture[Long]()
    channel.sendRequest(request, new ControllerRequestCompletionHandler() {
        override def onTimeout(): Unit = {
          producerIdStart.completeExceptionally(new TimeoutException("Request timed out"))
        }

        override def onComplete(response: ClientResponse): Unit = {
          val body = response.responseBody().asInstanceOf[AllocateProducerIdsResponse]
          if (body.data().errorCode() != 0) {
            producerIdStart.completeExceptionally(new RuntimeException(s"Received error code ${body.data().errorCode()}"))
          } else {
            producerIdStart.complete(body.data().producerIdStart())
          }
        }
      })
    producerIdStart
  }

  def readProducerIdBlock(zkClient: KafkaZkClient): ProducerIdsBlock = {
    val (dataOpt, _) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)
    dataOpt.map(ProducerIdBlockZNode.parseProducerIdBlockData).get
  }

  def alterBrokerConfigs(admin: Admin): Unit = {
    val defaultBrokerResource = new ConfigResource(ConfigResource.Type.BROKER, "")
    val defaultBrokerConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "86400000"), AlterConfigOp.OpType.SET),
    ).asJavaCollection
    val broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, "0")
    val broker1Resource = new ConfigResource(ConfigResource.Type.BROKER, "1")
    val specificBrokerConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "43200000"), AlterConfigOp.OpType.SET),
    ).asJavaCollection

    TestUtils.retry(60000) {
      val result = admin.incrementalAlterConfigs(Map(
        defaultBrokerResource -> defaultBrokerConfigs,
        broker0Resource -> specificBrokerConfigs,
        broker1Resource -> specificBrokerConfigs
      ).asJava)
      try {
        result.all().get(10, TimeUnit.SECONDS)
      } catch {
        case t: Throwable => fail("Alter Broker Configs had an error", t)
      }
    }
  }

  def alterTopicConfig(admin: Admin): Unit = {
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "test")
    val alterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.SEGMENT_BYTES_CONFIG, "204800"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.SEGMENT_MS_CONFIG, null), AlterConfigOp.OpType.DELETE)
    ).asJavaCollection
    TestUtils.retry(60000) {
      try {
        admin.incrementalAlterConfigs(Map(topicResource -> alterConfigs).asJava).all().get(10, TimeUnit.SECONDS)
      } catch {
        case t: Throwable => fail("Alter Topic Configs had an error", t)
      }
    }
  }

  def alterClientQuotas(admin: Admin): Unit = {
    val quotas = new util.ArrayList[ClientQuotaAlteration]()
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user@1").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user@1", "client-id" -> "clientA").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 800.0), new ClientQuotaAlteration.Op("producer_byte_rate", 100.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Collections.singletonMap("user", null)),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 900.0), new ClientQuotaAlteration.Op("producer_byte_rate", 100.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("ip" -> "8.8.8.8").asJava),
      List(new ClientQuotaAlteration.Op("connection_creation_rate", 10.0)).asJava))
    TestUtils.retry(60000) {
      try {
        admin.alterClientQuotas(quotas).all().get(10, TimeUnit.SECONDS)
      } catch {
        case t: Throwable => fail("Alter Client Quotas had an error", t)
      }
    }
  }

  def createUserScramCredentials(admin: Admin): AlterUserScramCredentialsResult = {
    val alterations = new util.ArrayList[UserScramCredentialAlteration]()
    alterations.add(new UserScramCredentialUpsertion("user1",
        new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 8190), "password0"))
    admin.alterUserScramCredentials(alterations)
  }

  def alterUserScramCredentials(admin: Admin): AlterUserScramCredentialsResult = {
    val alterations = new util.ArrayList[UserScramCredentialAlteration]()
    alterations.add(new UserScramCredentialUpsertion("user1",
        new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 8191), "password1"))
    alterations.add(new UserScramCredentialUpsertion("user@2",
        new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 8192), "password2"))
    admin.alterUserScramCredentials(alterations)
  }

  def verifyTopicConfigs(zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      val propsAfter = zkClient.getEntityConfigs(ConfigType.TOPIC, "test")
      assertEquals("204800", propsAfter.getProperty(TopicConfig.SEGMENT_BYTES_CONFIG))
      assertFalse(propsAfter.containsKey(TopicConfig.SEGMENT_MS_CONFIG))
    }
  }

  def verifyBrokerConfigs(zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      val defaultBrokerProps = zkClient.getEntityConfigs(ConfigType.BROKER, "<default>")
      assertEquals("86400000", defaultBrokerProps.getProperty(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG))

      val broker0Props = zkClient.getEntityConfigs(ConfigType.BROKER, "0")
      assertEquals("43200000", broker0Props.getProperty(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG))

      val broker1Props = zkClient.getEntityConfigs(ConfigType.BROKER, "1")
      assertEquals("43200000", broker1Props.getProperty(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG))
    }
  }

  def verifyClientQuotas(zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      assertEquals("1000", zkClient.getEntityConfigs(ConfigType.USER, Sanitizer.sanitize("user@1")).getProperty("consumer_byte_rate"))
      assertEquals("900", zkClient.getEntityConfigs(ConfigType.USER, "<default>").getProperty("consumer_byte_rate"))
      assertEquals("800", zkClient.getEntityConfigs("users/" + Sanitizer.sanitize("user@1") + "/clients", "clientA").getProperty("consumer_byte_rate"))
      assertEquals("100", zkClient.getEntityConfigs("users/" + Sanitizer.sanitize("user@1") + "/clients", "clientA").getProperty("producer_byte_rate"))
      assertEquals("10", zkClient.getEntityConfigs(ConfigType.IP, "8.8.8.8").getProperty("connection_creation_rate"))
    }
  }

  def verifyUserScramCredentials(zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      val propertyValue1 = zkClient.getEntityConfigs(ConfigType.USER, Sanitizer.sanitize("user1")).getProperty("SCRAM-SHA-256")
      val scramCredentials1 = ScramCredentialUtils.credentialFromString(propertyValue1)
      assertEquals(8191, scramCredentials1.iterations)

      val propertyValue2 = zkClient.getEntityConfigs(ConfigType.USER, Sanitizer.sanitize("user@2")).getProperty("SCRAM-SHA-256")
      assertNotNull(propertyValue2)
      val scramCredentials2 = ScramCredentialUtils.credentialFromString(propertyValue2)
      assertEquals(8192, scramCredentials2.iterations)
    }
  }

  def shutdownInSequence(zkCluster: ClusterInstance, kraftCluster: KafkaClusterTestKit): Unit = {
    zkCluster.brokerIds().forEach(zkCluster.shutdownBroker(_))
    kraftCluster.close()
    zkCluster.stop()
  }
}
