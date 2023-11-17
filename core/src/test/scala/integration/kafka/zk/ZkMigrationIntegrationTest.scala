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

import kafka.security.authorizer.AclEntry.{WildcardHost, WildcardPrincipalString}
import kafka.server.{ConfigType, ControllerRequestCompletionHandler, KafkaConfig}
import kafka.test.{ClusterConfig, ClusterGenerator, ClusterInstance}
import kafka.test.annotation.{AutoStart, ClusterConfigProperty, ClusterTemplate, ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import kafka.utils.{PasswordEncoder, TestUtils}
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
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.authorizer.StandardAcl
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion, ProducerIdsBlock}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertNotNull, assertTrue, fail}
import org.junit.jupiter.api.{Assumptions, Timeout}
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory

import java.util
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit}
import java.util.{Properties, UUID}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

object ZkMigrationIntegrationTest {
  def addZkBrokerProps(props: Properties): Unit = {
    props.setProperty("inter.broker.listener.name", "EXTERNAL")
    props.setProperty("listeners", "PLAINTEXT://localhost:0,EXTERNAL://localhost:0")
    props.setProperty("advertised.listeners", "PLAINTEXT://localhost:0,EXTERNAL://localhost:0")
    props.setProperty("listener.security.protocol.map", "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
  }

  def zkClustersForAllMigrationVersions(clusterGenerator: ClusterGenerator): Unit = {
    Seq(
      MetadataVersion.IBP_3_4_IV0,
      MetadataVersion.IBP_3_5_IV2,
      MetadataVersion.IBP_3_6_IV2,
      MetadataVersion.IBP_3_7_IV0,
      MetadataVersion.IBP_3_7_IV1
    ).foreach { mv =>
      val clusterConfig = ClusterConfig.defaultClusterBuilder()
        .metadataVersion(mv)
        .brokers(3)
        .`type`(Type.ZK)
        .build()
      addZkBrokerProps(clusterConfig.serverProperties())
      clusterGenerator.accept(clusterConfig)
    }
  }
}

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@Timeout(300)
class ZkMigrationIntegrationTest {

  val log = LoggerFactory.getLogger(classOf[ZkMigrationIntegrationTest])

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
    brokers = 3, clusterType = Type.ZK, autoStart = AutoStart.YES,
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
    val wildcardPrincipal = SecurityUtils.parseKafkaPrincipal(WildcardPrincipalString)

    val acl1 = new AclBinding(resource1, new AccessControlEntry(principal.toString, WildcardHost, READ, ALLOW))
    val acl2 = new AclBinding(resource1, new AccessControlEntry(principal.toString, "192.168.0.1", WRITE, ALLOW))
    val acl3 = new AclBinding(resource2, new AccessControlEntry(principal.toString, WildcardHost, DESCRIBE, ALLOW))
    val acl4 = new AclBinding(prefixedResource, new AccessControlEntry(wildcardPrincipal.toString, WildcardHost, READ, ALLOW))

    val result = admin.createAcls(List(acl1, acl2, acl3, acl4).asJava)
    result.all().get

    val underlying = clusterInstance.asInstanceOf[ZkClusterInstance].getUnderlying()
    val zkClient = underlying.zkClient
    val migrationClient = ZkMigrationClient(zkClient, PasswordEncoder.noop())
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
    brokers = 3, clusterType = Type.ZK, autoStart = AutoStart.YES,
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
      .setConfigProp(KafkaConfig.MigrationEnabledProp, "true")
      .setConfigProp(KafkaConfig.ZkConnectProp, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig());
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart() // This would throw if authorizers weren't allowed
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
  @ClusterTest(brokers = 3, clusterType = Type.ZK, metadataVersion = MetadataVersion.IBP_3_4_IV0)
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
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1", "client-id" -> "clientA").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 800.0), new ClientQuotaAlteration.Op("producer_byte_rate", 100.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("ip" -> "8.8.8.8").asJava),
      List(new ClientQuotaAlteration.Op("connection_creation_rate", 10.0)).asJava))
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
      case None => PasswordEncoder.noop()
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
      assertEquals(3, clientQuotas.size())
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
      .setConfigProp(KafkaConfig.MigrationEnabledProp, "true")
      .setConfigProp(KafkaConfig.ZkConnectProp, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart()

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
            assertEquals(3, partition.replicas().size(), s"Unexpected number of replicas for ${topic}-${partition.partition()}")
            assertEquals(3, partition.isr().size(), s"Unexpected ISR for ${topic}-${partition.partition()}")
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
  @ClusterTest(clusterType = Type.ZK, brokers = 3, metadataVersion = MetadataVersion.IBP_3_5_IV2, serverProperties = Array(
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
      .setConfigProp(KafkaConfig.MigrationEnabledProp, "true")
      .setConfigProp(KafkaConfig.ZkConnectProp, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart()
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

  // SCRAM and Quota are intermixed. Test Quota Only here
  @ClusterTemplate("zkClustersForAllMigrationVersions")
  def testDualWrite(zkCluster: ClusterInstance): Unit = {
    // Create a topic in ZK mode
    var admin = zkCluster.createAdminClient()
    val newTopics = new util.ArrayList[NewTopic]()
    newTopics.add(new NewTopic("test", 2, 3.toShort)
      .configs(Map(TopicConfig.SEGMENT_BYTES_CONFIG -> "102400", TopicConfig.SEGMENT_MS_CONFIG -> "300000").asJava))
    val createTopicResult = admin.createTopics(newTopics)
    createTopicResult.all().get(60, TimeUnit.SECONDS)
    admin.close()

    // Verify the configs exist in ZK
    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
    val propsBefore = zkClient.getEntityConfigs(ConfigType.Topic, "test")
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
      .setConfigProp(KafkaConfig.MigrationEnabledProp, "true")
      .setConfigProp(KafkaConfig.ZkConnectProp, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Allocate a block of producer IDs while in ZK mode
      val nextProducerId = sendAllocateProducerIds(zkCluster.asInstanceOf[ZkClusterInstance]).get(30, TimeUnit.SECONDS)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart()
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
      alterTopicConfig(admin).all().get(60, TimeUnit.SECONDS)
      alterClientQuotas(admin).all().get(60, TimeUnit.SECONDS)

      // Verify the changes made to KRaft are seen in ZK
      log.info("Verifying metadata changes with ZK")
      verifyTopicConfigs(zkClient)
      verifyClientQuotas(zkClient)
      val nextKRaftProducerId = sendAllocateProducerIds(zkCluster.asInstanceOf[ZkClusterInstance]).get(30, TimeUnit.SECONDS)
      assertNotEquals(nextProducerId, nextKRaftProducerId)

    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  // SCRAM and Quota are intermixed. Test both here
  @ClusterTest(clusterType = Type.ZK, brokers = 3, metadataVersion = MetadataVersion.IBP_3_5_IV2, serverProperties = Array(
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
      .setConfigProp(KafkaConfig.MigrationEnabledProp, "true")
      .setConfigProp(KafkaConfig.ZkConnectProp, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart()
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
      alterClientQuotas(admin).all().get(60, TimeUnit.SECONDS)

      // Verify the changes made to KRaft are seen in ZK
      log.info("Verifying metadata changes with ZK")
      verifyUserScramCredentials(zkClient)
      verifyClientQuotas(zkClient)
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 3, metadataVersion = MetadataVersion.IBP_3_4_IV0, serverProperties = Array(
    new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
    new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
  ))
  def testNewAndChangedTopicsInDualWrite(zkCluster: ClusterInstance): Unit = {
    // Create a topic in ZK mode
    val topicName = "test"
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
      .setConfigProp(KafkaConfig.MigrationEnabledProp, "true")
      .setConfigProp(KafkaConfig.ZkConnectProp, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, kraftCluster.quorumVotersConfig())
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart()
      zkCluster.waitForReadyBrokers()
      readyFuture.get(30, TimeUnit.SECONDS)

      // Wait for migration to begin
      log.info("Waiting for ZK migration to begin")
      TestUtils.waitUntilTrue(
        () => zkClient.getControllerId.contains(3000),
        "Timed out waiting for KRaft controller to take over",
        30000)

      // Alter the metadata
      log.info("Create new topic with AdminClient")
      admin = zkCluster.createAdminClient()
      val newTopics = new util.ArrayList[NewTopic]()
      newTopics.add(new NewTopic(topicName, 2, 3.toShort))
      val createTopicResult = admin.createTopics(newTopics)
      createTopicResult.all().get(60, TimeUnit.SECONDS)

      val existingPartitions = Seq(new TopicPartition(topicName, 0), new TopicPartition(topicName, 1))
      // Verify the changes made to KRaft are seen in ZK
      verifyTopicPartitionMetadata(topicName, existingPartitions, zkClient)

      val newPartitionCount = 3
      log.info("Create new partitions with AdminClient")
      admin.createPartitions(Map(topicName -> NewPartitions.increaseTo(newPartitionCount)).asJava).all().get(60, TimeUnit.SECONDS)
      val (topicDescOpt, _) = TestUtils.computeUntilTrue(topicDesc(topicName, admin))(td => {
        td.isDefined && td.get.partitions().asScala.size == newPartitionCount
      })
      assertTrue(topicDescOpt.isDefined)
      val partitions = topicDescOpt.get.partitions().asScala
      assertEquals(newPartitionCount, partitions.size)

      // Verify the changes seen in Zk.
      verifyTopicPartitionMetadata(topicName, existingPartitions ++ Seq(new TopicPartition(topicName, 2)), zkClient)
    } finally {
      shutdownInSequence(zkCluster, kraftCluster)
    }
  }

  def verifyTopicPartitionMetadata(topicName: String, partitions: Seq[TopicPartition], zkClient: KafkaZkClient): Unit = {
    val (topicIdReplicaAssignment, success) = TestUtils.computeUntilTrue(
      zkClient.getReplicaAssignmentAndTopicIdForTopics(Set(topicName)).headOption) {
      x => x.exists(_.assignment.size == partitions.size)
    }
    assertTrue(success, "Unable to find topic metadata in Zk")
    TestUtils.waitUntilTrue(() =>{
      val lisrMap = zkClient.getTopicPartitionStates(partitions.toSeq)
      lisrMap.size == partitions.size &&
        lisrMap.forall { case (tp, lisr) =>
          lisr.leaderAndIsr.leader >= 0 &&
            topicIdReplicaAssignment.exists(_.assignment(tp).replicas == lisr.leaderAndIsr.isr)
        }
    }, "Unable to find topic partition metadata")
  }

  def topicDesc(topic: String, admin: Admin): Option[TopicDescription] = {
    try {
      admin.describeTopics(util.Collections.singleton(topic)).allTopicNames().get().asScala.get(topic)
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
          producerIdStart.complete(body.data().producerIdStart())
        }
      })
    producerIdStart
  }

  def readProducerIdBlock(zkClient: KafkaZkClient): ProducerIdsBlock = {
    val (dataOpt, _) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)
    dataOpt.map(ProducerIdBlockZNode.parseProducerIdBlockData).get
  }

  def alterTopicConfig(admin: Admin): AlterConfigsResult = {
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "test")
    val alterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.SEGMENT_BYTES_CONFIG, "204800"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.SEGMENT_MS_CONFIG, null), AlterConfigOp.OpType.DELETE)
    ).asJavaCollection
    admin.incrementalAlterConfigs(Map(topicResource -> alterConfigs).asJava)
  }

  def alterClientQuotas(admin: Admin): AlterClientQuotasResult = {
    val quotas = new util.ArrayList[ClientQuotaAlteration]()
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1", "client-id" -> "clientA").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 800.0), new ClientQuotaAlteration.Op("producer_byte_rate", 100.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("ip" -> "8.8.8.8").asJava),
      List(new ClientQuotaAlteration.Op("connection_creation_rate", 10.0)).asJava))
    admin.alterClientQuotas(quotas)
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
    alterations.add(new UserScramCredentialUpsertion("user2",
        new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 8192), "password2"))
    admin.alterUserScramCredentials(alterations)
  }

  def verifyTopicConfigs(zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      val propsAfter = zkClient.getEntityConfigs(ConfigType.Topic, "test")
      assertEquals("204800", propsAfter.getProperty(TopicConfig.SEGMENT_BYTES_CONFIG))
      assertFalse(propsAfter.containsKey(TopicConfig.SEGMENT_MS_CONFIG))
    }
  }

  def verifyClientQuotas(zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      assertEquals("1000", zkClient.getEntityConfigs(ConfigType.User, "user1").getProperty("consumer_byte_rate"))
      assertEquals("800", zkClient.getEntityConfigs("users/user1/clients", "clientA").getProperty("consumer_byte_rate"))
      assertEquals("100", zkClient.getEntityConfigs("users/user1/clients", "clientA").getProperty("producer_byte_rate"))
      assertEquals("10", zkClient.getEntityConfigs(ConfigType.Ip, "8.8.8.8").getProperty("connection_creation_rate"))
    }
  }

  def verifyUserScramCredentials(zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      val propertyValue1 = zkClient.getEntityConfigs(ConfigType.User, "user1").getProperty("SCRAM-SHA-256")
      val scramCredentials1 = ScramCredentialUtils.credentialFromString(propertyValue1)
      assertEquals(8191, scramCredentials1.iterations)

      val propertyValue2 = zkClient.getEntityConfigs(ConfigType.User, "user2").getProperty("SCRAM-SHA-256")
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
