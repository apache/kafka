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

package kafka.integration

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException
import scala.util.Random
import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq}
import kafka.server.{KafkaBroker, KafkaConfig, QuorumTestHarness}
import kafka.utils.{TestInfoUtils, TestUtils}
import kafka.utils.TestUtils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{InvalidConfigurationException, TimeoutException}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AlterConfigOp, AlterConfigsResult, ConfigEntry, FeatureUpdate}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.logging.log4j.{Level, LogManager}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import com.yammer.metrics.core.Meter
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.LeaderConstants
import org.apache.kafka.server.common.MetadataVersion
import org.apache.logging.log4j.core.config.Configurator

import java.io.File

class UncleanLeaderElectionTest extends QuorumTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  // controlled shutdown is needed for these tests, but we can trim the retry count and backoff interval to
  // reduce test execution time
  val enableControlledShutdown = true

  var configProps1: Properties = _
  var configProps2: Properties = _

  var configs: Seq[KafkaConfig] = Seq.empty[KafkaConfig]
  var brokers: Seq[KafkaBroker] = Seq.empty[KafkaBroker]

  var admin: Admin = _

  val random = new Random()
  val topic = "topic" + random.nextLong()
  val partitionId = 0
  val topicPartition = new TopicPartition(topic, partitionId)

  val kafkaApisLogger = LogManager.getLogger(classOf[kafka.server.KafkaApis])
  val networkProcessorLogger = LogManager.getLogger(classOf[kafka.network.Processor])

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    configProps1 = createBrokerConfig(brokerId1)
    configProps2 = createBrokerConfig(brokerId2)

    for (configProps <- List(configProps1, configProps2)) {
      configProps.put("controlled.shutdown.enable", enableControlledShutdown.toString)
    }

    // temporarily set loggers to a higher level so that tests run quietly
    Configurator.setLevel(kafkaApisLogger.getName, Level.FATAL)
    Configurator.setLevel(networkProcessorLogger.getName, Level.FATAL)
  }

  @AfterEach
  override def tearDown(): Unit = {
    brokers.foreach(broker => shutdownBroker(broker))
    brokers.foreach(broker => broker.config.logDirs.forEach(f => Utils.delete(new File(f))))

    // restore log levels
    Configurator.setLevel(kafkaApisLogger.getName, Level.ERROR)
    Configurator.setLevel(networkProcessorLogger.getName, Level.ERROR)

    admin.close()

    super.tearDown()
  }

  override def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    val properties = new Properties()
    if (testInfo.getTestMethod.get().getName.contains("testUncleanLeaderElectionEnabled")) {
      properties.setProperty("unclean.leader.election.enable", "true")
    }
    properties.setProperty("unclean.leader.election.interval.ms", "10")
    Seq(properties)
  }

  private def startBrokers(cluster: Seq[Properties]): Unit = {
    for (props <- cluster) {
      val config = KafkaConfig.fromProps(props)
      val broker = createBroker(config = config)
      configs ++= List(config)
      brokers ++= List(broker)
    }

    val adminConfigs = new Properties
    admin = TestUtils.createAdminClient(brokers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), adminConfigs)
  }

  private def disableEligibleLeaderReplicas(): Unit = {
    if (metadataVersion.isAtLeast(MetadataVersion.IBP_4_1_IV0)) {
      admin.updateFeatures(
        util.Map.of("eligible.leader.replicas.version", new FeatureUpdate(0, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE))).all().get()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testUncleanLeaderElectionEnabled(groupProtocol: String): Unit = {
    // enable unclean leader election
    configProps1.put("unclean.leader.election.enable", "true")
    configProps2.put("unclean.leader.election.enable", "true")
    startBrokers(Seq(configProps1, configProps2))
    disableEligibleLeaderReplicas()

    // create topic with 1 partition, 2 replicas, one on each broker
    TestUtils.createTopicWithAdmin(admin, topic, brokers, controllerServers, replicaAssignment =  Map(partitionId -> Seq(brokerId1, brokerId2)))
    verifyUncleanLeaderElectionEnabled()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testUncleanLeaderElectionDisabled(groupProtocol: String): Unit = {
    // unclean leader election is disabled by default
    startBrokers(Seq(configProps1, configProps2))
    disableEligibleLeaderReplicas()

    // create topic with 1 partition, 2 replicas, one on each broker
    TestUtils.createTopicWithAdmin(admin, topic, brokers, controllerServers, replicaAssignment =  Map(partitionId -> Seq(brokerId1, brokerId2)))

    verifyUncleanLeaderElectionDisabled()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testUncleanLeaderElectionEnabledByTopicOverride(groupProtocol: String): Unit = {
    // disable unclean leader election globally, but enable for our specific test topic
    configProps1.put("unclean.leader.election.enable", "false")
    configProps2.put("unclean.leader.election.enable", "false")
    startBrokers(Seq(configProps1, configProps2))
    disableEligibleLeaderReplicas()

    // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election enabled
    val topicProps = new Properties()
    topicProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, topic, brokers, controllerServers, replicaAssignment = Map(partitionId -> Seq(brokerId1, brokerId2)), topicConfig = topicProps)

    verifyUncleanLeaderElectionEnabled()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testUncleanLeaderElectionDisabledByTopicOverride(groupProtocol: String): Unit = {
    // enable unclean leader election globally, but disable for our specific test topic
    configProps1.put("unclean.leader.election.enable", "true")
    configProps2.put("unclean.leader.election.enable", "true")
    startBrokers(Seq(configProps1, configProps2))
    disableEligibleLeaderReplicas()

    // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election disabled
    val topicProps = new Properties()
    topicProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false")
    TestUtils.createTopicWithAdmin(admin, topic, brokers, controllerServers, replicaAssignment = Map(partitionId -> Seq(brokerId1, brokerId2)), topicConfig = topicProps)

    verifyUncleanLeaderElectionDisabled()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testUncleanLeaderElectionInvalidTopicOverride(groupProtocol: String): Unit = {
    startBrokers(Seq(configProps1))
    disableEligibleLeaderReplicas()

    // create topic with an invalid value for unclean leader election
    val topicProps = new Properties()
    topicProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "invalid")

    val e = assertThrows(classOf[ExecutionException],
      () => TestUtils.createTopicWithAdmin(admin, topic, brokers, controllerServers, replicaAssignment = Map(partitionId -> Seq(brokerId1, brokerId2)), topicConfig = topicProps))

    assertEquals(classOf[InvalidConfigurationException], e.getCause.getClass)
  }

  def verifyUncleanLeaderElectionEnabled(): Unit = {
    // wait until leader is elected
    val leaderId = awaitLeaderChange(brokers, topicPartition)
    debug("Leader for " + topic + " is elected to be: %s".format(leaderId))
    assertTrue(leaderId == brokerId1 || leaderId == brokerId2,
      "Leader id is set to expected value for topic: " + topic)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1
    debug("Follower for " + topic + " is: %s".format(followerId))

    produceMessage(brokers, topic, "first")
    waitForPartitionMetadata(brokers, topic, partitionId)
    assertEquals(List("first"), consumeAllMessages(topic, 1))

    // shutdown follower server
    brokers.filter(broker => broker.config.brokerId == followerId).map(broker => shutdownBroker(broker))

    produceMessage(brokers, topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic, 2))

    //verify that unclean election metric count is 0
    val uncleanLeaderElectionsPerSecGauge = getGauge("UncleanLeaderElectionsPerSec")
    @volatile var uncleanLeaderElectionsPerSec = uncleanLeaderElectionsPerSecGauge.count()
    assertEquals(0, uncleanLeaderElectionsPerSec)

    // shutdown leader and then restart follower
    brokers.filter(_.config.brokerId == leaderId).map(shutdownBroker)
    val followerBroker = brokers.find(_.config.brokerId == followerId).get
    followerBroker.startup()

    // wait until new leader is (uncleanly) elected
    awaitLeaderChange(brokers, topicPartition, expectedLeaderOpt = Some(followerId), timeout = 30000)
    uncleanLeaderElectionsPerSec = uncleanLeaderElectionsPerSecGauge.count()
    assertEquals(1, uncleanLeaderElectionsPerSec)

    produceMessage(brokers, topic, "third")

    // second message was lost due to unclean election
    assertEquals(List("first", "third"), consumeAllMessages(topic, 2))
  }

  def verifyUncleanLeaderElectionDisabled(): Unit = {
    // wait until leader is elected
    val leaderId = awaitLeaderChange(brokers, topicPartition)
    debug("Leader for " + topic  + " is elected to be: %s".format(leaderId))
    assertTrue(leaderId == brokerId1 || leaderId == brokerId2,
      "Leader id is set to expected value for topic: " + topic)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1
    debug("Follower for " + topic  + " is: %s".format(followerId))

    produceMessage(brokers, topic, "first")
    waitForPartitionMetadata(brokers, topic, partitionId)
    assertEquals(List("first"), consumeAllMessages(topic, 1))

    // shutdown follower server
    brokers.filter(broker => broker.config.brokerId == followerId).map(broker => shutdownBroker(broker))

    produceMessage(brokers, topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic, 2))

    //remove any previous unclean election metric
    val uncleanLeaderElectionsPerSecGauge = getGauge("UncleanLeaderElectionsPerSec")
    @volatile var uncleanLeaderElectionsPerSec = uncleanLeaderElectionsPerSecGauge.count()
    assertEquals(0, uncleanLeaderElectionsPerSec)

    // shutdown leader and then restart follower
    brokers.filter(_.config.brokerId == leaderId).map(shutdownBroker)
    val followerServer = brokers.find(_.config.brokerId == followerId).get
    followerServer.startup()

    // verify that unclean election to non-ISR follower does not occur.
    // That is, leader should be NO_LEADER(-1) and the ISR should has only old leaderId.
    waitForNoLeaderAndIsrHasOldLeaderId(followerServer.replicaManager.metadataCache, leaderId)
    uncleanLeaderElectionsPerSec = uncleanLeaderElectionsPerSecGauge.count()
    assertEquals(0, uncleanLeaderElectionsPerSec)

    // message production and consumption should both fail while leader is down
    val e = assertThrows(classOf[ExecutionException], () => produceMessage(brokers, topic, "third", deliveryTimeoutMs = 1000, requestTimeoutMs = 1000))
    assertEquals(classOf[TimeoutException], e.getCause.getClass)

    assertEquals(List.empty[String], consumeAllMessages(topic, 0))

    // restart leader temporarily to send a successfully replicated message
    brokers.find(_.config.brokerId == leaderId).get.startup()
    awaitLeaderChange(brokers, topicPartition, expectedLeaderOpt = Some(leaderId))

    produceMessage(brokers, topic, "third")
    //make sure follower server joins the ISR
    TestUtils.waitUntilTrue(() => {
      val partitionInfoOpt = followerServer.metadataCache.getLeaderAndIsr(topic, partitionId)
      partitionInfoOpt.isPresent() && partitionInfoOpt.get.isr.contains(followerId)
    }, "Inconsistent metadata after first server startup")

    brokers.filter(_.config.brokerId == leaderId).map(shutdownBroker)

    // verify clean leader transition to ISR follower
    awaitLeaderChange(brokers, topicPartition, expectedLeaderOpt = Some(followerId))
    // verify messages can be consumed from ISR follower that was just promoted to leader
    assertEquals(List("first", "second", "third"), consumeAllMessages(topic, 3))
  }

  private def getGauge(metricName: String) = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (k, _) => k.getName.endsWith(metricName) }
      .getOrElse(throw new AssertionError("Unable to find metric " + metricName))
      ._2.asInstanceOf[Meter]
  }

  private def shutdownBroker(broker: KafkaBroker) = {
    broker.shutdown()
    broker.awaitShutdown()
  }

  private def consumeAllMessages(topic: String, numMessages: Int): Seq[String] = {
    val brokerList = TestUtils.plaintextBootstrapServers(brokers)
    // Don't rely on coordinator as it may be down when this method is called
    val consumer = TestUtils.createConsumer(brokerList,
      groupProtocolFromTestParameters(),
      groupId = "group" + random.nextLong(),
      enableAutoCommit = false,
      valueDeserializer = new StringDeserializer)
    try {
      val tp = new TopicPartition(topic, partitionId)
      consumer.assign(util.List.of(tp))
      consumer.seek(tp, 0)
      TestUtils.consumeRecords(consumer, numMessages).map(_.value)
    } finally consumer.close()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testTopicUncleanLeaderElectionEnableWithAlterTopicConfigs(groupProtocol: String): Unit = {
    // unclean leader election is disabled by default
    startBrokers(Seq(configProps1, configProps2))
    disableEligibleLeaderReplicas()

    // create topic with 1 partition, 2 replicas, one on each broker
    TestUtils.createTopicWithAdmin(admin, topic, brokers, controllerServers, replicaAssignment = Map(partitionId -> Seq(brokerId1, brokerId2)))

    // wait until leader is elected
    val leaderId = awaitLeaderChange(brokers, topicPartition)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1

    produceMessage(brokers, topic, "first")
    waitForPartitionMetadata(brokers, topic, partitionId)
    assertEquals(List("first"), consumeAllMessages(topic, 1))

    // Verify the "unclean.leader.election.enable" won't be triggered even if it is enabled/disabled dynamically,
    // because the leader is still alive
    val adminClient = createAdminClient()
    try {
      val newProps = new Properties
      newProps.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
      alterTopicConfigs(adminClient, topic, newProps).all.get
      // leader should not change to followerId
      awaitLeaderChange(brokers, topicPartition, expectedLeaderOpt = Some(leaderId), timeout = 10000)

      newProps.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false")
      alterTopicConfigs(adminClient, topic, newProps).all.get
      // leader should not change to followerId
      awaitLeaderChange(brokers, topicPartition, expectedLeaderOpt = Some(leaderId), timeout = 10000)
    } finally {
      adminClient.close()
    }

    // shutdown follower server
    brokers.filter(broker => broker.config.brokerId == followerId).map(broker => shutdownBroker(broker))

    produceMessage(brokers, topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic, 2))

    // verify that unclean election metric count is 0
    val uncleanLeaderElectionsPerSecGauge = getGauge("UncleanLeaderElectionsPerSec")
    @volatile var uncleanLeaderElectionsPerSec = uncleanLeaderElectionsPerSecGauge.count()
    assertEquals(0, uncleanLeaderElectionsPerSec)

    // shutdown leader and then restart follower
    brokers.filter(_.config.brokerId == leaderId).map(shutdownBroker)
    val followerBroker = brokers.find(_.config.brokerId == followerId).get
    followerBroker.startup()

    // verify that unclean election to non-ISR follower does not occur.
    // That is, leader should be NO_LEADER(-1) and the ISR should has only old leaderId.
    waitForNoLeaderAndIsrHasOldLeaderId(followerBroker.replicaManager.metadataCache, leaderId)
    uncleanLeaderElectionsPerSec = uncleanLeaderElectionsPerSecGauge.count()
    assertEquals(0, uncleanLeaderElectionsPerSec)

    // message production and consumption should both fail while leader is down
    val e = assertThrows(classOf[ExecutionException], () => produceMessage(brokers, topic, "third", deliveryTimeoutMs = 1000, requestTimeoutMs = 1000))
    assertEquals(classOf[TimeoutException], e.getCause.getClass)

    assertEquals(List.empty[String], consumeAllMessages(topic, 0))

    // Enable unclean leader election for topic
    val adminClient2 = createAdminClient()
    try {
      val newProps = new Properties
      newProps.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
      alterTopicConfigs(adminClient2, topic, newProps).all.get
    } finally {
      adminClient2.close()
    }

    // wait until new leader is (uncleanly) elected
    awaitLeaderChange(brokers, topicPartition, expectedLeaderOpt = Some(followerId), timeout = 30000)
    uncleanLeaderElectionsPerSec = uncleanLeaderElectionsPerSecGauge.count()
    assertEquals(1, uncleanLeaderElectionsPerSec)

    produceMessage(brokers, topic, "third")

    // second message was lost due to unclean election
    assertEquals(List("first", "third"), consumeAllMessages(topic, 2))
  }

  private def alterTopicConfigs(adminClient: Admin, topic: String, topicConfigs: Properties): AlterConfigsResult = {
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)

    val configEntries = topicConfigs.entrySet().stream()
      .map(e => new ConfigEntry(e.getKey.toString, e.getValue.toString))
      .map(e => new AlterConfigOp(e, AlterConfigOp.OpType.SET))
      .toList

    adminClient.incrementalAlterConfigs(util.Map.of(configResource, configEntries))
  }

  private def createAdminClient(): Admin = {
    val config = new Properties
    val bootstrapServers = TestUtils.plaintextBootstrapServers(brokers)
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    Admin.create(config)
  }

  private def waitForNoLeaderAndIsrHasOldLeaderId(metadataCache: MetadataCache, leaderId: Int): Unit = {
    waitUntilTrue(() => metadataCache.getLeaderAndIsr(topic, partitionId).isPresent() &&
      metadataCache.getLeaderAndIsr(topic, partitionId).get.leader() == LeaderConstants.NO_LEADER &&
      util.Set.of(leaderId).equals(metadataCache.getLeaderAndIsr(topic, partitionId).get.isr()),
      "Timed out waiting for broker metadata cache updates the info for topic partition:" + topicPartition)
  }
}
