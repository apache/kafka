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

import org.apache.kafka.common.config.ConfigException
import org.junit.{After, Before, Ignore, Test}

import scala.util.Random
import scala.collection.JavaConverters._
import org.apache.log4j.{Level, Logger}
import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{CoreUtils, TestUtils}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.junit.Assert._
import org.scalatest.Assertions.intercept

class UncleanLeaderElectionTest extends ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  // controlled shutdown is needed for these tests, but we can trim the retry count and backoff interval to
  // reduce test execution time
  val enableControlledShutdown = true

  var configProps1: Properties = null
  var configProps2: Properties = null

  var configs: Seq[KafkaConfig] = Seq.empty[KafkaConfig]
  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  val random = new Random()
  val topic = "topic" + random.nextLong
  val partitionId = 0

  val kafkaApisLogger = Logger.getLogger(classOf[kafka.server.KafkaApis])
  val networkProcessorLogger = Logger.getLogger(classOf[kafka.network.Processor])

  @Before
  override def setUp() {
    super.setUp()

    configProps1 = createBrokerConfig(brokerId1, zkConnect)
    configProps2 = createBrokerConfig(brokerId2, zkConnect)

    for (configProps <- List(configProps1, configProps2)) {
      configProps.put("controlled.shutdown.enable", enableControlledShutdown.toString)
      configProps.put("controlled.shutdown.max.retries", "1")
      configProps.put("controlled.shutdown.retry.backoff.ms", "1000")
    }

    // temporarily set loggers to a higher level so that tests run quietly
    kafkaApisLogger.setLevel(Level.FATAL)
    networkProcessorLogger.setLevel(Level.FATAL)
  }

  @After
  override def tearDown() {
    servers.foreach(server => shutdownServer(server))
    servers.foreach(server => CoreUtils.delete(server.config.logDirs))

    // restore log levels
    kafkaApisLogger.setLevel(Level.ERROR)
    networkProcessorLogger.setLevel(Level.ERROR)

    super.tearDown()
  }

  private def startBrokers(cluster: Seq[Properties]) {
    for (props <- cluster) {
      val config = KafkaConfig.fromProps(props)
      val server = createServer(config)
      configs ++= List(config)
      servers ++= List(server)
    }
  }

  @Test
  def testUncleanLeaderElectionEnabled(): Unit = {
    // enable unclean leader election
    configProps1.put("unclean.leader.election.enable", "true")
    configProps2.put("unclean.leader.election.enable", "true")
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker
    TestUtils.createTopic(zkClient, topic, Map(partitionId -> Seq(brokerId1, brokerId2)), servers)

    verifyUncleanLeaderElectionEnabled
  }

  @Test
  def testUncleanLeaderElectionDisabled(): Unit = {
    // unclean leader election is disabled by default
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker
    TestUtils.createTopic(zkClient, topic, Map(partitionId -> Seq(brokerId1, brokerId2)), servers)

    verifyUncleanLeaderElectionDisabled
  }

  @Test
  def testUncleanLeaderElectionEnabledByTopicOverride(): Unit = {
    // disable unclean leader election globally, but enable for our specific test topic
    configProps1.put("unclean.leader.election.enable", "false")
    configProps2.put("unclean.leader.election.enable", "false")
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election enabled
    val topicProps = new Properties()
    topicProps.put("unclean.leader.election.enable", "true")
    TestUtils.createTopic(zkClient, topic, Map(partitionId -> Seq(brokerId1, brokerId2)), servers, topicProps)

    verifyUncleanLeaderElectionEnabled
  }

  @Test
  def testUncleanLeaderElectionDisabledByTopicOverride(): Unit = {
    // enable unclean leader election globally, but disable for our specific test topic
    configProps1.put("unclean.leader.election.enable", "true")
    configProps2.put("unclean.leader.election.enable", "true")
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election disabled
    val topicProps = new Properties()
    topicProps.put("unclean.leader.election.enable", "false")
    TestUtils.createTopic(zkClient, topic, Map(partitionId -> Seq(brokerId1, brokerId2)), servers, topicProps)

    verifyUncleanLeaderElectionDisabled
  }

  @Test
  def testUncleanLeaderElectionInvalidTopicOverride(): Unit = {
    startBrokers(Seq(configProps1))

    // create topic with an invalid value for unclean leader election
    val topicProps = new Properties()
    topicProps.put("unclean.leader.election.enable", "invalid")

    intercept[ConfigException] {
      TestUtils.createTopic(zkClient, topic, Map(partitionId -> Seq(brokerId1)), servers, topicProps)
    }
  }

  def verifyUncleanLeaderElectionEnabled(): Unit = {
    // wait until leader is elected
    val leaderId = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)
    debug("Leader for " + topic  + " is elected to be: %s".format(leaderId))
    assertTrue("Leader id is set to expected value for topic: " + topic, leaderId == brokerId1 || leaderId == brokerId2)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1
    debug("Follower for " + topic  + " is: %s".format(followerId))

    produceMessage(servers, topic, "first")
    waitUntilMetadataIsPropagated(servers, topic, partitionId)
    assertEquals(List("first"), consumeAllMessages(topic, 1))

    // shutdown follower server
    servers.filter(server => server.config.brokerId == followerId).map(server => shutdownServer(server))

    produceMessage(servers, topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic, 2))

    //remove any previous unclean election metric
    servers.map(_.kafkaController.controllerContext.stats.removeMetric("UncleanLeaderElectionsPerSec"))

    // shutdown leader and then restart follower
    servers.filter(_.config.brokerId == leaderId).map(shutdownServer)
    val followerServer = servers.find(_.config.brokerId == followerId).get
    followerServer.startup()

    // wait until new leader is (uncleanly) elected
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, newLeaderOpt = Some(followerId))
    assertEquals(1, followerServer.kafkaController.controllerContext.stats.uncleanLeaderElectionRate.count())

    produceMessage(servers, topic, "third")

    // second message was lost due to unclean election
    assertEquals(List("first", "third"), consumeAllMessages(topic, 2))
  }

  def verifyUncleanLeaderElectionDisabled(): Unit = {
    // wait until leader is elected
    val leaderId = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)
    debug("Leader for " + topic  + " is elected to be: %s".format(leaderId))
    assertTrue("Leader id is set to expected value for topic: " + topic, leaderId == brokerId1 || leaderId == brokerId2)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1
    debug("Follower for " + topic  + " is: %s".format(followerId))

    produceMessage(servers, topic, "first")
    waitUntilMetadataIsPropagated(servers, topic, partitionId)
    assertEquals(List("first"), consumeAllMessages(topic, 1))

    // shutdown follower server
    servers.filter(server => server.config.brokerId == followerId).foreach(server => shutdownServer(server))

    produceMessage(servers, topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic, 2))

    //remove any previous unclean election metric
    servers.foreach(server => server.kafkaController.controllerContext.stats.removeMetric("UncleanLeaderElectionsPerSec"))

    // shutdown leader and then restart follower
    servers.filter(server => server.config.brokerId == leaderId).foreach(server => shutdownServer(server))
    val followerServer = servers.find(_.config.brokerId == followerId).get
    followerServer.startup()

    // verify that unclean election to non-ISR follower does not occur
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, newLeaderOpt = Some(-1))
    assertEquals(0, followerServer.kafkaController.controllerContext.stats.uncleanLeaderElectionRate.count())

    // message production and consumption should both fail while leader is down
    try {
      produceMessage(servers, topic, "third", deliveryTimeoutMs = 1000, requestTimeoutMs = 1000)
      fail("Message produced while leader is down should fail, but it succeeded")
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[TimeoutException] => // expected
    }

    assertEquals(List.empty[String], consumeAllMessages(topic, 0))

    // restart leader temporarily to send a successfully replicated message
    servers.filter(server => server.config.brokerId == leaderId).foreach(server => server.startup())
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, newLeaderOpt = Some(leaderId))

    produceMessage(servers, topic, "third")
    //make sure follower server joins the ISR
    TestUtils.waitUntilTrue(() => {
      val partitionInfoOpt = followerServer.metadataCache.getPartitionInfo(topic, partitionId)
      partitionInfoOpt.isDefined && partitionInfoOpt.get.basePartitionState.isr.contains(followerId)
    }, "Inconsistent metadata after first server startup")

    servers.filter(server => server.config.brokerId == leaderId).foreach(server => shutdownServer(server))
    // verify clean leader transition to ISR follower
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, newLeaderOpt = Some(followerId))

    // verify messages can be consumed from ISR follower that was just promoted to leader
    assertEquals(List("first", "second", "third"), consumeAllMessages(topic, 3))
  }

  private def shutdownServer(server: KafkaServer) = {
    server.shutdown()
    server.awaitShutdown()
  }

  private def consumeAllMessages(topic: String, numMessages: Int): Seq[String] = {
    val brokerList = TestUtils.bootstrapServers(servers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    // Don't rely on coordinator as it may be down when this method is called
    val consumer = TestUtils.createConsumer(brokerList,
      groupId = "group" + random.nextLong,
      enableAutoCommit = false,
      valueDeserializer = new StringDeserializer)
    try {
      val tp = new TopicPartition(topic, partitionId)
      consumer.assign(Seq(tp).asJava)
      consumer.seek(tp, 0)
      TestUtils.consumeRecords(consumer, numMessages).map(_.value)
    } finally consumer.close()
  }

  @Test
  def testTopicUncleanLeaderElectionEnable(): Unit = {
    // unclean leader election is disabled by default
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker
    adminZkClient.createTopicWithAssignment(topic, config = new Properties(), Map(partitionId -> Seq(brokerId1, brokerId2)))

    // wait until leader is elected
    val leaderId = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1

    produceMessage(servers, topic, "first")
    waitUntilMetadataIsPropagated(servers, topic, partitionId)
    assertEquals(List("first"), consumeAllMessages(topic, 1))

    // shutdown follower server
    servers.filter(server => server.config.brokerId == followerId).map(server => shutdownServer(server))

    produceMessage(servers, topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic, 2))

    //remove any previous unclean election metric
    servers.map(server => server.kafkaController.controllerContext.stats.removeMetric("UncleanLeaderElectionsPerSec"))

    // shutdown leader and then restart follower
    servers.filter(server => server.config.brokerId == leaderId).map(server => shutdownServer(server))
    val followerServer = servers.find(_.config.brokerId == followerId).get
    followerServer.startup()

    assertEquals(0, followerServer.kafkaController.controllerContext.stats.uncleanLeaderElectionRate.count())

    // message production and consumption should both fail while leader is down
    try {
      produceMessage(servers, topic, "third", deliveryTimeoutMs = 1000, requestTimeoutMs = 1000)
      fail("Message produced while leader is down should fail, but it succeeded")
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[TimeoutException] => // expected
    }

    assertEquals(List.empty[String], consumeAllMessages(topic, 0))

    // Enable unclean leader election for topic
    val adminClient = createAdminClient()
    val newProps = new Properties
    newProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, "true")
    TestUtils.alterTopicConfigs(adminClient, topic, newProps).all.get
    adminClient.close()

    // wait until new leader is (uncleanly) elected
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, newLeaderOpt = Some(followerId))
    assertEquals(1, followerServer.kafkaController.controllerContext.stats.uncleanLeaderElectionRate.count())

    produceMessage(servers, topic, "third")

    // second message was lost due to unclean election
    assertEquals(List("first", "third"), consumeAllMessages(topic, 2))
  }

  private def createAdminClient(): AdminClient = {
    val config = new Properties
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName("PLAINTEXT"))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    AdminClient.create(config)
  }
}
