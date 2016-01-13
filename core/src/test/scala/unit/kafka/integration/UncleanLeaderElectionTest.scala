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
import org.junit.{Test, After, Before}

import scala.util.Random
import org.apache.log4j.{Level, Logger}
import java.util.Properties
import kafka.admin.AdminUtils
import kafka.common.FailedToSendMessageException
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.serializer.StringDecoder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.CoreUtils
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.junit.Assert._

class UncleanLeaderElectionTest extends ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  // controlled shutdown is needed for these tests, but we can trim the retry count and backoff interval to
  // reduce test execution time
  val enableControlledShutdown = true

  var configProps1: Properties = null
  var configProps2: Properties = null

  var configs = Seq.empty[KafkaConfig]
  var servers = Seq.empty[KafkaServer]

  val random = new Random()
  val topic = "topic" + random.nextLong
  val partitionId = 0

  val kafkaApisLogger = Logger.getLogger(classOf[kafka.server.KafkaApis])
  val networkProcessorLogger = Logger.getLogger(classOf[kafka.network.Processor])
  val syncProducerLogger = Logger.getLogger(classOf[kafka.producer.SyncProducer])
  val eventHandlerLogger = Logger.getLogger(classOf[kafka.producer.async.DefaultEventHandler[Object, Object]])

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
    syncProducerLogger.setLevel(Level.FATAL)
    eventHandlerLogger.setLevel(Level.FATAL)
  }

  @After
  override def tearDown() {
    servers.foreach(shutdownServer)
    servers.foreach(server => CoreUtils.rm(server.config.logDirs))

    // restore log levels
    kafkaApisLogger.setLevel(Level.ERROR)
    networkProcessorLogger.setLevel(Level.ERROR)
    syncProducerLogger.setLevel(Level.ERROR)
    eventHandlerLogger.setLevel(Level.ERROR)

    super.tearDown()
  }

  private def startBrokers(cluster: Seq[Properties]) {
    for (props <- cluster) {
      val config = KafkaConfig.fromProps(props)
      configs ++= List(config)
      servers ++= List(createServer(config))
    }
  }

  @Test
  def testUncleanLeaderElectionEnabled {
    // unclean leader election is enabled by default
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(partitionId -> Seq(brokerId1, brokerId2)))

    verifyUncleanLeaderElectionEnabled
  }

  @Test
  def testUncleanLeaderElectionDisabled {
    val clusterProps = Seq(configProps1, configProps2)
    clusterProps.foreach(_.put("unclean.leader.election.enable", "false"))
    startBrokers(clusterProps)

    // create topic with 1 partition, 2 replicas, one on each broker
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(partitionId -> Seq(brokerId1, brokerId2)))

    verifyUncleanLeaderElectionDisabled
  }

  @Test
  def testUncleanLeaderElectionEnabledByTopicOverride {
    // disable unclean leader election globally, but enable for our specific test topic
    val clusterProps = Seq(configProps1, configProps2)
    clusterProps.foreach(_.put("unclean.leader.election.enable", "false"))
    startBrokers(clusterProps)

    // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election enabled
    val topicProps = new Properties()
    topicProps.put("unclean.leader.election.enable", "true")
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(partitionId -> Seq(brokerId1, brokerId2)),
      topicProps)

    verifyUncleanLeaderElectionEnabled
  }

  @Test
  def testCleanLeaderElectionDisabledByTopicOverride {
    // enable unclean leader election globally, but disable for our specific test topic
    val clusterProps = Seq(configProps1, configProps2)
    clusterProps.foreach(_.put("unclean.leader.election.enable", "true"))
    startBrokers(clusterProps)

    // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election disabled
    val topicProps = new Properties()
    topicProps.put("unclean.leader.election.enable", String.valueOf(false))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(partitionId -> Seq(brokerId1, brokerId2)),
      topicProps)

    verifyUncleanLeaderElectionDisabled
  }

  @Test
  def testUncleanLeaderElectionInvalidTopicOverride {
    startBrokers(Seq(configProps1))

    // create topic with an invalid value for unclean leader election
    val topicProps = new Properties()
    topicProps.put("unclean.leader.election.enable", "invalid")

    intercept[ConfigException] {
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(partitionId -> Seq(brokerId1)), topicProps)
    }
  }

  def verifyUncleanLeaderElectionEnabled {
    // wait until leader is elected
    val leaderId = waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId)
    debug("Leader for " + topic  + " is elected to be: %s".format(leaderId))
    assertTrue("Leader id is set to expected value for topic: " + topic, leaderId == brokerId1 || leaderId == brokerId2)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1
    debug("Follower for " + topic  + " is: %s".format(followerId))

    sendMessage(servers, topic, "first")
    waitUntilMetadataIsPropagated(servers, topic, partitionId)
    assertEquals(List("first"), consumeAllMessages(topic))

    // shutdown follower server
    servers.filter(_.config.brokerId == followerId).map(shutdownServer)

    sendMessage(servers, topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic))

    // shutdown leader and then restart follower
    servers.filter(_.config.brokerId == leaderId).map(shutdownServer)
    servers.filter(_.config.brokerId == followerId).map(_.startup())

    // wait until new leader is (uncleanly) elected
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId, newLeaderOpt = Some(followerId))

    sendMessage(servers, topic, "third")

    // second message was lost due to unclean election
    assertEquals(List("first", "third"), consumeAllMessages(topic))
  }

  def verifyUncleanLeaderElectionDisabled {
    // wait until leader is elected
    val leaderId = waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId)
    debug(s"Leader for $topic is elected to be: $leaderId")
    assertTrue(s"Leader id is set to expected value for topic: $topic", leaderId == brokerId1 || leaderId == brokerId2)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1
    debug(s"Follower for $topic is: $followerId")

    sendMessage(servers, topic, "first")
    waitUntilMetadataIsPropagated(servers, topic, partitionId)
    assertEquals(List("first"), consumeAllMessages(topic))

    // shutdown follower server
    servers.filter(_.config.brokerId == followerId).map(shutdownServer)

    sendMessage(servers, topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic))

    // shutdown leader and then restart follower
    servers.filter(_.config.brokerId == leaderId).map(shutdownServer)
    servers.filter(_.config.brokerId == followerId).map(_.startup())

    // verify that unclean election to non-ISR follower does not occur
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId, newLeaderOpt = Some(-1))

    // message production and consumption should both fail while leader is down
    intercept[FailedToSendMessageException] {
      sendMessage(servers, topic, "third")
    }
    assertEquals(List.empty[String], consumeAllMessages(topic))

    // restart leader temporarily to send a successfully replicated message
    servers.filter(_.config.brokerId == leaderId).map(_.startup())
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId, newLeaderOpt = Some(leaderId))

    sendMessage(servers, topic, "third")
    waitUntilMetadataIsPropagated(servers, topic, partitionId)
    servers.filter(_.config.brokerId == leaderId).map(shutdownServer)

    // verify clean leader transition to ISR follower
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId, newLeaderOpt = Some(followerId))

    // verify messages can be consumed from ISR follower that was just promoted to leader
    assertEquals(List("first", "second", "third"), consumeAllMessages(topic))
  }

  private def shutdownServer(server: KafkaServer) = {
    server.shutdown()
    server.awaitShutdown()
  }

  private def consumeAllMessages(topic: String) : List[String] = {
    // use a fresh consumer group every time so that we don't need to mess with disabling auto-commit or
    // resetting the ZK offset
    val consumerProps = createConsumerProperties(zkConnect, "group" + random.nextLong, "id", 1000)
    val consumerConnector = Consumer.create(new ConsumerConfig(consumerProps))
    val messageStream = consumerConnector.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())

    val messages = getMessages(messageStream)
    consumerConnector.shutdown

    messages
  }
}
