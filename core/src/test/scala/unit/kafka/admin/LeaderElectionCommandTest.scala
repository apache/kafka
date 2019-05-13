/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.CoreUtils
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.junit.After
import org.junit.Assert._
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import scala.collection.JavaConverters._

final class LeaderElectionCommandTest extends ZooKeeperTestHarness {
  var servers = Seq.empty[KafkaServer]
  val preferredId = 0
  val replicaId = 1
  val topic = "test-topic"
  val partitionId = 0

  val kafkaApisLogger = Logger.getLogger(classOf[kafka.server.KafkaApis])
  val networkProcessorLogger = Logger.getLogger(classOf[kafka.network.Processor])

  @Before
  override def setUp() {
    super.setUp()

    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
    servers = brokerConfigs.map { config =>
      config.setProperty("auto.leader.rebalance.enable", "false")
      config.setProperty("controlled.shutdown.enable", "true")
      config.setProperty("controlled.shutdown.max.retries", "1")
      config.setProperty("controlled.shutdown.retry.backoff.ms", "1000")
      TestUtils.createServer(KafkaConfig.fromProps(config))
    }

    // temporarily set loggers to a higher level so that tests run quietly
    kafkaApisLogger.setLevel(Level.FATAL)
    networkProcessorLogger.setLevel(Level.FATAL)

    // TODO: Make sure that authorization is being tests in another place

    TestUtils.createTopic(zkClient, topic, Map(partitionId -> List(preferredId, replicaId)), servers)
  }

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)

    // restore log levels
    kafkaApisLogger.setLevel(Level.ERROR)
    networkProcessorLogger.setLevel(Level.ERROR)

    super.tearDown()
  }

  @Test
  @Ignore
  def testMultipleBrokers(): Unit = {
    val leaderId = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)
    val followerId = if (leaderId == preferredId) replicaId else preferredId

    val replicatedMessage = "first"
    TestUtils.produceMessage(servers, topic, replicatedMessage)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, partitionId)
    assertEquals(List(replicatedMessage), consumeAllMessages(topic, partitionId, 1))

    // shutdown all but leader
    servers.filter(_.config.brokerId != leaderId).foreach { server =>
      server.shutdown()
      server.awaitShutdown()
    }

    val unreplicatedMessage = "second"
    TestUtils.produceMessage(servers, topic, unreplicatedMessage)
    assertEquals(List(replicatedMessage, unreplicatedMessage), consumeAllMessages(topic, partitionId, 2))

    // shutdown leader and then start follower
    servers.find(_.config.brokerId == leaderId).foreach { server =>
      server.shutdown()
      server.awaitShutdown()
    }
    servers.find(_.config.brokerId == followerId).foreach(_.startup())

    LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", bootstrapServers,
        "--election-type", "unclean",
        "--all-topic-partitions"
      )
    )

    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, newLeaderOpt = Option(followerId))
  }

  @Test
  @Ignore
  def testInvalidBroker(): Unit = ???

  @Test
  @Ignore
  def testAllTopicPartiton(): Unit = ???

  @Test
  @Ignore
  def testTopicPartition(): Unit = ???

  @Test
  @Ignore
  def testPathToJsonFile(): Unit = ???

  @Test
  @Ignore
  def testTopicWithoutPartition(): Unit = ???

  @Test
  @Ignore
  def testTopicDoesNotExist(): Unit = ???

  @Test
  @Ignore
  def testWithOfflinePreferredReplica(): Unit = ???

  @Test
  @Ignore
  def testWithAllReplicaOffline(): Unit = ???

  @Test
  @Ignore
  def testTimeOut(): Unit = ???

  @Test
  @Ignore
  def testUnauthorized(): Unit = ???

  @Test
  @Ignore
  def testMissingElectionType(): Unit = ???

  @Test
  @Ignore
  def testMissingTopicPartitionSelection(): Unit = ???

  private def consumeAllMessages(topic: String, partitionId: Int, numMessages: Int): Seq[String] = {
    val brokerList = TestUtils.bootstrapServers(servers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    val consumer = TestUtils.createConsumer(
      brokerList,
      groupId = "groupId",
      enableAutoCommit = false,
      valueDeserializer = new StringDeserializer
    )
    try {
      val topicPartition = new TopicPartition(topic, partitionId)
      consumer.assign(List(topicPartition).asJava)
      consumer.seek(topicPartition, 0)
      TestUtils.consumeRecords(consumer, numMessages).map(_.value)
    } finally consumer.close()
  }

  private def bootstrapServers: String = {
    servers.map { server =>
      val port = server.socketServer.boundPort(ListenerName.normalised("PLAINTEXT"))
      s"localhost:$port"
    }.mkString(",")
  }
}
