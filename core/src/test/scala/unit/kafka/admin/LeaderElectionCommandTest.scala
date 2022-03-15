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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import kafka.common.AdminCommandFailedException
import kafka.server.IntegrationTestUtils.createTopic
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.test.annotation.{ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.{ClusterConfig, ClusterInstance}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Disabled, Tag}

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.BOTH, brokers = 3)
@Tag("integration")
final class LeaderElectionCommandTest(cluster: ClusterInstance) {
  import LeaderElectionCommandTest._

  val broker1 = 0
  val broker2 = 1
  val broker3 = 2

  @BeforeEach
  def setup(clusterConfig: ClusterConfig): Unit = {
    TestUtils.verifyNoUnexpectedThreads("@BeforeEach")
    clusterConfig.serverProperties().put(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
    clusterConfig.serverProperties().put(KafkaConfig.ControlledShutdownEnableProp, "true")
    clusterConfig.serverProperties().put(KafkaConfig.ControlledShutdownMaxRetriesProp, "1")
    clusterConfig.serverProperties().put(KafkaConfig.ControlledShutdownRetryBackoffMsProp, "1000")
    clusterConfig.serverProperties().put(KafkaConfig.OffsetsTopicReplicationFactorProp, "2")
  }

  @ClusterTest
  def testAllTopicPartition(): Unit = {
    val client = cluster.createAdminClient()
    val topic = "unclean-topic"
    val partition = 0
    val assignment = Seq(broker2, broker3)

    cluster.waitForReadyBrokers()
    createTopic(client, topic, Map(partition -> assignment))

    val topicPartition = new TopicPartition(topic, partition)

    TestUtils.assertLeader(client, topicPartition, broker2)
    cluster.shutdownBroker(broker3)
    TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))
    cluster.shutdownBroker(broker2)
    TestUtils.assertNoLeader(client, topicPartition)
    cluster.startBroker(broker3)
    TestUtils.waitForOnlineBroker(client, broker3)

    LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", cluster.bootstrapServers(),
        "--election-type", "unclean",
        "--all-topic-partitions"
      )
    )

    TestUtils.assertLeader(client, topicPartition, broker3)
  }

  @ClusterTest
  @Disabled // TODO: re-enable until we fixed KAFKA-8541
  def testTopicPartition(): Unit = {
    val client = cluster.createAdminClient()
    val topic = "unclean-topic"
    val partition = 0
    val assignment = Seq(broker2, broker3)

    cluster.waitForReadyBrokers()
    createTopic(client, topic, Map(partition -> assignment))

    val topicPartition = new TopicPartition(topic, partition)

    TestUtils.assertLeader(client, topicPartition, broker2)

    cluster.shutdownBroker(broker3)
    TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))
    cluster.shutdownBroker(broker2)
    TestUtils.assertNoLeader(client, topicPartition)
    cluster.startBroker(broker3)
    TestUtils.waitForOnlineBroker(client, broker3)

    LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", cluster.bootstrapServers(),
        "--election-type", "unclean",
        "--topic", topic,
        "--partition", partition.toString
      )
    )

    TestUtils.assertLeader(client, topicPartition, broker3)
  }

  @ClusterTest
  @Disabled // TODO: re-enable until we fixed KAFKA-8785
  def testPathToJsonFile(): Unit = {
    val client = cluster.createAdminClient()
    val topic = "unclean-topic"
    val partition = 0
    val assignment = Seq(broker2, broker3)

    cluster.waitForReadyBrokers()
    createTopic(client, topic, Map(partition -> assignment))

    val topicPartition = new TopicPartition(topic, partition)

    TestUtils.assertLeader(client, topicPartition, broker2)

    cluster.shutdownBroker(broker3)
    TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))
    cluster.shutdownBroker(broker2)
    TestUtils.assertNoLeader(client, topicPartition)
    cluster.startBroker(broker3)
    TestUtils.waitForOnlineBroker(client, broker3)

    val topicPartitionPath = tempTopicPartitionFile(Set(topicPartition))

    LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", cluster.bootstrapServers(),
        "--election-type", "unclean",
        "--path-to-json-file", topicPartitionPath.toString
      )
    )

    TestUtils.assertLeader(client, topicPartition, broker3)
  }

  @ClusterTest
  @Disabled // TODO: re-enable after KAFKA-13737 is fixed
  def testPreferredReplicaElection(): Unit = {
    val client = cluster.createAdminClient()
    val topic = "preferred-topic"
    val partition = 0
    val assignment = Seq(broker2, broker3)

    cluster.waitForReadyBrokers()
    createTopic(client, topic, Map(partition -> assignment))

    val topicPartition = new TopicPartition(topic, partition)

    TestUtils.assertLeader(client, topicPartition, broker2)

    cluster.shutdownBroker(broker2)
    TestUtils.assertLeader(client, topicPartition, broker3)
    cluster.startBroker(broker2)
    TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker2))

    LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", cluster.bootstrapServers(),
        "--election-type", "preferred",
        "--all-topic-partitions"
      )
    )

    TestUtils.assertLeader(client, topicPartition, broker2)
  }

  @ClusterTest
  def testTopicDoesNotExist(): Unit = {
    val e = assertThrows(classOf[AdminCommandFailedException], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", cluster.bootstrapServers(),
        "--election-type", "preferred",
        "--topic", "unknown-topic-name",
        "--partition", "0"
      )
    ))
    assertTrue(e.getSuppressed()(0).isInstanceOf[UnknownTopicOrPartitionException])
  }

  @ClusterTest
  def testElectionResultOutput(): Unit = {
    val client = cluster.createAdminClient()
    val topic = "non-preferred-topic"
    val partition0 = 0
    val partition1 = 1
    val assignment0 = Seq(broker2, broker3)
    val assignment1 = Seq(broker3, broker2)

    cluster.waitForReadyBrokers()
    createTopic(client, topic, Map(
      partition0 -> assignment0,
      partition1 -> assignment1
    ))

    val topicPartition0 = new TopicPartition(topic, partition0)
    val topicPartition1 = new TopicPartition(topic, partition1)

    TestUtils.assertLeader(client, topicPartition0, broker2)
    TestUtils.assertLeader(client, topicPartition1, broker3)

    cluster.shutdownBroker(broker2)
    TestUtils.assertLeader(client, topicPartition0, broker3)
    cluster.startBroker(broker2)
    TestUtils.waitForBrokersInIsr(client, topicPartition0, Set(broker2))
    TestUtils.waitForBrokersInIsr(client, topicPartition1, Set(broker2))

    val topicPartitionPath = tempTopicPartitionFile(Set(topicPartition0, topicPartition1))
    val output = TestUtils.grabConsoleOutput(
      LeaderElectionCommand.main(
        Array(
          "--bootstrap-server", cluster.bootstrapServers(),
          "--election-type", "preferred",
          "--path-to-json-file", topicPartitionPath.toString
        )
      )
    )

    val electionResultOutputIter = output.split("\n").iterator

    assertTrue(electionResultOutputIter.hasNext)
    val firstLine = electionResultOutputIter.next()
    assertTrue(firstLine.contains(s"Successfully completed leader election (PREFERRED) for partitions $topicPartition0"),
    s"Unexpected output: $firstLine")

    assertTrue(electionResultOutputIter.hasNext)
    val secondLine = electionResultOutputIter.next()
    assertTrue(secondLine.contains(s"Valid replica already elected for partitions $topicPartition1"),
    s"Unexpected output: $secondLine")
  }
}

object LeaderElectionCommandTest {
  def createConfig(servers: Seq[KafkaServer]): Map[String, Object] = {
    Map(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers(servers),
      AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG -> "20000",
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "10000"
    )
  }

  def bootstrapServers(servers: Seq[KafkaServer]): String = {
    TestUtils.plaintextBootstrapServers(servers)
  }

  def tempTopicPartitionFile(partitions: Set[TopicPartition]): Path = {
    val file = File.createTempFile("leader-election-command", ".json")
    file.deleteOnExit()

    val jsonString = TestUtils.stringifyTopicPartitions(partitions)

    Files.write(file.toPath, jsonString.getBytes(StandardCharsets.UTF_8))

    file.toPath
  }
}
