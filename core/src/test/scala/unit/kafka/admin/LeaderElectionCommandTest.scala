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
import java.nio.file.Files
import java.nio.file.Path

import kafka.common.AdminCommandFailedException
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.{Json, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.network.ListenerName
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.concurrent.duration._

final class LeaderElectionCommandTest extends ZooKeeperTestHarness {
  import LeaderElectionCommandTest._

  var servers = Seq.empty[KafkaServer]
  //val broker0 = 0
  val broker1 = 1
  val broker2 = 2

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
    servers = brokerConfigs.map { config =>
      config.setProperty("auto.leader.rebalance.enable", "false")
      config.setProperty("controlled.shutdown.enable", "true")
      config.setProperty("controlled.shutdown.max.retries", "1")
      config.setProperty("controlled.shutdown.retry.backoff.ms", "1000")
      TestUtils.createServer(KafkaConfig.fromProps(config))
    }
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)

    super.tearDown()
  }

  @Test
  def testAllTopicPartition(): Unit = {
    TestUtils.resource(Admin.create(createConfig(servers).asJava)) { client =>
      val topic = "unclean-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2)

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers)

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.assertLeader(client, topicPartition, broker1)

      servers(broker2).shutdown()
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker2))
      servers(broker1).shutdown()
      TestUtils.assertNoLeader(client, topicPartition)
      servers(broker2).startup()

      LeaderElectionCommand.main(
        Array(
          "--bootstrap-server", bootstrapServers(servers),
          "--election-type", "unclean",
          "--all-topic-partitions"
        )
      )

      TestUtils.assertLeader(client, topicPartition, broker2)
    }
  }

  @Test
  def testTopicPartition(): Unit = {
    TestUtils.resource(Admin.create(createConfig(servers).asJava)) { client =>
      val topic = "unclean-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2)

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers)

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.assertLeader(client, topicPartition, broker1)

      servers(broker2).shutdown()
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker2))
      servers(broker1).shutdown()
      TestUtils.assertNoLeader(client, topicPartition)
      servers(broker2).startup()

      LeaderElectionCommand.main(
        Array(
          "--bootstrap-server", bootstrapServers(servers),
          "--election-type", "unclean",
          "--topic", topic,
          "--partition", partition.toString
        )
      )

      TestUtils.assertLeader(client, topicPartition, broker2)
    }
  }

  @Test
  def testPathToJsonFile(): Unit = {
    TestUtils.resource(Admin.create(createConfig(servers).asJava)) { client =>
      val topic = "unclean-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2)

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers)

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.assertLeader(client, topicPartition, broker1)

      servers(broker2).shutdown()
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker2))
      servers(broker1).shutdown()
      TestUtils.assertNoLeader(client, topicPartition)
      servers(broker2).startup()

      val topicPartitionPath = tempTopicPartitionFile(Set(topicPartition))

      LeaderElectionCommand.main(
        Array(
          "--bootstrap-server", bootstrapServers(servers),
          "--election-type", "unclean",
          "--path-to-json-file", topicPartitionPath.toString
        )
      )

      TestUtils.assertLeader(client, topicPartition, broker2)
    }
  }

  @Test
  def testPreferredReplicaElection(): Unit = {
    TestUtils.resource(Admin.create(createConfig(servers).asJava)) { client =>
      val topic = "unclean-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2)

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers)

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.assertLeader(client, topicPartition, broker1)

      servers(broker1).shutdown()
      TestUtils.assertLeader(client, topicPartition, broker2)
      servers(broker1).startup()
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1))

      LeaderElectionCommand.main(
        Array(
          "--bootstrap-server", bootstrapServers(servers),
          "--election-type", "preferred",
          "--all-topic-partitions"
        )
      )

      TestUtils.assertLeader(client, topicPartition, broker1)
    }
  }

  @Test
  def testTopicWithoutPartition(): Unit = {
    val e = assertThrows(classOf[Throwable], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", bootstrapServers(servers),
        "--election-type", "unclean",
        "--topic", "some-topic"
      )
    ))
    assertTrue(e.getMessage.startsWith("Missing required option(s)"))
    assertTrue(e.getMessage.contains(" partition"))
  }

  @Test
  def testPartitionWithoutTopic(): Unit = {
    val e = assertThrows(classOf[Throwable], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", bootstrapServers(servers),
        "--election-type", "unclean",
        "--all-topic-partitions",
        "--partition", "0"
      )
    ))
    assertEquals("Option partition is only allowed if topic is used", e.getMessage)
  }

  @Test
  def testTopicDoesNotExist(): Unit = {
    val e = assertThrows(classOf[AdminCommandFailedException], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", bootstrapServers(servers),
        "--election-type", "preferred",
        "--topic", "unknown-topic-name",
        "--partition", "0"
      )
    ))
    assertTrue(e.getSuppressed()(0).isInstanceOf[UnknownTopicOrPartitionException])
  }

  @Test
  def testMissingElectionType(): Unit = {
    val e = assertThrows(classOf[Throwable], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", bootstrapServers(servers),
        "--topic", "some-topic",
        "--partition", "0"
      )
    ))
    assertTrue(e.getMessage.startsWith("Missing required option(s)"))
    assertTrue(e.getMessage.contains(" election-type"))
  }

  @Test
  def testRecommendedElectionTopicAndPartitionWithoutLeader(): Unit = {
    val e = assertThrows(classOf[Throwable], () => RecommendedLeaderElectionCommand.main(
      Array(
        "--bootstrap-server", bootstrapServers(servers),
        "--topic", "some-topic",
        "--partition", "0"
      )
    ))
    assertTrue(e.getMessage.startsWith("Exactly one of the following combinations is required"))
  }

  @Test
  def testRecommendedElectionTopicAndLeaderWithoutPartition(): Unit = {
    val e = assertThrows(classOf[Throwable], () => RecommendedLeaderElectionCommand.main(
      Array(
        "--bootstrap-server", bootstrapServers(servers),
        "--topic", "some-topic",
        "--leader", "2"
      )
    ))
    assertTrue(e.getMessage.startsWith("Exactly one of the following combinations is required"))
  }

  @Test
  def testRecommendedElectionBothJsonAndTopic(): Unit = {
    val fakeJsonFile = "/tmp/fakeTopicPartitions.json"
    val e = assertThrows(classOf[Throwable], () => RecommendedLeaderElectionCommand.main(
      Array(
        "--bootstrap-server", bootstrapServers(servers),
        "--path-to-json-file", fakeJsonFile,
        "--topic", "some-topic"
      )
    ))
    assertTrue(e.getMessage.startsWith("Exactly one of the following combinations is required"))
  }

  @Test
  def testRecommendedElectionHappyPathTopicPartitionLeader(): Unit = {
    TestUtils.resource(Admin.create(createConfig(servers).asJava)) { client =>
      val topic = "random-topic"
      val partition = 0
      val recommendedLeader = broker2
      val assignment = Seq(broker1, broker2)

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers)

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.assertLeader(client, topicPartition, broker1)

      RecommendedLeaderElectionCommand.main(
        Array(
          "--bootstrap-server", bootstrapServers(servers),
          "--topic", topic,
          "--partition", partition.toString,
          "--leader", recommendedLeader.toString,
          "--preferred-brokers", "0,2" // broker0 and broker2 "healthy"; broker1 not
        )
      )

      TestUtils.assertLeader(client, topicPartition, broker2)
    }
  }

  @Test
  def testRecommendedElectionHappyPathJson(): Unit = {
    TestUtils.resource(Admin.create(createConfig(servers).asJava)) { client =>
      val topic = "random-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2)

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers)

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.assertLeader(client, topicPartition, broker1)

      val jsonPath = tempTopicPartitionLeaderIsrFile(topicPartition, broker1, assignment)

      RecommendedLeaderElectionCommand.main(
        Array(
          "--bootstrap-server", bootstrapServers(servers),
          "--path-to-json-file", jsonPath.toString,
          "--preferred-brokers", "0,2" // broker0 and broker2 "healthy"; broker1 not
        )
      )

      // broker2 is the only healthy (preferred) broker that's also in the ISR (= assignment), so the
      // controller should pick it:
      TestUtils.assertLeader(client, topicPartition, broker2)
    }
  }

  @Test
  def testMissingTopicPartitionSelection(): Unit = {
    val e = assertThrows(classOf[Throwable], () => LeaderElectionCommand.main(
      Array(
        "--bootstrap-server", bootstrapServers(servers),
        "--election-type", "preferred"
      )
    ))
    assertTrue(e.getMessage.startsWith("One and only one of the following options is required: "))
    assertTrue(e.getMessage.contains(" all-topic-partitions"))
    assertTrue(e.getMessage.contains(" topic"))
    assertTrue(e.getMessage.contains(" path-to-json-file"))
  }

  @Test
  def testInvalidBroker(): Unit = {
    val e = assertThrows(classOf[AdminCommandFailedException], () => LeaderElectionCommand.run(
      Array(
        "--bootstrap-server", "example.com:1234",
        "--election-type", "unclean",
        "--all-topic-partitions"
      ),
      1.seconds
    ))
    assertTrue(e.getCause.isInstanceOf[TimeoutException])
  }

  @Test
  def testElectionResultOutput(): Unit = {
    TestUtils.resource(Admin.create(createConfig(servers).asJava)) { client =>
      val topic = "non-preferred-topic"
      val partition0 = 0
      val partition1 = 1
      val assignment0 = Seq(broker1, broker2)
      val assignment1 = Seq(broker2, broker1)

      TestUtils.createTopic(zkClient, topic, Map(partition0 -> assignment0, partition1 -> assignment1), servers)

      val topicPartition0 = new TopicPartition(topic, partition0)
      val topicPartition1 = new TopicPartition(topic, partition1)

      TestUtils.assertLeader(client, topicPartition0, broker1)
      TestUtils.assertLeader(client, topicPartition1, broker2)

      servers(broker1).shutdown()
      TestUtils.assertLeader(client, topicPartition0, broker2)
      servers(broker1).startup()
      TestUtils.waitForBrokersInIsr(client, topicPartition0, Set(broker1))
      TestUtils.waitForBrokersInIsr(client, topicPartition1, Set(broker1))

      val topicPartitionPath = tempTopicPartitionFile(Set(topicPartition0, topicPartition1))
      val output = TestUtils.grabConsoleOutput(
        LeaderElectionCommand.main(
          Array(
            "--bootstrap-server", bootstrapServers(servers),
            "--election-type", "preferred",
            "--path-to-json-file", topicPartitionPath.toString
          )
        )
      )

      val electionResultOutputIter = output.split("\n").iterator
      assertTrue(electionResultOutputIter.hasNext)
      assertTrue(electionResultOutputIter.next().contains(s"Successfully completed leader election (PREFERRED) for partitions $topicPartition0"))
      assertTrue(electionResultOutputIter.hasNext)
      assertTrue(electionResultOutputIter.next().contains(s"Valid replica already elected for partitions $topicPartition1"))
    }
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
    TestUtils.bootstrapServers(servers, new ListenerName("PLAINTEXT"))
  }

  def tempTopicPartitionFile(partitions: Set[TopicPartition]): Path = {
    val file = File.createTempFile("leader-election-command", ".json")
    file.deleteOnExit()

    val jsonString = TestUtils.stringifyTopicPartitions(partitions)

    Files.write(file.toPath, jsonString.getBytes(StandardCharsets.UTF_8))

    file.toPath
  }

  def tempTopicPartitionLeaderIsrFile(tp: TopicPartition, currentLeader: Int, currentIsr: Seq[Int]): Path = {
    val file = File.createTempFile("leader-election-command", ".json")
    file.deleteOnExit()

    // This is more or less our target syntax, in pretty-print form (except this method supports only one element):
    //
    // val fakeJsonString = "{
    //   \"partitions\":[
    //     {
    //       \"topic\": \"foo\",
    //       \"partition\": 1,
    //       \"leader\": 8001,
    //       \"in-sync\": [6221, 1309, 8001]
    //     },
    //     {
    //       \"topic\": \"foobar\",
    //       \"partition\": 2,
    //       \"leader\": 7911,
    //       \"in-sync\": [5577, 7911, 7174]
    //     }
    //   ]
    // }"
    val jsonString = Json.encodeAsString(
      Map(
        "partitions" -> Seq(
          Map(
            "topic" -> tp.topic,
            "partition" -> tp.partition,
            "leader" -> currentLeader,
            "in-sync" -> currentIsr.asJava
          ).asJava
        ).asJava
      ).asJava
    )
    //println(s"\n\nDEBUG:  jsonString = ${jsonString}\n\n")

    Files.write(file.toPath, jsonString.getBytes(StandardCharsets.UTF_8))

    file.toPath
  }
}
