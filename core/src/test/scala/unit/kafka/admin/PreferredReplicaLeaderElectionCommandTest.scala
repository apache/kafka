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
import java.nio.file.{Files, Paths}
import java.util
import java.util.Properties

import scala.collection.Seq
import kafka.common.AdminCommandFailedException
import kafka.security.authorizer.AclAuthorizer
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.PreferredLeaderNotAvailableException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}
import org.apache.kafka.test
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

import scala.jdk.CollectionConverters._

class PreferredReplicaLeaderElectionCommandTest extends ZooKeeperTestHarness with Logging {
  var servers: Seq[KafkaServer] = Seq()

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  private def createTestTopicAndCluster(topicPartition: Map[TopicPartition, List[Int]],
                                        authorizer: Option[String] = None): Unit = {
    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
    brokerConfigs.foreach(p => p.setProperty("auto.leader.rebalance.enable", "false"))
    authorizer match {
      case Some(className) =>
        brokerConfigs.foreach(p => p.setProperty("authorizer.class.name", className))
      case None =>
    }
    createTestTopicAndCluster(topicPartition, brokerConfigs)
  }

  private def createTestTopicAndCluster(partitionsAndAssignments: Map[TopicPartition, List[Int]],
                                        brokerConfigs: Seq[Properties]): Unit = {
    // create brokers
    servers = brokerConfigs.map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    partitionsAndAssignments.foreach { case (tp, assignment) =>
      zkClient.createTopicAssignment(tp.topic, Some(Uuid.randomUuid()),
      Map(tp -> assignment))
    }
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(
      () =>
        servers.forall { server =>
          partitionsAndAssignments.forall { partitionAndAssignment =>
            server.getLogManager.getLog(partitionAndAssignment._1).isDefined
          }
        },
      "Replicas for topic test not created"
    )
  }

  /** Bounce the given targetServer and wait for all servers to get metadata for the given partition */
  private def bounceServer(targetServer: Int, partition: TopicPartition): Unit = {
    debug(s"Shutting down server $targetServer so a non-preferred replica becomes leader")
    servers(targetServer).shutdown()
    debug(s"Starting server $targetServer now that a non-preferred replica is leader")
    servers(targetServer).startup()
    TestUtils.waitUntilTrue(() => servers.forall { server =>
      server.metadataCache.getPartitionInfo(partition.topic, partition.partition).exists { partitionState =>
        partitionState.isr.contains(targetServer)
      }
    },
      s"Replicas for partition $partition not created")
  }

  private def getController() = {
    servers.find(p => p.kafkaController.isActive)
  }

  private def awaitLeader(topicPartition: TopicPartition, timeoutMs: Long = test.TestUtils.DEFAULT_MAX_WAIT_MS): Int = {
    TestUtils.awaitValue(() => {
      servers.head.metadataCache.getPartitionInfo(topicPartition.topic, topicPartition.partition).map(_.leader)
    }, s"Timed out waiting to find current leader of $topicPartition", timeoutMs)
  }

  private def bootstrapServer(broker: Int = 0): String = {
    val port = servers(broker).socketServer.boundPort(ListenerName.normalised("PLAINTEXT"))
    debug("Server bound to port "+port)
    s"localhost:$port"
  }

  val testPartition = new TopicPartition("test", 0)
  val testPartitionAssignment = List(1, 2, 0)
  val testPartitionPreferredLeader = testPartitionAssignment.head
  val testPartitionAndAssignment = Map(testPartition -> testPartitionAssignment)

  /** Test the case multiple values are given for --bootstrap-broker */
  @Test
  def testMultipleBrokersGiven(): Unit = {
    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    assertNotEquals(testPartitionPreferredLeader, awaitLeader(testPartition))
    PreferredReplicaLeaderElectionCommand.run(Array(
      "--bootstrap-server", s"${bootstrapServer(1)},${bootstrapServer(0)}"))
    // Check the leader for the partition IS the preferred one
    assertEquals(testPartitionPreferredLeader, awaitLeader(testPartition))
  }

  /** Test the case when an invalid broker is given for --bootstrap-broker */
  @Test
  def testInvalidBrokerGiven(): Unit = {
    val e = assertThrows(classOf[AdminCommandFailedException], () => PreferredReplicaLeaderElectionCommand.run(Array(
      "--bootstrap-server", "example.com:1234"), timeout = 1000))
    assertTrue(e.getCause.isInstanceOf[TimeoutException])
  }

  /** Test the case where no partitions are given (=> elect all partitions) */
  @Test
  def testNoPartitionsGiven(): Unit = {
    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    assertNotEquals(testPartitionPreferredLeader, awaitLeader(testPartition))
    PreferredReplicaLeaderElectionCommand.run(Array(
      "--bootstrap-server", bootstrapServer()))
    // Check the leader for the partition IS the preferred one
    assertEquals(testPartitionPreferredLeader, awaitLeader(testPartition))
  }

  private def toJsonFile(partitions: Set[TopicPartition]): File = {
    val jsonFile = File.createTempFile("preferredreplicaelection", ".js")
    jsonFile.deleteOnExit()
    val jsonString = TestUtils.stringifyTopicPartitions(partitions)
    debug("Using json: "+jsonString)
    Files.write(Paths.get(jsonFile.getAbsolutePath), jsonString.getBytes(StandardCharsets.UTF_8))
    jsonFile
  }

  /** Test the case where a list of partitions is given */
  @Test
  def testSingletonPartitionGiven(): Unit = {
    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    assertNotEquals(testPartitionPreferredLeader, awaitLeader(testPartition))
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer(),
        "--path-to-json-file", jsonFile.getAbsolutePath))
    } finally {
      jsonFile.delete()
    }
    // Check the leader for the partition IS the preferred one
    assertEquals(testPartitionPreferredLeader, awaitLeader(testPartition))
  }

  /** Test the case where a topic does not exist */
  @Test
  def testTopicDoesNotExist(): Unit = {
    val nonExistentPartition = new TopicPartition("does.not.exist", 0)
    val nonExistentPartitionAssignment = List(1, 2, 0)
    val nonExistentPartitionAndAssignment = Map(nonExistentPartition -> nonExistentPartitionAssignment)

    createTestTopicAndCluster(testPartitionAndAssignment)
    val jsonFile = toJsonFile(nonExistentPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer(),
        "--path-to-json-file", jsonFile.getAbsolutePath))
    } catch {
      case e: AdminCommandFailedException =>
        val suppressed = e.getSuppressed()(0)
        assertTrue(suppressed.isInstanceOf[UnknownTopicOrPartitionException])
      case e: Throwable =>
        e.printStackTrace()
        throw e
    } finally {
      jsonFile.delete()
    }
  }

  /** Test the case where several partitions are given */
  @Test
  def testMultiplePartitionsSameAssignment(): Unit = {
    val testPartitionA = new TopicPartition("testA", 0)
    val testPartitionB = new TopicPartition("testB", 0)
    val testPartitionAssignment = List(1, 2, 0)
    val testPartitionPreferredLeader = testPartitionAssignment.head
    val testPartitionAndAssignment = Map(testPartitionA -> testPartitionAssignment, testPartitionB -> testPartitionAssignment)

    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartitionA)
    // Check the leader for the partition is not the preferred one
    assertNotEquals(testPartitionPreferredLeader, awaitLeader(testPartitionA))
    assertNotEquals(testPartitionPreferredLeader, awaitLeader(testPartitionB))
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer(),
        "--path-to-json-file", jsonFile.getAbsolutePath))
    } finally {
      jsonFile.delete()
    }
    // Check the leader for the partition IS the preferred one
    assertEquals(testPartitionPreferredLeader, awaitLeader(testPartitionA))
    assertEquals(testPartitionPreferredLeader, awaitLeader(testPartitionB))
  }

  /** What happens when the preferred replica is already the leader? */
  @Test
  def testNoopElection(): Unit = {
    createTestTopicAndCluster(testPartitionAndAssignment)
    // Don't bounce the server. Doublecheck the leader for the partition is the preferred one
    assertEquals(testPartitionPreferredLeader, awaitLeader(testPartition))
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      // Now do the election, even though the preferred replica is *already* the leader
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer(),
        "--path-to-json-file", jsonFile.getAbsolutePath))
      // Check the leader for the partition still is the preferred one
      assertEquals(testPartitionPreferredLeader, awaitLeader(testPartition))
    } finally {
      jsonFile.delete()
    }
  }

  /** What happens if the preferred replica is offline? */
  @Test
  def testWithOfflinePreferredReplica(): Unit = {
    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    val leader = awaitLeader(testPartition)
    assertNotEquals(testPartitionPreferredLeader, leader)
    // Now kill the preferred one
    servers(testPartitionPreferredLeader).shutdown()
    // Now try to elect the preferred one
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer(),
        "--path-to-json-file", jsonFile.getAbsolutePath))
      fail();
    } catch {
      case e: AdminCommandFailedException =>
        assertEquals("1 preferred replica(s) could not be elected", e.getMessage)
        val suppressed = e.getSuppressed()(0)
        assertTrue(suppressed.isInstanceOf[PreferredLeaderNotAvailableException])
        assertTrue(suppressed.getMessage.contains("Failed to elect leader for partition test-0 under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          suppressed.getMessage)
        // Check we still have the same leader
        assertEquals(leader, awaitLeader(testPartition))
    } finally {
      jsonFile.delete()
    }
  }

  /** What happens if the controller gets killed just before an election? */
  @Test
  def testTimeout(): Unit = {
    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    val leader = awaitLeader(testPartition)
    assertNotEquals(testPartitionPreferredLeader, leader)
    // Now kill the controller just before we trigger the election
    val controller = getController().get.config.brokerId
    servers(controller).shutdown()
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer(controller),
        "--path-to-json-file", jsonFile.getAbsolutePath),
        timeout = 2000)
      fail();
    } catch {
      case e: AdminCommandFailedException =>
        assertEquals("Timeout waiting for election results", e.getMessage)
        // Check we still have the same leader
        assertEquals(leader, awaitLeader(testPartition))
    } finally {
      jsonFile.delete()
    }
  }

  /** Test the case where client is not authorized */
  @Test
  def testAuthzFailure(): Unit = {
    createTestTopicAndCluster(testPartitionAndAssignment, Some(classOf[PreferredReplicaLeaderElectionCommandTestAuthorizer].getName))
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    val leader = awaitLeader(testPartition)
    assertNotEquals(testPartitionPreferredLeader, leader)
    // Check the leader for the partition is not the preferred one
    assertNotEquals(testPartitionPreferredLeader, awaitLeader(testPartition))
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer(),
        "--path-to-json-file", jsonFile.getAbsolutePath))
      fail()
    } catch {
      case e: AdminCommandFailedException =>
        assertEquals("Not authorized to perform leader election", e.getMessage)
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
        // Check we still have the same leader
        assertEquals(leader, awaitLeader(testPartition))
    } finally {
      jsonFile.delete()
    }
  }

  @Test
  def testPreferredReplicaJsonData(): Unit = {
    // write preferred replica json data to zk path
    val partitionsForPreferredReplicaElection = Set(new TopicPartition("test", 1), new TopicPartition("test2", 1))
    PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkClient, partitionsForPreferredReplicaElection)
    // try to read it back and compare with what was written
    val partitionsUndergoingPreferredReplicaElection = zkClient.getPreferredReplicaElection
    assertEquals(partitionsForPreferredReplicaElection, partitionsUndergoingPreferredReplicaElection,
      "Preferred replica election ser-de failed")
  }

  @Test
  def testBasicPreferredReplicaElection(): Unit = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    val partition = 0
    val preferredReplica = 0
    // create brokers
    val brokerRack = Map(0 -> "rack0", 1 -> "rack1", 2 -> "rack2")
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false, rackInfo = brokerRack).map(KafkaConfig.fromProps)
    // create the topic
    adminZkClient.createTopicWithAssignment(topic, config = new Properties, expectedReplicaAssignment)
    servers = serverConfigs.reverse.map(s => TestUtils.createServer(s))
    // broker 2 should be the leader since it was started first
    val currentLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partition, oldLeaderOpt = None)
    // trigger preferred replica election
    val preferredReplicaElection = new PreferredReplicaLeaderElectionCommand(zkClient, Set(new TopicPartition(topic, partition)))
    preferredReplicaElection.moveLeaderToPreferredReplica()
    val newLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partition, oldLeaderOpt = Some(currentLeader))
    assertEquals(preferredReplica, newLeader, "Preferred replica election failed")
  }
}

class PreferredReplicaLeaderElectionCommandTestAuthorizer extends AclAuthorizer {
  override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
    actions.asScala.map { action =>
      if (action.operation != AclOperation.ALTER || action.resourcePattern.resourceType != ResourceType.CLUSTER)
        AuthorizationResult.ALLOWED
      else
        AuthorizationResult.DENIED
    }.asJava
  }
}
