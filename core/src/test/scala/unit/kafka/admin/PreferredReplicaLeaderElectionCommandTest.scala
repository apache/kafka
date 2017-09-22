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
package unit.kafka.admin

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util
import java.util.Properties

import kafka.admin.{AdminUtils, PreferredReplicaLeaderElectionCommand}
import kafka.common.{AdminCommandFailedException, TopicAndPartition}
import kafka.network.RequestChannel
import kafka.security.auth._
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{Logging, TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{LeaderNotAvailableException, UnknownServerException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.Assert._
import org.junit.{After, Ignore, Test}

import scala.concurrent.ExecutionException

class PreferredReplicaLeaderElectionCommandTest extends ZooKeeperTestHarness with Logging /*with RackAwareTest*/ {

  var servers: Seq[KafkaServer] = Seq()

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  private def createTestTopicAndCluster(topicPartition: Map[TopicPartition, List[Int]],
                                        authorizer: Option[String] = None) {

    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
    brokerConfigs.foreach(p => p.setProperty("auto.leader.rebalance.enable", "false"))
    authorizer match {
      case Some(className) =>
        brokerConfigs.foreach(p => p.setProperty("authorizer.class.name", className))
      case None =>
    }
    createTestTopicAndCluster(topicPartition,brokerConfigs)
  }

  private def createTestTopicAndCluster(partitionsAndAssignments: Map[TopicPartition, List[Int]],
                                        brokerConfigs: Seq[Properties]) {
    // create brokers
    servers = brokerConfigs.map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    partitionsAndAssignments.foreach { partitionAndAssignment =>
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, partitionAndAssignment._1.topic(),
        Map(partitionAndAssignment._1.partition -> partitionAndAssignment._2))
    }
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.forall(server => partitionsAndAssignments.forall(partitionAndAssignment => server.getLogManager().getLog(partitionAndAssignment._1).isDefined)),
      "Replicas for topic test not created")
  }

  /** Bounce the given server and wait for all servers to get metadata for the given partition */
  private def bounceServer(server: Int, partition: TopicPartition) {
    info(s"Shutting down server $server so a non-preferred replica becomes leader")
    servers(server).shutdown()
    info(s"Starting server $server now that a non-preferred replica is leader")
    servers(server).startup()
    TestUtils.waitUntilTrue(() => servers.forall(server => server.metadataCache.getPartitionInfo(partition.topic(), partition.partition()).isDefined),
      s"Replicas for partition $partition not created")

    servers
  }

  private def getController() = {
    servers.find(p => p.kafkaController.isActive)
  }

  private def getLeader(topicPartition: TopicPartition) = {
    servers(0).metadataCache.getPartitionInfo(topicPartition.topic(), topicPartition.partition()).get.basePartitionState.leader
  }

  private def bootstrapServer: String = {
    val port = servers(0).socketServer.boundPort(ListenerName.normalised("PLAINTEXT"))
    info("Server bound to port "+port)
    "localhost:" + port
  }

  val testPartition = new TopicPartition("test", 0)
  val testPartitionAssignment = List(1, 2, 0)
  val testPartitionPreferredLeader = testPartitionAssignment.head
  val testPartitionAndAssignment = Map(testPartition -> testPartitionAssignment)

  /** Test the case where no partitions are given (=> elect all partitions) */
  @Test
  def testNoPartitionsGiven() {
    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    assertNotEquals(testPartitionPreferredLeader, getLeader(testPartition))
    PreferredReplicaLeaderElectionCommand.run(Array(
      "--bootstrap-server", bootstrapServer))
    // Check the leader for the partition IS the preferred one
    assertEquals(testPartitionPreferredLeader, getLeader(testPartition))
  }

  private def toJsonFile(partitions: scala.collection.Set[TopicPartition]): File = {
    val jsonFile = File.createTempFile("preferredreplicaelection", ".js")
    jsonFile.deleteOnExit()
    val jsonString = ZkUtils.preferredReplicaLeaderElectionZkData(partitions.map(new TopicAndPartition(_)))
    info("Using json: "+jsonString)
    Files.write(Paths.get(jsonFile.getAbsolutePath), jsonString.getBytes(StandardCharsets.UTF_8))
    jsonFile
  }

  /** Test the case where a list of partitions is given */
  @Test
  def testSingletonPartitionGiven() {
    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    assertNotEquals(testPartitionPreferredLeader, getLeader(testPartition))
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer,
        "--path-to-json-file", jsonFile.getAbsolutePath))
    } finally {
      jsonFile.delete()
    }
    // Check the leader for the partition IS the preferred one
    assertEquals(testPartitionPreferredLeader, getLeader(testPartition))
  }

  /** Test the case where a topic does not exist */
  @Test
  def testTopicDoesNotExist() {
    val nonExistentPartition = new TopicPartition("does.not.exist", 0)
    val nonExistentPartitionAssignment = List(1, 2, 0)
    val nonExistentPartitionAndAssignment = Map(nonExistentPartition -> nonExistentPartitionAssignment)

    createTestTopicAndCluster(testPartitionAndAssignment)
    val jsonFile = toJsonFile(nonExistentPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer,
        "--path-to-json-file", jsonFile.getAbsolutePath))
    } catch {
      case e: AdminCommandFailedException =>
        val suppressed = e.getSuppressed()(0)
        assertTrue(suppressed.isInstanceOf[UnknownTopicOrPartitionException])
    } finally {
      jsonFile.delete()
    }
  }


  /** Test the case where several partitions are given */
  @Test
  def testMultiplePartitionsSameAssignment() {
    val testPartitionA = new TopicPartition("testA", 0)
    val testPartitionB = new TopicPartition("testB", 0)
    val testPartitionAssignment = List(1, 2, 0)
    val testPartitionPreferredLeader = testPartitionAssignment.head
    val testPartitionAndAssignment = Map(testPartitionA -> testPartitionAssignment, testPartitionB -> testPartitionAssignment)

    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartitionA)
    // Check the leader for the partition is not the preferred one
    assertNotEquals(testPartitionPreferredLeader, getLeader(testPartitionA))
    assertNotEquals(testPartitionPreferredLeader, getLeader(testPartitionB))
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer,
        "--path-to-json-file", jsonFile.getAbsolutePath))
    } finally {
      jsonFile.delete()
    }
    // Check the leader for the partition IS the preferred one
    assertEquals(testPartitionPreferredLeader, getLeader(testPartitionA))
    assertEquals(testPartitionPreferredLeader, getLeader(testPartitionB))
  }

  /** What happens when the preferred replica is already the leader? */
  @Test
  def testNoopElection() {
    createTestTopicAndCluster(testPartitionAndAssignment)
    // Don't bounce the server. Doublec heck the leader for the partition is the preferred one
    assertEquals(testPartitionPreferredLeader, getLeader(testPartition))
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      // Now do the election, even though the preferred replica is *already* the leader
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer,
        "--path-to-json-file", jsonFile.getAbsolutePath))
      // Check the leader for the partition still is the preferred one
      assertEquals(testPartitionPreferredLeader, getLeader(testPartition))
    } finally {
      jsonFile.delete()
    }

  }

  /** What happens if the preferred replica is offline? */
  @Test
  def testWithOfflinePreferredReplica() {
    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    val leader = getLeader(testPartition)
    assertNotEquals(testPartitionPreferredLeader, leader)
    // Now kill the preferred one
    servers(testPartitionPreferredLeader).shutdown()
    // Now try to elect the preferred one
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer,
        "--path-to-json-file", jsonFile.getAbsolutePath))
      fail();
    } catch {
      case e: AdminCommandFailedException =>
        assertEquals("1 preferred replica(s) could not be elected", e.getMessage)
        val suppressed = e.getSuppressed()(0)
        assertTrue(suppressed.isInstanceOf[LeaderNotAvailableException])
        assertTrue(suppressed.getMessage, suppressed.getMessage.contains("is either not alive or not in the isr"))
        // Check we still have the same leader
        assertEquals(leader, getLeader(testPartition))
    } finally {
      jsonFile.delete()
    }
  }

  /** What happens if the controller gets killed just before an election? */
  @Test
  def testTimeout() {
    createTestTopicAndCluster(testPartitionAndAssignment)
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    val leader = getLeader(testPartition)
    assertNotEquals(testPartitionPreferredLeader, leader)
    // Now kill the controller just before we trigger the election
    servers(getController().get.kafkaController.getControllerID()).shutdown()
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer,
        "--path-to-json-file", jsonFile.getAbsolutePath))
      fail();
    } catch {
      case e: AdminCommandFailedException =>
        assertEquals("1 preferred replica(s) could not be elected", e.getMessage)
        assertTrue(e.getSuppressed()(0).getMessage.contains("Timed out waiting for a node assignment"))
        // Check we still have the same leader
        assertEquals(leader, getLeader(testPartition))
    } finally {
      jsonFile.delete()
    }
  }

  /** Test the case where a list of partitions is given */
  @Test
  def testAuthzFailure() {
    createTestTopicAndCluster(testPartitionAndAssignment, Some(classOf[PreferredReplicaLeaderElectionCommandTestAuthorizer].getName))
    bounceServer(testPartitionPreferredLeader, testPartition)
    // Check the leader for the partition is not the preferred one
    val leader = getLeader(testPartition)
    assertNotEquals(testPartitionPreferredLeader, leader)
    // Check the leader for the partition is not the preferred one
    assertNotEquals(testPartitionPreferredLeader, getLeader(testPartition))
    val jsonFile = toJsonFile(testPartitionAndAssignment.keySet)
    try {
      PreferredReplicaLeaderElectionCommand.run(Array(
        "--bootstrap-server", bootstrapServer,
        "--path-to-json-file", jsonFile.getAbsolutePath))
      fail();
    } catch {
      case e: AdminCommandFailedException =>
        assertEquals("1 preferred replica(s) could not be elected", e.getMessage)
        assertTrue(e.getSuppressed()(0).getMessage.contains("Cluster authorization failed"))
        // Check we still have the same leader
        assertEquals(leader, getLeader(testPartition))
    } finally {
      jsonFile.delete()
    }
  }

}

class PreferredReplicaLeaderElectionCommandTestAuthorizer extends SimpleAclAuthorizer {
  override def authorize(session: RequestChannel.Session, operation: Operation, resource: Resource): Boolean =
    operation != Alter || resource.resourceType != Cluster
}