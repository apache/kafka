/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.io.Closeable
import java.util.{Collections, HashMap, List}

import kafka.admin.ReassignPartitionsCommand._
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.zk.{KafkaZkClient, ZooKeeperTestHarness}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AlterConfigOp, ConfigEntry, DescribeLogDirsResult, NewTopic}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{TopicPartition, TopicPartitionReplica}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.junit.rules.Timeout
import org.junit.Assert.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.{After, Rule, Test}

import scala.collection.Map
import scala.jdk.CollectionConverters._
import scala.collection.{Seq, mutable}

class ReassignPartitionsIntegrationTest extends ZooKeeperTestHarness {
  @Rule
  def globalTimeout: Timeout = Timeout.millis(300000)

  var cluster: ReassignPartitionsTestCluster = null

  def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(5, zkConnect).map(KafkaConfig.fromProps)
  }

  @After
  override def tearDown(): Unit = {
    Utils.closeQuietly(cluster, "ReassignPartitionsTestCluster")
    super.tearDown()
  }

  val unthrottledBrokerConfigs =
    0.to(4).map { brokerId =>
      brokerId -> brokerLevelThrottles.map(throttle => (throttle, -1L)).toMap
    }.toMap

  /**
   * Test running a quick reassignment.
   */
  @Test
  def testReassignment(): Unit = {
    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    val assignment = """{"version":1,"partitions":""" +
      """[{"topic":"foo","partition":0,"replicas":[0,1,3],"log_dirs":["any","any","any"]},""" +
      """{"topic":"bar","partition":0,"replicas":[3,2,0],"log_dirs":["any","any","any"]}""" +
      """]}"""

    // Check that the assignment has not yet been started yet.
    val initialAssignment = Map(
      new TopicPartition("foo", 0) ->
        PartitionReassignmentState(Seq(0, 1, 2), Seq(0, 1, 3), true),
      new TopicPartition("bar", 0) ->
        PartitionReassignmentState(Seq(3, 2, 1), Seq(3, 2, 0), true)
    )
    waitForVerifyAssignment(cluster.adminClient, assignment, false,
      VerifyAssignmentResult(initialAssignment))
    waitForVerifyAssignment(zkClient, assignment, false,
      VerifyAssignmentResult(initialAssignment))

    // Execute the assignment
    runExecuteAssignment(cluster.adminClient, false, assignment, -1L, -1L)
    assertEquals(unthrottledBrokerConfigs,
      describeBrokerLevelThrottles(unthrottledBrokerConfigs.keySet.toSeq))
    val finalAssignment = Map(
      new TopicPartition("foo", 0) ->
        PartitionReassignmentState(Seq(0, 1, 3), Seq(0, 1, 3), true),
      new TopicPartition("bar", 0) ->
        PartitionReassignmentState(Seq(3, 2, 0), Seq(3, 2, 0), true)
    )

    // When using --zookeeper, we aren't able to see the new-style assignment
    assertFalse(runVerifyAssignment(zkClient, assignment, false).movesOngoing)

    // Wait for the assignment to complete
    waitForVerifyAssignment(zkClient, assignment, false,
      VerifyAssignmentResult(finalAssignment))

    assertEquals(unthrottledBrokerConfigs,
      describeBrokerLevelThrottles(unthrottledBrokerConfigs.keySet.toSeq))
  }

  @Test
  def testReassignmentFailOnInconsistentReplicationFactorBetweenPartitions(): Unit = {
    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    val assignment = """{"version":1,"partitions":""" +
      """[{"topic":"foo","partition":0,"replicas":[0,1],"log_dirs":["any","any"]},""" +
      """{"topic":"foo","partition":1,"replicas":[0,1,3],"log_dirs":["any","any","any"]},""" +
      """{"topic":"bar","partition":0,"replicas":[3,2,1,0],"log_dirs":["any","any","any","any"]},""" +
      """{"topic":"baz","partition":2,"replicas":[3,2,1,0],"log_dirs":["any","any","any","any"]}""" +
      """]}"""

    // Check that the assignment has not yet been started yet
    val initialAssignment = Map(
      new TopicPartition("foo", 0) ->
        PartitionReassignmentState(Seq(0, 1, 2), Seq(0, 1), true),
      new TopicPartition("foo", 1) ->
        PartitionReassignmentState(Seq(1, 2, 3), Seq(0, 1, 3), true),
      new TopicPartition("bar", 0) ->
        PartitionReassignmentState(Seq(3, 2, 1), Seq(3, 2, 1, 0), true),
      new TopicPartition("baz", 2) ->
        PartitionReassignmentState(Seq(0, 2, 1), Seq(3, 2, 1, 0), true)
    )
    waitForVerifyAssignment(cluster.adminClient, assignment, false,
      VerifyAssignmentResult(initialAssignment))
    waitForVerifyAssignment(zkClient, assignment, false,
      VerifyAssignmentResult(initialAssignment))

    // Execute the assignment
    val exception = assertThrows(classOf[TerseReassignmentFailureException],
      () => runExecuteAssignment(cluster.adminClient, false, assignment, -1L, -1L))
    assertTrue(exception.getMessage.contains(
      "foo-0: Inconsistent replication factor between partitions. Partitions [0, 1] will have replication factors [2, 3] respectively."))
    assertTrue(exception.getMessage.contains(
      "foo-1: Inconsistent replication factor between partitions. Partitions [0, 1] will have replication factors [2, 3] respectively."))
    assertTrue(exception.getMessage.contains(
      "baz-2: Inconsistent replication factor between partitions. Partitions [0, 1, 2] will have replication factors [3, 3, 4] respectively."))
    assertEquals(unthrottledBrokerConfigs,
      describeBrokerLevelThrottles(unthrottledBrokerConfigs.keySet.toSeq))
    val finalAssignment = Map(
      new TopicPartition("foo", 0) ->
        PartitionReassignmentState(Seq(0, 1, 2), Seq(0, 1), true),
      new TopicPartition("foo", 1) ->
        PartitionReassignmentState(Seq(1, 2, 3), Seq(0, 1, 3), true),
      new TopicPartition("bar", 0) ->
        PartitionReassignmentState(Seq(3, 2, 1, 0), Seq(3, 2, 1, 0), true),
      new TopicPartition("baz", 2) ->
        PartitionReassignmentState(Seq(0, 2, 1), Seq(3, 2, 1, 0), true)
    )

    // When using --zookeeper, we aren't able to see the new-style assignment
    assertFalse(runVerifyAssignment(zkClient, assignment, false).movesOngoing)

    // Check that the assigment has changed only for topics with the same replication factor for all partitions
    waitForVerifyAssignment(zkClient, assignment, false,
      VerifyAssignmentResult(finalAssignment))

    assertEquals(unthrottledBrokerConfigs,
      describeBrokerLevelThrottles(unthrottledBrokerConfigs.keySet.toSeq))
  }

  /**
   * Test running a quick reassignment with the --zookeeper option.
   */
  @Test
  def testLegacyReassignment(): Unit = {
    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    val assignment = """{"version":1,"partitions":""" +
      """[{"topic":"foo","partition":0,"replicas":[0,1,3],"log_dirs":["any","any","any"]},""" +
      """{"topic":"bar","partition":0,"replicas":[3,2,0],"log_dirs":["any","any","any"]}""" +
      """]}"""
    // Execute the assignment
    runExecuteAssignment(zkClient, assignment, -1L)
    val finalAssignment = Map(
      new TopicPartition("foo", 0) ->
        PartitionReassignmentState(Seq(0, 1, 3), Seq(0, 1, 3), true),
      new TopicPartition("bar", 0) ->
        PartitionReassignmentState(Seq(3, 2, 0), Seq(3, 2, 0), true)
    )
    // Wait for the assignment to complete
    waitForVerifyAssignment(cluster.adminClient, assignment, false,
      VerifyAssignmentResult(finalAssignment))
    waitForVerifyAssignment(zkClient, assignment, false,
      VerifyAssignmentResult(finalAssignment))
  }

  @Test
  def testHighWaterMarkAfterPartitionReassignment(): Unit = {
    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    val assignment = """{"version":1,"partitions":""" +
      """[{"topic":"foo","partition":0,"replicas":[3,1,2],"log_dirs":["any","any","any"]}""" +
      """]}"""

    // Set the high water mark of foo-0 to 123 on its leader.
    val part = new TopicPartition("foo", 0)
    cluster.servers(0).replicaManager.logManager.truncateFullyAndStartAt(part, 123L, false)

    // Execute the assignment
    runExecuteAssignment(cluster.adminClient, false, assignment, -1L, -1L)
    val finalAssignment = Map(part ->
        PartitionReassignmentState(Seq(3, 1, 2), Seq(3, 1, 2), true))

    // Wait for the assignment to complete
    waitForVerifyAssignment(cluster.adminClient, assignment, false,
      VerifyAssignmentResult(finalAssignment))

    TestUtils.waitUntilTrue(() => {
      cluster.servers(3).replicaManager.nonOfflinePartition(part).
        flatMap(_.leaderLogIfLocal).isDefined
      }, "broker 3 should be the new leader", pause = 10L)
    assertEquals(s"Expected broker 3 to have the correct high water mark for the " +
      "partition.", 123L, cluster.servers(3).replicaManager.
      localLogOrException(part).highWatermark)
  }

  @Test
  def testAlterReassignmentThrottle(): Unit = {
    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    cluster.produceMessages("foo", 0, 50)
    cluster.produceMessages("baz", 2, 60)
    val assignment = """{"version":1,"partitions":
      [{"topic":"foo","partition":0,"replicas":[0,3,2],"log_dirs":["any","any","any"]},
      {"topic":"baz","partition":2,"replicas":[3,2,1],"log_dirs":["any","any","any"]}
      ]}"""

    // Execute the assignment with a low throttle
    val initialThrottle = 1L
    runExecuteAssignment(cluster.adminClient, false, assignment, initialThrottle, -1L)
    waitForInterBrokerThrottle(Set(0, 1, 2, 3), initialThrottle)

    // Now update the throttle and verify the reassignment completes
    val updatedThrottle = 300000L
    runExecuteAssignment(cluster.adminClient, additional = true, assignment, updatedThrottle, -1L)
    waitForInterBrokerThrottle(Set(0, 1, 2, 3), updatedThrottle)

    val finalAssignment = Map(
      new TopicPartition("foo", 0) ->
        PartitionReassignmentState(Seq(0, 3, 2), Seq(0, 3, 2), true),
      new TopicPartition("baz", 2) ->
        PartitionReassignmentState(Seq(3, 2, 1), Seq(3, 2, 1), true))

    // Now remove the throttles.
    waitForVerifyAssignment(cluster.adminClient, assignment, false,
      VerifyAssignmentResult(finalAssignment))
    waitForBrokerLevelThrottles(unthrottledBrokerConfigs)
  }

  /**
   * Test running a reassignment with the interBrokerThrottle set.
   */
  @Test
  def testThrottledReassignment(): Unit = {
    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    cluster.produceMessages("foo", 0, 50)
    cluster.produceMessages("baz", 2, 60)
    val assignment = """{"version":1,"partitions":""" +
      """[{"topic":"foo","partition":0,"replicas":[0,3,2],"log_dirs":["any","any","any"]},""" +
      """{"topic":"baz","partition":2,"replicas":[3,2,1],"log_dirs":["any","any","any"]}""" +
      """]}"""

    // Check that the assignment has not yet been started yet.
    val initialAssignment = Map(
      new TopicPartition("foo", 0) ->
        PartitionReassignmentState(Seq(0, 1, 2), Seq(0, 3, 2), true),
      new TopicPartition("baz", 2) ->
        PartitionReassignmentState(Seq(0, 2, 1), Seq(3, 2, 1), true))
    assertEquals(VerifyAssignmentResult(initialAssignment),
      runVerifyAssignment(cluster.adminClient, assignment, false))
    assertEquals(VerifyAssignmentResult(initialAssignment),
      runVerifyAssignment(zkClient, assignment, false))
    assertEquals(unthrottledBrokerConfigs,
      describeBrokerLevelThrottles(unthrottledBrokerConfigs.keySet.toSeq))

    // Execute the assignment
    val interBrokerThrottle = 300000L
    runExecuteAssignment(cluster.adminClient, false, assignment, interBrokerThrottle, -1L)
    waitForInterBrokerThrottle(Set(0, 1, 2, 3), interBrokerThrottle)

    val finalAssignment = Map(
      new TopicPartition("foo", 0) ->
        PartitionReassignmentState(Seq(0, 3, 2), Seq(0, 3, 2), true),
      new TopicPartition("baz", 2) ->
        PartitionReassignmentState(Seq(3, 2, 1), Seq(3, 2, 1), true))

    // Wait for the assignment to complete
    TestUtils.waitUntilTrue(
      () => {
        // Check the reassignment status.
        val result = runVerifyAssignment(cluster.adminClient, assignment, true)
        if (!result.partsOngoing) {
          true
        } else {
          assertTrue("Expected at least one partition reassignment to be ongoing when " +
            s"result = ${result}", !result.partStates.forall(_._2.done))
          assertEquals(Seq(0, 3, 2),
            result.partStates(new TopicPartition("foo", 0)).targetReplicas)
          assertEquals(Seq(3, 2, 1),
            result.partStates(new TopicPartition("baz", 2)).targetReplicas)
          logger.info(s"Current result: ${result}")
          waitForInterBrokerThrottle(Set(0, 1, 2, 3), interBrokerThrottle)
          false
        }
      }, "Expected reassignment to complete.")
    waitForVerifyAssignment(cluster.adminClient, assignment, true,
      VerifyAssignmentResult(finalAssignment))
    waitForVerifyAssignment(zkClient, assignment, true,
      VerifyAssignmentResult(finalAssignment))
    // The throttles should still have been preserved, since we ran with --preserve-throttles
    waitForInterBrokerThrottle(Set(0, 1, 2, 3), interBrokerThrottle)
    // Now remove the throttles.
    waitForVerifyAssignment(cluster.adminClient, assignment, false,
      VerifyAssignmentResult(finalAssignment))
    waitForBrokerLevelThrottles(unthrottledBrokerConfigs)
  }

  @Test
  def testProduceAndConsumeWithReassignmentInProgress(): Unit = {
    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    cluster.produceMessages("baz", 2, 60)
    val assignment = """{"version":1,"partitions":""" +
      """[{"topic":"baz","partition":2,"replicas":[3,2,1],"log_dirs":["any","any","any"]}""" +
      """]}"""
    runExecuteAssignment(cluster.adminClient, false, assignment, 300L, -1L)
    cluster.produceMessages("baz", 2, 100)
    val consumer = TestUtils.createConsumer(cluster.brokerList)
    val part = new TopicPartition("baz", 2)
    try {
      consumer.assign(Seq(part).asJava)
      TestUtils.pollUntilAtLeastNumRecords(consumer, numRecords = 100)
    } finally {
      consumer.close()
    }
    TestUtils.removeReplicationThrottleForPartitions(cluster.adminClient, Seq(0,1,2,3), Set(part))
    val finalAssignment = Map(part ->
      PartitionReassignmentState(Seq(3, 2, 1), Seq(3, 2, 1), true))
    waitForVerifyAssignment(cluster.adminClient, assignment, false,
      VerifyAssignmentResult(finalAssignment))
  }

  /**
   * Test running a reassignment and then cancelling it.
   */
  @Test
  def testCancellation(): Unit = {
    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    cluster.produceMessages("foo", 0, 200)
    cluster.produceMessages("baz", 1, 200)
    val assignment = """{"version":1,"partitions":""" +
      """[{"topic":"foo","partition":0,"replicas":[0,1,3],"log_dirs":["any","any","any"]},""" +
      """{"topic":"baz","partition":1,"replicas":[0,2,3],"log_dirs":["any","any","any"]}""" +
      """]}"""
    assertEquals(unthrottledBrokerConfigs,
      describeBrokerLevelThrottles(unthrottledBrokerConfigs.keySet.toSeq))
    val interBrokerThrottle = 1L
    runExecuteAssignment(cluster.adminClient, false, assignment, interBrokerThrottle, -1L)
    waitForInterBrokerThrottle(Set(0, 1, 2, 3), interBrokerThrottle)

    // Verify that the reassignment is running.  The very low throttle should keep it
    // from completing before this runs.
    waitForVerifyAssignment(cluster.adminClient, assignment, true,
      VerifyAssignmentResult(Map(
        new TopicPartition("foo", 0) -> PartitionReassignmentState(Seq(0, 1, 3, 2), Seq(0, 1, 3), false),
        new TopicPartition("baz", 1) -> PartitionReassignmentState(Seq(0, 2, 3, 1), Seq(0, 2, 3), false)),
      true, Map(), false))
    // Cancel the reassignment.
    assertEquals((Set(
        new TopicPartition("foo", 0),
        new TopicPartition("baz", 1)
      ), Set()), runCancelAssignment(cluster.adminClient, assignment, true))
    // Broker throttles are still active because we passed --preserve-throttles
    waitForInterBrokerThrottle(Set(0, 1, 2, 3), interBrokerThrottle)
    // Cancelling the reassignment again should reveal nothing to cancel.
    assertEquals((Set(), Set()), runCancelAssignment(cluster.adminClient, assignment, false))
    // This time, the broker throttles were removed.
    waitForBrokerLevelThrottles(unthrottledBrokerConfigs)
    // Verify that there are no ongoing reassignments.
    assertFalse(runVerifyAssignment(cluster.adminClient, assignment, false).partsOngoing)
  }

  private def waitForLogDirThrottle(throttledBrokers: Set[Int], logDirThrottle: Long): Unit = {
    val throttledConfigMap = Map[String, Long](
      brokerLevelLeaderThrottle -> -1,
      brokerLevelFollowerThrottle -> -1,
      brokerLevelLogDirThrottle -> logDirThrottle)
    waitForBrokerThrottles(throttledBrokers, throttledConfigMap)
  }

  private def waitForInterBrokerThrottle(throttledBrokers: Set[Int], interBrokerThrottle: Long): Unit = {
    val throttledConfigMap = Map[String, Long](
      brokerLevelLeaderThrottle -> interBrokerThrottle,
      brokerLevelFollowerThrottle -> interBrokerThrottle,
      brokerLevelLogDirThrottle -> -1L)
    waitForBrokerThrottles(throttledBrokers, throttledConfigMap)
  }

  private def waitForBrokerThrottles(throttledBrokers: Set[Int], throttleConfig: Map[String, Long]): Unit = {
    val throttledBrokerConfigs = unthrottledBrokerConfigs.map { case (brokerId, unthrottledConfig) =>
      val expectedThrottleConfig = if (throttledBrokers.contains(brokerId)) {
        throttleConfig
      } else {
        unthrottledConfig
      }
      brokerId -> expectedThrottleConfig
    }
    waitForBrokerLevelThrottles(throttledBrokerConfigs)
  }

  private def waitForBrokerLevelThrottles(targetThrottles: Map[Int, Map[String, Long]]): Unit = {
    var curThrottles: Map[Int, Map[String, Long]] = Map.empty
    TestUtils.waitUntilTrue(() => {
      curThrottles = describeBrokerLevelThrottles(targetThrottles.keySet.toSeq)
      targetThrottles.equals(curThrottles)
    }, s"timed out waiting for broker throttle to become ${targetThrottles}.  " +
      s"Latest throttles were ${curThrottles}", pause = 25)
  }

  /**
   * Describe the broker-level throttles in the cluster.
   *
   * @return                A map whose keys are broker IDs and whose values are throttle
   *                        information.  The nested maps are keyed on throttle name.
   */
  private def describeBrokerLevelThrottles(brokerIds: Seq[Int]): Map[Int, Map[String, Long]] = {
    brokerIds.map { brokerId =>
      val props = zkClient.getEntityConfigs("brokers", brokerId.toString)
      val throttles = brokerLevelThrottles.map { throttleName =>
        (throttleName, props.getOrDefault(throttleName, "-1").asInstanceOf[String].toLong)
      }.toMap
      brokerId -> throttles
    }.toMap
  }

  /**
   * Test moving partitions between directories.
   */
  @Test
  def testLogDirReassignment(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)

    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    cluster.produceMessages(topicPartition.topic, topicPartition.partition, 700)

    val targetBrokerId = 0
    val replicas = Seq(0, 1, 2)
    val reassignment = buildLogDirReassignment(topicPartition, targetBrokerId, replicas)

    // Start the replica move, but throttle it to be very slow so that it can't complete
    // before our next checks happen.
    val logDirThrottle = 1L
    runExecuteAssignment(cluster.adminClient, additional = false, reassignment.json,
      interBrokerThrottle = -1L, logDirThrottle)

    // Check the output of --verify
    waitForVerifyAssignment(cluster.adminClient, reassignment.json, true,
      VerifyAssignmentResult(Map(
          topicPartition -> PartitionReassignmentState(Seq(0, 1, 2), Seq(0, 1, 2), true)
        ), false, Map(
          new TopicPartitionReplica(topicPartition.topic, topicPartition.partition, 0) ->
            ActiveMoveState(reassignment.currentDir, reassignment.targetDir, reassignment.targetDir)
        ), true))
    waitForLogDirThrottle(Set(0), logDirThrottle)

    // Remove the throttle
    cluster.adminClient.incrementalAlterConfigs(Collections.singletonMap(
      new ConfigResource(ConfigResource.Type.BROKER, "0"),
      Collections.singletonList(new AlterConfigOp(
        new ConfigEntry(brokerLevelLogDirThrottle, ""), AlterConfigOp.OpType.DELETE)))).
          all().get()
    waitForBrokerLevelThrottles(unthrottledBrokerConfigs)

    // Wait for the directory movement to complete.
    waitForVerifyAssignment(cluster.adminClient, reassignment.json, true,
        VerifyAssignmentResult(Map(
          topicPartition -> PartitionReassignmentState(Seq(0, 1, 2), Seq(0, 1, 2), true)
        ), false, Map(
          new TopicPartitionReplica(topicPartition.topic, topicPartition.partition, 0) ->
            CompletedMoveState(reassignment.targetDir)
        ), false))

    val info1 = new BrokerDirs(cluster.adminClient.describeLogDirs(0.to(4).
        map(_.asInstanceOf[Integer]).asJavaCollection), 0)
    assertEquals(reassignment.targetDir,
      info1.curLogDirs.getOrElse(topicPartition, ""))
  }

  @Test
  def testAlterLogDirReassignmentThrottle(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)

    cluster = new ReassignPartitionsTestCluster(zkConnect)
    cluster.setup()
    cluster.produceMessages(topicPartition.topic, topicPartition.partition, 700)

    val targetBrokerId = 0
    val replicas = Seq(0, 1, 2)
    val reassignment = buildLogDirReassignment(topicPartition, targetBrokerId, replicas)

    // Start the replica move with a low throttle so it does not complete
    val initialLogDirThrottle = 1L
    runExecuteAssignment(cluster.adminClient, false, reassignment.json,
      interBrokerThrottle = -1L, initialLogDirThrottle)
    waitForLogDirThrottle(Set(0), initialLogDirThrottle)

    // Now increase the throttle and verify that the log dir movement completes
    val updatedLogDirThrottle = 3000000L
    runExecuteAssignment(cluster.adminClient, additional = true, reassignment.json,
      interBrokerThrottle = -1L, replicaAlterLogDirsThrottle = updatedLogDirThrottle)
    waitForLogDirThrottle(Set(0), updatedLogDirThrottle)

    waitForVerifyAssignment(cluster.adminClient, reassignment.json, true,
      VerifyAssignmentResult(Map(
        topicPartition -> PartitionReassignmentState(Seq(0, 1, 2), Seq(0, 1, 2), true)
      ), false, Map(
        new TopicPartitionReplica(topicPartition.topic, topicPartition.partition, targetBrokerId) ->
          CompletedMoveState(reassignment.targetDir)
      ), false))
  }

  case class LogDirReassignment(json: String, currentDir: String, targetDir: String)

  private def buildLogDirReassignment(topicPartition: TopicPartition,
                                      brokerId: Int,
                                      replicas: Seq[Int]): LogDirReassignment = {

    val describeLogDirsResult = cluster.adminClient.describeLogDirs(
      0.to(4).map(_.asInstanceOf[Integer]).asJavaCollection)

    val logDirInfo = new BrokerDirs(describeLogDirsResult, brokerId)
    assertTrue(logDirInfo.futureLogDirs.isEmpty)

    val currentDir = logDirInfo.curLogDirs(topicPartition)
    val newDir = logDirInfo.logDirs.find(!_.equals(currentDir)).get

    val logDirs = replicas.map { replicaId =>
      if (replicaId == brokerId)
        s""""$newDir""""
      else
        "\"any\""
    }

    val reassignmentJson =
      s"""
         | { "version": 1,
         |  "partitions": [
         |    {
         |     "topic": "${topicPartition.topic}",
         |     "partition": ${topicPartition.partition},
         |     "replicas": [${replicas.mkString(",")}],
         |     "log_dirs": [${logDirs.mkString(",")}]
         |    }
         |   ]
         |  }
         |""".stripMargin

    LogDirReassignment(reassignmentJson, currentDir = currentDir, targetDir = newDir)
  }

  private def runVerifyAssignment(adminClient: Admin, jsonString: String,
                                  preserveThrottles: Boolean) = {
    println(s"==> verifyAssignment(adminClient, jsonString=${jsonString})")
    verifyAssignment(adminClient, jsonString, preserveThrottles)
  }

  private def waitForVerifyAssignment(adminClient: Admin, jsonString: String,
                                      preserveThrottles: Boolean,
                                      expectedResult: VerifyAssignmentResult): Unit = {
    var latestResult: VerifyAssignmentResult = null
    TestUtils.waitUntilTrue(
      () => {
        latestResult = runVerifyAssignment(adminClient, jsonString, preserveThrottles)
        expectedResult.equals(latestResult)
      }, s"Timed out waiting for verifyAssignment result ${expectedResult}.  " +
        s"The latest result was ${latestResult}", pause = 10L)
  }

  private def runVerifyAssignment(zkClient: KafkaZkClient, jsonString: String,
                                  preserveThrottles: Boolean) = {
    println(s"==> verifyAssignment(zkClient, jsonString=${jsonString})")
    verifyAssignment(zkClient, jsonString, preserveThrottles)
  }

  private def waitForVerifyAssignment(zkClient: KafkaZkClient, jsonString: String,
                                      preserveThrottles: Boolean,
                                      expectedResult: VerifyAssignmentResult): Unit = {
    var latestResult: VerifyAssignmentResult = null
    TestUtils.waitUntilTrue(
      () => {
        println(s"==> verifyAssignment(zkClient, jsonString=${jsonString}, " +
          s"preserveThrottles=${preserveThrottles})")
        latestResult = verifyAssignment(zkClient, jsonString, preserveThrottles)
        expectedResult.equals(latestResult)
      }, s"Timed out waiting for verifyAssignment result ${expectedResult}.  " +
        s"The latest result was ${latestResult}", pause = 10L)
  }

  private def runExecuteAssignment(adminClient: Admin,
                                   additional: Boolean,
                                   reassignmentJson: String,
                                   interBrokerThrottle: Long,
                                   replicaAlterLogDirsThrottle: Long) = {
    println(s"==> executeAssignment(adminClient, additional=${additional}, " +
      s"reassignmentJson=${reassignmentJson}, " +
      s"interBrokerThrottle=${interBrokerThrottle}, " +
      s"replicaAlterLogDirsThrottle=${replicaAlterLogDirsThrottle}))")
    executeAssignment(adminClient, additional, reassignmentJson,
      interBrokerThrottle, replicaAlterLogDirsThrottle)
  }

  private def runExecuteAssignment(zkClient: KafkaZkClient,
                                   reassignmentJson: String,
                                   interBrokerThrottle: Long) = {
    println(s"==> executeAssignment(adminClient, " +
      s"reassignmentJson=${reassignmentJson}, " +
      s"interBrokerThrottle=${interBrokerThrottle})")
    executeAssignment(zkClient, reassignmentJson, interBrokerThrottle)
  }

  private def runCancelAssignment(adminClient: Admin, jsonString: String,
                                  preserveThrottles: Boolean) = {
    println(s"==> cancelAssignment(adminClient, jsonString=${jsonString})")
    cancelAssignment(adminClient, jsonString, preserveThrottles)
  }

  class BrokerDirs(result: DescribeLogDirsResult, val brokerId: Int) {
    val logDirs = new mutable.HashSet[String]
    val curLogDirs = new mutable.HashMap[TopicPartition, String]
    val futureLogDirs = new mutable.HashMap[TopicPartition, String]
    result.descriptions.get(brokerId).get().forEach {
      case (logDirName, logDirInfo) => {
        logDirs.add(logDirName)
        logDirInfo.replicaInfos.forEach {
          case (part, info) =>
            if (info.isFuture) {
              futureLogDirs.put(part, logDirName)
            } else {
              curLogDirs.put(part, logDirName)
            }
        }
      }
    }
  }

  class ReassignPartitionsTestCluster(val zkConnect: String) extends Closeable {
    val brokers = Map(
      0 -> "rack0",
      1 -> "rack0",
      2 -> "rack1",
      3 -> "rack1",
      4 -> "rack1"
    )

    val topics = Map(
      "foo" -> Seq(Seq(0, 1, 2), Seq(1, 2, 3)),
      "bar" -> Seq(Seq(3, 2, 1)),
      "baz" -> Seq(Seq(1, 0, 2), Seq(2, 0, 1), Seq(0, 2, 1))
    )

    val brokerConfigs = brokers.map {
      case (brokerId, rack) =>
        val config = TestUtils.createBrokerConfig(
          nodeId = brokerId,
          zkConnect = zkConnect,
          rack = Some(rack),
          enableControlledShutdown = false, // shorten test time
          logDirCount = 3)
        // shorter backoff to reduce test durations when no active partitions are eligible for fetching due to throttling
        config.setProperty(KafkaConfig.ReplicaFetchBackoffMsProp, "100")
        // Don't move partition leaders automatically.
        config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
        config.setProperty(KafkaConfig.ReplicaLagTimeMaxMsProp, "1000")
        config
    }.toBuffer

    var servers = new mutable.ArrayBuffer[KafkaServer]

    var brokerList: String = null

    var adminClient: Admin = null

    def setup(): Unit = {
      createServers()
      createTopics()
    }

    def createServers(): Unit = {
      brokers.keySet.foreach {
        case brokerId =>
          servers += TestUtils.createServer(KafkaConfig(brokerConfigs(brokerId)))
      }
    }

    def createTopics(): Unit = {
      TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
      brokerList = TestUtils.bootstrapServers(servers,
        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
      adminClient = Admin.create(Map[String, Object](
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList
      ).asJava)
      adminClient.createTopics(topics.map {
        case (topicName, parts) =>
          val partMap = new HashMap[Integer, List[Integer]]()
          parts.zipWithIndex.foreach {
            case (part, index) => partMap.put(index, part.map(Integer.valueOf(_)).asJava)
          }
          new NewTopic(topicName, partMap)
      }.toList.asJava).all().get()
      topics.foreach {
        case (topicName, parts) =>
          parts.indices.foreach {
            index => TestUtils.waitUntilMetadataIsPropagated(servers, topicName, index)
          }
      }
    }

    def produceMessages(topic: String, partition: Int, numMessages: Int): Unit = {
      val records = (0 until numMessages).map(_ =>
        new ProducerRecord[Array[Byte], Array[Byte]](topic, partition,
          null, new Array[Byte](10000)))
      TestUtils.produceMessages(servers, records, -1)
    }

    override def close(): Unit = {
      brokerList = null
      Utils.closeQuietly(adminClient, "adminClient")
      adminClient = null
      try {
        TestUtils.shutdownServers(servers)
      } finally {
        servers.clear()
      }
    }
  }
}
