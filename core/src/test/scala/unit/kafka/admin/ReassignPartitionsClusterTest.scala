/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.admin

import kafka.admin.ReassignPartitionsCommand._
import kafka.common.AdminCommandFailedException
import kafka.controller.ReplicaAssignment
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestUtils}
import kafka.zk.{ReassignPartitionsZNode, ZkVersion, ZooKeeperTestHarness}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{After, Before, Test}
import kafka.admin.ReplicationQuotaUtils._
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewPartitionReassignment, NewPartitions, PartitionReassignment}
import org.apache.kafka.common.{TopicPartition, TopicPartitionReplica}

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq}
import scala.util.Random
import java.io.File
import java.util.{Collections, Optional, Properties}
import java.util.concurrent.ExecutionException

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.{NoReassignmentInProgressException, ReassignmentInProgressException}
import org.scalatest.Assertions.intercept


class ReassignPartitionsClusterTest extends ZooKeeperTestHarness with Logging {
  var servers: Seq[KafkaServer] = null
  var brokerIds: Seq[Int] = null
  val topicName = "my-topic"
  val tp0 = new TopicPartition(topicName, 0)
  val tp1 = new TopicPartition(topicName, 1)
  val delayMs = 1000
  var adminClient: Admin = null

  def zkUpdateDelay(): Unit = Thread.sleep(delayMs)

  @Before
  override def setUp(): Unit = {
    super.setUp()
  }

  def startBrokers(ids: Seq[Int]): Unit = {
    brokerIds = ids
    servers = ids.map { i =>
      val props = createBrokerConfig(i, zkConnect, enableControlledShutdown = false, logDirCount = 3)
      // shorter backoff to reduce test durations when no active partitions are eligible for fetching due to throttling
      props.put(KafkaConfig.ReplicaFetchBackoffMsProp, "100")
      props
    }.map(c => createServer(KafkaConfig.fromProps(c)))
  }

  def createAdminClient(servers: Seq[KafkaServer]): Admin = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.getBrokerListStrFromServers(servers))
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    Admin.create(props)
  }

  def getRandomLogDirAssignment(brokerId: Int): String = {
    val server = servers.find(_.config.brokerId == brokerId).get
    val logDirs = server.config.logDirs
    new File(logDirs(Random.nextInt(logDirs.size))).getAbsolutePath
  }

  @After
  override def tearDown(): Unit = {
    if (adminClient != null) {
      adminClient.close()
      adminClient = null
    }
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testHwAfterPartitionReassignment(): Unit = {
    //Given a single replica on server 100
    startBrokers(Seq(100, 101, 102))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    val leaderServer = servers.find(_.config.brokerId == 100).get
    leaderServer.replicaManager.logManager.truncateFullyAndStartAt(tp0, 100L, false)

    val topicJson = executeAssignmentJson(Seq(
      PartitionAssignmentJson(tp0, replicas=Seq(101, 102))
    ))
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, NoThrottle)

    val newLeaderServer = servers.find(_.config.brokerId == 101).get

    waitUntilTrue (
      () => newLeaderServer.replicaManager.nonOfflinePartition(tp0).flatMap(_.leaderLogIfLocal).isDefined,
      "broker 101 should be the new leader", pause = 1L
    )

    assertEquals(100, newLeaderServer.replicaManager.localLogOrException(tp0)
      .highWatermark)
    val newFollowerServer = servers.find(_.config.brokerId == 102).get
    waitUntilTrue(() => newFollowerServer.replicaManager.localLogOrException(tp0)
      .highWatermark == 100,
      "partition follower's highWatermark should be 100")
  }

  @Test
  def shouldMoveSinglePartition(): Unit = {
    //Given a single replica on server 100
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    // Get a random log directory on broker 101
    val expectedLogDir = getRandomLogDirAssignment(101)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    //When we move the replica on 100 to broker 101
    val topicJson = executeAssignmentJson(Seq(
      PartitionAssignmentJson(tp0, replicas = Seq(101), logDirectories = Some(Seq(expectedLogDir)))
    ))
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, NoThrottle)
    waitForZkReassignmentToComplete()

    //Then the replica should be on 101
    val partitionAssignment = zkClient.getPartitionAssignmentForTopics(Set(topicName)).get(topicName).get(tp0.partition())
    assertMoveForPartitionOccurred(Seq(101), partitionAssignment)
    // The replica should be in the expected log directory on broker 101
    val replica = new TopicPartitionReplica(topicName, 0, 101)
    assertEquals(expectedLogDir, adminClient.describeReplicaLogDirs(Collections.singleton(replica)).all().get.get(replica).getCurrentReplicaLogDir)
  }

  @Test
  def testReassignmentMatchesCurrentAssignment(): Unit = {
    // Given a single replica on server 100
    startBrokers(Seq(100))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    // Execute no-op reassignment
    val topicJson = executeAssignmentJson(Seq(
      PartitionAssignmentJson(tp0, replicas = Seq(100), logDirectories = None)
    ))
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, NoThrottle)
    waitForZkReassignmentToComplete()

    // The replica should remain on 100
    val partitionAssignment = zkClient.getPartitionAssignmentForTopics(Set(topicName))(topicName)(tp0.partition)
    assertMoveForPartitionOccurred(Seq(100), partitionAssignment)
  }

  @Test
  def shouldMoveSinglePartitionWithinBroker(): Unit = {
    // Given a single replica on server 100
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    val expectedLogDir = getRandomLogDirAssignment(100)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    // When we execute an assignment that moves an existing replica to another log directory on the same broker
    val topicJson = executeAssignmentJson(Seq(
      PartitionAssignmentJson(tp0, replicas = Seq(100), logDirectories = Some(Seq(expectedLogDir)))
    ))
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, NoThrottle)
    val replica = new TopicPartitionReplica(topicName, 0, 100)
    waitUntilTrue(() => {
      expectedLogDir == adminClient.describeReplicaLogDirs(Collections.singleton(replica)).all().get.get(replica).getCurrentReplicaLogDir
    }, "Partition should have been moved to the expected log directory", 1000)
  }

  @Test
  def shouldExpandCluster(): Unit = {
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName, Map(
      0 -> Seq(100, 101),
      1 -> Seq(100, 101),
      2 -> Seq(100, 101)
    ), servers = servers)

    //When rebalancing
    val newAssignment = generateAssignment(zkClient, brokers, generateAssignmentJson(topicName), true)._1
    // Find a partition in the new assignment on broker 102 and a random log directory on broker 102,
    // which currently does not have any partition for this topic
    val partition1 = newAssignment.find { case (_, brokerIds) => brokerIds.contains(102) }.get._1.partition
    val replica1 = new TopicPartitionReplica(topicName, partition1, 102)
    val expectedLogDir1 = getRandomLogDirAssignment(102)
    // Find a partition in the new assignment on broker 100 and a random log directory on broker 100,
    // which currently has partition for this topic
    val partition2 = newAssignment.find { case (_, brokerIds) => brokerIds.contains(100) }.get._1.partition
    val replica2 = new TopicPartitionReplica(topicName, partition2, 100)
    val expectedLogDir2 = getRandomLogDirAssignment(100)
    // Generate a replica assignment to reassign replicas on broker 100 and 102 respectively to a random log directory on the same broker.
    // Before this reassignment, the replica already exists on broker 100 but does not exist on broker 102
    val newReplicaAssignment = Map(replica1 -> expectedLogDir1, replica2 -> expectedLogDir2)
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient),
      ReassignPartitionsCommand.formatAsReassignmentJson(newAssignment, newReplicaAssignment), NoThrottle)
    waitForZkReassignmentToComplete()

    // Then the replicas should span all three brokers
    val actual = zkClient.getPartitionAssignmentForTopics(Set(topicName))(topicName)
    assertMoveForTopicOccurred(Seq(100, 101, 102), actual)
    // The replica should be in the expected log directory on broker 102 and 100
    waitUntilTrue(() => {
      expectedLogDir1 == adminClient.describeReplicaLogDirs(Collections.singleton(replica1)).all().get.get(replica1).getCurrentReplicaLogDir
    }, "Partition should have been moved to the expected log directory on broker 102", 1000)
    waitUntilTrue(() => {
      expectedLogDir2 == adminClient.describeReplicaLogDirs(Collections.singleton(replica2)).all().get.get(replica2).getCurrentReplicaLogDir
    }, "Partition should have been moved to the expected log directory on broker 100", 1000)
  }

  @Test
  def shouldShrinkCluster(): Unit = {
    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkClient, topicName, Map(
      0 -> Seq(100, 101),
      1 -> Seq(101, 102),
      2 -> Seq(102, 100)
    ), servers = servers)

    //When rebalancing
    val newAssignment = generateAssignment(zkClient, Array(100, 101), generateAssignmentJson(topicName), true)._1
    ReassignPartitionsCommand.executeAssignment(zkClient, None,
      ReassignPartitionsCommand.formatAsReassignmentJson(newAssignment, Map.empty), NoThrottle)
    waitForZkReassignmentToComplete()

    //Then replicas should only span the first two brokers
    val actual = zkClient.getPartitionAssignmentForTopics(Set(topicName))(topicName)
    assertMoveForTopicOccurred(Seq(100, 101), actual)
  }

  @Test
  def shouldMoveSubsetOfPartitions(): Unit = {
    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    adminClient = createAdminClient(servers)
    createTopic(zkClient, "topic1", Map(
      0 -> Seq(100, 101),
      1 -> Seq(101, 102),
      2 -> Seq(102, 100)
    ), servers = servers)
    createTopic(zkClient, "topic2", Map(
      0 -> Seq(100, 101),
      1 -> Seq(101, 102),
      2 -> Seq(102, 100)
    ), servers = servers)

    val proposed: Map[TopicPartition, Seq[Int]] = Map(
      new TopicPartition("topic1", 0) -> Seq(100, 102),
      new TopicPartition("topic1", 2) -> Seq(100, 102),
      new TopicPartition("topic2", 1) -> Seq(101, 100),
      new TopicPartition("topic2", 2) -> Seq(100, 102)
    )

    val replica1 = new TopicPartitionReplica("topic1", 0, 102)
    val replica2 = new TopicPartitionReplica("topic2", 1, 100)
    val proposedReplicaAssignment: Map[TopicPartitionReplica, String] = Map(
      replica1 -> getRandomLogDirAssignment(102),
      replica2 -> getRandomLogDirAssignment(100)
    )

    //When rebalancing
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient),
      ReassignPartitionsCommand.formatAsReassignmentJson(proposed, proposedReplicaAssignment), NoThrottle)
    waitForZkReassignmentToComplete()

    //Then the proposed changes should have been made
    val actual = zkClient.getPartitionAssignmentForTopics(Set("topic1", "topic2"))
    assertMoveForPartitionOccurred(Seq(100, 102), actual("topic1")(0)) //changed
    assertMoveForPartitionOccurred(Seq(101, 102), actual("topic1")(1))
    assertMoveForPartitionOccurred(Seq(100, 102), actual("topic1")(2)) //changed
    assertMoveForPartitionOccurred(Seq(100, 101), actual("topic2")(0))
    assertMoveForPartitionOccurred(Seq(101, 100), actual("topic2")(1)) //changed
    assertMoveForPartitionOccurred(Seq(100, 102), actual("topic2")(2)) //changed

    // The replicas should be in the expected log directories
    val replicaDirs = adminClient.describeReplicaLogDirs(List(replica1, replica2).asJava).all().get()
    assertEquals(proposedReplicaAssignment(replica1), replicaDirs.get(replica1).getCurrentReplicaLogDir)
    assertEquals(proposedReplicaAssignment(replica2), replicaDirs.get(replica2).getCurrentReplicaLogDir)
  }

  @Test
  def shouldExecuteThrottledReassignment(): Unit = {

    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkClient, topicName, Map(
      0 -> Seq(100, 101)
    ), servers = servers)

    //Given throttle set so replication will take a certain number of secs
    val initialThrottle = Throttle(10 * 1000 * 1000, -1, () => zkUpdateDelay)
    val expectedDurationSecs = 5
    val numMessages = 500
    val msgSize = 100 * 1000
    produceMessages(topicName, numMessages, acks = 0, msgSize)
    assertEquals(expectedDurationSecs, numMessages * msgSize / initialThrottle.interBrokerLimit)

    //Start rebalance which will move replica on 100 -> replica on 102
    val newAssignment = generateAssignment(zkClient, Array(101, 102), generateAssignmentJson(topicName), true)._1

    val start = System.currentTimeMillis()
    ReassignPartitionsCommand.executeAssignment(zkClient, None,
      ReassignPartitionsCommand.formatAsReassignmentJson(newAssignment, Map.empty), initialThrottle)

    //Check throttle config. Should be throttling replica 0 on 100 and 102 only.
    checkThrottleConfigAddedToZK(adminZkClient, initialThrottle.interBrokerLimit, servers, topicName, Set("0:100","0:101"), Set("0:102"))

    //Await completion
    waitForZkReassignmentToComplete()
    val took = System.currentTimeMillis() - start - delayMs

    //Check move occurred
    val actual = zkClient.getPartitionAssignmentForTopics(Set(topicName))(topicName)
    assertMoveForTopicOccurred(Seq(101, 102), actual)

    //Then command should have taken longer than the throttle rate
    assertTrue(s"Expected replication to be > ${expectedDurationSecs * 0.9 * 1000} but was $took",
      took > expectedDurationSecs * 0.9 * 1000)
    assertTrue(s"Expected replication to be < ${expectedDurationSecs * 2 * 1000} but was $took",
      took < expectedDurationSecs * 2 * 1000)
  }


  @Test
  def shouldOnlyThrottleMovingReplicas(): Unit = {
    //Given 6 brokers, two topics
    val brokers = Array(100, 101, 102, 103, 104, 105)
    startBrokers(brokers)
    createTopic(zkClient, "topic1", Map(
      0 -> Seq(100, 101),
      1 -> Seq(100, 101),
      2 -> Seq(103, 104) //will leave in place
    ), servers = servers)

    createTopic(zkClient, "topic2", Map(
      0 -> Seq(104, 105),
      1 -> Seq(104, 105),
      2 -> Seq(103, 104)//will leave in place
    ), servers = servers)

    //Given throttle set so replication will take a while
    val throttle: Long = 1000 * 1000
    produceMessages("topic1", 100, acks = 0, 100 * 1000)
    produceMessages("topic2", 100, acks = 0, 100 * 1000)

    //Start rebalance
    val newAssignment = Map(
      new TopicPartition("topic1", 0) -> Seq(100, 102),//moved 101=>102
      new TopicPartition("topic1", 1) -> Seq(100, 102),//moved 101=>102
      new TopicPartition("topic2", 0) -> Seq(103, 105),//moved 104=>103
      new TopicPartition("topic2", 1) -> Seq(103, 105),//moved 104=>103
      new TopicPartition("topic1", 2) -> Seq(103, 104), //didn't move
      new TopicPartition("topic2", 2) -> Seq(103, 104)  //didn't move
    )
    ReassignPartitionsCommand.executeAssignment(zkClient, None,
      ReassignPartitionsCommand.formatAsReassignmentJson(newAssignment, Map.empty), Throttle(throttle))

    //Check throttle config. Should be throttling specific replicas for each topic.
    checkThrottleConfigAddedToZK(adminZkClient, throttle, servers, "topic1",
      Set("1:100","1:101","0:100","0:101"), //All replicas for moving partitions should be leader-throttled
      Set("1:102","0:102") //Move destinations should be follower throttled.
    )
    checkThrottleConfigAddedToZK(adminZkClient, throttle, servers, "topic2",
      Set("1:104","1:105","0:104","0:105"), //All replicas for moving partitions should be leader-throttled
      Set("1:103","0:103") //Move destinations should be follower throttled.
    )
  }

  @Test
  def shouldChangeThrottleOnRerunAndRemoveOnVerify(): Unit = {
    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkClient, topicName, Map(
      0 -> Seq(100, 101)
    ), servers = servers)

    //Given throttle set so replication will take at least 20 sec (we won't wait this long)
    val initialThrottle: Long = 1000 * 1000
    produceMessages(topicName, numMessages = 200, acks = 0, valueLength = 100 * 1000)

    //Start rebalance
    val newAssignment = generateAssignment(zkClient, Array(101, 102), generateAssignmentJson(topicName), true)._1

    ReassignPartitionsCommand.executeAssignment(zkClient, None,
      ReassignPartitionsCommand.formatAsReassignmentJson(newAssignment, Map.empty), Throttle(initialThrottle))

    //Check throttle config
    checkThrottleConfigAddedToZK(adminZkClient, initialThrottle, servers, topicName, Set("0:100","0:101"), Set("0:102"))

    //Ensure that running Verify, whilst the command is executing, should have no effect
    verifyAssignment(zkClient, None, ReassignPartitionsCommand.formatAsReassignmentJson(newAssignment, Map.empty))

    //Check throttle config again
    checkThrottleConfigAddedToZK(adminZkClient, initialThrottle, servers, topicName, Set("0:100","0:101"), Set("0:102"))

    //Now re-run the same assignment with a larger throttle, which should only act to increase the throttle and make progress
    val newThrottle = initialThrottle * 1000

    ReassignPartitionsCommand.executeAssignment(zkClient, None,
      ReassignPartitionsCommand.formatAsReassignmentJson(newAssignment, Map.empty), Throttle(newThrottle))

    //Check throttle was changed
    checkThrottleConfigAddedToZK(adminZkClient, newThrottle, servers, topicName, Set("0:100","0:101"), Set("0:102"))

    //Await completion
    waitForZkReassignmentToComplete()

    //Verify should remove the throttle
    verifyAssignment(zkClient, None, ReassignPartitionsCommand.formatAsReassignmentJson(newAssignment, Map.empty))

    //Check removed
    checkThrottleConfigRemovedFromZK(adminZkClient, topicName, servers)

    //Check move occurred
    val actual = zkClient.getPartitionAssignmentForTopics(Set(topicName))(topicName)
    assertMoveForTopicOccurred(Seq(101, 102), actual)
  }

  @Test(expected = classOf[AdminCommandFailedException])
  def shouldFailIfProposedDoesNotMatchExisting(): Unit = {
    //Given a single replica on server 100
    startBrokers(Seq(100, 101))
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    //When we execute an assignment that includes an invalid partition (1:101 in this case)
    val topicJson = executeAssignmentJson(Seq(PartitionAssignmentJson(tp1, Seq(101))))
    ReassignPartitionsCommand.executeAssignment(zkClient, None, topicJson, NoThrottle)
  }

  @Test(expected = classOf[AdminCommandFailedException])
  def shouldFailIfProposedHasEmptyReplicaList(): Unit = {
    //Given a single replica on server 100
    startBrokers(Seq(100, 101))
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    //When we execute an assignment that specifies an empty replica list (0: empty list in this case)
    val topicJson = executeAssignmentJson(Seq(PartitionAssignmentJson(tp0, Seq())))
    ReassignPartitionsCommand.executeAssignment(zkClient, None, topicJson, NoThrottle)
  }

  @Test(expected = classOf[AdminCommandFailedException])
  def shouldFailIfProposedHasInvalidBrokerID(): Unit = {
    //Given a single replica on server 100
    startBrokers(Seq(100, 101))
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    //When we execute an assignment that specifies an invalid brokerID (102: invalid broker ID in this case)
    val topicJson = executeAssignmentJson(Seq(PartitionAssignmentJson(tp0, Seq(101, 102))))
    ReassignPartitionsCommand.executeAssignment(zkClient, None, topicJson, NoThrottle)
  }

  @Test(expected = classOf[AdminCommandFailedException])
  def shouldFailIfProposedHasInvalidLogDir(): Unit = {
    // Given a single replica on server 100
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    // When we execute an assignment that specifies an invalid log directory
    val topicJson = executeAssignmentJson(Seq(PartitionAssignmentJson(tp0, Seq(101), logDirectories = Some(Seq("invalidDir")))))
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, NoThrottle)
  }

  @Test(expected = classOf[AdminCommandFailedException])
  def shouldFailIfProposedHasInconsistentReplicasAndLogDirs(): Unit = {
    // Given a single replica on server 100
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    val logDir = getRandomLogDirAssignment(100)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    // When we execute an assignment whose length of replicas doesn't match that of log dirs
    val topicJson = executeAssignmentJson(Seq(PartitionAssignmentJson(tp0, Seq(101), logDirectories = Some(Seq(logDir, logDir)))))
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, NoThrottle)
  }

  @Test
  def shouldPerformThrottledReassignmentOverVariousTopics(): Unit = {
    val throttle = Throttle(1000L)

    startBrokers(Seq(0, 1, 2, 3))

    //With up several small topics
    createTopic(zkClient, "orders", Map(0 -> List(0, 1, 2), 1 -> List(0, 1, 2)), servers)
    createTopic(zkClient, "payments", Map(0 -> List(0, 1), 1 -> List(0, 1)), servers)
    createTopic(zkClient, "deliveries", Map(0 -> List(0)), servers)
    createTopic(zkClient, "customers", Map(0 -> List(0), 1 -> List(1), 2 -> List(2), 3 -> List(3)), servers)

    //Define a move for some of them
    val move = Map(
      new TopicPartition("orders", 0) -> Seq(0, 2, 3),//moves
      new TopicPartition("orders", 1) -> Seq(0, 1, 2),//stays
      new TopicPartition("payments", 1) -> Seq(1, 2), //only define one partition as moving
      new TopicPartition("deliveries", 0) -> Seq(1, 2) //increase replication factor
    )

    //When we run a throttled reassignment
    new ReassignPartitionsCommand(zkClient, None, move, adminZkClient = adminZkClient).reassignPartitions(throttle)

    waitForZkReassignmentToComplete()

    //Check moved replicas did move
    assertEquals(Seq(0, 2, 3), zkClient.getReplicasForPartition(new TopicPartition("orders", 0)))
    assertEquals(Seq(0, 1, 2), zkClient.getReplicasForPartition(new TopicPartition("orders", 1)))
    assertEquals(Seq(1, 2), zkClient.getReplicasForPartition(new TopicPartition("payments", 1)))
    assertEquals(Seq(1, 2), zkClient.getReplicasForPartition(new TopicPartition("deliveries", 0)))

    //Check untouched replicas are still there
    assertEquals(Seq(0, 1), zkClient.getReplicasForPartition(new TopicPartition("payments", 0)))
    assertEquals(Seq(0), zkClient.getReplicasForPartition(new TopicPartition("customers", 0)))
    assertEquals(Seq(1), zkClient.getReplicasForPartition(new TopicPartition("customers", 1)))
    assertEquals(Seq(2), zkClient.getReplicasForPartition(new TopicPartition("customers", 2)))
    assertEquals(Seq(3), zkClient.getReplicasForPartition(new TopicPartition("customers", 3)))
  }

  /**
   * Verifies that the Controller sets a watcher for the reassignment znode after reassignment completion.
   * This includes the case where the znode is set immediately after it's deleted (i.e. before the watch is set).
   * This case relies on the scheduling of the operations, so it won't necessarily fail every time, but it fails
   * often enough to detect a regression.
   */
  @Test
  def shouldPerformMultipleReassignmentOperationsOverVariousTopics(): Unit = {
    startBrokers(Seq(0, 1, 2, 3))

    createTopic(zkClient, "orders", Map(0 -> List(0, 1, 2), 1 -> List(0, 1, 2)), servers)
    createTopic(zkClient, "payments", Map(0 -> List(0, 1), 1 -> List(0, 1)), servers)
    createTopic(zkClient, "deliveries", Map(0 -> List(0)), servers)
    createTopic(zkClient, "customers", Map(0 -> List(0), 1 -> List(1), 2 -> List(2), 3 -> List(3)), servers)

    val firstMove = Map(
      new TopicPartition("orders", 0) -> Seq(0, 2, 3), //moves
      new TopicPartition("orders", 1) -> Seq(0, 1, 2), //stays
      new TopicPartition("payments", 1) -> Seq(1, 2), //only define one partition as moving
      new TopicPartition("deliveries", 0) -> Seq(1, 2) //increase replication factor
    )

    new ReassignPartitionsCommand(zkClient, None, firstMove, adminZkClient = adminZkClient).reassignPartitions()
    // Low pause to detect deletion of the reassign_partitions znode before the reassignment is complete
    waitForZkReassignmentToComplete(pause = 1L)

    // Check moved replicas did move
    assertEquals(Seq(0, 2, 3), zkClient.getReplicasForPartition(new TopicPartition("orders", 0)))
    assertEquals(Seq(0, 1, 2), zkClient.getReplicasForPartition(new TopicPartition("orders", 1)))
    assertEquals(Seq(1, 2), zkClient.getReplicasForPartition(new TopicPartition("payments", 1)))
    assertEquals(Seq(1, 2), zkClient.getReplicasForPartition(new TopicPartition("deliveries", 0)))

    // Check untouched replicas are still there
    assertEquals(Seq(0, 1), zkClient.getReplicasForPartition(new TopicPartition("payments", 0)))
    assertEquals(Seq(0), zkClient.getReplicasForPartition(new TopicPartition("customers", 0)))
    assertEquals(Seq(1), zkClient.getReplicasForPartition(new TopicPartition("customers", 1)))
    assertEquals(Seq(2), zkClient.getReplicasForPartition(new TopicPartition("customers", 2)))
    assertEquals(Seq(3), zkClient.getReplicasForPartition(new TopicPartition("customers", 3)))

    // Define a move for some of them
    val secondMove = Map(
      new TopicPartition("orders", 0) -> Seq(0, 2, 3), // stays
      new TopicPartition("orders", 1) -> Seq(3, 1, 2), // moves
      new TopicPartition("payments", 1) -> Seq(2, 1), // changed preferred leader
      new TopicPartition("deliveries", 0) -> Seq(1, 2, 3) //increase replication factor
    )

    new ReassignPartitionsCommand(zkClient, None, secondMove, adminZkClient = adminZkClient).reassignPartitions()
    // Low pause to detect deletion of the reassign_partitions znode before the reassignment is complete
    waitForZkReassignmentToComplete(pause = 1L)

    // Check moved replicas did move
    assertEquals(Seq(0, 2, 3), zkClient.getReplicasForPartition(new TopicPartition("orders", 0)))
    assertEquals(Seq(3, 1, 2), zkClient.getReplicasForPartition(new TopicPartition("orders", 1)))
    assertEquals(Seq(2, 1), zkClient.getReplicasForPartition(new TopicPartition("payments", 1)))
    assertEquals(Seq(1, 2, 3), zkClient.getReplicasForPartition(new TopicPartition("deliveries", 0)))

    //Check untouched replicas are still there
    assertEquals(Seq(0, 1), zkClient.getReplicasForPartition(new TopicPartition("payments", 0)))
    assertEquals(Seq(0), zkClient.getReplicasForPartition(new TopicPartition("customers", 0)))
    assertEquals(Seq(1), zkClient.getReplicasForPartition(new TopicPartition("customers", 1)))
    assertEquals(Seq(2), zkClient.getReplicasForPartition(new TopicPartition("customers", 2)))
    assertEquals(Seq(3), zkClient.getReplicasForPartition(new TopicPartition("customers", 3)))

    // We set the znode and then continuously attempt to set it again to exercise the case where the znode is set
    // immediately after deletion (i.e. before we set the watcher again)

    val thirdMove = Map(new TopicPartition("orders", 0) -> Seq(1, 2, 3))

    new ReassignPartitionsCommand(zkClient, None, thirdMove, adminZkClient = adminZkClient).reassignPartitions()

    val fourthMove = Map(new TopicPartition("payments", 1) -> Seq(2, 3))

    // Continuously attempt to set the reassignment znode with `fourthMove` until it succeeds. It will only succeed
    // after `thirdMove` completes.
    Iterator.continually {
      try new ReassignPartitionsCommand(zkClient, None, fourthMove, adminZkClient = adminZkClient).reassignPartitions()
      catch {
        case _: AdminCommandFailedException => false
      }
    }.exists(identity)

    // Low pause to detect deletion of the reassign_partitions znode before the reassignment is complete
    waitForZkReassignmentToComplete(pause = 1L)

    // Check moved replicas for thirdMove and fourthMove
    assertEquals(Seq(1, 2, 3), zkClient.getReplicasForPartition(new TopicPartition("orders", 0)))
    assertEquals(Seq(2, 3), zkClient.getReplicasForPartition(new TopicPartition("payments", 1)))

    //Check untouched replicas are still there
    assertEquals(Seq(3, 1, 2), zkClient.getReplicasForPartition(new TopicPartition("orders", 1)))
    assertEquals(Seq(1, 2, 3), zkClient.getReplicasForPartition(new TopicPartition("deliveries", 0)))
    assertEquals(Seq(0, 1), zkClient.getReplicasForPartition(new TopicPartition("payments", 0)))
    assertEquals(Seq(0), zkClient.getReplicasForPartition(new TopicPartition("customers", 0)))
    assertEquals(Seq(1), zkClient.getReplicasForPartition(new TopicPartition("customers", 1)))
    assertEquals(Seq(2), zkClient.getReplicasForPartition(new TopicPartition("customers", 2)))
    assertEquals(Seq(3), zkClient.getReplicasForPartition(new TopicPartition("customers", 3)))
  }

  /**
   * Set the `reassign_partitions` znode while the brokers are down and verify that the reassignment is triggered by
   * the Controller during start-up.
   */
  @Test
  def shouldTriggerReassignmentOnControllerStartup(): Unit = {
    startBrokers(Seq(0, 1, 2))
    createTopic(zkClient, "orders", Map(0 -> List(0, 1), 1 -> List(1, 2)), servers)
    servers.foreach(_.shutdown())

    val firstMove = Map(
      new TopicPartition("orders", 0) -> Seq(2, 1), // moves
      new TopicPartition("orders", 1) -> Seq(1, 2), // stays
      new TopicPartition("customers", 0) -> Seq(1, 2) // non-existent topic, triggers topic deleted path
    )

    // Set znode directly to avoid non-existent topic validation
    zkClient.setOrCreatePartitionReassignment(firstMove, ZkVersion.MatchAnyVersion)

    servers.foreach(_.startup())
    waitForZkReassignmentToComplete()

    assertEquals(Seq(2, 1), zkClient.getReplicasForPartition(new TopicPartition("orders", 0)))
    assertEquals(Seq(1, 2), zkClient.getReplicasForPartition(new TopicPartition("orders", 1)))
    assertEquals(Seq.empty, zkClient.getReplicasForPartition(new TopicPartition("customers", 0)))
  }

  /**
    * Set a reassignment through the `/topics/<topic>` znode and set the `reassign_partitions` znode while the brokers are down.
    * Verify that the reassignment is triggered by the Controller during start-up with the `reassign_partitions` znode taking precedence
    */
  @Test
  def shouldTriggerReassignmentWithZnodePrecedenceOnControllerStartup(): Unit = {
    startBrokers(Seq(0, 1, 2))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, "orders", Map(0 -> List(0, 1), 1 -> List(1, 2), 2 -> List(0, 1), 3 -> List(0, 1)), servers)
    val sameMoveTp = new TopicPartition("orders", 2)

    // Throttle to ensure we minimize race conditions and test flakiness
    throttle(Seq("orders"), throttleSettingForSeconds(10), Set(sameMoveTp))

    servers.foreach(_.shutdown())
    adminClient.close()

    zkClient.setTopicAssignment("orders", Map(
      new TopicPartition("orders", 0) -> ReplicaAssignment(List(0, 1), List(2), List(0)), // should be overwritten
      new TopicPartition("orders", 1) -> ReplicaAssignment(List(1, 2), List(3), List(1)), // should be overwritten
      // should be overwritten (so we know to remove it from ZK) even though we do the exact same move
      sameMoveTp -> ReplicaAssignment(List(0, 1, 2), List(2), List(0)),
      new TopicPartition("orders", 3) -> ReplicaAssignment(List(0, 1, 2), List(2), List(0)) // moves
    ))
    val move = Map(
      new TopicPartition("orders", 0) -> Seq(2, 1), // moves
      new TopicPartition("orders", 1) -> Seq(1, 2), // stays
      sameMoveTp -> Seq(1, 2), // same reassignment
      // orders-3 intentionally left for API
      new TopicPartition("customers", 0) -> Seq(1, 2) // non-existent topic, triggers topic deleted path
    )

    // Set znode directly to avoid non-existent topic validation
    zkClient.setOrCreatePartitionReassignment(move, ZkVersion.MatchAnyVersion)

    servers.foreach(_.startup())
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
    adminClient = createAdminClient(servers)
    TestUtils.resetBrokersThrottle(adminClient, brokerIds)

    waitForZkReassignmentToComplete()

    assertEquals(Seq(2, 1), zkClient.getReplicasForPartition(new TopicPartition("orders", 0)))
    assertEquals(Seq(1, 2), zkClient.getReplicasForPartition(new TopicPartition("orders", 1)))
    assertEquals(Seq(1, 2), zkClient.getReplicasForPartition(sameMoveTp))
    assertEquals(Seq(1, 2), zkClient.getReplicasForPartition(new TopicPartition("orders", 3)))
    assertEquals(Seq.empty, zkClient.getReplicasForPartition(new TopicPartition("customers", 0)))
  }

  @Test
  def shouldListReassignmentsTriggeredByZk(): Unit = {
    // Given a single replica on server 100
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    // Get a random log directory on broker 101
    val expectedLogDir = getRandomLogDirAssignment(101)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)
    // Given throttle set so replication will take at least 2 sec (to ensure we don't minimize race condition and test flakiness
    val throttle: Long = 1000 * 1000
    produceMessages(topicName, numMessages = 20, acks = 0, valueLength = 100 * 1000)

    // When we move the replica on 100 to broker 101
    val topicJson = executeAssignmentJson(Seq(
      PartitionAssignmentJson(tp0, replicas = Seq(101), Some(Seq(expectedLogDir)))))
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, Throttle(throttle))
    // Then the replica should be removing
    val reassigningPartitionsResult = adminClient.listPartitionReassignments(Set(tp0).asJava).reassignments().get().get(tp0)
    assertIsReassigning(from = Seq(100), to = Seq(101), reassigningPartitionsResult)

    waitForZkReassignmentToComplete()

    // Then the replica should be on 101
    val partitionAssignment = zkClient.getPartitionAssignmentForTopics(Set(topicName)).get(topicName).get(tp0.partition())
    assertMoveForPartitionOccurred(Seq(101), partitionAssignment)
  }

  @Test
  def shouldReassignThroughApi(): Unit = {
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    assertTrue(adminClient.listPartitionReassignments(Set(tp0).asJava).reassignments().get().isEmpty)
    assertEquals(Seq(100), zkClient.getReplicasForPartition(tp0))
    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tp0, Seq(101))).asJava
    ).all().get()

    waitForAllReassignmentsToComplete()
    assertEquals(Seq(101), zkClient.getReplicasForPartition(tp0))
  }

  @Test
  def testProduceAndConsumeWithReassignmentInProgress(): Unit = {
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100)), servers = servers)

    produceMessages(tp0.topic, 500, acks = -1, valueLength = 100 * 1000)

    TestUtils.setReplicationThrottleForPartitions(adminClient, Seq(101), Set(tp0), throttleBytes = 1)

    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tp0, Seq(100, 101))).asJava
    ).all().get()

    awaitReassignmentInProgress(tp0)

    produceMessages(tp0.topic, 500, acks = -1, valueLength = 64)
    val consumer = TestUtils.createConsumer(TestUtils.getBrokerListStrFromServers(servers))
    try {
      consumer.assign(Seq(tp0).asJava)
      pollUntilAtLeastNumRecords(consumer, numRecords = 1000)
    } finally {
      consumer.close()
    }

    assertTrue(isAssignmentInProgress(tp0))

    TestUtils.removeReplicationThrottleForPartitions(adminClient, brokerIds, Set(tp0))

    waitForAllReassignmentsToComplete()
    assertEquals(Seq(100, 101), zkClient.getReplicasForPartition(tp0))
  }

  @Test
  def shouldListMovingPartitionsThroughApi(): Unit = {
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    val topic2 = "topic2"
    val tp2 = new TopicPartition(topic2, 0)

    createTopic(zkClient, topicName,
      Map(tp0.partition() -> Seq(100),
          tp1.partition() -> Seq(101)),
      servers = servers)
    createTopic(zkClient, topic2,
      Map(tp2.partition() -> Seq(100)),
      servers = servers)
    assertTrue(adminClient.listPartitionReassignments().reassignments().get().isEmpty)

    // Throttle to ensure we minimize race conditions and test flakiness
    throttle(Seq(topicName), throttleSettingForSeconds(10), Set(tp0, tp2))

    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tp0, Seq(101)),
          reassignmentEntry(tp2, Seq(101))).asJava
    ).all().get()

    val reassignmentsInProgress = adminClient.listPartitionReassignments(Set(tp0, tp1, tp2).asJava).reassignments().get()
    assertFalse(reassignmentsInProgress.containsKey(tp1)) // tp1 is not reassigning
    assertIsReassigning(from = Seq(100), to = Seq(101), reassignmentsInProgress.get(tp0))
    assertIsReassigning(from = Seq(100), to = Seq(101), reassignmentsInProgress.get(tp2))

    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    waitForAllReassignmentsToComplete()
    assertEquals(Seq(101), zkClient.getReplicasForPartition(tp0))
    assertEquals(Seq(101), zkClient.getReplicasForPartition(tp2))
  }

  @Test
  def shouldUseLatestOrderingIfTwoConsecutiveReassignmentsHaveSameSetButDifferentOrdering(): Unit = {
    startBrokers(Seq(100, 101, 102))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName,
      Map(tp0.partition() -> Seq(100, 101),
          tp1.partition() -> Seq(100, 101)),
      servers = servers)

    // Throttle to ensure we minimize race conditions and test flakiness
    throttle(Seq(topicName), throttleSettingForSeconds(10), Set(tp0, tp1))

    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tp0, Seq(100, 101, 102)),
          reassignmentEntry(tp1, Seq(100, 101, 102))).asJava
    ).all().get()
    val apiReassignmentsInProgress = adminClient.listPartitionReassignments(Set(tp0, tp1).asJava).reassignments().get()
    assertIsReassigning(from = Seq(100, 101), to = Seq(100, 101, 102), apiReassignmentsInProgress.get(tp0))
    assertIsReassigning(from = Seq(100, 101), to = Seq(100, 101, 102), apiReassignmentsInProgress.get(tp1))

    // API reassignment to the same replicas but a different order
    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tp0, Seq(102, 101, 100)),
          reassignmentEntry(tp1, Seq(102, 101, 100))).asJava
    ).all().get()
    val apiReassignmentsInProgress2 = adminClient.listPartitionReassignments(Set(tp0, tp1).asJava).reassignments().get()
    // assert same replicas, ignoring ordering
    assertIsReassigning(from = Seq(100, 101), to = Seq(100, 101, 102), apiReassignmentsInProgress2.get(tp0))
    assertIsReassigning(from = Seq(100, 101), to = Seq(100, 101, 102), apiReassignmentsInProgress2.get(tp1))

    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    waitForAllReassignmentsToComplete()

    //Check move occurred
    val actual = zkClient.getPartitionAssignmentForTopics(Set(topicName))(topicName)
    assertMoveForPartitionOccurred(Seq(102, 101, 100), actual(tp0.partition()))
    assertMoveForPartitionOccurred(Seq(102, 101, 100), actual(tp1.partition()))
  }

  /**
    * 1. Trigger API reassignment for partitions
    * 2. Trigger ZK reassignment for partitions
    * Ensure ZK reassignment overrides API reassignment and znode is deleted
    */
  @Test
  def znodeReassignmentShouldOverrideApiTriggeredReassignment(): Unit = {
    startBrokers(Seq(100, 101, 102))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName,
      Map(tp0.partition() -> Seq(100),
          tp1.partition() -> Seq(100)),
      servers = servers)

    // Throttle to avoid race conditions
    val throttleSetting = throttleSettingForSeconds(10)
    throttle(Seq(topicName), throttleSetting, Set(tp0, tp1))

    // API reassignment to 101 for both partitions
    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tp0, Seq(101)),
          reassignmentEntry(tp1, Seq(101))).asJava
    ).all().get()
    val apiReassignmentsInProgress = adminClient.listPartitionReassignments(Set(tp0, tp1).asJava).reassignments().get()
    assertIsReassigning(from = Seq(100), to = Seq(101), apiReassignmentsInProgress.get(tp0))
    assertIsReassigning(from = Seq(100), to = Seq(101), apiReassignmentsInProgress.get(tp1))

    // znode reassignment to 102 for both partitions
    val topicJson = executeAssignmentJson(Seq(
      PartitionAssignmentJson(tp0, Seq(102)),
      PartitionAssignmentJson(tp1, Seq(102))
    ))
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, Throttle(throttleSetting.throttleBytes.toLong))
    waitUntilTrue(() => {
      !adminClient.listPartitionReassignments().reassignments().get().isEmpty
    }, "Controller should have picked up on znode creation", 1000)

    val zkReassignmentsInProgress = adminClient.listPartitionReassignments(Set(tp0, tp1).asJava).reassignments().get()
    assertIsReassigning(from = Seq(100), to = Seq(102), zkReassignmentsInProgress.get(tp0))
    assertIsReassigning(from = Seq(100), to = Seq(102), zkReassignmentsInProgress.get(tp1))

    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    waitForZkReassignmentToComplete()
    assertTrue(adminClient.listPartitionReassignments(Set(tp0, tp1).asJava).reassignments().get().isEmpty)
    assertEquals(Seq(102), zkClient.getReplicasForPartition(tp0))
    assertEquals(Seq(102), zkClient.getReplicasForPartition(tp1))
  }

  /**
    * 1. Trigger ZK reassignment for TP A-0, A-1
    * 2. Trigger API reassignment for partitions TP A-1, B-0
    * 3. Unthrottle A-0, A-1 so the ZK reassignment finishes quickly
    * 4. Ensure ZK node is emptied out after the API reassignment of 1 finishes
    */
  @Test
  def shouldDeleteReassignmentZnodeAfterApiReassignmentForPartitionCompletes(): Unit = {
    startBrokers(Seq(100, 101, 102))
    adminClient = createAdminClient(servers)
    val tpA0 = new TopicPartition("A", 0)
    val tpA1 = new TopicPartition("A", 1)
    val tpB0 = new TopicPartition("B", 0)

    createTopic(zkClient, "A",
      Map(tpA0.partition() -> Seq(100),
          tpA1.partition() -> Seq(100)),
      servers = servers)
    createTopic(zkClient, "B",
      Map(tpB0.partition() -> Seq(100)),
      servers = servers)

    // Throttle to avoid race conditions
    throttle(Seq("A", "B"), throttleSettingForSeconds(10), Set(tpA0, tpA1, tpB0))

    // 1. znode reassignment to 101 for TP A-0, A-1
    val topicJson = executeAssignmentJson(Seq(
      PartitionAssignmentJson(tpA0, replicas=Seq(101)),
      PartitionAssignmentJson(tpA1, replicas=Seq(101))
    ))
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, NoThrottle)
    waitUntilTrue(() => {
      !adminClient.listPartitionReassignments().reassignments().get().isEmpty
    }, "Controller should have picked up on znode creation", 1000)
    val zkReassignmentsInProgress = adminClient.listPartitionReassignments(Set(tpA0, tpA1).asJava).reassignments().get()
    assertIsReassigning(from = Seq(100), to = Seq(101), zkReassignmentsInProgress.get(tpA0))
    assertIsReassigning(from = Seq(100), to = Seq(101), zkReassignmentsInProgress.get(tpA1))

    // 2. API reassignment to 102 for TP A-1, B-0
    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tpA1, Seq(102)), reassignmentEntry(tpB0, Seq(102))).asJava
    ).all().get()
    val apiReassignmentsInProgress = adminClient.listPartitionReassignments(Set(tpA1, tpB0).asJava).reassignments().get()
    assertIsReassigning(from = Seq(100), to = Seq(102), apiReassignmentsInProgress.get(tpA1))
    assertIsReassigning(from = Seq(100), to = Seq(102), apiReassignmentsInProgress.get(tpB0))

    // 3. Unthrottle topic A
    TestUtils.removePartitionReplicaThrottles(adminClient, Set(tpA0, tpA1))
    waitForZkReassignmentToComplete()
    // 4. Ensure the API reassignment not part of the znode is still in progress
    val leftoverReassignments = adminClient.listPartitionReassignments(Set(tpA0, tpA1, tpB0).asJava).reassignments().get()
    assertTrue(leftoverReassignments.keySet().asScala.subsetOf(Set(tpA1, tpB0)))

    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    waitForAllReassignmentsToComplete()
    assertEquals(Seq(101), zkClient.getReplicasForPartition(tpA0))
    assertEquals(Seq(102), zkClient.getReplicasForPartition(tpA1))
    assertEquals(Seq(102), zkClient.getReplicasForPartition(tpB0))
  }

  @Test
  def shouldBeAbleToCancelThroughApi(): Unit = {
    startBrokers(Seq(100, 101, 102))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName, Map(tp0.partition() -> Seq(100, 101)), servers = servers)
    // Throttle to ensure we minimize race conditions and test flakiness
    throttle(Seq(topicName), throttleSettingForSeconds(10), Set(tp0))

    // move to [102, 101]
    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tp0, Seq(102, 101))).asJava
    ).all().get()
    val apiReassignmentsInProgress = adminClient.listPartitionReassignments().reassignments().get()
    val tpReassignment = apiReassignmentsInProgress.get(tp0)
    assertIsReassigning(from = Seq(100, 101), to = Seq(101, 102), tpReassignment)

    adminClient.alterPartitionReassignments(
      Map(cancelReassignmentEntry(tp0)).asJava
    ).all().get()

    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    waitForAllReassignmentsToComplete()
    assertEquals(Seq(100, 101), zkClient.getReplicasForPartition(tp0).sorted) // revert ordering is not guaranteed
  }

  @Test
  def shouldBeAbleToCancelZkTriggeredReassignmentThroughApi(): Unit = {
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName,
      Map(tp0.partition() -> Seq(100),
          tp1.partition() -> Seq(100)),
      servers = servers)

    // Throttle to avoid race conditions
    throttle(Seq(topicName), throttleSettingForSeconds(10), Set(tp0, tp1))

    val move = Map(
      tp0 -> Seq(101),
      tp1 -> Seq(101)
    )
    zkClient.setOrCreatePartitionReassignment(move, ZkVersion.MatchAnyVersion)
    waitUntilTrue(() => {
      !adminClient.listPartitionReassignments().reassignments().get().isEmpty
    }, "Controller should have picked up on znode creation", 1000)
    var reassignmentIsOngoing = adminClient.listPartitionReassignments().reassignments().get().size() > 0
    assertTrue(reassignmentIsOngoing)

    adminClient.alterPartitionReassignments(
      Map(cancelReassignmentEntry(tp0), cancelReassignmentEntry(tp1)).asJava
    ).all().get()

    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    waitForZkReassignmentToComplete()
    reassignmentIsOngoing = adminClient.listPartitionReassignments().reassignments().get().size() > 0
    assertFalse(reassignmentIsOngoing)
    assertEquals(Seq(100), zkClient.getReplicasForPartition(tp0))
    assertEquals(Seq(100), zkClient.getReplicasForPartition(tp1))
  }

  /**
    * Cancel and set reassignments in the same API call.
    * Even though one cancellation is invalid, ensure the other entries in the request pass
    */
  @Test
  def testCancelAndSetSomeReassignments(): Unit = {
    startBrokers(Seq(100, 101, 102))
    adminClient = createAdminClient(servers)
    val tp2 = new TopicPartition(topicName, 2)
    val tp3 = new TopicPartition(topicName, 3)

    createTopic(zkClient, topicName,
      Map(tp0.partition() -> Seq(100), tp1.partition() -> Seq(100), tp2.partition() -> Seq(100), tp3.partition() -> Seq(100)),
      servers = servers)

    // Throttle to avoid race conditions
    throttle(Seq(topicName), throttleSettingForSeconds(10), Set(tp0, tp1, tp2, tp3))

    // API reassignment to 101 for tp0 and tp1
    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tp0, Seq(101)), reassignmentEntry(tp1, Seq(101))).asJava
    ).all().get()

    // cancel tp0, reassign tp1 to 102 (override), assign tp2 to 101 (new reassignment) and cancel tp3 (it is not moving)
    val alterResults = adminClient.alterPartitionReassignments(
      Map(cancelReassignmentEntry(tp0), reassignmentEntry(tp1, Seq(102)),
          reassignmentEntry(tp2, Seq(101)), cancelReassignmentEntry(tp3)).asJava
    ).values()
    alterResults.get(tp0).get()
    alterResults.get(tp1).get()
    alterResults.get(tp2).get()
    try {
      alterResults.get(tp3).get()
    } catch {
      case exception: Exception =>
        assertEquals(exception.getCause.getClass, classOf[NoReassignmentInProgressException])
    }

    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    waitForAllReassignmentsToComplete()
    assertEquals(Seq(100), zkClient.getReplicasForPartition(tp0))
    assertEquals(Seq(102), zkClient.getReplicasForPartition(tp1))
    assertEquals(Seq(101), zkClient.getReplicasForPartition(tp2))
    assertEquals(Seq(100), zkClient.getReplicasForPartition(tp3))
  }

  /**
    * Three different Alter Reassignment calls should all create reassignments
    */
  @Test
  def shouldBeAbleToIncrementallyStackDifferentReassignments(): Unit = {
    startBrokers(Seq(100, 101))
    adminClient = createAdminClient(servers)
    val tpA0 = new TopicPartition("A", 0)
    val tpA1 = new TopicPartition("A", 1)
    val tpB0 = new TopicPartition("B", 0)

    createTopic(zkClient, "A",
      Map(tpA0.partition() -> Seq(100),
          tpA1.partition() -> Seq(100)),
      servers = servers)
    createTopic(zkClient, "B",
      Map(tpB0.partition() -> Seq(100)),
      servers = servers)

    // Throttle to avoid race conditions
    throttle(Seq("A", "B"), throttleSettingForSeconds(10), Set(tpA0, tpA1, tpB0))

    adminClient.alterPartitionReassignments(Map(reassignmentEntry(tpA0, Seq(101))).asJava).all().get()
    val apiReassignmentsInProgress1 = adminClient.listPartitionReassignments().reassignments().get()
    assertEquals(1, apiReassignmentsInProgress1.size())
    assertIsReassigning(
      from = Seq(100), to = Seq(101),
      apiReassignmentsInProgress1.get(tpA0)
    )

    adminClient.alterPartitionReassignments(Map(reassignmentEntry(tpA1, Seq(101))).asJava).all().get()
    val apiReassignmentsInProgress2 = adminClient.listPartitionReassignments().reassignments().get()
    assertEquals(2, apiReassignmentsInProgress2.size())
    assertIsReassigning(from = Seq(100), to = Seq(101), apiReassignmentsInProgress2.get(tpA0))
    assertIsReassigning(
      from = Seq(100), to = Seq(101),
      apiReassignmentsInProgress2.get(tpA1)
    )

    adminClient.alterPartitionReassignments(Map(reassignmentEntry(tpB0, Seq(101))).asJava).all().get()
    val apiReassignmentsInProgress3 = adminClient.listPartitionReassignments().reassignments().get()
    assertEquals(s"${apiReassignmentsInProgress3}", 3, apiReassignmentsInProgress3.size())
    assertIsReassigning(from = Seq(100), to = Seq(101), apiReassignmentsInProgress3.get(tpA0))
    assertIsReassigning(from = Seq(100), to = Seq(101), apiReassignmentsInProgress3.get(tpA1))
    assertIsReassigning(
      from = Seq(100), to = Seq(101),
      apiReassignmentsInProgress3.get(tpB0)
    )

    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    waitForAllReassignmentsToComplete()
    assertEquals(Seq(101), zkClient.getReplicasForPartition(tpA0))
    assertEquals(Seq(101), zkClient.getReplicasForPartition(tpA1))
    assertEquals(Seq(101), zkClient.getReplicasForPartition(tpB0))
  }

  /**
   * Verifies that partitions can be created for topics not in reassignment and for the topics that are in reassignment
   * an ReassignmentInProgressException should be thrown. The test creates two topics `topicName` and `otherTopicName`,
   * the `topicName` topic undergoes partition reassignment and the test validates that during reassignment createPartitions
   * call throws ReassignmentInProgressException `topicName` topic and for topic `otherTopicName` which is not being reassigned
   * successfully creates partitions. Further validates that after the reassignment is complete for topic `topicName`
   * createPartition is successful for that topic.
   */
  @Test
  def shouldCreatePartitionsForTopicNotInReassignment(): Unit = {
    startBrokers(Seq(100, 101))
    val otherTopicName = "anyTopic"
    val otp0 = new TopicPartition(otherTopicName, 0)
    val otp1 = new TopicPartition(otherTopicName, 1)
    adminClient = createAdminClient(servers)
    createTopic(zkClient, topicName,
      Map(otp0.partition() -> Seq(100),
          otp1.partition() -> Seq(100)),
      servers = servers)
    createTopic(zkClient, otherTopicName,
      Map(tp0.partition() -> Seq(100),
          tp1.partition() -> Seq(100)),
      servers = servers)

    // Throttle to avoid race conditions
    throttle(Seq(topicName), throttleSettingForSeconds(10), Set(tp0, tp1))

    // Alter `topicName` partition reassignment
    adminClient.alterPartitionReassignments(
      Map(reassignmentEntry(tp0, Seq(101)),
        reassignmentEntry(tp1, Seq(101))).asJava
    ).all().get()
    waitUntilTrue(() => {
      !adminClient.listPartitionReassignments().reassignments().get().isEmpty
    }, "Controller should have picked up reassignment", 1000)

    def testCreatePartitions(topicName: String, isTopicBeingReassigned: Boolean): Unit = {
      if (isTopicBeingReassigned)
        assertTrue("createPartitions for topic under reassignment should throw an exception", intercept[ExecutionException](
          adminClient.createPartitions(Map(topicName -> NewPartitions.increaseTo(4)).asJava).values.get(topicName).get()).
          getCause.isInstanceOf[ReassignmentInProgressException])
      else
        adminClient.createPartitions(Map(topicName -> NewPartitions.increaseTo(4)).asJava).values.get(topicName).get()
    }

    // Test case: createPartitions throws ReassignmentInProgressException Topics with partitions in reassignment.
    testCreatePartitions(topicName, true)
    // Test case: createPartitions is successful for Topics with partitions NOT in reassignment.
    testCreatePartitions(otherTopicName, false)

    // complete reassignment
    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    waitForAllReassignmentsToComplete()

    // Test case: createPartitions is successful for Topics with partitions after reassignment has completed.
    testCreatePartitions(topicName, false)
  }

  /**
    * Asserts that a replica is being reassigned from the given replicas to the target replicas
    */
  def assertIsReassigning(from: Seq[Int], to: Seq[Int], reassignment: PartitionReassignment): Unit = {
    assertReplicas((from ++ to).distinct, reassignment.replicas())
    assertReplicas(to.filterNot(from.contains(_)), reassignment.addingReplicas())
    assertReplicas(from.filterNot(to.contains(_)), reassignment.removingReplicas())
  }

  /**
   * Asserts that a topic's reassignments completed and span across the expected replicas
   */
  def assertMoveForTopicOccurred(expectedReplicas: Seq[Int],
                                 partitionAssignments: Map[Int, ReplicaAssignment]): Unit = {
    assertEquals(expectedReplicas, partitionAssignments.values.flatMap(_.replicas).toSeq.distinct.sorted)
    assertTrue(partitionAssignments.values.flatMap(_.addingReplicas).isEmpty)
    assertTrue(partitionAssignments.values.flatMap(_.removingReplicas).isEmpty)
  }

  /**
   * Asserts that a partition moved to the exact expected replicas in the specific order
   */
  def assertMoveForPartitionOccurred(expectedReplicas: Seq[Int],
                                     partitionAssignment: ReplicaAssignment): Unit = {
    assertEquals(expectedReplicas, partitionAssignment.replicas)
    assertTrue(partitionAssignment.addingReplicas.isEmpty)
    assertTrue(partitionAssignment.removingReplicas.isEmpty)
  }

  /**
   * Asserts that two replica sets are equal, ignoring ordering
   */
  def assertReplicas(expectedReplicas: Seq[Int], receivedReplicas: java.util.List[Integer]): Unit = {
    assertEquals(expectedReplicas.sorted, receivedReplicas.asScala.map(_.toInt).sorted)
  }

  def reassignmentEntry(tp: TopicPartition, replicas: Seq[Int]): (TopicPartition, java.util.Optional[NewPartitionReassignment]) =
    tp -> Optional.of(new NewPartitionReassignment((replicas.map(_.asInstanceOf[Integer]).asJava)))

  def cancelReassignmentEntry(tp: TopicPartition): (TopicPartition, java.util.Optional[NewPartitionReassignment]) =
    tp -> java.util.Optional.empty()

  def waitForZkReassignmentToComplete(pause: Long = 100L): Unit = {
    waitUntilTrue(() => !zkClient.reassignPartitionsInProgress,
      s"Znode ${ReassignPartitionsZNode.path} wasn't deleted", pause = pause)
  }

  def awaitReassignmentInProgress(topicPartition: TopicPartition): Unit = {
    waitUntilTrue(() => isAssignmentInProgress(topicPartition),
      "Timed out waiting for expected reassignment to begin")
  }

  def isAssignmentInProgress(topicPartition: TopicPartition): Boolean = {
    val reassignments = adminClient.listPartitionReassignments().reassignments().get()
    reassignments.asScala.get(topicPartition).isDefined
  }

  def waitForAllReassignmentsToComplete(pause: Long = 100L): Unit = {
    waitUntilTrue(() => adminClient.listPartitionReassignments().reassignments().get().isEmpty,
      s"There still are ongoing reassignments", pause = pause)
  }

  def generateAssignmentJson(topic: String*): String = {
    val topicStr = topic.map { t => "{\"topic\": \"" + t + "\"}" }.mkString(",")
    s"""{"topics": [$topicStr],"version":1}"""
  }

  def executeAssignmentJson(partitions: Seq[PartitionAssignmentJson]): String =
    s"""{"version":1,"partitions":[${partitions.map(_.toJson).mkString(",")}]}"""

  case class PartitionAssignmentJson(topicPartition: TopicPartition, replicas: Seq[Int],
                                     logDirectories: Option[Seq[String]] = None) {
    def toJson: String = {
      val logDirsSuffix = logDirectories match {
        case Some(dirs) => s""","log_dirs":[${dirs.map("\"" + _ + "\"").mkString(",")}]"""
        case None => ""
      }
      s"""{"topic":"${topicPartition.topic()}","partition":${topicPartition.partition()}""" +
        s""","replicas":[${replicas.mkString(",")}]""" +
        s"$logDirsSuffix}"
    }
  }

  case class ThrottleSetting(throttleBytes: String, numMessages: Int, messageSizeBytes: Int)

  def throttleSettingForSeconds(secondsDuration: Int): ThrottleSetting = {
    val throttle = 1000 * 1000 // 1 MB/s throttle
    val messageSize = 100 * 100 // 0.01 MB message size
    val messagesPerSecond = throttle / messageSize
    ThrottleSetting(throttle.toString, messagesPerSecond * secondsDuration, messageSize)
  }

  def throttle(topics: Seq[String], throttle: ThrottleSetting, partitions: Set[TopicPartition]): Unit = {
    val messagesPerTopic = throttle.numMessages / topics.size
    for (topic <- topics) {
      produceMessages(topic, numMessages = messagesPerTopic, acks = 0, valueLength = throttle.messageSizeBytes)
    }
    TestUtils.setReplicationThrottleForPartitions(adminClient, brokerIds, partitions, throttle.throttleBytes.toInt)
  }

  private def produceMessages(topic: String, numMessages: Int, acks: Int, valueLength: Int): Unit = {
    val records = (0 until numMessages).map(_ => new ProducerRecord[Array[Byte], Array[Byte]](topic,
      new Array[Byte](valueLength)))
    TestUtils.produceMessages(servers, records, acks)
  }
}
