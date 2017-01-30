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

import kafka.common.{AdminCommandFailedException, TopicAndPartition}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.ZkUtils._
import kafka.utils.{CoreUtils, Logging, TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Before, Test}
import kafka.admin.ReplicationQuotaUtils._

import scala.collection.{Map, Seq}


class ReassignPartitionsClusterTest extends ZooKeeperTestHarness with Logging {
  val partitionId = 0
  var servers: Seq[KafkaServer] = null
  val topicName = "my-topic"

  @Before
  override def setUp() {
    super.setUp()
  }

  def startBrokers(brokerIds: Seq[Int]) {
    servers = brokerIds.map(i => createBrokerConfig(i, zkConnect))
      .map(c => createServer(KafkaConfig.fromProps(c)))
  }

  @After
  override def tearDown() {
    servers.par.foreach(_.shutdown())
    servers.par.foreach(server => CoreUtils.delete(server.config.logDirs))
    super.tearDown()
  }

  @Test
  def shouldMoveSinglePartition {
    //Given a single replica on server 100
    startBrokers(Seq(100, 101))
    val partition = 0
    createTopic(zkUtils, topicName, Map(partition -> Seq(100)), servers = servers)

    //When we move the replica on 100 to broker 101
    ReassignPartitionsCommand.executeAssignment(zkUtils, s"""{"version":1,"partitions":[{"topic":"$topicName","partition":0,"replicas":[101]}]}""")
    waitForReassignmentToComplete()

    //Then the replica should be on 101
    assertEquals(Seq(101), zkUtils.getPartitionAssignmentForTopics(Seq(topicName)).get(topicName).get(partition))
  }

  @Test
  def shouldExpandCluster() {
    //Given partitions on 2 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkUtils, topicName, Map(
      0 -> Seq(100, 101),
      1 -> Seq(100, 101),
      2 -> Seq(100, 101)
    ), servers = servers)

    //When rebalancing
    val newAssignment = ReassignPartitionsCommand.generateAssignment(zkUtils, brokers, json(topicName), true)._1
    ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment))
    waitForReassignmentToComplete()

    //Then the replicas should span all three brokers
    val actual = zkUtils.getPartitionAssignmentForTopics(Seq(topicName))(topicName)
    assertEquals(Seq(100, 101, 102), actual.values.flatten.toSeq.distinct.sorted)
  }

  @Test
  def shouldShrinkCluster() {
    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkUtils, topicName, Map(
      0 -> Seq(100, 101),
      1 -> Seq(101, 102),
      2 -> Seq(102, 100)
    ), servers = servers)

    //When rebalancing
    val newAssignment = ReassignPartitionsCommand.generateAssignment(zkUtils, Array(100, 101), json(topicName), true)._1
    ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment))
    waitForReassignmentToComplete()

    //Then replicas should only span the first two brokers
    val actual = zkUtils.getPartitionAssignmentForTopics(Seq(topicName))(topicName)
    assertEquals(Seq(100, 101), actual.values.flatten.toSeq.distinct.sorted)
  }

  @Test
  def shouldMoveSubsetOfPartitions() {
    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkUtils, "topic1", Map(
      0 -> Seq(100, 101),
      1 -> Seq(101, 102),
      2 -> Seq(102, 100)
    ), servers = servers)
    createTopic(zkUtils, "topic2", Map(
      0 -> Seq(100, 101),
      1 -> Seq(101, 102),
      2 -> Seq(102, 100)
    ), servers = servers)

    val proposed: Map[TopicAndPartition, Seq[Int]] = Map(
      TopicAndPartition("topic1", 0) -> Seq(100, 102),
      TopicAndPartition("topic1", 2) -> Seq(100, 102),
      TopicAndPartition("topic2", 2) -> Seq(100, 102)
    )

    //When rebalancing
    ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(proposed))
    waitForReassignmentToComplete()

    //Then the proposed changes should have been made
    val actual = zkUtils.getPartitionAssignmentForTopics(Seq("topic1", "topic2"))
    assertEquals(Seq(100, 102), actual("topic1")(0))//changed
    assertEquals(Seq(101, 102), actual("topic1")(1))
    assertEquals(Seq(100, 102), actual("topic1")(2))//changed
    assertEquals(Seq(100, 101), actual("topic2")(0))
    assertEquals(Seq(101, 102), actual("topic2")(1))
    assertEquals(Seq(100, 102), actual("topic2")(2))//changed
  }

  @Test
  def shouldExecuteThrottledReassignment() {
    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkUtils, topicName, Map(
      0 -> Seq(100, 101)
    ), servers = servers)

    //Given throttle set so replication will take a certain number of secs
    val initialThrottle: Long = 10 * 1000 * 1000
    val expectedDurationSecs = 5
    val numMessages: Int = 500
    val msgSize: Int = 100 * 1000
    produceMessages(servers, topicName, numMessages, acks = 0, msgSize)
    assertEquals(expectedDurationSecs, numMessages * msgSize / initialThrottle)

    //Start rebalance which will move replica on 100 -> replica on 102
    val newAssignment = ReassignPartitionsCommand.generateAssignment(zkUtils, Array(101, 102), json(topicName), true)._1

    val start = System.currentTimeMillis()
    ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment), initialThrottle)

    //Check throttle config. Should be throttling replica 0 on 100 and 102 only.
    checkThrottleConfigAddedToZK(initialThrottle, servers, topicName, "0:100,0:101", "0:102")

    //Await completion
    waitForReassignmentToComplete()
    val took = System.currentTimeMillis() - start

    //Check move occurred
    val actual = zkUtils.getPartitionAssignmentForTopics(Seq(topicName))(topicName)
    assertEquals(Seq(101, 102), actual.values.flatten.toSeq.distinct.sorted)

    //Then command should have taken longer than the throttle rate
    assertTrue(s"Expected replication to be > ${expectedDurationSecs * 0.9 * 1000} but was $took", took > expectedDurationSecs * 0.9 * 1000)
    assertTrue(s"Expected replication to be < ${expectedDurationSecs * 2 * 1000} but was $took", took < expectedDurationSecs * 2 * 1000)
  }


  @Test
  def shouldOnlyThrottleMovingReplicas() {
    //Given 6 brokers, two topics
    val brokers = Array(100, 101, 102, 103, 104, 105)
    startBrokers(brokers)
    createTopic(zkUtils, "topic1", Map(
      0 -> Seq(100, 101),
      1 -> Seq(100, 101),
      2 -> Seq(103, 104) //will leave in place
    ), servers = servers)

    createTopic(zkUtils, "topic2", Map(
      0 -> Seq(104, 105),
      1 -> Seq(104, 105),
      2 -> Seq(103, 104)//will leave in place
    ), servers = servers)

    //Given throttle set so replication will take a while
    val throttle: Long = 1000 * 1000
    produceMessages(servers, "topic1", 100, acks = 0, 100 * 1000)
    produceMessages(servers, "topic2", 100, acks = 0, 100 * 1000)

    //Start rebalance
    val newAssignment = Map(
      TopicAndPartition("topic1", 0) -> Seq(100, 102),//moved 101=>102
      TopicAndPartition("topic1", 1) -> Seq(100, 102),//moved 101=>102
      TopicAndPartition("topic2", 0) -> Seq(103, 105),//moved 104=>103
      TopicAndPartition("topic2", 1) -> Seq(103, 105),//moved 104=>103
      TopicAndPartition("topic1", 2) -> Seq(103, 104), //didn't move
      TopicAndPartition("topic2", 2) -> Seq(103, 104)  //didn't move
    )
    ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment), throttle)

    //Check throttle config. Should be throttling specific replicas for each topic.
    checkThrottleConfigAddedToZK(throttle, servers, "topic1",
      "1:100,1:101,0:100,0:101", //All replicas for moving partitions should be leader-throttled
      "1:102,0:102" //Move destinations should be follower throttled.
    )
    checkThrottleConfigAddedToZK(throttle, servers, "topic2",
      "1:104,1:105,0:104,0:105", //All replicas for moving partitions should be leader-throttled
      "1:103,0:103" //Move destinations should be follower throttled.
    )
  }

  @Test
  def shouldChangeThrottleOnRerunAndRemoveOnVerify() {
    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkUtils, topicName, Map(
      0 -> Seq(100, 101)
    ), servers = servers)

    //Given throttle set so replication will take at least 20 sec (we won't wait this long)
    val initialThrottle: Long = 1000 * 1000
    produceMessages(servers, topicName, numMessages = 200, acks = 0, valueBytes = 100 * 1000)

    //Start rebalance
    val newAssignment = ReassignPartitionsCommand.generateAssignment(zkUtils, Array(101, 102), json(topicName), true)._1

    ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment), initialThrottle)

    //Check throttle config
    checkThrottleConfigAddedToZK(initialThrottle, servers, topicName, "0:100,0:101", "0:102")

    //Ensure that running Verify, whilst the command is executing, should have no effect
    ReassignPartitionsCommand.verifyAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment))

    //Check throttle config again
    checkThrottleConfigAddedToZK(initialThrottle, servers, topicName, "0:100,0:101", "0:102")

    //Now re-run the same assignment with a larger throttle, which should only act to increase the throttle and make progress
    val newThrottle = initialThrottle * 1000

    ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment), newThrottle)

    //Check throttle was changed
    checkThrottleConfigAddedToZK(newThrottle, servers, topicName, "0:100,0:101", "0:102")

    //Await completion
    waitForReassignmentToComplete()

    //Verify should remove the throttle
    ReassignPartitionsCommand.verifyAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment))

    //Check removed
    checkThrottleConfigRemovedFromZK(topicName, servers)

    //Check move occurred
    val actual = zkUtils.getPartitionAssignmentForTopics(Seq(topicName))(topicName)
    assertEquals(Seq(101, 102), actual.values.flatten.toSeq.distinct.sorted)
  }

  @Test(expected = classOf[AdminCommandFailedException])
  def shouldFailIfProposedDoesNotMatchExisting() {
    //Given a single replica on server 100
    startBrokers(Seq(100, 101))
    createTopic(zkUtils, topicName, Map(0 -> Seq(100)), servers = servers)

    //When we execute an assignment that includes an invalid partition (1:101 in this case)
    ReassignPartitionsCommand.executeAssignment(zkUtils, s"""{"version":1,"partitions":[{"topic":"$topicName","partition":1,"replicas":[101]}]}""")
  }

  @Test
  def shouldPerformThrottledReassignmentOverVariousTopics() {
    val throttle = 1000L

    //Given four brokers
    servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(conf => TestUtils.createServer(KafkaConfig.fromProps(conf)))

    //With up several small topics
    createTopic(zkUtils, "orders", Map(0 -> List(0, 1, 2), 1 -> List(0, 1, 2)), servers)
    createTopic(zkUtils, "payments", Map(0 -> List(0, 1), 1 -> List(0, 1)), servers)
    createTopic(zkUtils, "deliveries", Map(0 -> List(0)), servers)
    createTopic(zkUtils, "customers", Map(0 -> List(0), 1 -> List(1), 2 -> List(2), 3 -> List(3)), servers)

    //Define a move for some of them
    val move = Map(
      TopicAndPartition("orders", 0) -> Seq(0, 2, 3),//moves
      TopicAndPartition("orders", 1) -> Seq(0, 1, 2),//stays
      TopicAndPartition("payments", 1) -> Seq(1, 2), //only define one partition as moving
      TopicAndPartition("deliveries", 0) -> Seq(1, 2) //increase replication factor
    )

    //When we run a throttled reassignment
    new ReassignPartitionsCommand(zkUtils, move).reassignPartitions(throttle)

    waitForReassignmentToComplete()

    //Check moved replicas did move
    assertEquals(Seq(0, 2, 3), zkUtils.getReplicasForPartition("orders", 0))
    assertEquals(Seq(0, 1, 2), zkUtils.getReplicasForPartition("orders", 1))
    assertEquals(Seq(1, 2), zkUtils.getReplicasForPartition("payments", 1))
    assertEquals(Seq(1, 2), zkUtils.getReplicasForPartition("deliveries", 0))

    //Check untouched replicas are still there
    assertEquals(Seq(0, 1), zkUtils.getReplicasForPartition("payments", 0))
    assertEquals(Seq(0), zkUtils.getReplicasForPartition("customers", 0))
    assertEquals(Seq(1), zkUtils.getReplicasForPartition("customers", 1))
    assertEquals(Seq(2), zkUtils.getReplicasForPartition("customers", 2))
    assertEquals(Seq(3), zkUtils.getReplicasForPartition("customers", 3))
  }

  def waitForReassignmentToComplete() {
    waitUntilTrue(() => !zkUtils.pathExists(ReassignPartitionsPath), s"Znode ${ZkUtils.ReassignPartitionsPath} wasn't deleted")
  }

  def json(topic: String*): String = {
    val topicStr = topic.map { t => "{\"topic\": \"" + t + "\"}" }.mkString(",")
    s"""{"topics": [$topicStr],"version":1}"""
  }
}
