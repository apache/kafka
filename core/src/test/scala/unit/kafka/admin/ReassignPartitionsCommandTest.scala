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

import kafka.common.TopicAndPartition
import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.junit.Test
import org.junit.Assert.assertEquals

class ReassignPartitionsCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  @Test
  def testRackAwareReassign() {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3")
    TestUtils.createBrokersInZk(toBrokerMetadata(rackInfo), zkUtils)

    val numPartitions = 18
    val replicationFactor = 3

    // create a non rack aware assignment topic first
    val createOpts = new kafka.admin.TopicCommand.TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--disable-rack-aware",
      "--topic", "foo"))
    kafka.admin.TopicCommand.createTopic(zkUtils, createOpts)

    val topicJson = """{"topics": [{"topic": "foo"}], "version":1}"""
    val (proposedAssignment, currentAssignment) = ReassignPartitionsCommand.generateAssignment(zkUtils,
      rackInfo.keys.toSeq.sorted, topicJson, disableRackAware = false)

    val assignment = proposedAssignment map { case (topicPartition, replicas) =>
      (topicPartition.partition, replicas)
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor)
  }

  @Test
  def shouldFindMovingReplicas() {
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given partition 0 moves from broker 100 -> 102
    val existing = Map(TopicAndPartition("topic1",0) -> Seq(100, 101))
    val proposed = Map(TopicAndPartition("topic1",0) -> Seq(101, 102))

      //When
    val moves = assigner.replicaMoves(existing, proposed)

    //Then moving replicas should be throttled
    assertEquals("0:100,0:102", moves.get("topic1").get)
  }

  @Test
  def shouldFindMovingReplicasMultiplePartitions() {
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given partitions 0 & 1 moves from broker 100 -> 102
    val existing = Map(TopicAndPartition("topic1",0) -> Seq(100, 101), TopicAndPartition("topic1",1) -> Seq(100, 101))
    val proposed = Map(TopicAndPartition("topic1",0) -> Seq(101, 102), TopicAndPartition("topic1",1) -> Seq(101, 102))

      //When
    val moves = assigner.replicaMoves(existing, proposed)

    //Then moving replicas should be throttled
    assertEquals("0:100,0:102,1:100,1:102", moves.get("topic1").get)
  }

  @Test
  def shouldFindMovingReplicasMultipleTopics() {
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given partition 0 on topics 1 & 2 move from broker 100 -> 102
    val existing = Map(TopicAndPartition("topic1",0) -> Seq(100, 101), TopicAndPartition("topic2",0) -> Seq(100, 101))
    val proposed = Map(TopicAndPartition("topic1",0) -> Seq(101, 102), TopicAndPartition("topic2",0) -> Seq(101, 102))

    //When
    val moves = assigner.replicaMoves(existing, proposed)

    //Then moving replicas should be throttled
    assertEquals("0:100,0:102", moves.get("topic1").get)
    assertEquals("0:100,0:102", moves.get("topic2").get)
  }

  @Test
  def shouldFindMovingReplicasMultipleTopicsAndPartitions() {
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given
    val existing = Map(
      TopicAndPartition("topic1",0) -> Seq(100, 101),
      TopicAndPartition("topic1",1) -> Seq(100, 101),
      TopicAndPartition("topic2",0) -> Seq(100, 101),
      TopicAndPartition("topic2",1) -> Seq(100, 101)
    )
    val proposed = Map(
      TopicAndPartition("topic1",0) -> Seq(101, 102),
      TopicAndPartition("topic1",1) -> Seq(101, 102),
      TopicAndPartition("topic2",0) -> Seq(101, 102),
      TopicAndPartition("topic2",1) -> Seq(101, 102)
    )

    //When
    val moves = assigner.replicaMoves(existing, proposed)

    //Then moving replicas should be throttled
    assertEquals("0:100,0:102,1:100,1:102", moves.get("topic1").get)
    assertEquals("0:100,0:102,1:100,1:102", moves.get("topic2").get)
  }

  @Test
  def shouldFindTwoMovingReplicasInSamePartition() {
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given partition 0 has 2 moves from broker 102 -> 104 & 103 -> 105
    val existing = Map(TopicAndPartition("topic1",0) -> Seq(100, 101, 102, 103))
    val proposed = Map(TopicAndPartition("topic1",0) -> Seq(100, 101, 104, 105))

    //When
    val moves = assigner.replicaMoves(existing, proposed)

    //Then moving replicas should be throttled
    assertEquals( "0:102,0:103,0:104,0:105", moves.get("topic1").get)
  }
}
