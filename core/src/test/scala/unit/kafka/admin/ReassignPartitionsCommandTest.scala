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

import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.log.LogConfig._
import kafka.utils.{CoreUtils, Logging, TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.junit.{Before, Test}
import org.junit.Assert.assertEquals

class ReassignPartitionsCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {
  var calls = 0

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
    val control = TopicAndPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given partition 0 moves from broker 100 -> 102. Partition 1 does not move.
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(101, 102), control)


    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        assertEquals("0:102", configChange.get(FollowerReplicationThrottledReplicasProp)) //Should only be follower-throttle the moving replica
        assertEquals("0:100,0:101", configChange.get(LeaderReplicationThrottledReplicasProp)) //Should leader-throttle all existing (pre move) replicas
        calls += 1
      }
    }

    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasMultiplePartitions() {
    val control = TopicAndPartition("topic1", 2) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given partitions 0 & 1 moves from broker 100 -> 102. Partition 2 does not move.
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101), TopicAndPartition("topic1", 1) -> Seq(100, 101), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(101, 102), TopicAndPartition("topic1", 1) -> Seq(101, 102), control)

    // Then
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        assertEquals("0:102,1:102", configChange.get(FollowerReplicationThrottledReplicasProp)) //Should only be follower-throttle the moving replica
        assertEquals("0:100,0:101,1:100,1:101", configChange.get(LeaderReplicationThrottledReplicasProp)) //Should leader-throttle all existing (pre move) replicas
        calls += 1
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasMultipleTopics() {
    val control = TopicAndPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given topics 1 -> move from broker 100 -> 102, topics 2 -> move from broker 101 -> 100
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101), TopicAndPartition("topic2", 0) -> Seq(101, 102), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(101, 102), TopicAndPartition("topic2", 0) -> Seq(100, 102), control)

    //Then
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        topic match {
          case "topic1" =>
            assertEquals("0:100,0:101", configChange.get(LeaderReplicationThrottledReplicasProp))
            assertEquals("0:102", configChange.get(FollowerReplicationThrottledReplicasProp))
          case "topic2" =>
            assertEquals("0:101,0:102", configChange.get(LeaderReplicationThrottledReplicasProp))
            assertEquals("0:100", configChange.get(FollowerReplicationThrottledReplicasProp))
          case _ => fail("Unexpected topic $topic")
        }
        calls += 1
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(2, calls)
  }

  @Test
  def shouldFindMovingReplicasMultipleTopicsAndPartitions() {
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given
    val existing = Map(
      TopicAndPartition("topic1", 0) -> Seq(100, 101),
      TopicAndPartition("topic1", 1) -> Seq(100, 101),
      TopicAndPartition("topic2", 0) -> Seq(101, 102),
      TopicAndPartition("topic2", 1) -> Seq(101, 102)
    )
    val proposed = Map(
      TopicAndPartition("topic1", 0) -> Seq(101, 102), //moves to 102
      TopicAndPartition("topic1", 1) -> Seq(101, 102), //moves to 102
      TopicAndPartition("topic2", 0) -> Seq(100, 102), //moves to 100
      TopicAndPartition("topic2", 1) -> Seq(101, 100)  //moves to 100
    )

    //Then
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        topic match {
          case "topic1" =>
            assertEquals("0:102,1:102", configChange.get(FollowerReplicationThrottledReplicasProp))
            assertEquals("0:100,0:101,1:100,1:101", configChange.get(LeaderReplicationThrottledReplicasProp))
          case "topic2" =>
            assertEquals("0:100,1:100", configChange.get(FollowerReplicationThrottledReplicasProp))
            assertEquals("0:101,0:102,1:101,1:102", configChange.get(LeaderReplicationThrottledReplicasProp))
          case _ => fail()
        }
        calls += 1
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(2, calls)
  }

  @Test
  def shouldFindTwoMovingReplicasInSamePartition() {
    val control = TopicAndPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null)

    //Given partition 0 has 2 moves from broker 102 -> 104 & 103 -> 105
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101, 102, 103), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101, 104, 105), control)

    // Then
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties) = {
        assertEquals("0:104,0:105", configChange.get(FollowerReplicationThrottledReplicasProp)) //Should only be follower-throttle the moving replicas
        assertEquals("0:100,0:101,0:102,0:103", configChange.get(LeaderReplicationThrottledReplicasProp)) //Should leader-throttle all existing (pre move) replicas
        calls += 1
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(1, calls)
  }

  @Test
  def shouldNotOverwriteEntityConfigsWhenUpdatingThrottledReplicas(): Unit = {
    val control = TopicAndPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null)
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(101, 102), control)

    //Given partition there are existing properties
    val existingProperties = CoreUtils.propsWith("some-key", "some-value")

    //Then the dummy property should still be there
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        assertEquals("some-value", configChange.getProperty("some-key"))
        calls += 1
      }

      override def fetchEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String): Properties = {
        existingProperties
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(1, calls)
  }

  @Before
  def setup(): Unit = {
    calls = 0
  }
}
