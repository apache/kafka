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

import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.junit.Assert._
import org.junit.Test

import scala.collection.{Map, Seq}

class AdminRackAwareTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  @Test
  def testGetRackAlternatedBrokerListAndAssignReplicasToBrokers() {
    val rackMap = Map(0 -> "rack1", 1 -> "rack3", 2 -> "rack3", 3 -> "rack2", 4 -> "rack2", 5 -> "rack1")
    val newList = AdminUtils.getRackAlternatedBrokerList(rackMap)
    assertEquals(List(0, 3, 1, 5, 4, 2), newList)
    val anotherList = AdminUtils.getRackAlternatedBrokerList(rackMap - 5)
    assertEquals(List(0, 3, 1, 4, 2), anotherList)
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(rackMap), 7, 3, 0, 0)
    val expected = Map(0 -> List(0, 3, 1),
                       1 -> List(3, 1, 5),
                       2 -> List(1, 5, 4),
                       3 -> List(5, 4, 2),
                       4 -> List(4, 2, 0),
                       5 -> List(2, 0, 3),
                       6 -> List(0, 4, 2))
    assertEquals(expected, assignment)
  }

  @Test
  def testAssignmentWithRackAware() {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 6
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, 2, 0)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWithRackAwareWithRandomStartIndex() {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 6
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWithRackAwareWithUnevenReplicas() {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 13
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, 0, 0)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor, verifyLeaderDistribution = false, verifyReplicasDistribution = false)
  }

  @Test
  def testAssignmentWithRackAwareWithUnevenRacks() {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor, verifyReplicasDistribution = false)
  }

  @Test
  def testAssignmentWith2ReplicasRackAware() {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testRackAwareExpansion() {
    val brokerRackMapping = Map(6 -> "rack1", 7 -> "rack2", 8 -> "rack2", 9 -> "rack3", 10 -> "rack3", 11 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, startPartitionId = 12)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6Partitions() {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 6
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6PartitionsAnd3Brokers() {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 4 -> "rack3")
    val numPartitions = 3
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions, replicationFactor)
  }

  @Test
  def testLargeNumberPartitionsAssignment() {
    val numPartitions = 96
    val replicationFactor = 3
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1",
      6 -> "rack1", 7 -> "rack2", 8 -> "rack2", 9 -> "rack3", 10 -> "rack1", 11 -> "rack3")
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testMoreReplicasThanRacks() {
    val numPartitions = 6
    val replicationFactor = 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack2")
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor)
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 until numPartitions)
      assertEquals(3, distribution.partitionRacks(partition).toSet.size)
  }

  @Test
  def testLessReplicasThanRacks() {
    val numPartitions = 6
    val replicationFactor = 2
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack2")
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 to 5)
      assertEquals(2, distribution.partitionRacks(partition).toSet.size)
  }

  @Test
  def testSingleRack() {
    val numPartitions = 6
    val replicationFactor = 3
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack1", 3 -> "rack1", 4 -> "rack1", 5 -> "rack1")
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor)
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 until numPartitions)
      assertEquals(1, distribution.partitionRacks(partition).toSet.size)
    for (broker <- brokerRackMapping.keys)
      assertEquals(1, distribution.brokerLeaderCount(broker))
  }

  @Test
  def testSkipBrokerWithReplicaAlreadyAssigned() {
    val rackInfo = Map(0 -> "a", 1 -> "b", 2 -> "c", 3 -> "a", 4 -> "a")
    val brokerList = 0 to 4
    val numPartitions = 6
    val replicationFactor = 4
    val brokerMetadatas = toBrokerMetadata(rackInfo)
    assertEquals(brokerList, brokerMetadatas.map(_.id))
    val assignment = AdminUtils.assignReplicasToBrokers(brokerMetadatas, numPartitions, replicationFactor,
      fixedStartIndex = 2)
    checkReplicaDistribution(assignment, rackInfo, 5, 6, 4,
      verifyRackAware = false, verifyLeaderDistribution = false, verifyReplicasDistribution = false)
  }

  @Test
  def testGetBrokerMetadatas() {
    // broker 4 has no rack information
    val brokerList = 0 to 5
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 5 -> "rack3")
    val brokerMetadatas = toBrokerMetadata(rackInfo, brokersWithoutRack = brokerList.filterNot(rackInfo.keySet))
    TestUtils.createBrokersInZk(brokerMetadatas, zkUtils)

    val processedMetadatas1 = AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Disabled)
    assertEquals(brokerList, processedMetadatas1.map(_.id))
    assertEquals(List.fill(brokerList.size)(None), processedMetadatas1.map(_.rack))

    val processedMetadatas2 = AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Safe)
    assertEquals(brokerList, processedMetadatas2.map(_.id))
    assertEquals(List.fill(brokerList.size)(None), processedMetadatas2.map(_.rack))

    intercept[AdminOperationException] {
      AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Enforced)
    }

    val partialList = List(0, 1, 2, 3, 5)
    val processedMetadatas3 = AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Enforced, Some(partialList))
    assertEquals(partialList, processedMetadatas3.map(_.id))
    assertEquals(partialList.map(rackInfo), processedMetadatas3.flatMap(_.rack))

    val numPartitions = 3
    AdminUtils.createTopic(zkUtils, "foo", numPartitions, 2, rackAwareMode = RackAwareMode.Safe)
    val assignment = zkUtils.getReplicaAssignmentForTopics(Seq("foo"))
    assertEquals(numPartitions, assignment.size)
  }

}
