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

import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidReplicationFactorException
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._

import scala.collection.Map

class AdminRackAwareTest extends RackAwareTest with Logging {

  @Test
  def testGetRackAlternatedBrokerListAndAssignReplicasToBrokers(): Unit = {
    val rackMap = Map(0 -> "rack1", 1 -> "rack3", 2 -> "rack3", 3 -> "rack2", 4 -> "rack2", 5 -> "rack1")
    val newList = AdminUtils.getRackAlternatedBrokerList(rackMap)
    assertEquals(List(0, 3, 1, 5, 4, 2), newList)
    val anotherList = AdminUtils.getRackAlternatedBrokerList(rackMap.toMap - 5)
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
  def testAssignmentWithRackAware(): Unit = {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 6
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, 2, 0)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWithRackAwareWithRandomStartIndex(): Unit = {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 6
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWithRackAwareWithUnevenReplicas(): Unit = {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 13
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, 0, 0)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor, verifyLeaderDistribution = false, verifyReplicasDistribution = false)
  }

  @Test
  def testAssignmentWithRackAwareWithUnevenRacks(): Unit = {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor, verifyReplicasDistribution = false)
  }

  @Test
  def testAssignmentWith2ReplicasRackAware(): Unit = {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testRackAwareExpansion(): Unit = {
    val brokerRackMapping = Map(6 -> "rack1", 7 -> "rack2", 8 -> "rack2", 9 -> "rack3", 10 -> "rack3", 11 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, startPartitionId = 12)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6Partitions(): Unit = {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 6
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6PartitionsAnd3Brokers(): Unit = {
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 4 -> "rack3")
    val numPartitions = 3
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor)
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions, replicationFactor)
  }

  @Test
  def testLargeNumberPartitionsAssignment(): Unit = {
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
  def testMoreReplicasThanRacks(): Unit = {
    val numPartitions = 6
    val replicationFactor = 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack2")
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor)
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.toIndexedSeq.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 until numPartitions)
      assertEquals(3, distribution.partitionRacks(partition).toSet.size)
  }

  @Test
  def testLessReplicasThanRacks(): Unit = {
    val numPartitions = 6
    val replicationFactor = 2
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack2")
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor)
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.toIndexedSeq.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 to 5)
      assertEquals(2, distribution.partitionRacks(partition).toSet.size)
  }

  @Test
  def testSingleRack(): Unit = {
    val numPartitions = 6
    val replicationFactor = 3
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack1", 3 -> "rack1", 4 -> "rack1", 5 -> "rack1")
    val assignment = AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor)
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.toIndexedSeq.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 until numPartitions)
      assertEquals(1, distribution.partitionRacks(partition).toSet.size)
    for (broker <- brokerRackMapping.keys)
      assertEquals(1, distribution.brokerLeaderCount(broker))
  }

  @Test
  def testSkipBrokerWithReplicaAlreadyAssigned(): Unit = {
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
  def testReplicaAssignment(): Unit = {
    val brokerMetadatas = (0 to 4).map(new BrokerMetadata(_, None))

    // test 0 replication factor
    intercept[InvalidReplicationFactorException] {
      AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 0)
    }

    // test wrong replication factor
    intercept[InvalidReplicationFactorException] {
      AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 6)
    }

    // correct assignment
    val expectedAssignment = Map(
        0 -> List(0, 1, 2),
        1 -> List(1, 2, 3),
        2 -> List(2, 3, 4),
        3 -> List(3, 4, 0),
        4 -> List(4, 0, 1),
        5 -> List(0, 2, 3),
        6 -> List(1, 3, 4),
        7 -> List(2, 4, 0),
        8 -> List(3, 0, 1),
        9 -> List(4, 1, 2))

    val actualAssignment = AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 3, 0)
    assertEquals(expectedAssignment, actualAssignment)
  }
}
