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
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 7, 3, 0, 0, rackMap)
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
    val brokerList = 0 to 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 6
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, 2, 0,
      brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor)
  }

  @Test
  def testAssignmentWithRackAwareWithRandomStartIndex() {
    val brokerList = 0 to 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 6
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor)
  }

  @Test
  def testAssignmentWithRackAwareWithNotEnoughPartitions() {
    val brokerList = 0 to 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 13
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, 0, 0, brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor,
      verifyLeaderDistribution = false, verifyReplicasDistribution = false)
  }

  @Test
  def testAssignmentWithRackAwareWithUnevenRacks() {
    val brokerList = 0 to 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor,
      verifyReplicasDistribution = false)
  }


  @Test
  def testAssignmentWithRackAwareWith12Partitions() {
    val brokerList = 0 to 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 3
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor)
  }

  @Test
  def testAssignmentWith2ReplicasRackAware() {
    val brokerList = 0 to 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor)
  }

  @Test
  def testRackAwareExpansion() {
    val brokerList = 6 to 11
    val brokerRackMapping = Map(6 -> "rack1", 7 -> "rack2", 8 -> "rack2", 9 -> "rack3", 10 -> "rack3", 11 -> "rack1")
    val numPartitions = 12
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor,
      startPartitionId = 12, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6Partitions() {
    val brokerList = 0 to 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 6
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6PartitionsAnd3Brokers() {
    val brokerList = List(0, 1, 4)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val numPartitions = 3
    val replicationFactor = 2
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor)
  }

  @Test
  def testLargeNumberPartitionsAssignment() {
    val brokerList = 0 to 11
    val numPartitions = 96
    val replicationFactor = 3
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1",
      6 -> "rack1", 7 -> "rack2", 8 -> "rack2", 9 -> "rack3", 10 -> "rack1", 11 -> "rack3")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, brokerList.size, numPartitions, replicationFactor)
  }

  @Test
  def testMoreReplicasThanRacks() {
    val brokerList = 0 to 5
    val numPartitions = 6
    val replicationFactor = 5
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack2")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor,
      rackInfo = brokerRackMapping)
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 until numPartitions)
      assertEquals(3, distribution.partitionRacks(partition).toSet.size)
  }

  @Test
  def testLessReplicasThanRacks() {
    val brokerList = 0 to 5
    val numPartitions = 6
    val replicationFactor = 2
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack2")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, rackInfo = brokerRackMapping)
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 to 5)
      assertEquals(2, distribution.partitionRacks(partition).toSet.size)
  }

  @Test
  def testSingleRack() {
    val brokerList = 0 to 5
    val numPartitions = 6
    val replicationFactor = 3
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack1", 3 -> "rack1", 4 -> "rack1", 5 -> "rack1")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor, rackInfo = brokerRackMapping)
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 until numPartitions)
      assertEquals(1, distribution.partitionRacks(partition).toSet.size)
    for (broker <- brokerList)
      assertEquals(1, distribution.brokerLeaderCount(broker))
  }

  @Test
  def testGetBrokersAndRacks() {
    val brokers = 0 to 5
    // broker 4 has no rack information
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 5 -> "rack3")
    TestUtils.createBrokersInZk(zkUtils, brokers, rackInfo)
    val (brokerList, brokerRack) = AdminUtils.getBrokersAndRackInfo(zkUtils, RackAwareMode.Disabled)
    assertEquals(brokers, brokerList)
    assertEquals(Map(), brokerRack)
    val (_, brokerRack1) = AdminUtils.getBrokersAndRackInfo(zkUtils, RackAwareMode.Safe)
    assertEquals(Map(), brokerRack1)
    intercept[AdminOperationException] {
      AdminUtils.getBrokersAndRackInfo(zkUtils, RackAwareMode.Enforced)
    }
    val partialList = List(0, 1, 2, 3, 5)
    val (brokerList2, brokerRack2) = AdminUtils.getBrokersAndRackInfo(zkUtils, RackAwareMode.Enforced, Some(partialList))
    assertEquals(partialList, brokerList2)
    assertEquals(rackInfo, brokerRack2)
    val numPartitions = 3
    AdminUtils.createTopic(zkUtils, "foo", numPartitions, 2, rackAwareMode = RackAwareMode.Safe)
    val assignment = zkUtils.getReplicaAssignmentForTopics(Seq("foo"))
    assertEquals(numPartitions, assignment.size)
  }

}
