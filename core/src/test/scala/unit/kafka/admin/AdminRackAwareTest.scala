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

import junit.framework.Assert._
import kafka.common.TopicAndPartition
import kafka.utils.{ZkUtils, Logging}
import kafka.zk.ZooKeeperTestHarness
import org.I0Itec.zkclient.ZkClient
import org.junit.Test
import org.scalatest.junit.JUnit3Suite

import scala.collection.{mutable, Map, Seq}

class AdminRackAwareTest extends RackAwareTest  {

  @Test
  def testInterlaceBrokersByRack(): Unit = {
    val rackMap = Map(0 -> "rack1", 1 -> "rack3", 2 -> "rack3", 3 -> "rack2", 4 -> "rack2", 5 -> "rack1")
    val newList = AdminUtils.interlaceBrokersByRack(rackMap)
    assertEquals(List(0, 3, 1, 5, 4, 2), newList)
    val anotherList = AdminUtils.interlaceBrokersByRack(rackMap - 5)
    assertEquals(List(0, 3, 1, 4, 2), anotherList)
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val assignment: scala.collection.Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerList, 7, 3, 0, 0, rackMap)
    val expected: Map[Int, Seq[Int]] = Map(0 -> List(0,3,1),
                                           1 -> List(3,1,5),
                                           2 -> List(1,5,4),
                                           3 -> List(5,4,2),
                                           4 -> List(4,2,0),
                                           5 -> List(2,0,3),
                                           6 -> List(0,4,2))
    assertEquals(expected, assignment)
  }

  @Test
  def testAssignmentWithRackAware() {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val assignment: scala.collection.Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerList, 6, 3, 2, 0, brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 6, 6, 3)
  }

  @Test
  def testAssignmentWithRackAwareWithRandomStartIndex() {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val assignment: scala.collection.Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerList, 6, 3, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 6, 6, 3)
  }

  @Test
  def testAssignmentWithRackAwareWithNotEnoughPartitions() {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val assignment: scala.collection.Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerList, 13, 3, 0, 0, brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 6, 13, 3, verifyRackAware = true)
  }

  def testAssignmentWithRackAwareWithUnEvenRacks() {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val assignment: scala.collection.Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerList, 12, 3, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 6, 12, 3, verifyRackAware = true, verifyLeaderDistribution = true)
  }


  @Test
  def testAssignmentWithRackAwareWith12Partitions() {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 12, 3, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 6, 12, 3)
  }

  @Test
  def testAssignmentWith2ReplicasRackAware() {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 12, 2, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 6, 12, 2)
  }

  @Test
  def testRackAwareExpansion(): Unit = {
    val brokerList = List(6, 7, 8, 9, 10, 11)
    val brokerRackMapping = Map(6 -> "rack1", 7 -> "rack2", 8 -> "rack2", 9 -> "rack3", 10 -> "rack3", 11 -> "rack1")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 12, 2, startPartitionId = 12, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 6, 12, 2)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6Partitions() {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 6, 2, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 6, 6, 2)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6PartitionsAnd3Brokers() {
    val brokerList = List(0, 1, 4)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 3, 2, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 3, 3, 2)
  }

  @Test
  def testLargeNumberPartitionsAssignment(): Unit = {
    val brokerList = 0 to 11
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack1",
      6 -> "rack1", 7 -> "rack2", 8 -> "rack2", 9 -> "rack3", 10 -> "rack1", 11 -> "rack3")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 96, 3, rackInfo = brokerRackMapping)
    ensureRackAwareAndEvenDistribution(assignment, brokerRackMapping, 12, 96, 3)
  }

  @Test
  def testMoreReplicasThanRacks(): Unit = {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack2")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 6, 5, rackInfo = brokerRackMapping)
    assignment.foreach {
      case (_, a) => assertEquals(5, a.size)
    }
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 to 5) {
      assertEquals(3, distribution.partitionRacks(partition).toSet.size)
    }
  }

  @Test
  def testLessReplicasThanRacks(): Unit = {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack3", 4 -> "rack3", 5 -> "rack2")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 6, 2, rackInfo = brokerRackMapping)
    assignment.foreach {
      case (_, a) => assertEquals(2, a.size)
    }
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 to 5) {
      assertEquals(2, distribution.partitionRacks(partition).toSet.size)
    }
  }

  @Test
  def testSingleRack(): Unit = {
    val brokerList = List(0, 1, 2, 3, 4, 5)
    val brokerRackMapping = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack1", 3 -> "rack1", 4 -> "rack1", 5 -> "rack1")
    val assignment = AdminUtils.assignReplicasToBrokers(brokerList, 6, 3, rackInfo = brokerRackMapping)
    assignment.foreach {
      case (_, a) => assertEquals(3, a.size)
    }
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 to 5) {
      assertEquals(1, distribution.partitionRacks(partition).toSet.size)
    }
    for (broker <- 0 to 5) {
      assertEquals(1, distribution.brokerLeaderCount(broker))
    }

  }

}
