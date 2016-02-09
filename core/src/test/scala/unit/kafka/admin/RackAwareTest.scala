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

import junit.framework.Assert._
import kafka.common.TopicAndPartition
import kafka.utils.{ZkUtils, Logging}
import kafka.zk.ZooKeeperTestHarness
import org.I0Itec.zkclient.ZkClient
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import scala.collection.JavaConverters._

import scala.collection.{mutable, Map, Seq}

trait RackAwareTest {
  def ensureRackAwareAndEvenDistribution(assignment: scala.collection.Map[Int, Seq[Int]], brokerRackMapping: Map[Int, String], numBrokers: Int,
                                         numPartitions: Int, replicationFactor: Int): Unit = {
    checkDistribution(assignment, brokerRackMapping, numBrokers, numPartitions, replicationFactor, true, true, true)
  }

  def checkDistribution(assignment: scala.collection.Map[Int, Seq[Int]], brokerRackMapping: Map[Int, String], numBrokers: Int,
                        numPartitions: Int, replicationFactor: Int, verifyRackAware: Boolean = false,
                        verifyLeaderDistribution: Boolean = false, verifyReplicasDistribution: Boolean = false): Unit = {
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    val leaderCount: Map[Int, Int] = distribution.brokerLeaderCount
    val partitionCount: Map[Int, Int] = distribution.brokerReplicasCount
    val partitionRackMap: Map[Int, Seq[String]] = distribution.partitionRacks
    val numReplicasPerBroker = numPartitions * replicationFactor / numBrokers
    val leaderCountPerBroker = numPartitions / numBrokers
    if (verifyRackAware) {
      assertEquals(List.fill(numPartitions)(replicationFactor), partitionRackMap.values.toList.map(_.size))
      assertEquals(List.fill(numPartitions)(replicationFactor), partitionRackMap.values.toList.map(_.toSet.size))
    }
    if (verifyLeaderDistribution) {
      assertEquals(List.fill(numBrokers)(leaderCountPerBroker), leaderCount.values.toList)
    }
    if (verifyReplicasDistribution) {
      assertEquals(List.fill(numBrokers)(numReplicasPerBroker), partitionCount.values.toList)
    }
  }

  def getReplicaDistribution(assignment: scala.collection.Map[Int, Seq[Int]], brokerRackMapping: Map[Int, String]): ReplicaDistributions = {
    // ensure leader distribution is even
    val leaderCount: mutable.Map[Int, Int] = mutable.Map()
    // ensure total partitions distribution is even
    val partitionCount: mutable.Map[Int, Int] = mutable.Map()
    // ensure replicas are assigned to unique racks
    val partitionRackMap: mutable.Map[Int, List[String]] = mutable.Map()
    assignment.foreach {
      case (partitionId, replicaList) => {
        val leader = replicaList.head
        leaderCount(leader) = leaderCount.getOrElse(leader, 0) + 1
        for (brokerId <- replicaList) {
          partitionCount(brokerId) = partitionCount.getOrElse(brokerId, 0) + 1
          partitionRackMap(partitionId) =  brokerRackMapping(brokerId) :: partitionRackMap.getOrElse(partitionId, List())
        }
      }
    }
    ReplicaDistributions(partitionRackMap, leaderCount, partitionCount)
  }

  def ensureSameNumberOfBrokersInRack(brokerList: Seq[Int], brokerRackMap: Map[Int, String]): Unit = {
    val subRackInfo = brokerRackMap.filterKeys(brokerList.contains(_))
    if (brokerRackMap.size > 0 && subRackInfo.size != brokerList.size) {
      throw new AdminOperationException("Incomplete broker-rack mapping supplied for broker list " + brokerList)
    }
    val reverseMap = AdminUtils.getInverseMap(subRackInfo)
    if (reverseMap.values.map(_.size).toSet.size > 1) {
      throw new AdminOperationException("The number of brokers are not the same for all racks: %s".format(reverseMap))
    }
  }
}

case class ReplicaDistributions(partitionRacks: Map[Int, Seq[String]], brokerLeaderCount: Map[Int, Int], brokerReplicasCount: Map[Int, Int])
