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

import scala.collection.{Map, Seq, mutable}
import org.junit.Assert._

trait RackAwareTest {

  def checkReplicaDistribution(assignment: Map[Int, Seq[Int]],
                               brokerRackMapping: Map[Int, String],
                               numBrokers: Int,
                               numPartitions: Int,
                               replicationFactor: Int,
                               verifyRackAware: Boolean = true,
                               verifyLeaderDistribution: Boolean = true,
                               verifyReplicasDistribution: Boolean = true) {
    // always verify that no broker will be assigned for more than one replica
    for ((_, brokerList) <- assignment) {
      assertEquals("More than one replica is assigned to same broker for the same partition", brokerList.toSet.size, brokerList.size)
    }
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)

    if (verifyRackAware) {
      val partitionRackMap = distribution.partitionRacks
      assertEquals("More than one replica of the same partition is assigned to the same rack",
        List.fill(numPartitions)(replicationFactor), partitionRackMap.values.toList.map(_.distinct.size))
    }

    if (verifyLeaderDistribution) {
      val leaderCount = distribution.brokerLeaderCount
      val leaderCountPerBroker = numPartitions / numBrokers
      assertEquals("Preferred leader count is not even for brokers", List.fill(numBrokers)(leaderCountPerBroker), leaderCount.values.toList)
    }

    if (verifyReplicasDistribution) {
      val replicasCount = distribution.brokerReplicasCount
      val numReplicasPerBroker = numPartitions * replicationFactor / numBrokers
      assertEquals("Replica count is not even for broker", List.fill(numBrokers)(numReplicasPerBroker), replicasCount.values.toList)
    }
  }

  def getReplicaDistribution(assignment: Map[Int, Seq[Int]], brokerRackMapping: Map[Int, String]): ReplicaDistributions = {
    val leaderCount = mutable.Map[Int, Int]()
    val partitionCount = mutable.Map[Int, Int]()
    val partitionRackMap = mutable.Map[Int, List[String]]()
    assignment.foreach { case (partitionId, replicaList) =>
      val leader = replicaList.head
      leaderCount(leader) = leaderCount.getOrElse(leader, 0) + 1
      for (brokerId <- replicaList) {
        partitionCount(brokerId) = partitionCount.getOrElse(brokerId, 0) + 1
        val rack = brokerRackMapping.getOrElse(brokerId, sys.error(s"No mapping found for $brokerId in `brokerRackMapping`"))
        partitionRackMap(partitionId) = rack :: partitionRackMap.getOrElse(partitionId, List())
      }
    }
    ReplicaDistributions(partitionRackMap, leaderCount, partitionCount)
  }

  def toBrokerMetadata(rackMap: Map[Int, String], brokersWithoutRack: Seq[Int] = Seq.empty): Seq[BrokerMetadata] =
    rackMap.toSeq.map { case (brokerId, rack) =>
      BrokerMetadata(brokerId, Some(rack))
    } ++ brokersWithoutRack.map { brokerId =>
      BrokerMetadata(brokerId, None)
    }.sortBy(_.id)

}

case class ReplicaDistributions(partitionRacks: Map[Int, Seq[String]], brokerLeaderCount: Map[Int, Int], brokerReplicasCount: Map[Int, Int])
