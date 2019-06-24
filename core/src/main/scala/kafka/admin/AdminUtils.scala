/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.util.Random

import kafka.utils.Logging
import org.apache.kafka.common.errors.{InvalidPartitionsException, InvalidReplicationFactorException}

import collection.{Map, mutable, _}

object AdminUtils extends Logging {
  val rand = new Random
  val AdminClientId = "__admin_client"

  /**
   * There are 3 goals of replica assignment:
   *
   * 1. Spread the replicas evenly among brokers.
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
   * 3. If all brokers have rack information, assign the replicas for each partition to different racks if possible
   *
   * To achieve this goal for replica assignment without considering racks, we:
   * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
   * 2. Assign the remaining replicas of each partition with an increasing shift.
   *
   * Here is an example of assigning
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3nd replica)
   * p7        p8        p9        p5        p6       (3nd replica)
   *
   * To create rack aware assignment, this API will first create a rack alternated broker list. For example,
   * from this brokerID -> rack mapping:
   *
   * 0 -> "rack1", 1 -> "rack3", 2 -> "rack3", 3 -> "rack2", 4 -> "rack2", 5 -> "rack1"
   *
   * The rack alternated list will be:
   *
   * 0, 3, 1, 5, 4, 2
   *
   * Then an easy round-robin assignment can be applied. Assume 6 partitions with replication factor of 3, the assignment
   * will be:
   *
   * 0 -> 0,3,1
   * 1 -> 3,1,5
   * 2 -> 1,5,4
   * 3 -> 5,4,2
   * 4 -> 4,2,0
   * 5 -> 2,0,3
   *
   * Once it has completed the first round-robin, if there are more partitions to assign, the algorithm will start
   * shifting the followers. This is to ensure we will not always get the same set of sequences.
   * In this case, if there is another partition to assign (partition #6), the assignment will be:
   *
   * 6 -> 0,4,2 (instead of repeating 0,3,1 as partition 0)
   *
   * The rack aware assignment always chooses the 1st replica of the partition using round robin on the rack alternated
   * broker list. For rest of the replicas, it will be biased towards brokers on racks that do not have
   * any replica assignment, until every rack has a replica. Then the assignment will go back to round-robin on
   * the broker list.
   *
   * As the result, if the number of replicas is equal to or greater than the number of racks, it will ensure that
   * each rack will get at least one replica. Otherwise, each rack will get at most one replica. In a perfect
   * situation where the number of replicas is the same as the number of racks and each rack has the same number of
   * brokers, it guarantees that the replica distribution is even across brokers and racks.
   *
   * @return a Map from partition id to replica ids
   * @throws AdminOperationException If rack information is supplied but it is incomplete, or if it is not possible to
   *                                 assign each replica to a unique rack.
   *
   */
  def assignReplicasToBrokers(brokerMetadatas: Seq[BrokerMetadata],
                              nPartitions: Int,
                              replicationFactor: Int,
                              fixedStartIndex: Int = -1,
                              startPartitionId: Int = -1): Map[Int, Seq[Int]] = {
    if (nPartitions <= 0)
      throw new InvalidPartitionsException("Number of partitions must be larger than 0.")
    if (replicationFactor <= 0)
      throw new InvalidReplicationFactorException("Replication factor must be larger than 0.")
    if (replicationFactor > brokerMetadatas.size)
      throw new InvalidReplicationFactorException(s"Replication factor: $replicationFactor larger than available brokers: ${brokerMetadatas.size}.")
    if (brokerMetadatas.forall(_.rack.isEmpty))
      assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.map(_.id), fixedStartIndex,
        startPartitionId)
    else {
      if (brokerMetadatas.exists(_.rack.isEmpty))
        throw new AdminOperationException("Not all brokers have rack information for replica rack aware assignment.")
      assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,
        startPartitionId)
    }
  }

  private def assignReplicasToBrokersRackUnaware(nPartitions: Int,
                                                 replicationFactor: Int,
                                                 brokerList: Seq[Int],
                                                 fixedStartIndex: Int,
                                                 startPartitionId: Int): Map[Int, Seq[Int]] = {
    val ret = mutable.Map[Int, Seq[Int]]()
    val brokerArray = brokerList.toArray
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
    var currentPartitionId = math.max(0, startPartitionId)
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
    for (_ <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
      val replicaBuffer = mutable.ArrayBuffer(brokerArray(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaBuffer += brokerArray(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length))
      ret.put(currentPartitionId, replicaBuffer)
      currentPartitionId += 1
    }
    ret
  }

  private def assignReplicasToBrokersRackAware(nPartitions: Int,
                                               replicationFactor: Int,
                                               brokerMetadatas: Seq[BrokerMetadata],
                                               fixedStartIndex: Int,
                                               startPartitionId: Int): Map[Int, Seq[Int]] = {
    val brokerRackMap = brokerMetadatas.collect { case BrokerMetadata(id, Some(rack)) =>
      id -> rack
    }.toMap
    val numRacks = brokerRackMap.values.toSet.size
    val arrangedBrokerList = getRackAlternatedBrokerList(brokerRackMap)
    val numBrokers = arrangedBrokerList.size
    val ret = mutable.Map[Int, Seq[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    var currentPartitionId = math.max(0, startPartitionId)
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    for (_ <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % arrangedBrokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % arrangedBrokerList.size
      val leader = arrangedBrokerList(firstReplicaIndex)
      val replicaBuffer = mutable.ArrayBuffer(leader)
      val racksWithReplicas = mutable.Set(brokerRackMap(leader))
      val brokersWithReplicas = mutable.Set(leader)
      var k = 0
      for (_ <- 0 until replicationFactor - 1) {
        var done = false
        while (!done) {
          val broker = arrangedBrokerList(replicaIndex(firstReplicaIndex, nextReplicaShift * numRacks, k, arrangedBrokerList.size))
          val rack = brokerRackMap(broker)
          // Skip this broker if
          // 1. there is already a broker in the same rack that has assigned a replica AND there is one or more racks
          //    that do not have any replica, or
          // 2. the broker has already assigned a replica AND there is one or more brokers that do not have replica assigned
          if ((!racksWithReplicas.contains(rack) || racksWithReplicas.size == numRacks)
              && (!brokersWithReplicas.contains(broker) || brokersWithReplicas.size == numBrokers)) {
            replicaBuffer += broker
            racksWithReplicas += rack
            brokersWithReplicas += broker
            done = true
          }
          k += 1
        }
      }
      ret.put(currentPartitionId, replicaBuffer)
      currentPartitionId += 1
    }
    ret
  }

  /**
    * Given broker and rack information, returns a list of brokers alternated by the rack. Assume
    * this is the rack and its brokers:
    *
    * rack1: 0, 1, 2
    * rack2: 3, 4, 5
    * rack3: 6, 7, 8
    *
    * This API would return the list of 0, 3, 6, 1, 4, 7, 2, 5, 8
    *
    * This is essential to make sure that the assignReplicasToBrokers API can use such list and
    * assign replicas to brokers in a simple round-robin fashion, while ensuring an even
    * distribution of leader and replica counts on each broker and that replicas are
    * distributed to all racks.
    */
  private[admin] def getRackAlternatedBrokerList(brokerRackMap: Map[Int, String]): IndexedSeq[Int] = {
    val brokersIteratorByRack = getInverseMap(brokerRackMap).map { case (rack, brokers) =>
      (rack, brokers.toIterator)
    }
    val racks = brokersIteratorByRack.keys.toArray.sorted
    val result = new mutable.ArrayBuffer[Int]
    var rackIndex = 0
    while (result.size < brokerRackMap.size) {
      val rackIterator = brokersIteratorByRack(racks(rackIndex))
      if (rackIterator.hasNext)
        result += rackIterator.next()
      rackIndex = (rackIndex + 1) % racks.length
    }
    result
  }

  private[admin] def getInverseMap(brokerRackMap: Map[Int, String]): Map[String, Seq[Int]] = {
    brokerRackMap.toSeq.map { case (id, rack) => (rack, id) }
      .groupBy { case (rack, _) => rack }
      .map { case (rack, rackAndIdList) => (rack, rackAndIdList.map { case (_, id) => id }.sorted) }
  }

  private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }

}
