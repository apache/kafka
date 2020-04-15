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
import org.apache.kafka.common.errors.{InvalidPartitionsException, InvalidReplicationFactorException, WillExceedPartitionLimitsException}

import collection.{Map, mutable, _}

object AdminUtils extends Logging {
  val rand = new Random
  val AdminClientId = "__admin_client"

  /**
   * There are 3 goals of replica assignment:
   *
   * <ol>
   * <li> Spread the replicas evenly among brokers.</li>
   * <li> For partitions assigned to a particular broker, their other replicas are spread over the other brokers.</li>
   * <li> If all brokers have rack information, assign the replicas for each partition to different racks if possible.</li>
   * <li> Ensure that the assignment does not violate partition limits at the broker level and cluster level.</li>
   * </ol>
   *
   * To achieve this goal for replica assignment without considering racks, we:
   * <ol>
   * <li> Assign the first replica of each partition by round-robin, starting from a random position in the broker list.</li>
   * <li> Assign the remaining replicas of each partition with an increasing shift.</li>
   * <li> Incrementally, remove any brokers from consideration when they have hit the broker-level max partition limit.</li>
   * </ol>
   *
   * Here is an example of assigning without considering partition limits.
   * <table cellpadding="2" cellspacing="2">
   * <tr><th>broker-0</th><th>broker-1</th><th>broker-2</th><th>broker-3</th><th>broker-4</th><th>&nbsp;</th></tr>
   * <tr><td>p0      </td><td>p1      </td><td>p2      </td><td>p3      </td><td>p4      </td><td>(1st replica)</td></tr>
   * <tr><td>p5      </td><td>p6      </td><td>p7      </td><td>p8      </td><td>p9      </td><td>(1st replica)</td></tr>
   * <tr><td>p4      </td><td>p0      </td><td>p1      </td><td>p2      </td><td>p3      </td><td>(2nd replica)</td></tr>
   * <tr><td>p8      </td><td>p9      </td><td>p5      </td><td>p6      </td><td>p7      </td><td>(2nd replica)</td></tr>
   * <tr><td>p3      </td><td>p4      </td><td>p0      </td><td>p1      </td><td>p2      </td><td>(3nd replica)</td></tr>
   * <tr><td>p7      </td><td>p8      </td><td>p9      </td><td>p5      </td><td>p6      </td><td>(3nd replica)</td></tr>
   * </table>
   *
   * <p>
   * To create rack aware assignment, this API will first create a rack alternated broker list. For example,
   * from this brokerID -> rack mapping:</p>
   * 0 -> "rack1", 1 -> "rack3", 2 -> "rack3", 3 -> "rack2", 4 -> "rack2", 5 -> "rack1"
   * <br><br>
   * <p>
   * The rack alternated list will be:
   * </p>
   * 0, 3, 1, 5, 4, 2
   * <br><br>
   * <p>
   * Then an easy round-robin assignment can be applied. Assume 6 partitions with replication factor of 3, the assignment
   * will be:
   * </p>
   * 0 -> 0,3,1 <br>
   * 1 -> 3,1,5 <br>
   * 2 -> 1,5,4 <br>
   * 3 -> 5,4,2 <br>
   * 4 -> 4,2,0 <br>
   * 5 -> 2,0,3 <br>
   * <br>
   * <p>
   * Once it has completed the first round-robin, if there are more partitions to assign, the algorithm will start
   * shifting the followers. This is to ensure we will not always get the same set of sequences.
   * In this case, if there is another partition to assign (partition #6), the assignment will be:
   * </p>
   * 6 -> 0,4,2 (instead of repeating 0,3,1 as partition 0)
   * <br><br>
   * <p>
   * The rack aware assignment always chooses the 1st replica of the partition using round robin on the rack alternated
   * broker list. For rest of the replicas, it will be biased towards brokers on racks that do not have
   * any replica assignment, until every rack has a replica. Then the assignment will go back to round-robin on
   * the broker list.
   * </p>
   * <br>
   * <p>
   * As the result, if the number of replicas is equal to or greater than the number of racks, it will ensure that
   * each rack will get at least one replica. Otherwise, each rack will get at most one replica. In a perfect
   * situation where the number of replicas is the same as the number of racks and each rack has the same number of
   * brokers, it guarantees that the replica distribution is even across brokers and racks.
   * </p>
   * @return a Map from partition id to replica ids
   * @throws AdminOperationException If rack information is supplied but it is incomplete, or if it is not possible to
   *                                 assign each replica to a unique rack.
   * @throws WillExceedPartitionLimitsException If the assignment will exceed broker-level or cluster-level partition limits.
   */
  def assignReplicasToBrokers(brokerMetadatas: Seq[BrokerMetadata],
                              nPartitions: Int,
                              replicationFactor: Int,
                              maxPartitions: Int = Int.MaxValue,
                              maxBrokerPartitions: Int = Int.MaxValue,
                              partitionsByBroker: Map[Int, Int] = Map.empty[Int, Int],
                              fixedStartIndex: Int = -1,
                              startPartitionId: Int = -1): Map[Int, Seq[Int]] = {
    if (nPartitions <= 0)
      throw new InvalidPartitionsException("Number of partitions must be larger than 0.")
    if (replicationFactor <= 0)
      throw new InvalidReplicationFactorException("Replication factor must be larger than 0.")
    if (replicationFactor > brokerMetadatas.size)
      throw new InvalidReplicationFactorException(s"Replication factor: $replicationFactor larger than available brokers: ${brokerMetadatas.size}.")

    maybeThrowWillExceedMaxPartitionLimitsException(partitionsByBroker, nPartitions * replicationFactor, maxPartitions)

    if (brokerMetadatas.forall(_.rack.isEmpty))
      assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.map(_.id), fixedStartIndex,
        startPartitionId, maxBrokerPartitions, partitionsByBroker)
    else {
      if (brokerMetadatas.exists(_.rack.isEmpty))
        throw new AdminOperationException("Not all brokers have rack information for replica rack aware assignment.")
      assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,
        startPartitionId, maxBrokerPartitions, partitionsByBroker)
    }
  }

  def verifyReplicaAssignmentAgainstPartitionLimits(assignments: Map[Int, Seq[Int]],
                                                    maxPartitions: Int = Int.MaxValue,
                                                    maxBrokerPartitions: Int = Int.MaxValue,
                                                    partitionsByBroker: Map[Int, Int] = Map.empty[Int, Int]): Map[Int, Seq[Int]] = {
    maybeThrowWillExceedMaxPartitionLimitsException(partitionsByBroker, assignments.size, maxPartitions)

    val additionalPartitionsByBroker =
      assignments
        .map { case (partition, replicas) =>
          replicas.map(r => (r, partition))
        }
        .flatten
        .groupBy(_._1)
        .mapValues(_.size)

    val finalPartitionsByBroker =
      (partitionsByBroker.toSeq ++ additionalPartitionsByBroker.toSeq)
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)

    // We will only consider those brokers as exceeding the limit that are relevant to the requested
    // assignment. This is necessary because some brokers which are not relevant to the requested
    // assignment may already be exceeding the limit (for example if the limit was applied after the broker
    // had more partitions than allowed by the limit), and we do not want this to cause the verification
    // to fail.
    val brokersThatExceedLimits =
      finalPartitionsByBroker
          .filter { case (broker, numPartitions) =>
            additionalPartitionsByBroker.contains(broker) && numPartitions > maxBrokerPartitions }
          .map { case (broker, _) => broker }

    if (!brokersThatExceedLimits.isEmpty) {
      throw new WillExceedPartitionLimitsException(
        s"Provided assignment will cause brokers [${brokersThatExceedLimits.mkString(",")}] " +
          s"to exceed the limit of $maxBrokerPartitions on the number of partition replicas per broker .")
    }

    assignments
  }

  private def assignReplicasToBrokersRackUnaware(nPartitions: Int,
                                                 replicationFactor: Int,
                                                 brokerList: Seq[Int],
                                                 fixedStartIndex: Int,
                                                 startPartitionId: Int,
                                                 maxBrokerPartitions: Int,
                                                 partitionsByBroker: Map[Int, Int]): Map[Int, Seq[Int]] = {
    var currentPartitionId = math.max(0, startPartitionId)
    val partitionCapacityByBroker = getPartitionCapacityByBroker(brokerList, partitionsByBroker, maxBrokerPartitions)
    var brokerArray = partitionCapacityByBroker.keys.toSeq.sorted
    if (brokerArray.isEmpty) {
      throwAllBrokersHaveAlreadyReachedBrokerPartitionLimitsException(maxBrokerPartitions)
    }

    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex % brokerArray.length else rand.nextInt(brokerArray.length)
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex % brokerArray.length else rand.nextInt(brokerArray.length)
    val ret = mutable.Map[Int, Seq[Int]]()
    for (nP <- 0 until nPartitions) {
      if (brokerArray.length < replicationFactor) {
        throwUnableToAssignMorePartitionsDueToBrokerPartitionLimitsException(nP, maxBrokerPartitions)
      }

      if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0))
        nextReplicaShift += 1

      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
      val replicaBuffer = mutable.ArrayBuffer[Int]()
      var partitionCapacityZeroedForSomeBroker = false
      for (j <- -1 until replicationFactor - 1) {
        // j == -1 in case of first replica (i.e. the leader).
        val replicaBrokerIndex = if (j == -1) firstReplicaIndex else replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length)
        val replicaBroker = brokerArray(replicaBrokerIndex)

        // Given how we compute the replica broker index, we are guaranteed that the same replica broker
        // is not visited more than once within an iteration of this loop. Therefore, we can assume that
        // since the broker had capacity before the starting of the loop, that it has capacity now.
        replicaBuffer += replicaBroker
        if (reduceBrokerPartitionCapacity(partitionCapacityByBroker, replicaBroker)) {
          partitionCapacityZeroedForSomeBroker = true
        }
      }
      ret.put(currentPartitionId, replicaBuffer)
      currentPartitionId += 1

      if (partitionCapacityZeroedForSomeBroker) {
        // We recreate this array only when partition capacity dropped to zero for some broker
        // as a result of replica assignment for this partition. This is strictly an optimization
        // to avoid unnecessary copies. This optimization guarantees that the copy cannot happen more times
        // than the total number of brokers.
        brokerArray = partitionCapacityByBroker.keys.toSeq.sorted
      }
    }
    ret
  }

  private def assignReplicasToBrokersRackAware(nPartitions: Int,
                                               replicationFactor: Int,
                                               brokerMetadatas: Seq[BrokerMetadata],
                                               fixedStartIndex: Int,
                                               startPartitionId: Int,
                                               maxBrokerPartitions: Int,
                                               partitionsByBroker: Map[Int, Int]): Map[Int, Seq[Int]] = {
    var currentPartitionId = math.max(0, startPartitionId)
    val partitionCapacityByBroker =
      getPartitionCapacityByBroker(brokerMetadatas.map(bm => bm.id), partitionsByBroker, maxBrokerPartitions)
    var brokerRackMap =
      brokerMetadatas
        .filter { case bm => partitionCapacityByBroker.contains(bm.id) }
        .collect { case BrokerMetadata(id, Some(rack)) => id -> rack }
      .toMap
    if (brokerRackMap.isEmpty) {
      throwAllBrokersHaveAlreadyReachedBrokerPartitionLimitsException(maxBrokerPartitions)
    }

    var arrangedBrokerList = getRackAlternatedBrokerList(brokerRackMap)
    var numRacks = brokerRackMap.values.toSet.size
    var numBrokers = arrangedBrokerList.size
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex % numBrokers else rand.nextInt(numBrokers)
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex % numBrokers else rand.nextInt(numBrokers)
    val ret = mutable.Map[Int, Seq[Int]]()
    for (nP <- 0 until nPartitions) {
      if (numBrokers < replicationFactor) {
        throwUnableToAssignMorePartitionsDueToBrokerPartitionLimitsException(nP, maxBrokerPartitions)
      }

      if (currentPartitionId > 0 && (currentPartitionId % numBrokers == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % numBrokers
      val replicaBuffer = mutable.ArrayBuffer[Int]()
      val racksWithReplicas = mutable.Set[String]()
      val brokersWithReplicas = mutable.Set[Int]()
      var partitionCapacityZeroedForSomeBroker = false
      var k = 0
      for (j <- -1 until replicationFactor - 1) {
        var broker = -1 // dummy value.
        var rack = "" // dummy value.
        if (j == -1) {
          // in case of first replica (i.e. the leader).
          broker = arrangedBrokerList(firstReplicaIndex)
          rack = brokerRackMap(broker)
        } else {
          var done = false
          while (!done) {
            broker = arrangedBrokerList(replicaIndex(firstReplicaIndex, nextReplicaShift * numRacks, k, numBrokers))
            rack = brokerRackMap(broker)
            // Skip this broker if
            // 1. there is already a broker in the same rack that has assigned a replica AND there is one or more racks
            //    that do not have any replica, or
            // 2. the broker has already assigned a replica AND there is one or more brokers that do not have replica assigned
            if ((!racksWithReplicas.contains(rack) || racksWithReplicas.size == numRacks)
              && (!brokersWithReplicas.contains(broker) || brokersWithReplicas.size == numBrokers)) {
              done = true
            }
            k += 1
          }
        }
        replicaBuffer += broker
        racksWithReplicas += rack
        brokersWithReplicas += broker
        if (reduceBrokerPartitionCapacity(partitionCapacityByBroker, broker)) {
          partitionCapacityZeroedForSomeBroker = true
        }
      }
      ret.put(currentPartitionId, replicaBuffer)
      currentPartitionId += 1

      if (partitionCapacityZeroedForSomeBroker) {
        // We recreate the following structures only when partition capacity dropped to zero for some broker
        // as a result of replica assignment for this partition. This is strictly an optimization
        // to avoid unnecessary copies. This optimization guarantees that the copy cannot happen more times
        // than the total number of brokers.
        brokerRackMap =
          brokerMetadatas
            .filter { case bm => partitionCapacityByBroker.contains(bm.id) }
            .collect { case BrokerMetadata(id, Some(rack)) => id -> rack }
            .toMap
        arrangedBrokerList = getRackAlternatedBrokerList(brokerRackMap)
        numRacks = brokerRackMap.values.toSet.size
        numBrokers = arrangedBrokerList.size
      }
    }
    ret
  }

  /**
   * Returns a map with the partition capacities for the brokers in brokerList, which have
   * greater than zero partition capacity.
   */
  private def getPartitionCapacityByBroker(brokerList: Seq[Int],
                                           partitionsByBroker: Map[Int, Int],
                                           maxBrokerPartitions: Int): collection.mutable.Map[Int, Int] = {
    collection.mutable.Map(
      brokerList
        .map { case broker => broker -> Math.max(0, maxBrokerPartitions - partitionsByBroker.getOrElse(broker, 0)) }
        .filter(_._2 > 0)
      : _*)
  }

  /**
   * Reduces partition capacity of broker by 1. This needs to be done when the broker is assigned a partition.
   * @return true if the broker's partition capacity went to 0 as a result, and therefore the entry for the broker was removed.
   */
  private def reduceBrokerPartitionCapacity(partitionCapacityByBroker: collection.mutable.Map[Int, Int],
                                            broker: Int): Boolean = {
    val partitionCapacityForBroker = partitionCapacityByBroker.get(broker).get
    if (partitionCapacityForBroker == 1) {
      partitionCapacityByBroker.remove(broker) // because we just assigned this broker a replica.
      true
    } else {
      // partition capacity greater than 1.
      partitionCapacityByBroker.put(broker, partitionCapacityForBroker - 1)
      false
    }
  }

  private def throwAllBrokersHaveAlreadyReachedBrokerPartitionLimitsException(maxBrokerPartitions: Int): Unit = {
    throw new WillExceedPartitionLimitsException(
      s"All brokers have already reached / exceeded the limit of $maxBrokerPartitions " +
        s"partition replicas per broker (specified via max.broker.partitions).")
  }

  private def throwUnableToAssignMorePartitionsDueToBrokerPartitionLimitsException(numPartitionsAble: Int,
                                                                                   maxBrokerPartitions: Int) = {
    throw new WillExceedPartitionLimitsException(
      s"Could not assign replicas for more than $numPartitionsAble additional partitions " +
        s"given the limit of $maxBrokerPartitions partition replicas per broker " +
        s"(specified via max.broker.partitions).")
  }

  private def maybeThrowWillExceedMaxPartitionLimitsException(partitionsByBroker: Map[Int, Int],
                                                              numNewPartitions: Int,
                                                              maxPartitions: Int) = {
    val numCurrentPartitions = partitionsByBroker.foldLeft(0)(_ + _._2)
    if (numCurrentPartitions > maxPartitions) {
      throw new WillExceedPartitionLimitsException(
        s"All the brokers combined already host a total of $numCurrentPartitions partition replicas. " +
          s"This exceeds the cluster-wide partition replica limit of $maxPartitions (specified via max.partitions).")
    }
    if (numCurrentPartitions + numNewPartitions > maxPartitions) {
      throw new WillExceedPartitionLimitsException(
        s"All the brokers combined already host a total of $numCurrentPartitions partition replicas. " +
          s"This request requires adding $numNewPartitions more partition replicas, which will result " +
          s"in exceeding the cluster-wide partition replica limit of $maxPartitions (specified via max.partitions).")
    }
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
      (rack, brokers.iterator)
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
