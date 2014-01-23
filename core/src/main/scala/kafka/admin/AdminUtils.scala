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
import kafka.utils.{Logging, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import scala.collection._
import scala.collection.mutable
import kafka.common._

object AdminUtils extends Logging {
  val rand = new Random
  val AdminEpoch = -1

  /**
   * There are 2 goals of replica assignment:
   * 1. Spread the replicas evenly among brokers.
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
   *
   * To achieve this goal, we:
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
   */
  def assignReplicasToBrokers(zkClient: ZkClient, brokerList: Seq[Int], nPartitions: Int, replicationFactor: Int,
                              fixedStartIndex: Int = -1, startPartitionId: Int = -1, maxReplicaPerRack: Int = -1)
  : Map[Int, Seq[Int]] = {
    if (nPartitions <= 0)
      throw new AdministrationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdministrationException("replication factor must be larger than 0")
    if (replicationFactor > brokerList.size)
      throw new AdministrationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + brokerList.size)
    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    var currentPartitionId = if (startPartitionId >= 0) startPartitionId else 0

    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    if (maxReplicaPerRack <= 0) {
      for (i <- 0 until nPartitions) {
        if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
          nextReplicaShift += 1
        val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
        var replicaList = List(brokerList(firstReplicaIndex))
        for (j <- 0 until replicationFactor - 1)
          replicaList ::= brokerList(getWrappedIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
        ret.put(currentPartitionId, replicaList.reverse)
        currentPartitionId = currentPartitionId + 1
      }
    } else {
      val brokerToRackMap: Map[Int, Int] = brokerList.map(brokerId => (brokerId -> (ZkUtils.getBrokerInfo(zkClient, brokerId) match {
        case Some(broker) => broker.rack
        case None => throw new AdministrationException("broker " + brokerId + " must have rack id")
      }) )).toMap
      for (i <- 0 until nPartitions) {
        if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
          nextReplicaShift += 1
        val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
        var replicaList = List(brokerList(firstReplicaIndex))
        var rackReplicaCount: mutable.Map[Int, Int] = mutable.Map(brokerToRackMap(brokerList(firstReplicaIndex)) -> 1)
        var k = 0
        for (j <- 0 until replicationFactor - 1) {
          var done = false;
          while (!done && k < brokerList.size) {
            val broker = brokerList(getWrappedIndex(firstReplicaIndex, nextReplicaShift, k, brokerList.size))
            val rack = brokerToRackMap(broker)
            if (!(rackReplicaCount contains rack)) {
              replicaList ::= broker
              rackReplicaCount += (rack -> 1)
              done = true;
            } else if (rackReplicaCount(rack) < maxReplicaPerRack) {
              rackReplicaCount(rack) = rackReplicaCount(rack) + 1
              replicaList ::= broker
              done = true;
            }
            k = k + 1
          }
          if (!done) {
            throw new AdministrationException("not enough brokers available in unique racks to meet maxReplicaPerRack limit of " + maxReplicaPerRack)
          }
        }
        ret.put(currentPartitionId, replicaList.reverse)
        currentPartitionId = currentPartitionId + 1
      }
    }
    ret.toMap
  }

  def createOrUpdateTopicPartitionAssignmentPathInZK(topic: String, replicaAssignment: Map[Int, Seq[Int]], zkClient: ZkClient, update: Boolean = false) {
    try {
      val zkPath = ZkUtils.getTopicPath(topic)
      val jsonPartitionData = ZkUtils.replicaAssignmentZkdata(replicaAssignment.map(e => (e._1.toString -> e._2)))

      if (!update) {
        info("Topic creation " + jsonPartitionData.toString)
        ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionData)
      } else {
        info("Topic update " + jsonPartitionData.toString)
        ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionData)
      }
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case e: ZkNodeExistsException => throw new TopicExistsException("topic %s already exists".format(topic))
      case e2: Throwable => throw new AdministrationException(e2.toString)
    }
  }

  private def getWrappedIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }
}

class AdministrationException(val errorMessage: String) extends RuntimeException(errorMessage) {
  def this() = this(null)
}
