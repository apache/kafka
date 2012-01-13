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
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.utils.{SystemTime, Utils, ZkUtils}

object AdminUtils {
  val rand = new Random

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
  def assginReplicasToBrokers(brokerList: Seq[String], nPartitions: Int, replicationFactor: Int,
          fixedStartIndex: Int = -1)  // for testing only
    : Array[List[String]] = {
    if (nPartitions <= 0)
      throw new AdministrationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdministrationException("replication factor must be larger than 0")
    if (replicationFactor > brokerList.size)
      throw new AdministrationException("replication factor: " + replicationFactor +
              " larger than available brokers: " + brokerList.size)
    val ret = new Array[List[String]](nPartitions)
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)

    var secondReplicaShift = -1
    for (i <- 0 until nPartitions) {
      if (i % brokerList.size == 0)
        secondReplicaShift += 1
      val firstReplicaIndex = (i + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(getWrappedIndex(firstReplicaIndex, secondReplicaShift, j, brokerList.size))
      ret(i) = replicaList.reverse
    }

    ret
  }

  def createReplicaAssignmentPathInZK(topic: String, replicaAssignmentList: Seq[List[String]], zkClient: ZkClient) {
    try {
      val topicVersion = SystemTime.milliseconds
      ZkUtils.createPersistentPath(zkClient, ZkUtils.BrokerTopicsPath + "/" + topic, topicVersion.toString)
      for (i <- 0 until replicaAssignmentList.size) {
        val zkPath = ZkUtils.getTopicPartReplicasPath(topic, i.toString)
        ZkUtils.updatePersistentPath(zkClient, zkPath, Utils.seqToCSV(replicaAssignmentList(i)))
      }
    }
    catch {
      case e: ZkNodeExistsException =>
        throw new AdministrationException("topic " + topic + " already exists, with version "
          + ZkUtils.getTopicVersion (zkClient, topic))
      case e2 =>
        throw new AdministrationException(e2.toString)      
    }
  }

  def getTopicMetaDataFromZK(topic: String, zkClient: ZkClient): Option[Seq[PartitionMetaData]] = {
    if (!ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic)))
      return None
    
    val topicPartitionsPath = ZkUtils.getTopicPartsPath(topic)
    val partitions = ZkUtils.getChildrenParentMayNotExist(zkClient, topicPartitionsPath).sortWith((s,t) => s.toInt < t.toInt)
    val ret = new Array[PartitionMetaData](partitions.size)
    for (i <-0 until ret.size) {
      val replicas = ZkUtils.readData(zkClient, ZkUtils.getTopicPartReplicasPath(topic, partitions(i)))
      val inSync = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.getTopicPartInSyncPath(topic, partitions(i)))
      val leader = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.getTopicPartLeaderPath(topic, partitions(i)))
      ret(i) = new PartitionMetaData(partitions(i),
                                     Utils.getCSVList(replicas).toList,
                                     Utils.getCSVList(inSync).toList,
                                     if (leader == null) None else Some(leader)
                                    )
    }
    Some(ret)
  }

  private def getWrappedIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }
}

class AdministrationException(val errorMessage: String) extends RuntimeException(errorMessage) {
  def this() = this(null)
}
