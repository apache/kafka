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
import kafka.api.{TopicMetadata, PartitionMetadata}
import kafka.cluster.Broker
import kafka.utils.{Logging, Utils, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import scala.collection._
import scala.collection.mutable
import kafka.common._
import scala.Some

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
  def assignReplicasToBrokers(brokerList: Seq[String], nPartitions: Int, replicationFactor: Int,
                              fixedStartIndex: Int = -1)  // for testing only
  : Map[Int, Seq[String]] = {
    if (nPartitions <= 0)
      throw new AdministrationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdministrationException("replication factor must be larger than 0")
    if (replicationFactor > brokerList.size)
      throw new AdministrationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + brokerList.size)
    val ret = new mutable.HashMap[Int, List[String]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)

    var secondReplicaShift = -1
    for (i <- 0 until nPartitions) {
      if (i % brokerList.size == 0)
        secondReplicaShift += 1
      val firstReplicaIndex = (i + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(getWrappedIndex(firstReplicaIndex, secondReplicaShift, j, brokerList.size))
      ret.put(i, replicaList.reverse)
    }
    ret.toMap
  }

  def createTopicPartitionAssignmentPathInZK(topic: String, replicaAssignment: Map[Int, Seq[String]], zkClient: ZkClient) {
    try {
      val zkPath = ZkUtils.getTopicPath(topic)
      val jsonPartitionMap = Utils.mapToJson(replicaAssignment.map(e => (e._1.toString -> e._2)))
      ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionMap)
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
    } catch {
      case e: ZkNodeExistsException => throw new TopicExistsException("topic %s already exists".format(topic))
      case e2 => throw new AdministrationException(e2.toString)
    }
  }
  
  def fetchTopicMetadataFromZk(topic: String, zkClient: ZkClient): TopicMetadata = 
    fetchTopicMetadataFromZk(topic, zkClient, new mutable.HashMap[Int, Broker])

  def fetchTopicMetadataFromZk(topics: Set[String], zkClient: ZkClient): Set[TopicMetadata] = {
    val cachedBrokerInfo = new mutable.HashMap[Int, Broker]()
    topics.map(topic => fetchTopicMetadataFromZk(topic, zkClient, cachedBrokerInfo))
  }
  
  private def fetchTopicMetadataFromZk(topic: String, zkClient: ZkClient, cachedBrokerInfo: mutable.HashMap[Int, Broker]): TopicMetadata = {
    if(ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic))) {
      val topicPartitionAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic)).get(topic).get
      val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)

      val partitionMetadata = sortedPartitions.map { partitionMap =>
      val partition = partitionMap._1
      val replicas = partitionMap._2
      val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partition)
      val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
      debug("replicas = " + replicas + ", in sync replicas = " + inSyncReplicas + ", leader = " + leader)

      var leaderInfo: Option[Broker] = None
      var replicaInfo: Seq[Broker] = Nil
      var isrInfo: Seq[Broker] = Nil
      try {
        try {
          leaderInfo = leader match {
            case Some(l) => Some(getBrokerInfoFromCache(zkClient, cachedBrokerInfo, List(l)).head)
            case None => throw new LeaderNotAvailableException("No leader exists for partition " + partition)
          }
        } catch {
          case e => throw new LeaderNotAvailableException("Leader not available for topic %s partition %d".format(topic, partition))
        }

        try {
          replicaInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, replicas.map(id => id.toInt))
          isrInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, inSyncReplicas)
        } catch {
          case e => throw new ReplicaNotAvailableException(e)
        }

          new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError)
        } catch {
          case e: ReplicaNotAvailableException =>
            new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo,
                                  ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
          case le: LeaderNotAvailableException =>
            new PartitionMetadata(partition, None, replicaInfo, isrInfo,
                                  ErrorMapping.codeFor(le.getClass.asInstanceOf[Class[Throwable]]))
        }
      }
      new TopicMetadata(topic, partitionMetadata)
    } else {
      // topic doesn't exist, send appropriate error code
      new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)
    }
  }

  private def getBrokerInfoFromCache(zkClient: ZkClient,
                                     cachedBrokerInfo: scala.collection.mutable.Map[Int, Broker],
                                     brokerIds: Seq[Int]): Seq[Broker] = {
    brokerIds.map { id =>
      val optionalBrokerInfo = cachedBrokerInfo.get(id)
      optionalBrokerInfo match {
        case Some(brokerInfo) => brokerInfo // return broker info from the cache
        case None => // fetch it from zookeeper
          ZkUtils.getBrokerInfo(zkClient, id) match {
            case Some(brokerInfo) =>
              cachedBrokerInfo += (id -> brokerInfo)
              brokerInfo
            case None => throw new BrokerNotAvailableException("Failed to fetch broker info for broker " + id)
          }
      }
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
