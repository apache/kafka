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
import java.util.Properties
import kafka.api.{TopicMetadata, PartitionMetadata}
import kafka.cluster.Broker
import kafka.utils.{Logging, ZkUtils}
import kafka.log.LogConfig
import kafka.server.TopicConfigManager
import kafka.utils.{Logging, Utils, ZkUtils, Json}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import scala.collection._
import mutable.ListBuffer
import scala.collection.mutable
import kafka.common._
import scala.Some

object AdminUtils extends Logging {
  val rand = new Random
  val TopicConfigChangeZnodePrefix = "config_change_"

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
  def assignReplicasToBrokers(brokers: Seq[Int], 
                              partitions: Int, 
                              replicationFactor: Int,
                              fixedStartIndex: Int = -1)  // for testing only
  : Map[Int, Seq[Int]] = {
    if (partitions <= 0)
      throw new AdminOperationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdminOperationException("replication factor must be larger than 0")
    if (replicationFactor > brokers.size)
      throw new AdminOperationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + brokers.size)
    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokers.size)

    var secondReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokers.size)
    for (i <- 0 until partitions) {
      if (i > 0 && (i % brokers.size == 0))
        secondReplicaShift += 1
      val firstReplicaIndex = (i + startIndex) % brokers.size
      var replicaList = List(brokers(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokers(replicaIndex(firstReplicaIndex, secondReplicaShift, j, brokers.size))
      ret.put(i, replicaList.reverse)
    }
    ret.toMap
  }
  
  def deleteTopic(zkClient: ZkClient, topic: String) {
    zkClient.deleteRecursive(ZkUtils.getTopicPath(topic))
    zkClient.deleteRecursive(ZkUtils.getTopicConfigPath(topic))
  }
  
  def topicExists(zkClient: ZkClient, topic: String): Boolean = 
    zkClient.exists(ZkUtils.getTopicPath(topic))
    
  def createTopic(zkClient: ZkClient,
                  topic: String,
                  partitions: Int, 
                  replicationFactor: Int, 
                  topicConfig: Properties = new Properties) {
    val brokerList = ZkUtils.getSortedBrokerList(zkClient)
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, partitions, replicationFactor)
    AdminUtils.createTopicWithAssignment(zkClient, topic, replicaAssignment, topicConfig)
  }
                  
  def createTopicWithAssignment(zkClient: ZkClient, 
                                topic: String, 
                                partitionReplicaAssignment: Map[Int, Seq[Int]], 
                                config: Properties = new Properties) {
    // validate arguments
    Topic.validate(topic)
    LogConfig.validate(config)
    require(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, "All partitions should have the same number of replicas.")

    val topicPath = ZkUtils.getTopicPath(topic)
    if(zkClient.exists(topicPath))
      throw new TopicExistsException("Topic \"%s\" already exists.".format(topic))
    partitionReplicaAssignment.values.foreach(reps => require(reps.size == reps.toSet.size, "Duplicate replica assignment found: "  + partitionReplicaAssignment))
    
    // write out the config if there is any, this isn't transactional with the partition assignments
    writeTopicConfig(zkClient, topic, config)
    
    // create the partition assignment
    writeTopicPartitionAssignment(zkClient, topic, partitionReplicaAssignment)
  }
  
  private def writeTopicPartitionAssignment(zkClient: ZkClient, topic: String, replicaAssignment: Map[Int, Seq[Int]]) {
    try {
      val zkPath = ZkUtils.getTopicPath(topic)
      val jsonPartitionData = ZkUtils.replicaAssignmentZkdata(replicaAssignment.map(e => (e._1.toString -> e._2)))
      ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionData)
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case e: ZkNodeExistsException => throw new TopicExistsException("topic %s already exists".format(topic))
      case e2 => throw new AdminOperationException(e2.toString)
    }
  }
  
  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   */
  def changeTopicConfig(zkClient: ZkClient, topic: String, config: Properties) {
    LogConfig.validate(config)
    if(!topicExists(zkClient, topic))
      throw new AdminOperationException("Topic \"%s\" does not exist.".format(topic))
    
    // write the new config--may not exist if there were previously no overrides
    writeTopicConfig(zkClient, topic, config)
    
    // create the change notification
    zkClient.createPersistentSequential(ZkUtils.TopicConfigChangesPath + "/" + TopicConfigChangeZnodePrefix, Json.encode(topic))
  }
  
  /**
   * Write out the topic config to zk, if there is any
   */
  private def writeTopicConfig(zkClient: ZkClient, topic: String, config: Properties) {
    if(config.size > 0) {
      val map = Map("version" -> 1, "config" -> JavaConversions.asMap(config))
      ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicConfigPath(topic), Json.encode(map))
    }
  }
  
  /**
   * Read the topic config (if any) from zk
   */
  def fetchTopicConfig(zkClient: ZkClient, topic: String): Properties = {
    val str: String = zkClient.readData(ZkUtils.getTopicConfigPath(topic), true)
    val props = new Properties()
    if(str != null) {
      Json.parseFull(str) match {
        case None => // there are no config overrides
        case Some(map: Map[String, _]) => 
          require(map("version") == 1)
          map.get("config") match {
            case Some(config: Map[String, String]) =>
              for((k,v) <- config)
                props.setProperty(k, v)
            case _ => throw new IllegalArgumentException("Invalid topic config: " + str)
          }

        case o => throw new IllegalArgumentException("Unexpected value in config: "  + str)
      }
    }
    props
  }

  def fetchAllTopicConfigs(zkClient: ZkClient): Map[String, Properties] =
    ZkUtils.getAllTopics(zkClient).map(topic => (topic, fetchTopicConfig(zkClient, topic))).toMap

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
          leaderInfo = leader match {
            case Some(l) =>
              try {
                Some(getBrokerInfoFromCache(zkClient, cachedBrokerInfo, List(l)).head)
              } catch {
                case e => throw new LeaderNotAvailableException("Leader not available for topic %s partition %d".format(topic, partition), e)
              }
            case None => throw new LeaderNotAvailableException("No leader exists for partition " + partition)
          }
          try {
            replicaInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, replicas.map(id => id.toInt))
            isrInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, inSyncReplicas)
          } catch {
            case e => throw new ReplicaNotAvailableException(e)
          }
          if(replicaInfo.size < replicas.size)
            throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
              replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
          if(isrInfo.size < inSyncReplicas.size)
            throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
              inSyncReplicas.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
          new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError)
        } catch {
          case e =>
            error("Error while fetching metadata for partition [%s,%d]".format(topic, partition), e)
            new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo,
              ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
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
    var failedBrokerIds: ListBuffer[Int] = new ListBuffer()
    val brokerMetadata = brokerIds.map { id =>
      val optionalBrokerInfo = cachedBrokerInfo.get(id)
      optionalBrokerInfo match {
        case Some(brokerInfo) => Some(brokerInfo) // return broker info from the cache
        case None => // fetch it from zookeeper
          ZkUtils.getBrokerInfo(zkClient, id) match {
            case Some(brokerInfo) =>
              cachedBrokerInfo += (id -> brokerInfo)
              Some(brokerInfo)
            case None =>
              failedBrokerIds += id
              None
          }
      }
    }
    brokerMetadata.filter(_.isDefined).map(_.get)
  }

  private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }
}
