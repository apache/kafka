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

import kafka.common._
import kafka.cluster.Broker

import kafka.log.LogConfig
import kafka.server.ConfigType
import kafka.utils._
import kafka.utils.ZkUtils._

import java.util.Random
import java.util.Properties
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.{ReplicaNotAvailableException, InvalidTopicException, LeaderNotAvailableException}
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.MetadataResponse

import scala.Predef._
import scala.collection._
import scala.collection.JavaConverters._
import scala.collection.mutable
import mutable.ListBuffer
import collection.Map
import collection.Set


import org.I0Itec.zkclient.exception.ZkNodeExistsException

object AdminUtils extends Logging {
  val rand = new Random
  val AdminClientId = "__admin_client"
  val EntityConfigChangeZnodePrefix = "config_change_"

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
  def assignReplicasToBrokers(brokerList: Seq[Int],
                              nPartitions: Int,
                              replicationFactor: Int,
                              fixedStartIndex: Int = -1,
                              startPartitionId: Int = -1)
  : Map[Int, Seq[Int]] = {
    if (nPartitions <= 0)
      throw new AdminOperationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdminOperationException("replication factor must be larger than 0")
    if (replicationFactor > brokerList.size)
      throw new AdminOperationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + brokerList.size)
    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    var currentPartitionId = if (startPartitionId >= 0) startPartitionId else 0

    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    for (i <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
      ret.put(currentPartitionId, replicaList.reverse)
      currentPartitionId = currentPartitionId + 1
    }
    ret.toMap
  }


 /**
  * Add partitions to existing topic with optional replica assignment
  *
  * @param zkUtils Zookeeper utilities
  * @param topic Topic for adding partitions to
  * @param numPartitions Number of partitions to be set
  * @param replicaAssignmentStr Manual replica assignment
  * @param checkBrokerAvailable Ignore checking if assigned replica broker is available. Only used for testing
  */
  def addPartitions(zkUtils: ZkUtils,
                    topic: String,
                    numPartitions: Int = 1,
                    replicaAssignmentStr: String = "",
                    checkBrokerAvailable: Boolean = true) {
    val existingPartitionsReplicaList = zkUtils.getReplicaAssignmentForTopics(List(topic))
    if (existingPartitionsReplicaList.size == 0)
      throw new AdminOperationException("The topic %s does not exist".format(topic))

    val existingReplicaListForPartitionZero = existingPartitionsReplicaList.find(p => p._1.partition == 0) match {
      case None => throw new AdminOperationException("the topic does not have partition with id 0, it should never happen")
      case Some(headPartitionReplica) => headPartitionReplica._2
    }
    val partitionsToAdd = numPartitions - existingPartitionsReplicaList.size
    if (partitionsToAdd <= 0)
      throw new AdminOperationException("The number of partitions for a topic can only be increased")

    // create the new partition replication list
    val brokerList = zkUtils.getSortedBrokerList()
    val newPartitionReplicaList = if (replicaAssignmentStr == null || replicaAssignmentStr == "") {
      var startIndex = brokerList.indexWhere(_ >= existingReplicaListForPartitionZero.head)
      if(startIndex < 0) {
        startIndex = 0
      }
      AdminUtils.assignReplicasToBrokers(brokerList, partitionsToAdd, existingReplicaListForPartitionZero.size, startIndex, existingPartitionsReplicaList.size)
    }
    else
      getManualReplicaAssignment(replicaAssignmentStr, brokerList.toSet, existingPartitionsReplicaList.size, checkBrokerAvailable)

    // check if manual assignment has the right replication factor
    val unmatchedRepFactorList = newPartitionReplicaList.values.filter(p => (p.size != existingReplicaListForPartitionZero.size))
    if (unmatchedRepFactorList.size != 0)
      throw new AdminOperationException("The replication factor in manual replication assignment " +
        " is not equal to the existing replication factor for the topic " + existingReplicaListForPartitionZero.size)

    info("Add partition list for %s is %s".format(topic, newPartitionReplicaList))
    val partitionReplicaList = existingPartitionsReplicaList.map(p => p._1.partition -> p._2)
    // add the new list
    partitionReplicaList ++= newPartitionReplicaList
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, partitionReplicaList, update = true)
  }

  def getManualReplicaAssignment(replicaAssignmentList: String, availableBrokerList: Set[Int], startPartitionId: Int, checkBrokerAvailable: Boolean = true): Map[Int, List[Int]] = {
    var partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    var partitionId = startPartitionId
    partitionList = partitionList.takeRight(partitionList.size - partitionId)
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      if (brokerList.size <= 0)
        throw new AdminOperationException("replication factor must be larger than 0")
      if (brokerList.size != brokerList.toSet.size)
        throw new AdminOperationException("duplicate brokers in replica assignment: " + brokerList)
      if (checkBrokerAvailable && !brokerList.toSet.subsetOf(availableBrokerList))
        throw new AdminOperationException("some specified brokers not available. specified brokers: " + brokerList.toString +
          "available broker:" + availableBrokerList.toString)
      ret.put(partitionId, brokerList.toList)
      if (ret(partitionId).size != ret(startPartitionId).size)
        throw new AdminOperationException("partition " + i + " has different replication factor: " + brokerList)
      partitionId = partitionId + 1
    }
    ret.toMap
  }

  def deleteTopic(zkUtils: ZkUtils, topic: String) {
    try {
      zkUtils.createPersistentPath(getDeleteTopicPath(topic))
    } catch {
      case e1: ZkNodeExistsException => throw new TopicAlreadyMarkedForDeletionException(
        "topic %s is already marked for deletion".format(topic))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }

  def isConsumerGroupActive(zkUtils: ZkUtils, group: String) = {
    zkUtils.getConsumersInGroup(group).nonEmpty
  }

  /**
   * Delete the whole directory of the given consumer group if the group is inactive.
   *
   * @param zkUtils Zookeeper utilities
   * @param group Consumer group
   * @return whether or not we deleted the consumer group information
   */
  def deleteConsumerGroupInZK(zkUtils: ZkUtils, group: String) = {
    if (!isConsumerGroupActive(zkUtils, group)) {
      val dir = new ZKGroupDirs(group)
      zkUtils.deletePathRecursive(dir.consumerGroupDir)
      true
    }
    else false
  }

  /**
   * Delete the given consumer group's information for the given topic in Zookeeper if the group is inactive.
   * If the consumer group consumes no other topics, delete the whole consumer group directory.
   *
   * @param zkUtils Zookeeper utilities
   * @param group Consumer group
   * @param topic Topic of the consumer group information we wish to delete
   * @return whether or not we deleted the consumer group information for the given topic
   */
  def deleteConsumerGroupInfoForTopicInZK(zkUtils: ZkUtils, group: String, topic: String) = {
    val topics = zkUtils.getTopicsByConsumerGroup(group)
    if (topics == Seq(topic)) {
      deleteConsumerGroupInZK(zkUtils, group)
    }
    else if (!isConsumerGroupActive(zkUtils, group)) {
      val dir = new ZKGroupTopicDirs(group, topic)
      zkUtils.deletePathRecursive(dir.consumerOwnerDir)
      zkUtils.deletePathRecursive(dir.consumerOffsetDir)
      true
    }
    else false
  }

  /**
   * Delete every inactive consumer group's information about the given topic in Zookeeper.
   *
   * @param zkUtils Zookeeper utilities
   * @param topic Topic of the consumer group information we wish to delete
   */
  def deleteAllConsumerGroupInfoForTopicInZK(zkUtils: ZkUtils, topic: String) {
    val groups = zkUtils.getAllConsumerGroupsForTopic(topic)
    groups.foreach(group => deleteConsumerGroupInfoForTopicInZK(zkUtils, group, topic))
  }

  def topicExists(zkUtils: ZkUtils, topic: String): Boolean =
    zkUtils.zkClient.exists(getTopicPath(topic))

  def createTopic(zkUtils: ZkUtils,
                  topic: String,
                  partitions: Int,
                  replicationFactor: Int,
                  topicConfig: Properties = new Properties) {
    val brokerList = zkUtils.getSortedBrokerList()
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, partitions, replicationFactor)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, replicaAssignment, topicConfig)
  }

  def createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils: ZkUtils,
                                                     topic: String,
                                                     partitionReplicaAssignment: Map[Int, Seq[Int]],
                                                     config: Properties = new Properties,
                                                     update: Boolean = false) {
    // validate arguments
    Topic.validate(topic)
    require(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, "All partitions should have the same number of replicas.")

    val topicPath = getTopicPath(topic)

    if (!update) {
      if (zkUtils.zkClient.exists(topicPath))
        throw new TopicExistsException("Topic \"%s\" already exists.".format(topic))
      else if (Topic.hasCollisionChars(topic)) {
        val allTopics = zkUtils.getAllTopics()
        val collidingTopics = allTopics.filter(t => Topic.hasCollision(topic, t))
        if (collidingTopics.nonEmpty) {
          throw new InvalidTopicException("Topic \"%s\" collides with existing topics: %s".format(topic, collidingTopics.mkString(", ")))
        }
      }
    }

    partitionReplicaAssignment.values.foreach(reps => require(reps.size == reps.toSet.size, "Duplicate replica assignment found: "  + partitionReplicaAssignment))

    // Configs only matter if a topic is being created. Changing configs via AlterTopic is not supported
    if (!update) {
      // write out the config if there is any, this isn't transactional with the partition assignments
      LogConfig.validate(config)
      writeEntityConfig(zkUtils, ConfigType.Topic, topic, config)
    }

    // create the partition assignment
    writeTopicPartitionAssignment(zkUtils, topic, partitionReplicaAssignment, update)
  }

  private def writeTopicPartitionAssignment(zkUtils: ZkUtils, topic: String, replicaAssignment: Map[Int, Seq[Int]], update: Boolean) {
    try {
      val zkPath = getTopicPath(topic)
      val jsonPartitionData = zkUtils.replicaAssignmentZkData(replicaAssignment.map(e => (e._1.toString -> e._2)))

      if (!update) {
        info("Topic creation " + jsonPartitionData.toString)
        zkUtils.createPersistentPath(zkPath, jsonPartitionData)
      } else {
        info("Topic update " + jsonPartitionData.toString)
        zkUtils.updatePersistentPath(zkPath, jsonPartitionData)
      }
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case e: ZkNodeExistsException => throw new TopicExistsException("topic %s already exists".format(topic))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }

  /**
   * Update the config for a client and create a change notification so the change will propagate to other brokers
   * @param zkUtils Zookeeper utilities used to write the config to ZK
   * @param clientId: The clientId for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeClientIdConfig(zkUtils: ZkUtils, clientId: String, configs: Properties) {
    changeEntityConfig(zkUtils, ConfigType.Client, clientId, configs)
  }

  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   * @param zkUtils Zookeeper utilities used to write the config to ZK
   * @param topic: The topic for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeTopicConfig(zkUtils: ZkUtils, topic: String, configs: Properties) {
    if(!topicExists(zkUtils, topic))
      throw new AdminOperationException("Topic \"%s\" does not exist.".format(topic))
    // remove the topic overrides
    LogConfig.validate(configs)
    changeEntityConfig(zkUtils, ConfigType.Topic, topic, configs)
  }

  private def changeEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String, configs: Properties) {
    // write the new config--may not exist if there were previously no overrides
    writeEntityConfig(zkUtils, entityType, entityName, configs)

    // create the change notification
    val seqNode = ZkUtils.EntityConfigChangesPath + "/" + EntityConfigChangeZnodePrefix
    val content = Json.encode(getConfigChangeZnodeData(entityType, entityName))
    zkUtils.zkClient.createPersistentSequential(seqNode, content)
  }

  def getConfigChangeZnodeData(entityType: String, entityName: String) : Map[String, Any] = {
    Map("version" -> 1, "entity_type" -> entityType, "entity_name" -> entityName)
  }

  /**
   * Write out the topic config to zk, if there is any
   */
  private def writeEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String, config: Properties) {
    val configMap: mutable.Map[String, String] = {
      import JavaConversions._
      config
    }
    val map = Map("version" -> 1, "config" -> configMap)
    zkUtils.updatePersistentPath(getEntityConfigPath(entityType, entityName), Json.encode(map))
  }

  /**
   * Read the entity (topic or client) config (if any) from zk
   */
  def fetchEntityConfig(zkUtils: ZkUtils, entityType: String, entity: String): Properties = {
    val str: String = zkUtils.zkClient.readData(getEntityConfigPath(entityType, entity), true)
    val props = new Properties()
    if(str != null) {
      Json.parseFull(str) match {
        case None => // there are no config overrides
        case Some(mapAnon: Map[_, _]) =>
          val map = mapAnon collect { case (k: String, v: Any) => k -> v }
          require(map("version") == 1)
          map.get("config") match {
            case Some(config: Map[_, _]) =>
              for(configTup <- config)
                configTup match {
                  case (k: String, v: String) =>
                    props.setProperty(k, v)
                  case _ => throw new IllegalArgumentException("Invalid " + entityType + " config: " + str)
                }
            case _ => throw new IllegalArgumentException("Invalid " + entityType + " config: " + str)
          }

        case o => throw new IllegalArgumentException("Unexpected value in config:(%s), entity_type: (%s), entity: (%s)"
                                                             .format(str, entityType, entity))
      }
    }
    props
  }

  def fetchAllTopicConfigs(zkUtils: ZkUtils): Map[String, Properties] =
    zkUtils.getAllTopics().map(topic => (topic, fetchEntityConfig(zkUtils, ConfigType.Topic, topic))).toMap

  def fetchAllEntityConfigs(zkUtils: ZkUtils, entityType: String): Map[String, Properties] =
    zkUtils.getAllEntitiesWithConfig(entityType).map(entity => (entity, fetchEntityConfig(zkUtils, entityType, entity))).toMap

  def fetchTopicMetadataFromZk(topic: String, zkUtils: ZkUtils): MetadataResponse.TopicMetadata =
    fetchTopicMetadataFromZk(topic, zkUtils, new mutable.HashMap[Int, Broker])

  def fetchTopicMetadataFromZk(topics: Set[String], zkUtils: ZkUtils): Set[MetadataResponse.TopicMetadata] = {
    val cachedBrokerInfo = new mutable.HashMap[Int, Broker]()
    topics.map(topic => fetchTopicMetadataFromZk(topic, zkUtils, cachedBrokerInfo))
  }

  private def fetchTopicMetadataFromZk(topic: String,
                                       zkUtils: ZkUtils,
                                       cachedBrokerInfo: mutable.HashMap[Int, Broker],
                                       protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): MetadataResponse.TopicMetadata = {
    if(zkUtils.pathExists(getTopicPath(topic))) {
      val topicPartitionAssignment = zkUtils.getPartitionAssignmentForTopics(List(topic)).get(topic).get
      val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
      val partitionMetadata = sortedPartitions.map { partitionMap =>
        val partition = partitionMap._1
        val replicas = partitionMap._2
        val inSyncReplicas = zkUtils.getInSyncReplicasForPartition(topic, partition)
        val leader = zkUtils.getLeaderForPartition(topic, partition)
        debug("replicas = " + replicas + ", in sync replicas = " + inSyncReplicas + ", leader = " + leader)

        var leaderInfo: Node = Node.noNode()
        var replicaInfo: Seq[Node] = Nil
        var isrInfo: Seq[Node] = Nil
        try {
          leaderInfo = leader match {
            case Some(l) =>
              try {
                getBrokerInfoFromCache(zkUtils, cachedBrokerInfo, List(l)).head.getNode(protocol)
              } catch {
                case e: Throwable => throw new LeaderNotAvailableException("Leader not available for partition [%s,%d]".format(topic, partition), e)
              }
            case None => throw new LeaderNotAvailableException("No leader exists for partition " + partition)
          }
          try {
            replicaInfo = getBrokerInfoFromCache(zkUtils, cachedBrokerInfo, replicas).map(_.getNode(protocol)).toList
            isrInfo = getBrokerInfoFromCache(zkUtils, cachedBrokerInfo, inSyncReplicas).map(_.getNode(protocol)).toList
          } catch {
            case e: Throwable => throw new ReplicaNotAvailableException(e)
          }
          if(replicaInfo.size < replicas.size)
            throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
              replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
          if(isrInfo.size < inSyncReplicas.size)
            throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
              inSyncReplicas.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
          new MetadataResponse.PartitionMetadata(Errors.NONE, partition, leaderInfo, replicaInfo.asJava, isrInfo.asJava)
        } catch {
          case e: Throwable =>
            debug("Error while fetching metadata for partition [%s,%d]".format(topic, partition), e)
            new MetadataResponse.PartitionMetadata(Errors.forException(e), partition, leaderInfo, replicaInfo.asJava, isrInfo.asJava)
        }
      }
      new MetadataResponse.TopicMetadata(Errors.NONE, topic, partitionMetadata.toList.asJava)
    } else {
      // topic doesn't exist, send appropriate error code
      new MetadataResponse.TopicMetadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, java.util.Collections.emptyList())
    }
  }

  private def getBrokerInfoFromCache(zkUtils: ZkUtils,
                                     cachedBrokerInfo: scala.collection.mutable.Map[Int, Broker],
                                     brokerIds: Seq[Int]): Seq[Broker] = {
    var failedBrokerIds: ListBuffer[Int] = new ListBuffer()
    val brokerMetadata = brokerIds.map { id =>
      val optionalBrokerInfo = cachedBrokerInfo.get(id)
      optionalBrokerInfo match {
        case Some(brokerInfo) => Some(brokerInfo) // return broker info from the cache
        case None => // fetch it from zookeeper
          zkUtils.getBrokerInfo(id) match {
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
