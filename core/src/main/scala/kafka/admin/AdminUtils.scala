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

import kafka.log.LogConfig
import kafka.server.{ConfigEntityName, ConfigType, DynamicConfig}
import kafka.utils._
import kafka.utils.ZkUtils._
import java.util.Random
import java.util.Properties

import kafka.common.TopicAlreadyMarkedForDeletionException
import org.apache.kafka.common.errors._

import collection.{Map, Set, mutable, _}
import scala.collection.JavaConverters._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.kafka.common.internals.Topic

@deprecated("This class is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
trait AdminUtilities {
  def changeTopicConfig(zkUtils: ZkUtils, topic: String, configs: Properties)
  def changeClientIdConfig(zkUtils: ZkUtils, clientId: String, configs: Properties)
  def changeUserOrUserClientIdConfig(zkUtils: ZkUtils, sanitizedEntityName: String, configs: Properties)
  def changeBrokerConfig(zkUtils: ZkUtils, brokerIds: Seq[Int], configs: Properties)

  def changeConfigs(zkUtils: ZkUtils, entityType: String, entityName: String, configs: Properties): Unit = {

    def parseBroker(broker: String): Int = {
      try broker.toInt
      catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"Error parsing broker $broker. The broker's Entity Name must be a single integer value")
      }
    }

    entityType match {
      case ConfigType.Topic => changeTopicConfig(zkUtils, entityName, configs)
      case ConfigType.Client => changeClientIdConfig(zkUtils, entityName, configs)
      case ConfigType.User => changeUserOrUserClientIdConfig(zkUtils, entityName, configs)
      case ConfigType.Broker => changeBrokerConfig(zkUtils, Seq(parseBroker(entityName)), configs)
      case _ => throw new IllegalArgumentException(s"$entityType is not a known entityType. Should be one of ${ConfigType.Topic}, ${ConfigType.Client}, ${ConfigType.Broker}")
    }
  }

  def fetchEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String): Properties
}

object AdminUtils extends Logging with AdminUtilities {
  val rand = new Random
  val AdminClientId = "__admin_client"
  val EntityConfigChangeZnodePrefix = "config_change_"

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
 /**
  * Add partitions to existing topic with optional replica assignment
  *
  * @param zkUtils Zookeeper utilities
  * @param topic Topic for adding partitions to
  * @param existingAssignment A map from partition id to its assigned replicas
  * @param allBrokers All brokers in the cluster
  * @param numPartitions Number of partitions to be set
  * @param replicaAssignment Manual replica assignment, or none
  * @param validateOnly If true, validate the parameters without actually adding the partitions
  * @return the updated replica assignment
  */
  @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def addPartitions(zkUtils: ZkUtils,
                    topic: String,
                    existingAssignment: Map[Int, Seq[Int]],
                    allBrokers: Seq[BrokerMetadata],
                    numPartitions: Int = 1,
                    replicaAssignment: Option[Map[Int, Seq[Int]]] = None,
                    validateOnly: Boolean = false): Map[Int, Seq[Int]] = {
    val existingAssignmentPartition0 = existingAssignment.getOrElse(0,
      throw new AdminOperationException(
        s"Unexpected existing replica assignment for topic '$topic', partition id 0 is missing. " +
          s"Assignment: $existingAssignment"))

    val partitionsToAdd = numPartitions - existingAssignment.size
    if (partitionsToAdd <= 0)
      throw new InvalidPartitionsException(
        s"The number of partitions for a topic can only be increased. " +
          s"Topic $topic currently has ${existingAssignment.size} partitions, " +
          s"$numPartitions would not be an increase.")

    replicaAssignment.foreach { proposedReplicaAssignment =>
      validateReplicaAssignment(proposedReplicaAssignment, existingAssignmentPartition0,
        allBrokers.map(_.id).toSet)
    }

    val proposedAssignmentForNewPartitions = replicaAssignment.getOrElse {
      val startIndex = math.max(0, allBrokers.indexWhere(_.id >= existingAssignmentPartition0.head))
      AdminUtils.assignReplicasToBrokers(allBrokers, partitionsToAdd, existingAssignmentPartition0.size,
        startIndex, existingAssignment.size)
    }
    val proposedAssignment = existingAssignment ++ proposedAssignmentForNewPartitions
    if (!validateOnly) {
      info(s"Creating $partitionsToAdd partitions for '$topic' with the following replica assignment: " +
        s"$proposedAssignmentForNewPartitions.")
      // add the combined new list
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, proposedAssignment, update = true)
    }
    proposedAssignment

  }

  /**
    * Parse a replica assignment string of the form:
    * {{{
    * broker_id_for_part1_replica1:broker_id_for_part1_replica2,
    * broker_id_for_part2_replica1:broker_id_for_part2_replica2,
    * ...
    * }}}
    */
  def parseReplicaAssignment(replicaAssignmentsString: String, startPartitionId: Int): Map[Int, Seq[Int]] = {
    val assignmentStrings = replicaAssignmentsString.split(",")
    val assignmentMap = mutable.Map[Int, Seq[Int]]()
    var partitionId = startPartitionId
    for (assignmentString <- assignmentStrings) {
      val brokerIds = assignmentString.split(":").map(_.trim.toInt).toSeq
      assignmentMap.put(partitionId, brokerIds)
      partitionId = partitionId + 1
    }
    assignmentMap
  }

  private def validateReplicaAssignment(replicaAssignment: Map[Int, Seq[Int]],
                                        existingAssignmentPartition0: Seq[Int],
                                        availableBrokerIds: Set[Int]): Unit = {

    replicaAssignment.foreach { case (partitionId, replicas) =>
      if (replicas.isEmpty)
        throw new InvalidReplicaAssignmentException(
          s"Cannot have replication factor of 0 for partition id $partitionId.")
      if (replicas.size != replicas.toSet.size)
        throw new InvalidReplicaAssignmentException(
          s"Duplicate brokers not allowed in replica assignment: " +
            s"${replicas.mkString(", ")} for partition id $partitionId.")
      if (!replicas.toSet.subsetOf(availableBrokerIds))
        throw new BrokerNotAvailableException(
          s"Some brokers specified for partition id $partitionId are not available. " +
            s"Specified brokers: ${replicas.mkString(", ")}, " +
            s"available brokers: ${availableBrokerIds.mkString(", ")}.")
      partitionId -> replicas.size
    }
    val badRepFactors = replicaAssignment.collect {
      case (partition, replicas) if replicas.size != existingAssignmentPartition0.size => partition -> replicas.size
    }
    if (badRepFactors.nonEmpty) {
      val sortedBadRepFactors = badRepFactors.toSeq.sortBy { case (partitionId, _) => partitionId }
      val partitions = sortedBadRepFactors.map { case (partitionId, _) => partitionId }
      val repFactors = sortedBadRepFactors.map { case (_, rf) => rf }
      throw new InvalidReplicaAssignmentException(s"Inconsistent replication factor between partitions, " +
        s"partition 0 has ${existingAssignmentPartition0.size} while partitions [${partitions.mkString(", ")}] have " +
        s"replication factors [${repFactors.mkString(", ")}], respectively.")
    }
  }

  @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def deleteTopic(zkUtils: ZkUtils, topic: String) {
    if (topicExists(zkUtils, topic)) {
      try {
        zkUtils.createPersistentPath(getDeleteTopicPath(topic))
      } catch {
        case _: ZkNodeExistsException => throw new TopicAlreadyMarkedForDeletionException(
          "topic %s is already marked for deletion".format(topic))
        case e2: Throwable => throw new AdminOperationException(e2)
      }
    } else {
      throw new UnknownTopicOrPartitionException(s"Topic `$topic` to delete does not exist")
    }
  }

  def topicExists(zkUtils: ZkUtils, topic: String): Boolean =
    zkUtils.pathExists(getTopicPath(topic))

  @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def getBrokerMetadatas(zkUtils: ZkUtils, rackAwareMode: RackAwareMode = RackAwareMode.Enforced,
                         brokerList: Option[Seq[Int]] = None): Seq[BrokerMetadata] = {
    val allBrokers = zkUtils.getAllBrokersInCluster()
    val brokers = brokerList.map(brokerIds => allBrokers.filter(b => brokerIds.contains(b.id))).getOrElse(allBrokers)
    val brokersWithRack = brokers.filter(_.rack.nonEmpty)
    if (rackAwareMode == RackAwareMode.Enforced && brokersWithRack.nonEmpty && brokersWithRack.size < brokers.size) {
      throw new AdminOperationException("Not all brokers have rack information. Add --disable-rack-aware in command line" +
        " to make replica assignment without rack information.")
    }
    val brokerMetadatas = rackAwareMode match {
      case RackAwareMode.Disabled => brokers.map(broker => BrokerMetadata(broker.id, None))
      case RackAwareMode.Safe if brokersWithRack.size < brokers.size =>
        brokers.map(broker => BrokerMetadata(broker.id, None))
      case _ => brokers.map(broker => BrokerMetadata(broker.id, broker.rack))
    }
    brokerMetadatas.sortBy(_.id)
  }

  @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def createTopic(zkUtils: ZkUtils,
                  topic: String,
                  partitions: Int,
                  replicationFactor: Int,
                  topicConfig: Properties = new Properties,
                  rackAwareMode: RackAwareMode = RackAwareMode.Enforced) {
    val brokerMetadatas = getBrokerMetadatas(zkUtils, rackAwareMode)
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerMetadatas, partitions, replicationFactor)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, replicaAssignment, topicConfig)
  }

  @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def validateCreateOrUpdateTopic(zkUtils: ZkUtils,
                                  topic: String,
                                  partitionReplicaAssignment: Map[Int, Seq[Int]],
                                  config: Properties,
                                  update: Boolean): Unit = {
    // validate arguments
    Topic.validate(topic)

    if (!update) {
      if (topicExists(zkUtils, topic))
        throw new TopicExistsException(s"Topic '$topic' already exists.")
      else if (Topic.hasCollisionChars(topic)) {
        val allTopics = zkUtils.getAllTopics()
        // check again in case the topic was created in the meantime, otherwise the
        // topic could potentially collide with itself
        if (allTopics.contains(topic))
          throw new TopicExistsException(s"Topic '$topic' already exists.")
        val collidingTopics = allTopics.filter(Topic.hasCollision(topic, _))
        if (collidingTopics.nonEmpty) {
          throw new InvalidTopicException(s"Topic '$topic' collides with existing topics: ${collidingTopics.mkString(", ")}")
        }
      }
    }

    if (partitionReplicaAssignment.values.map(_.size).toSet.size != 1)
      throw new InvalidReplicaAssignmentException("All partitions should have the same number of replicas")

    partitionReplicaAssignment.values.foreach(reps =>
      if (reps.size != reps.toSet.size)
        throw new InvalidReplicaAssignmentException("Duplicate replica assignment found: " + partitionReplicaAssignment)
    )


    // Configs only matter if a topic is being created. Changing configs via AlterTopic is not supported
    if (!update)
      LogConfig.validate(config)
  }

  @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils: ZkUtils,
                                                     topic: String,
                                                     partitionReplicaAssignment: Map[Int, Seq[Int]],
                                                     config: Properties = new Properties,
                                                     update: Boolean = false) {
    validateCreateOrUpdateTopic(zkUtils, topic, partitionReplicaAssignment, config, update)

    // Configs only matter if a topic is being created. Changing configs via AlterTopic is not supported
    if (!update) {
      // write out the config if there is any, this isn't transactional with the partition assignments
      writeEntityConfig(zkUtils, getEntityConfigPath(ConfigType.Topic, topic), config)
    }

    // create the partition assignment
    writeTopicPartitionAssignment(zkUtils, topic, partitionReplicaAssignment, update)
  }

  private def writeTopicPartitionAssignment(zkUtils: ZkUtils, topic: String, replicaAssignment: Map[Int, Seq[Int]], update: Boolean) {
    try {
      val zkPath = getTopicPath(topic)
      val jsonPartitionData = zkUtils.replicaAssignmentZkData(replicaAssignment.map(e => e._1.toString -> e._2))

      if (!update) {
        info("Topic creation " + jsonPartitionData.toString)
        zkUtils.createPersistentPath(zkPath, jsonPartitionData)
      } else {
        info("Topic update " + jsonPartitionData.toString)
        zkUtils.updatePersistentPath(zkPath, jsonPartitionData)
      }
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case _: ZkNodeExistsException => throw new TopicExistsException(s"Topic '$topic' already exists.")
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }

  /**
   * Update the config for a client and create a change notification so the change will propagate to other brokers.
   * If clientId is <default>, default clientId config is updated. ClientId configs are used only if <user, clientId>
   * and <user> configs are not specified.
   *
   * @param zkUtils Zookeeper utilities used to write the config to ZK
   * @param sanitizedClientId: The sanitized clientId for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
   @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def changeClientIdConfig(zkUtils: ZkUtils, sanitizedClientId: String, configs: Properties) {
    DynamicConfig.Client.validate(configs)
    changeEntityConfig(zkUtils, ConfigType.Client, sanitizedClientId, configs)
  }

  /**
   * Update the config for a <user> or <user, clientId> and create a change notification so the change will propagate to other brokers.
   * User and/or clientId components of the path may be <default>, indicating that the configuration is the default
   * value to be applied if a more specific override is not configured.
   *
   * @param zkUtils Zookeeper utilities used to write the config to ZK
   * @param sanitizedEntityName: <sanitizedUserPrincipal> or <sanitizedUserPrincipal>/clients/<clientId>
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
   @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def changeUserOrUserClientIdConfig(zkUtils: ZkUtils, sanitizedEntityName: String, configs: Properties) {
    if (sanitizedEntityName == ConfigEntityName.Default || sanitizedEntityName.contains("/clients"))
      DynamicConfig.Client.validate(configs)
    else
      DynamicConfig.User.validate(configs)
    changeEntityConfig(zkUtils, ConfigType.User, sanitizedEntityName, configs)
  }

  def validateTopicConfig(zkUtils: ZkUtils, topic: String, configs: Properties): Unit = {
    Topic.validate(topic)
    if (!topicExists(zkUtils, topic))
      throw new AdminOperationException("Topic \"%s\" does not exist.".format(topic))
    // remove the topic overrides
    LogConfig.validate(configs)
  }

  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   *
   * @param zkUtils Zookeeper utilities used to write the config to ZK
   * @param topic: The topic for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
   @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def changeTopicConfig(zkUtils: ZkUtils, topic: String, configs: Properties) {
    validateTopicConfig(zkUtils, topic, configs)
    changeEntityConfig(zkUtils, ConfigType.Topic, topic, configs)
  }

  /**
    * Override the broker config on some set of brokers. These overrides will be persisted between sessions, and will
    * override any defaults entered in the broker's config files
    *
    * @param zkUtils: Zookeeper utilities used to write the config to ZK
    * @param brokers: The list of brokers to apply config changes to
    * @param configs: The config to change, as properties
    */
   @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def changeBrokerConfig(zkUtils: ZkUtils, brokers: Seq[Int], configs: Properties): Unit = {
    DynamicConfig.Broker.validate(configs)
    brokers.foreach { broker =>
      changeEntityConfig(zkUtils, ConfigType.Broker, broker.toString, configs)
    }
  }

  private def changeEntityConfig(zkUtils: ZkUtils, rootEntityType: String, fullSanitizedEntityName: String, configs: Properties) {
    val sanitizedEntityPath = rootEntityType + '/' + fullSanitizedEntityName
    val entityConfigPath = getEntityConfigPath(rootEntityType, fullSanitizedEntityName)
    // write the new config--may not exist if there were previously no overrides
    writeEntityConfig(zkUtils, entityConfigPath, configs)

    // create the change notification
    val seqNode = ZkUtils.ConfigChangesPath + "/" + EntityConfigChangeZnodePrefix
    val content = Json.legacyEncodeAsString(getConfigChangeZnodeData(sanitizedEntityPath))
    zkUtils.createSequentialPersistentPath(seqNode, content)
  }

  def getConfigChangeZnodeData(sanitizedEntityPath: String) : Map[String, Any] = {
    Map("version" -> 2, "entity_path" -> sanitizedEntityPath)
  }

  /**
   * Write out the entity config to zk, if there is any
   */
  private def writeEntityConfig(zkUtils: ZkUtils, entityPath: String, config: Properties) {
    val map = Map("version" -> 1, "config" -> config.asScala)
    zkUtils.updatePersistentPath(entityPath, Json.legacyEncodeAsString(map))
  }

  /**
   * Read the entity (topic, broker, client, user or <user, client>) config (if any) from zk
   * sanitizedEntityName is <topic>, <broker>, <client-id>, <user> or <user>/clients/<client-id>.
   */
   @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def fetchEntityConfig(zkUtils: ZkUtils, rootEntityType: String, sanitizedEntityName: String): Properties = {
    val entityConfigPath = getEntityConfigPath(rootEntityType, sanitizedEntityName)
    // readDataMaybeNull returns Some(null) if the path exists, but there is no data
    val str = zkUtils.readDataMaybeNull(entityConfigPath)._1.orNull
    val props = new Properties()
    if (str != null) {
      Json.parseFull(str).foreach { jsValue =>
        val jsObject = jsValue.asJsonObjectOption.getOrElse {
          throw new IllegalArgumentException(s"Unexpected value in config: $str, entity_config_path: $entityConfigPath")
        }
        require(jsObject("version").to[Int] == 1)
        val config = jsObject.get("config").flatMap(_.asJsonObjectOption).getOrElse {
          throw new IllegalArgumentException(s"Invalid $entityConfigPath config: $str")
        }
        config.iterator.foreach { case (k, v) => props.setProperty(k, v.to[String]) }
      }
    }
    props
  }

  @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def fetchAllTopicConfigs(zkUtils: ZkUtils): Map[String, Properties] =
    zkUtils.getAllTopics().map(topic => (topic, fetchEntityConfig(zkUtils, ConfigType.Topic, topic))).toMap

  @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def fetchAllEntityConfigs(zkUtils: ZkUtils, entityType: String): Map[String, Properties] =
    zkUtils.getAllEntitiesWithConfig(entityType).map(entity => (entity, fetchEntityConfig(zkUtils, entityType, entity))).toMap

  @deprecated("This method is deprecated and will be replaced by kafka.zk.AdminZkClient.", "1.1.0")
  def fetchAllChildEntityConfigs(zkUtils: ZkUtils, rootEntityType: String, childEntityType: String): Map[String, Properties] = {
    def entityPaths(zkUtils: ZkUtils, rootPath: Option[String]): Seq[String] = {
      val root = rootPath match {
        case Some(path) => rootEntityType + '/' + path
        case None => rootEntityType
      }
      val entityNames = zkUtils.getAllEntitiesWithConfig(root)
      rootPath match {
        case Some(path) => entityNames.map(entityName => path + '/' + entityName)
        case None => entityNames
      }
    }
    entityPaths(zkUtils, None)
      .flatMap(entity => entityPaths(zkUtils, Some(entity + '/' + childEntityType)))
      .map(entityPath => (entityPath, fetchEntityConfig(zkUtils, rootEntityType, entityPath))).toMap
  }

  private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }

}
