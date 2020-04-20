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
package kafka.zk

import java.util.Properties

import kafka.admin.{AdminOperationException, AdminUtils, BrokerMetadata, RackAwareMode}
import kafka.common.TopicAlreadyMarkedForDeletionException
import kafka.controller.ReplicaAssignment
import kafka.log.LogConfig
import kafka.server.{ConfigEntityName, ConfigType, DynamicConfig}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.zookeeper.KeeperException.NodeExistsException

import scala.collection.{Map, Seq}

/**
 * Provides admin related methods for interacting with ZooKeeper.
 *
 * This is an internal class and no compatibility guarantees are provided,
 * see org.apache.kafka.clients.admin.AdminClient for publicly supported APIs.
 */
class AdminZkClient(zkClient: KafkaZkClient) extends Logging {

  /**
   * Creates the topic with given configuration
   * @param topic topic name to create
   * @param partitions  Number of partitions to be set
   * @param replicationFactor Replication factor
   * @param topicConfig  topic configs
   * @param rackAwareMode
   */
  def createTopic(topic: String,
                  partitions: Int,
                  replicationFactor: Int,
                  topicConfig: Properties = new Properties,
                  rackAwareMode: RackAwareMode = RackAwareMode.Enforced,
                  maxPartitions: Int = Int.MaxValue,
                  maxBrokerPartitions: Int = Int.MaxValue,
                  partitionsByBroker: Map[Int, Int] = Map.empty[Int, Int]): Unit = {
    val brokerMetadatas = getBrokerMetadatas(rackAwareMode)
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(
      brokerMetadatas, partitions, replicationFactor,
      maxPartitions, maxBrokerPartitions, partitionsByBroker)
    createTopicWithAssignment(topic, topicConfig, replicaAssignment)
  }

  /**
   * Gets broker metadata list
   * @param rackAwareMode
   * @param brokerList
   * @return
   */
  def getBrokerMetadatas(rackAwareMode: RackAwareMode = RackAwareMode.Enforced,
                         brokerList: Option[Seq[Int]] = None): Seq[BrokerMetadata] = {
    val allBrokers = zkClient.getAllBrokersInCluster
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

  def createTopicWithAssignment(topic: String,
                                config: Properties,
                                partitionReplicaAssignment: Map[Int, Seq[Int]],
                                maxPartitions: Int = Int.MaxValue,
                                maxBrokerPartitions: Int = Int.MaxValue,
                                partitionsByBroker: Map[Int, Int] = Map.empty[Int, Int]): Unit = {
    validateTopicCreate(
      topic, partitionReplicaAssignment, config,
      maxPartitions, maxBrokerPartitions, partitionsByBroker)

    info(s"Creating topic $topic with configuration $config and initial partition " +
      s"assignment $partitionReplicaAssignment")

    // write out the config if there is any, this isn't transactional with the partition assignments
    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic, config)

    // create the partition assignment
    writeTopicPartitionAssignment(topic, partitionReplicaAssignment.map { case (k, v) => k -> ReplicaAssignment(v) },
      isUpdate = false)
  }

  /**
   * Validate topic creation parameters
   */
  def validateTopicCreate(topic: String,
                          partitionReplicaAssignment: Map[Int, Seq[Int]],
                          config: Properties,
                          maxPartitions: Int = Int.MaxValue,
                          maxBrokerPartitions: Int = Int.MaxValue,
                          partitionsByBroker: Map[Int, Int] = Map.empty[Int, Int]): Unit = {
    Topic.validate(topic)

    if (zkClient.topicExists(topic))
      throw new TopicExistsException(s"Topic '$topic' already exists.")
    else if (Topic.hasCollisionChars(topic)) {
      val allTopics = zkClient.getAllTopicsInCluster()
      // check again in case the topic was created in the meantime, otherwise the
      // topic could potentially collide with itself
      if (allTopics.contains(topic))
        throw new TopicExistsException(s"Topic '$topic' already exists.")
      val collidingTopics = allTopics.filter(Topic.hasCollision(topic, _))
      if (collidingTopics.nonEmpty) {
        throw new InvalidTopicException(s"Topic '$topic' collides with existing topics: ${collidingTopics.mkString(", ")}")
      }
    }

    if (partitionReplicaAssignment.values.map(_.size).toSet.size != 1)
      throw new InvalidReplicaAssignmentException("All partitions should have the same number of replicas")

    partitionReplicaAssignment.values.foreach(reps =>
      if (reps.size != reps.toSet.size)
        throw new InvalidReplicaAssignmentException("Duplicate replica assignment found: " + partitionReplicaAssignment)
    )

    val partitionSize = partitionReplicaAssignment.size
    val sequenceSum = partitionSize * (partitionSize - 1) / 2
    if (partitionReplicaAssignment.size != partitionReplicaAssignment.toSet.size ||
        partitionReplicaAssignment.keys.filter(_ >= 0).sum != sequenceSum)
        throw new InvalidReplicaAssignmentException("partitions should be a consecutive 0-based integer sequence")

    LogConfig.validate(config)

    AdminUtils.verifyReplicaAssignmentAgainstPartitionLimits(
      partitionReplicaAssignment, maxPartitions, maxBrokerPartitions, partitionsByBroker)
  }

  private def writeTopicPartitionAssignment(topic: String, replicaAssignment: Map[Int, ReplicaAssignment], isUpdate: Boolean): Unit = {
    try {
      val assignment = replicaAssignment.map { case (partitionId, replicas) => (new TopicPartition(topic,partitionId), replicas) }.toMap

      if (!isUpdate) {
        zkClient.createTopicAssignment(topic, assignment.map { case (k, v) => k -> v.replicas })
      } else {
        zkClient.setTopicAssignment(topic, assignment)
      }
      debug("Updated path %s with %s for replica assignment".format(TopicZNode.path(topic), assignment))
    } catch {
      case _: NodeExistsException => throw new TopicExistsException(s"Topic '$topic' already exists.")
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }

  /**
   * Creates a delete path for a given topic
   * @param topic
   */
  def deleteTopic(topic: String): Unit = {
    if (zkClient.topicExists(topic)) {
      try {
        zkClient.createDeleteTopicPath(topic)
      } catch {
        case _: NodeExistsException => throw new TopicAlreadyMarkedForDeletionException(
          "topic %s is already marked for deletion".format(topic))
        case e: Throwable => throw new AdminOperationException(e.getMessage)
       }
    } else {
      throw new UnknownTopicOrPartitionException(s"Topic `$topic` to delete does not exist")
    }
  }

  /**
  * Add partitions to existing topic with optional replica assignment
  *
  * @param topic Topic for adding partitions to
  * @param existingAssignment A map from partition id to its assignment
  * @param allBrokers All brokers in the cluster
  * @param numPartitions Number of partitions to be set
  * @param replicaAssignment Manual replica assignment, or none
  * @param validateOnly If true, validate the parameters without actually adding the partitions
  * @return the updated replica assignment
  */
  def addPartitions(topic: String,
                    existingAssignment: Map[Int, ReplicaAssignment],
                    allBrokers: Seq[BrokerMetadata],
                    numPartitions: Int = 1,
                    replicaAssignment: Option[Map[Int, Seq[Int]]] = None,
                    validateOnly: Boolean = false,
                    maxPartitions: Int = Int.MaxValue,
                    maxBrokerPartitions: Int = Int.MaxValue,
                    partitionsByBroker: Map[Int, Int] = Map.empty[Int, Int]) : Map[Int, Seq[Int]] = {
    val existingAssignmentPartition0 = existingAssignment.getOrElse(0,
      throw new AdminOperationException(
        s"Unexpected existing replica assignment for topic '$topic', partition id 0 is missing. " +
          s"Assignment: $existingAssignment")).replicas

    val partitionsToAdd = numPartitions - existingAssignment.size
    if (partitionsToAdd <= 0)
      throw new InvalidPartitionsException(
        s"The number of partitions for a topic can only be increased. " +
          s"Topic $topic currently has ${existingAssignment.size} partitions, " +
          s"$numPartitions would not be an increase.")

    replicaAssignment.foreach { proposedReplicaAssignment =>
      validateReplicaAssignment(proposedReplicaAssignment, existingAssignmentPartition0.size,
        allBrokers.map(_.id).toSet, maxPartitions, maxBrokerPartitions, partitionsByBroker)
    }

    val proposedAssignmentForNewPartitions = replicaAssignment.getOrElse {
      val startIndex = math.max(0, allBrokers.indexWhere(_.id >= existingAssignmentPartition0.head))
      AdminUtils.assignReplicasToBrokers(
        allBrokers, partitionsToAdd, existingAssignmentPartition0.size,
        maxPartitions, maxBrokerPartitions, partitionsByBroker,
        startIndex, existingAssignment.size)
    }

    val proposedAssignment = existingAssignment ++ proposedAssignmentForNewPartitions.map { case (tp, replicas) =>
      tp -> ReplicaAssignment(replicas, List(), List())
    }
    if (!validateOnly) {
      info(s"Creating $partitionsToAdd partitions for '$topic' with the following replica assignment: " +
        s"$proposedAssignmentForNewPartitions.")

      writeTopicPartitionAssignment(topic, proposedAssignment, isUpdate = true)
    }
    proposedAssignment.map { case (k, v) => k -> v.replicas }
  }

  private def validateReplicaAssignment(replicaAssignment: Map[Int, Seq[Int]],
                                        expectedReplicationFactor: Int,
                                        availableBrokerIds: Set[Int],
                                        maxPartitions: Int,
                                        maxBrokerPartitions: Int,
                                        partitionsByBroker: Map[Int, Int]): Unit = {

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
      case (partition, replicas) if replicas.size != expectedReplicationFactor => partition -> replicas.size
    }
    if (badRepFactors.nonEmpty) {
      val sortedBadRepFactors = badRepFactors.toSeq.sortBy { case (partitionId, _) => partitionId }
      val partitions = sortedBadRepFactors.map { case (partitionId, _) => partitionId }
      val repFactors = sortedBadRepFactors.map { case (_, rf) => rf }
      throw new InvalidReplicaAssignmentException(s"Inconsistent replication factor between partitions, " +
        s"partition 0 has $expectedReplicationFactor while partitions [${partitions.mkString(", ")}] have " +
        s"replication factors [${repFactors.mkString(", ")}], respectively.")
    }

    if (!replicaAssignment.isEmpty) {
      AdminUtils.verifyReplicaAssignmentAgainstPartitionLimits(
        replicaAssignment, maxPartitions, maxBrokerPartitions, partitionsByBroker)
    }
  }

  def parseBroker(broker: String): Option[Int] = {
    broker match {
      case ConfigEntityName.Default => None
      case _ =>
        try Some(broker.toInt)
        catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"Error parsing broker $broker. The broker's Entity Name must be a single integer value")
        }
    }
  }

  /**
   * Change the configs for a given entityType and entityName
   * @param entityType
   * @param entityName
   * @param configs
   */
  def changeConfigs(entityType: String, entityName: String, configs: Properties): Unit = {

    entityType match {
      case ConfigType.Topic => changeTopicConfig(entityName, configs)
      case ConfigType.Client => changeClientIdConfig(entityName, configs)
      case ConfigType.User => changeUserOrUserClientIdConfig(entityName, configs)
      case ConfigType.Broker => changeBrokerConfig(parseBroker(entityName), configs)
      case _ => throw new IllegalArgumentException(s"$entityType is not a known entityType. Should be one of ${ConfigType.Topic}, ${ConfigType.Client}, ${ConfigType.Broker}")
    }
  }

  /**
   * Update the config for a client and create a change notification so the change will propagate to other brokers.
   * If clientId is <default>, default clientId config is updated. ClientId configs are used only if <user, clientId>
   * and <user> configs are not specified.
   *
   * @param sanitizedClientId: The sanitized clientId for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeClientIdConfig(sanitizedClientId: String, configs: Properties): Unit = {
    DynamicConfig.Client.validate(configs)
    changeEntityConfig(ConfigType.Client, sanitizedClientId, configs)
  }

  /**
   * Update the config for a <user> or <user, clientId> and create a change notification so the change will propagate to other brokers.
   * User and/or clientId components of the path may be <default>, indicating that the configuration is the default
   * value to be applied if a more specific override is not configured.
   *
   * @param sanitizedEntityName: <sanitizedUserPrincipal> or <sanitizedUserPrincipal>/clients/<clientId>
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeUserOrUserClientIdConfig(sanitizedEntityName: String, configs: Properties): Unit = {
    if (sanitizedEntityName == ConfigEntityName.Default || sanitizedEntityName.contains("/clients"))
      DynamicConfig.Client.validate(configs)
    else
      DynamicConfig.User.validate(configs)
    changeEntityConfig(ConfigType.User, sanitizedEntityName, configs)
  }

  /**
   * validates the topic configs
   * @param topic
   * @param configs
   */
  def validateTopicConfig(topic: String, configs: Properties): Unit = {
    Topic.validate(topic)
    if (!zkClient.topicExists(topic))
      throw new AdminOperationException("Topic \"%s\" does not exist.".format(topic))
    // remove the topic overrides
    LogConfig.validate(configs)
  }

  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   *
   * @param topic: The topic for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
   def changeTopicConfig(topic: String, configs: Properties): Unit = {
    validateTopicConfig(topic, configs)
    changeEntityConfig(ConfigType.Topic, topic, configs)
  }

  /**
    * Override the broker config on some set of brokers. These overrides will be persisted between sessions, and will
    * override any defaults entered in the broker's config files
    *
    * @param brokers: The list of brokers to apply config changes to
    * @param configs: The config to change, as properties
    */
  def changeBrokerConfig(brokers: Seq[Int], configs: Properties): Unit = {
    validateBrokerConfig(configs)
    brokers.foreach {
      broker => changeEntityConfig(ConfigType.Broker, broker.toString, configs)
    }
  }

  /**
    * Override a broker override or broker default config. These overrides will be persisted between sessions, and will
    * override any defaults entered in the broker's config files
    *
    * @param broker: The broker to apply config changes to or None to update dynamic default configs
    * @param configs: The config to change, as properties
    */
  def changeBrokerConfig(broker: Option[Int], configs: Properties): Unit = {
    validateBrokerConfig(configs)
    changeEntityConfig(ConfigType.Broker, broker.map(_.toString).getOrElse(ConfigEntityName.Default), configs)
  }

  /**
    * Validate dynamic broker configs. Since broker configs may contain custom configs, the validation
    * only verifies that the provided config does not contain any static configs.
    * @param configs configs to validate
    */
  def validateBrokerConfig(configs: Properties): Unit = {
    DynamicConfig.Broker.validate(configs)
  }

  private def changeEntityConfig(rootEntityType: String, fullSanitizedEntityName: String, configs: Properties): Unit = {
    val sanitizedEntityPath = rootEntityType + '/' + fullSanitizedEntityName
    zkClient.setOrCreateEntityConfigs(rootEntityType, fullSanitizedEntityName, configs)

    // create the change notification
    zkClient.createConfigChangeNotification(sanitizedEntityPath)
  }

  /**
   * Read the entity (topic, broker, client, user or <user, client>) config (if any) from zk
   * sanitizedEntityName is <topic>, <broker>, <client-id>, <user> or <user>/clients/<client-id>.
   * @param rootEntityType
   * @param sanitizedEntityName
   * @return
   */
  def fetchEntityConfig(rootEntityType: String, sanitizedEntityName: String): Properties = {
    zkClient.getEntityConfigs(rootEntityType, sanitizedEntityName)
  }

  /**
   * Gets all topic configs
   * @return
   */
  def getAllTopicConfigs(): Map[String, Properties] =
    zkClient.getAllTopicsInCluster().map(topic => (topic, fetchEntityConfig(ConfigType.Topic, topic))).toMap

  /**
   * Gets all the entity configs for a given entityType
   * @param entityType
   * @return
   */
  def fetchAllEntityConfigs(entityType: String): Map[String, Properties] =
    zkClient.getAllEntitiesWithConfig(entityType).map(entity => (entity, fetchEntityConfig(entityType, entity))).toMap

  /**
   * Gets all the entity configs for a given childEntityType
   * @param rootEntityType
   * @param childEntityType
   * @return
   */
  def fetchAllChildEntityConfigs(rootEntityType: String, childEntityType: String): Map[String, Properties] = {
    def entityPaths(rootPath: Option[String]): Seq[String] = {
      val root = rootPath match {
        case Some(path) => rootEntityType + '/' + path
        case None => rootEntityType
      }
      val entityNames = zkClient.getAllEntitiesWithConfig(root)
      rootPath match {
        case Some(path) => entityNames.map(entityName => path + '/' + entityName)
        case None => entityNames
      }
    }
    entityPaths(None)
      .flatMap(entity => entityPaths(Some(entity + '/' + childEntityType)))
      .map(entityPath => (entityPath, fetchEntityConfig(rootEntityType, entityPath))).toMap
  }

}

