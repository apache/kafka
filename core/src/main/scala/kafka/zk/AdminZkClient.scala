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

import java.util.{Collections, Optional, Properties}
import kafka.admin.RackAwareMode
import kafka.common.TopicAlreadyMarkedForDeletionException
import kafka.controller.ReplicaAssignment
import kafka.server.{ConfigEntityName, ConfigType, DynamicConfig, KafkaConfig}
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.admin.{AdminUtils, BrokerMetadata}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.server.common.AdminOperationException
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.zookeeper.KeeperException.NodeExistsException

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq}

/**
 * Provides admin related methods for interacting with ZooKeeper.
 *
 * This is an internal class and no compatibility guarantees are provided,
 * see org.apache.kafka.clients.admin.AdminClient for publicly supported APIs.
 */
class AdminZkClient(zkClient: KafkaZkClient,
                    kafkaConfig: Option[KafkaConfig] = None) extends Logging {

  /**
   * Creates the topic with given configuration
   * @param topic topic name to create
   * @param partitions  Number of partitions to be set
   * @param replicationFactor Replication factor
   * @param topicConfig  topic configs
   * @param rackAwareMode rack aware mode for replica assignment
   * @param usesTopicId Boolean indicating whether the topic ID will be created
   */
  def createTopic(topic: String,
                  partitions: Int,
                  replicationFactor: Int,
                  topicConfig: Properties = new Properties,
                  rackAwareMode: RackAwareMode = RackAwareMode.Enforced,
                  usesTopicId: Boolean = false): Unit = {
    val brokerMetadatas = getBrokerMetadatas(rackAwareMode).asJava
    val replicaAssignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(brokerMetadatas, partitions, replicationFactor))
    createTopicWithAssignment(topic, topicConfig, replicaAssignment, usesTopicId = usesTopicId)
  }

  /**
   * Gets broker metadata list
   *
   * @param rackAwareMode rack aware mode for replica assignment
   * @param brokerList The brokers to gather metadata about.
   * @return The metadata for each broker that was found.
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
      case RackAwareMode.Disabled => brokers.map(broker => new BrokerMetadata(broker.id, Optional.empty()))
      case RackAwareMode.Safe if brokersWithRack.size < brokers.size =>
        brokers.map(broker => new BrokerMetadata(broker.id, Optional.empty()))
      case _ => brokers.map(broker => new BrokerMetadata(broker.id, Optional.ofNullable(broker.rack.orNull)))
    }
    brokerMetadatas.sortBy(_.id)
  }

  /**
   * Create topic and optionally validate its parameters. Note that this method is used by the
   * TopicCommand as well.
   *
   * @param topic The name of the topic
   * @param config The config of the topic
   * @param partitionReplicaAssignment The assignments of the topic
   * @param validate Boolean indicating if parameters must be validated or not (true by default)
   * @param usesTopicId Boolean indicating whether the topic ID will be created
   */
  def createTopicWithAssignment(topic: String,
                                config: Properties,
                                partitionReplicaAssignment: Map[Int, Seq[Int]],
                                validate: Boolean = true,
                                usesTopicId: Boolean = false): Unit = {
    if (validate)
      validateTopicCreate(topic, partitionReplicaAssignment, config)

    info(s"Creating topic $topic with configuration $config and initial partition " +
      s"assignment $partitionReplicaAssignment")

    // write out the config if there is any, this isn't transactional with the partition assignments
    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic, config)

    // create the partition assignment
    writeTopicPartitionAssignment(topic, partitionReplicaAssignment.map { case (k, v) => k -> ReplicaAssignment(v) },
      isUpdate = false, usesTopicId)
  }

  /**
   * Validate topic creation parameters. Note that this method is indirectly used by the
   * TopicCommand via the `createTopicWithAssignment` method.
   *
   * @param topic The name of the topic
   * @param partitionReplicaAssignment The assignments of the topic
   * @param config The config of the topic
   */
  def validateTopicCreate(topic: String,
                          partitionReplicaAssignment: Map[Int, Seq[Int]],
                          config: Properties): Unit = {
    Topic.validate(topic)
    if (zkClient.isTopicMarkedForDeletion(topic)) {
      throw new TopicExistsException(s"Topic '$topic' is marked for deletion.")
    }
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

    LogConfig.validate(config,
      kafkaConfig.map(_.extractLogConfigMap).getOrElse(Collections.emptyMap()),
      kafkaConfig.exists(_.isRemoteLogStorageSystemEnabled))
  }

  private def writeTopicPartitionAssignment(topic: String, replicaAssignment: Map[Int, ReplicaAssignment],
                                            isUpdate: Boolean, usesTopicId: Boolean = false): Unit = {
    try {
      val assignment = replicaAssignment.map { case (partitionId, replicas) => (new TopicPartition(topic,partitionId), replicas) }.toMap

      if (!isUpdate) {
        val topicIdOpt = if (usesTopicId) Some(Uuid.randomUuid()) else None
        zkClient.createTopicAssignment(topic, topicIdOpt, assignment.map { case (k, v) => k -> v.replicas })
      } else {
        val topicIds = zkClient.getTopicIdsForTopics(Set(topic))
        zkClient.setTopicAssignment(topic, topicIds.get(topic), assignment)
      }
      debug("Updated path %s with %s for replica assignment".format(TopicZNode.path(topic), assignment))
    } catch {
      case _: NodeExistsException => throw new TopicExistsException(s"Topic '$topic' already exists.")
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }

  /**
   * Creates a delete path for a given topic
   * @param topic Topic name to delete
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
   * Add partitions to existing topic with optional replica assignment. Note that this
   * method is used by the TopicCommand.
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
                    validateOnly: Boolean = false): Map[Int, Seq[Int]] = {

    val proposedAssignmentForNewPartitions = createNewPartitionsAssignment(
      topic,
      existingAssignment,
      allBrokers,
      numPartitions,
      replicaAssignment
    )

    if (validateOnly) {
      (existingAssignment ++ proposedAssignmentForNewPartitions)
        .map { case (k, v) => k -> v.replicas }
    } else {
      createPartitionsWithAssignment(topic, existingAssignment, proposedAssignmentForNewPartitions)
        .map { case (k, v) => k -> v.replicas }
    }
  }

  /**
   * Create assignment to add the given number of partitions while validating the
   * provided arguments.
   *
   * @param topic Topic for adding partitions to
   * @param existingAssignment A map from partition id to its assignment
   * @param allBrokers All brokers in the cluster
   * @param numPartitions Number of partitions to be set
   * @param replicaAssignment Manual replica assignment, or none
   * @return the assignment for the new partitions
   */
  def createNewPartitionsAssignment(topic: String,
                                    existingAssignment: Map[Int, ReplicaAssignment],
                                    allBrokers: Seq[BrokerMetadata],
                                    numPartitions: Int = 1,
                                    replicaAssignment: Option[Map[Int, Seq[Int]]] = None): Map[Int, ReplicaAssignment] = {
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
        allBrokers.map(_.id).toSet)
    }

    val proposedAssignmentForNewPartitions = replicaAssignment.getOrElse {
      val startIndex = math.max(0, allBrokers.indexWhere(_.id >= existingAssignmentPartition0.head))
      CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(allBrokers.asJava, partitionsToAdd, existingAssignmentPartition0.size,
        startIndex, existingAssignment.size))
    }

    proposedAssignmentForNewPartitions.map { case (tp, replicas) =>
      tp -> ReplicaAssignment(replicas, List(), List())
    }
  }

  /**
   * Add partitions to the existing topic with the provided assignment. This method does
   * not validate the provided assignments. Validation must be done beforehand.
   *
   * @param topic Topic for adding partitions to
   * @param existingAssignment A map from partition id to its assignment
   * @param newPartitionAssignment The assignments to add
   * @return the updated replica assignment
   */
  def createPartitionsWithAssignment(topic: String,
                                     existingAssignment: Map[Int, ReplicaAssignment],
                                     newPartitionAssignment: Map[Int, ReplicaAssignment]): Map[Int, ReplicaAssignment] = {

    info(s"Creating ${newPartitionAssignment.size} partitions for '$topic' with the following replica assignment: " +
      s"$newPartitionAssignment.")

    val combinedAssignment = existingAssignment ++ newPartitionAssignment

    writeTopicPartitionAssignment(topic, combinedAssignment, isUpdate = true)

    combinedAssignment
  }

  private def validateReplicaAssignment(replicaAssignment: Map[Int, Seq[Int]],
                                        expectedReplicationFactor: Int,
                                        availableBrokerIds: Set[Int]): Unit = {

    replicaAssignment.forKeyValue { (partitionId, replicas) =>
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
  }

  /**
   * Parse broker from entity name to integer id
   * @param broker The broker entity name to parse
   * @return Integer brokerId after successfully parsed or default None
   */
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
   * @param entityType The entityType of the configs that will be changed
   * @param entityName The entityName of the entityType
   * @param configs The config of the entityName
   * @param isUserClientId If true, this entity is user and clientId entity
   */
  def changeConfigs(entityType: String, entityName: String, configs: Properties, isUserClientId: Boolean = false): Unit = {

    entityType match {
      case ConfigType.Topic => changeTopicConfig(entityName, configs)
      case ConfigType.Client => changeClientIdConfig(entityName, configs)
      case ConfigType.User => changeUserOrUserClientIdConfig(entityName, configs, isUserClientId)
      case ConfigType.Broker => changeBrokerConfig(parseBroker(entityName), configs)
      case ConfigType.Ip => changeIpConfig(entityName, configs)
      case _ => throw new IllegalArgumentException(s"$entityType is not a known entityType. Should be one of ${ConfigType.all}")
    }
  }

  /**
   * Try to clean quota nodes in zk, if the configs of the node are empty and there are no children left,
   * to avoid infinite growth of quota nodes
   * @param entityType The entityType of the node we are trying to clean
   * @param entityName The entityName of the entityType
   * @param isUserClientId If true, this entity is user and clientId entity
   * @return True, if the node is deleted
   */
  private def tryCleanQuotaNodes(entityType: String, entityName: String, isUserClientId: Boolean): Boolean = {
    val currPath = ConfigEntityZNode.path(entityType, entityName)
    if (zkClient.getChildren(currPath).isEmpty) {
      var pathToDelete = currPath
      // If the entity is user and clientId, we need to do some further check if the parent user node is also empty
      // after current userClientId node deleted. If so, we also need to try cleaning the corresponding user node.
      if (isUserClientId) {
        val user = entityName.substring(0, entityName.indexOf("/"))
        val clientId = entityName.substring(entityName.lastIndexOf("/") + 1)
        val clientsPath = ConfigEntityZNode.path(ConfigType.User, user + "/" + ConfigType.Client)
        val clientsChildren = zkClient.getChildren(clientsPath)
        // If current client is the only child of clients, the node of clients can also be deleted.
        if (clientsChildren == Seq(clientId)) {
          pathToDelete = clientsPath
          val userData = fetchEntityConfig(ConfigType.User, user)
          val userPath = ConfigEntityZNode.path(ConfigType.User, user)
          val userChildren = zkClient.getChildren(userPath)
          // If the configs of the user are empty and the clients node is the only child of the user,
          // the node of user can also be deleted.
          if (userData.isEmpty && userChildren == Seq(ConfigType.Client)) {
            pathToDelete = userPath
          }
        }
      }
      info(s"Deleting zk node $pathToDelete since node of entityType $entityType and entityName $entityName is empty.")
      zkClient.deletePath(pathToDelete)
      true
    } else
      false
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
   * @param isUserClientId If true, this entity is user and clientId entity
   *
   */
  def changeUserOrUserClientIdConfig(sanitizedEntityName: String, configs: Properties, isUserClientId: Boolean = false): Unit = {
    if (sanitizedEntityName == ConfigEntityName.Default || sanitizedEntityName.contains("/clients"))
      DynamicConfig.Client.validate(configs)
    else
      DynamicConfig.User.validate(configs)
    changeEntityConfig(ConfigType.User, sanitizedEntityName, configs, isUserClientId)
  }

  /**
   * Validates the IP configs.
   * @param ip ip for which configs are being validated
   * @param configs properties to validate for the IP
   */
  def validateIpConfig(ip: String, configs: Properties): Unit = {
    if (!DynamicConfig.Ip.isValidIpEntity(ip))
      throw new AdminOperationException(s"$ip is not a valid IP or resolvable host.")
    DynamicConfig.Ip.validate(configs)
  }

  /**
   * Update the config for an IP. These overrides will be persisted between sessions, and will override any default
   * IP properties.
   * @param ip ip for which configs are being updated
   * @param configs properties to update for the IP
   */
  def changeIpConfig(ip: String, configs: Properties): Unit = {
    validateIpConfig(ip, configs)
    changeEntityConfig(ConfigType.Ip, ip, configs)
  }

  /**
   * validates the topic configs
   * @param topic topic for which configs are being validated
   * @param configs properties to validate for the topic
   */
  def validateTopicConfig(topic: String, configs: Properties): Unit = {
    Topic.validate(topic)
    if (!zkClient.topicExists(topic))
      throw new UnknownTopicOrPartitionException(s"Topic '$topic' does not exist.")
    // remove the topic overrides
    LogConfig.validate(configs,
      kafkaConfig.map(_.extractLogConfigMap).getOrElse(Collections.emptyMap()),
      kafkaConfig.exists(_.isRemoteLogStorageSystemEnabled))
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

  private def changeEntityConfig(rootEntityType: String, fullSanitizedEntityName: String, configs: Properties, isUserClientId: Boolean = false): Unit = {
    val sanitizedEntityPath = rootEntityType + '/' + fullSanitizedEntityName
    var needUpdateConfigs = true
    // If the entityType is quota and node is empty, which means if the configs are empty and no children left,
    // we should try to clean up to avoid continuous increment of zk nodes.
    if ((ConfigType.Client.equals(rootEntityType) || ConfigType.User.equals(rootEntityType) || ConfigType.Ip.equals(rootEntityType)) && configs.isEmpty) {
      if (tryCleanQuotaNodes(rootEntityType, fullSanitizedEntityName, isUserClientId)) {
        needUpdateConfigs = false
      }
    }
    if (needUpdateConfigs) {
      zkClient.setOrCreateEntityConfigs(rootEntityType, fullSanitizedEntityName, configs)
    }

    // create the change notification
    zkClient.createConfigChangeNotification(sanitizedEntityPath)
  }

  /**
   * Read the entity (topic, broker, client, user, <user, client> or <ip>) config (if any) from zk
   * sanitizedEntityName is <topic>, <broker>, <client-id>, <user>, <user>/clients/<client-id> or <ip>.
   * @param rootEntityType entityType for which configs are being fetched
   * @param sanitizedEntityName entityName of the entityType
   * @return The successfully gathered configs
   */
  def fetchEntityConfig(rootEntityType: String, sanitizedEntityName: String): Properties = {
    zkClient.getEntityConfigs(rootEntityType, sanitizedEntityName)
  }

  /**
   * Gets all topic configs
   * @return The successfully gathered configs of all topics
   */
  def getAllTopicConfigs(): Map[String, Properties] =
    zkClient.getAllTopicsInCluster().map(topic => (topic, fetchEntityConfig(ConfigType.Topic, topic))).toMap

  /**
   * Gets all the entity configs for a given entityType
   * @param entityType entityType for which configs are being fetched
   * @return The successfully gathered configs of the entityType
   */
  def fetchAllEntityConfigs(entityType: String): Map[String, Properties] =
    zkClient.getAllEntitiesWithConfig(entityType).map(entity => (entity, fetchEntityConfig(entityType, entity))).toMap

  /**
   * Gets all the entity configs for a given childEntityType
   * @param rootEntityType rootEntityType for which configs are being fetched
   * @param childEntityType childEntityType of the rootEntityType
   * @return The successfully gathered configs of the childEntityType
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

