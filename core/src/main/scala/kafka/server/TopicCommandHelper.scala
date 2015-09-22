/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.common._
import kafka.consumer.Whitelist
import kafka.log.LogConfig
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.admin.AlterTopicRequest.AlterTopicArguments
import org.apache.kafka.common.requests.admin.{AlterTopicRequest, CreateTopicRequest}
import org.apache.kafka.common.requests.admin.CreateTopicRequest.CreateTopicArguments

import scala.collection._

object TopicCommandHelper extends Logging {

  this.logIdent = "[TopicCommandHelper]"

  private def getTopics(zkClient: ZkClient, topicOpt: Option[String]): List[String] = {
    val allTopics = ZkUtils.getAllTopics(zkClient).sorted.toList

    topicOpt match {
      case Some(topic) =>
        val topicsFilter = new Whitelist(topic)
        allTopics.filter(topicsFilter.isTopicAllowed(_, excludeInternalTopics = false))
      case None =>
        allTopics
    }
  }

  /**
   * @param topicOpt optionally specified topic which should be checked for existence
   * @return List of available topics that are not in the process of removal
   */
  private def listTopics(zkClient: ZkClient, topicOpt: Option[String] = None): List[String] = {
    val topicsQueuedForDeletion = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.DeleteTopicsPath).toSet

    getTopics(zkClient, topicOpt)
      .filterNot(topic => topicsQueuedForDeletion.contains(topic))
  }

  def createTopics(zkClient: ZkClient, createTopicArguments: Map[String, CreateTopicArguments]): Map[String, Errors] = {
    import JavaConverters._

    val allTopics = listTopics(zkClient).toSet

    /**
     * Creates a single topic or throws a specific exception
     */
    def createTopic(zkClient: ZkClient, topic: String,
                    topicConfigs: Seq[(String, String)],
                    replicaAssignmentMap: Map[Int, Seq[Int]],
                    partitions: Int,
                    replicationFactor: Int) {
      logger.debug("Starting topic creation with arguments: topic=%s, partitions=%d, replication-factor=%d, replica-assignment=%s, configs=%s"
        .format(topic, partitions, replicationFactor, replicaAssignmentMap, topicConfigs))

      if (allTopics.contains(topic)) {
        // do this check first to avoid other subtle validations
        throw new TopicExistsException("Topic " + topic + " already exists")
      }

      val configs = parseConfigs(topicConfigs)

      if (replicaAssignmentMap.nonEmpty) {
        // Note: we don't check that replicaAssignment doesn't contain unknown brokers - unlike in add-partitions case,
        // this follows the existing logic in TopicCommand
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssignmentMap, configs)
      } else {
        (partitions, replicationFactor) match {
          case Pair(CreateTopicRequest.NO_PARTITIONS_SIGN, CreateTopicRequest.NO_REPLICATION_FACTOR_SIGN) =>
            throw new InvalidReplicaAssignmentException("If Partitions and ReplicationFactor are not specified then ReplicaAssignment must be defined")
          case Pair(CreateTopicRequest.NO_PARTITIONS_SIGN, _) =>
            throw new InvalidPartitionsException("If ReplicaAssignment and ReplicationFactor are not specified then Partitions must be defined")
          case Pair(_, CreateTopicRequest.NO_REPLICATION_FACTOR_SIGN) =>
            throw new InvalidPartitionsException("If Partitions and ReplicaAssignment are not specified then ReplicationFactor must be defined")
          case _ =>
            val brokerList = ZkUtils.getSortedBrokerList(zkClient)
            val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, partitions, replicationFactor)
            AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssignment, configs)
        }
      }
    }

    createTopicArguments.map {
      case (topic, args) =>
        val error =
          try {
            val configs = args.configs.asScala.map(entry => (entry.configKey(), entry.configValue())).toSeq
            val replicaAssignment = args.replicasAssignments.asScala.map {
              partitionReplicaAssignment =>
                (partitionReplicaAssignment.partition, partitionReplicaAssignment.replicas.asScala.map(_.intValue()))
            }.toMap

            createTopic(zkClient, topic, configs, replicaAssignment, args.partitions, args.replicationFactor)
            Errors.NONE
          } catch {
            case e1: TopicExistsException =>
              logger.debug("Topic %s creation failed".format(topic), e1)
              Errors.TOPIC_ALREADY_EXISTS
            case e2: InvalidTopicException =>
              logger.debug("Topic %s creation failed".format(topic), e2)
              Errors.INVALID_TOPIC_EXCEPTION
            case e3: InvalidTopicConfigurationException =>
              logger.debug("Topic %s creation failed".format(topic), e3)
              Errors.INVALID_ENTITY_CONFIG
            case e4: InvalidReplicaAssignmentException =>
              logger.debug("Topic %s creation failed".format(topic), e4)
              Errors.INVALID_REPLICA_ASSIGNMENT
            case e5: InvalidPartitionsException =>
              logger.debug("Topic %s creation failed".format(topic), e5)
              Errors.INVALID_PARTITIONS
            case e6: InvalidReplicationFactorException =>
              logger.debug("Topic %s creation failed".format(topic), e6)
              Errors.INVALID_REPLICATION_FACTOR
            case e: Exception =>
              Errors.forException(e)
          }
        topic -> error
    }
  }


  /**
   * AlterTopicRequest accept Partitions, ReplicationFactor and ReplicaAssignment as a subject of change but
   * will take into account only one from the list. The precedence is the following:
   * ReplicaAssignment, Partitions, ReplicationFactor.
   *
   * 1) If ReplicaAssignment is defined:
   * 1.1) If replica-assignment contains only existing partitions - it's a reassign-partitions case
   * 1.2) Else if replica-assignment contains only new partition ids - it's an add-partitions case
   * 1.3) ReplicaAssignment is invalid
   *
   * 2) Else if both Partitions and ReplicationFactor are not defined - everything is not defined - return InvalidReplicaAssignment
   * 3) Else Partitions is defined (regardless what ReplicationFactor is) - it's an add-partitions case
   * 4) Else (only ReplicationFactor is defined) - it's a reassign-partitions case
   */
  def alterTopics(zkClient: ZkClient, alterTopicArguments: Map[String, AlterTopicArguments]): Map[String, Errors] = {
    import JavaConverters._

    val partitionsToBeReassigned: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty

    def alterTopic(topic: String,
                   partitions: Int,
                   replicationFactor: Int,
                   partitionsReplicaAssignment: Map[Int, Seq[Int]]) {
      logger.debug("Starting topic alteration with arguments: topic=%s, partitions=%d, replication-factor=%d, replica-assignment=%s"
        .format(topic, partitions, replicationFactor, partitionsReplicaAssignment))

      val currentPartitionsReplicaAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, Seq(topic)).getOrElse(topic,
        throw new InvalidTopicException("Topic %s does not exist".format(topic)))
      if (currentPartitionsReplicaAssignment.isEmpty)
        throw new InvalidTopicException("Topic %s does not exist".format(topic))

      val currentPartitions = currentPartitionsReplicaAssignment.keySet
      val currentPartitionsSize = currentPartitionsReplicaAssignment.keySet.size
      val currentReplicationFactor = currentPartitionsReplicaAssignment.head._2.size
      val brokerList = ZkUtils.getSortedBrokerList(zkClient)

      if (partitionsReplicaAssignment.nonEmpty) {
        val targetPartitions = partitionsReplicaAssignment.keySet
        if (targetPartitions.subsetOf(currentPartitions)){
          // reassign-partitions case
          partitionsReplicaAssignment.foreach {
            case (p, replicas) =>
              if (replicas.size != replicas.toSet.size)
                throw new InvalidReplicaAssignmentException("Target replica assignment for partition %d topic %s contains duplicates"
                  .format(p, topic))
          }

          if (
            partitionsReplicaAssignment.exists {
              case (p, replicas) =>
                replicas.size != partitionsReplicaAssignment.head._2.size
            })
            throw new InvalidReplicaAssignmentException("Target partitions replica assignment for topic %s have different replication factors"
              .format(topic))


          // accumulate reassignment to trigger it just once for the whole AlterTopicRequest batch
          partitionsReplicaAssignment.foreach {
            case (p, replicas) =>
              partitionsToBeReassigned.put(TopicAndPartition(topic, p), replicas)
          }
        } else {
          // add partitions case
          if (targetPartitions.intersect(currentPartitions).nonEmpty)
            throw new InvalidReplicaAssignmentException("Target replica assignment for topic %s contains both new and existing partitions".format(topic))

          // current 1-2-3-4 then target if has size 3 should be 5-6-7
          if ((targetPartitions ++ currentPartitions) != 0.until(targetPartitions.size + currentPartitions.size).toSet)
            throw new InvalidReplicaAssignmentException(("Target replica assignment partitions %s for topic %s cannot be added " +
              "because they don't form a sequence with existing partitions %s").format(targetPartitions, topic, currentPartitions))

          logger.debug("Adding partitions %s to topic %s".format(partitionsReplicaAssignment, topic))
          AdminUtils.addPartitions(zkClient, topic, checkBrokerAvailable = true,
            currentPartitionsReplicaAssignment, partitionsReplicaAssignment)
        }
      } else {
        (partitions, replicationFactor) match {
          case Pair(AlterTopicRequest.NO_PARTITIONS_SIGN, AlterTopicRequest.NO_REPLICATION_FACTOR_SIGN) =>
            // nothing is defined - at least one of Partition, ReplicationFactor, ReplicaAssignment must be defined
            throw new InvalidReplicaAssignmentException("Partitions and ReplicationFactor is not specified - ReplicaAssignment must be defined")

          case Pair(AlterTopicRequest.NO_PARTITIONS_SIGN, _) =>
            // change replication-factor case - regenerate RA so that all existing partitions have correct new replication-factor

            val reassignment = AdminUtils.assignReplicasToBrokers(brokerList, currentPartitionsSize, replicationFactor)

            reassignment.foreach {
              case (p, replicas) =>
                partitionsToBeReassigned.put(TopicAndPartition(topic, p), replicas)
            }
          case _ =>
            // user either defined replication-factor and partitions or just partitions - either way
            // partitions takes precedence, replication-factor is ignored

            if (partitions <= currentPartitionsSize)
              throw new InvalidPartitionsException(("Number of expected partitions %d for topic %s is " +
                "less than current number of partitions %d").format(partitions, topic, currentPartitionsSize))

            val addedPartitions = AdminUtils.assignReplicasToBrokers(brokerList, partitions - currentPartitionsSize,
              currentReplicationFactor, startPartitionId = currentPartitionsSize)
            logger.debug("Adding partitions %s to topic %s".format(addedPartitions, topic))
            AdminUtils.addPartitions(zkClient, topic, checkBrokerAvailable = false, currentPartitionsReplicaAssignment, addedPartitions)
        }
      }
    }

    // this will add partitions and fill the partitionsToBeReassigned
    val addPartitionsErrors =
      alterTopicArguments.map {
        case (topic, args) =>
          val error =
            try {
              alterTopic(topic, args.partitions, args.replicationFactor, args.replicasAssignments.asScala.map {
                partitionReplicaAssignment =>
                  (partitionReplicaAssignment.partition, partitionReplicaAssignment.replicas.asScala.map(_.intValue()))
              }.toMap)
              Errors.NONE
            }
            catch {
              case e1: InvalidTopicException =>
                logger.debug("Topic %s alteration failed".format(topic), e1)
                Errors.INVALID_TOPIC_EXCEPTION
              case e2: InvalidReplicaAssignmentException =>
                logger.debug("Topic %s alteration failed".format(topic), e2)
                Errors.INVALID_REPLICA_ASSIGNMENT
              case e3: InvalidPartitionsException =>
                logger.debug("Topic %s alteration failed".format(topic), e3)
                Errors.INVALID_PARTITIONS
              case e4: InvalidReplicationFactorException =>
                logger.debug("Topic %s alteration failed".format(topic), e4)
                Errors.INVALID_REPLICATION_FACTOR
              case e: Exception =>
                logger.debug("Topic %s alteration failed".format(topic), e)
                Errors.forException(e)
            }

          topic -> error
      }.toMap

    // finally try to start reassign-partitions
    val startReassignmentErrors =
      try {
        if (partitionsToBeReassigned.nonEmpty)
          AdminUtils.triggerReassignPartitions(zkClient, partitionsToBeReassigned)
        Map.empty[String, Errors]
      } catch {
        case e: ReassignPartitionsInProgressException =>
          partitionsToBeReassigned.keySet.map(tap => (tap.topic, Errors.REASSIGN_PARTITIONS_IN_PROGRESS)).toMap
      }

    addPartitionsErrors ++ startReassignmentErrors
  }

  /**
   * @return per topic error map or throws an exception if topic deletion request cannot
   *         be executed (e.g. failed to fetch existing topic list)
   */
  def deleteTopics(zkClient: ZkClient, topics: Set[String]): Map[String, Errors] = {
    val allTopics = getTopics(zkClient, None).toSet

    def deleteTopic(topic: String): Errors = {
      logger.debug("Starting topic=%s deletion".format(topic))

      if (!allTopics.contains(topic)) {
        logger.debug("Topic %s deletion failed because it doesn't exist".format(topic))
        Errors.INVALID_TOPIC_EXCEPTION
      } else {
        AdminUtils.deleteTopic(zkClient, topic)
        Errors.NONE
      }
    }

    topics.map {
      topic =>
        val error =
          try {
            deleteTopic(topic)
          }
          catch {
            case amde: TopicAlreadyMarkedForDeletionException =>
              // since all topic commands are async we ignore if topic already marked for deletion,
              // the idea is only to initiate topic deletion
              Errors.NONE
            case e: Exception =>
              logger.debug("Topic %s deletion failed".format(topic), e)
              Errors.forException(e)
          }

        topic -> error
    }.toMap
  }

  /**
   * Puts configs as key-value pairs into properties and validate LogConfig
   */
  def parseConfigs(configs: Seq[(String, String)]): Properties = {
    try {
      val props = new Properties
      configs.foreach { case (k, v) => props.setProperty(k.trim, v.trim)}

      LogConfig.validate(props)
      props
    } catch {
      case e: Exception => throw new InvalidTopicConfigurationException(e)
    }
  }
}