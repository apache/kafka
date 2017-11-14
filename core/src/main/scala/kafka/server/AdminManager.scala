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
package kafka.server

import java.util.{Collections, Properties}

import kafka.admin.{AdminOperationException, AdminUtils}
import kafka.common.TopicAlreadyMarkedForDeletionException
import kafka.log.LogConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, ConfigResource}
import org.apache.kafka.common.errors.{ApiException, InvalidPartitionsException, InvalidReplicaAssignmentException, InvalidRequestException, PolicyViolationException, ReassignmentInProgressException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.CreateTopicsRequest._
import org.apache.kafka.common.requests.{AlterConfigsRequest, ApiError, DescribeConfigsResponse, Resource, ResourceType}
import org.apache.kafka.server.policy.{AlterConfigPolicy, CreateTopicPolicy}
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata

import scala.collection._
import scala.collection.JavaConverters._

class AdminManager(val config: KafkaConfig,
                   val metrics: Metrics,
                   val metadataCache: MetadataCache,
                   val zkUtils: ZkUtils) extends Logging with KafkaMetricsGroup {

  this.logIdent = "[Admin Manager on Broker " + config.brokerId + "]: "

  private val topicPurgatory = DelayedOperationPurgatory[DelayedOperation]("topic", config.brokerId)

  private val createTopicPolicy =
    Option(config.getConfiguredInstance(KafkaConfig.CreateTopicPolicyClassNameProp, classOf[CreateTopicPolicy]))

  private val alterConfigPolicy =
    Option(config.getConfiguredInstance(KafkaConfig.AlterConfigPolicyClassNameProp, classOf[AlterConfigPolicy]))

  def hasDelayedTopicOperations = topicPurgatory.delayed != 0

  /**
    * Try to complete delayed topic operations with the request key
    */
  def tryCompleteDelayedTopicOperations(topic: String) {
    val key = TopicKey(topic)
    val completed = topicPurgatory.checkAndComplete(key)
    debug(s"Request key ${key.keyLabel} unblocked $completed topic requests.")
  }

  /**
    * Create topics and wait until the topics have been completely created.
    * The callback function will be triggered either when timeout, error or the topics are created.
    */
  def createTopics(timeout: Int,
                   validateOnly: Boolean,
                   createInfo: Map[String, TopicDetails],
                   responseCallback: Map[String, ApiError] => Unit) {

    // 1. map over topics creating assignment and calling zookeeper
    val brokers = metadataCache.getAliveBrokers.map { b => kafka.admin.BrokerMetadata(b.id, b.rack) }
    val metadata = createInfo.map { case (topic, arguments) =>
      try {
        val configs = new Properties()
        arguments.configs.asScala.foreach { case (key, value) =>
          configs.setProperty(key, value)
        }
        LogConfig.validate(configs)

        val assignments = {
          if ((arguments.numPartitions != NO_NUM_PARTITIONS || arguments.replicationFactor != NO_REPLICATION_FACTOR)
            && !arguments.replicasAssignments.isEmpty)
            throw new InvalidRequestException("Both numPartitions or replicationFactor and replicasAssignments were set. " +
              "Both cannot be used at the same time.")
          else if (!arguments.replicasAssignments.isEmpty) {
            // Note: we don't check that replicaAssignment contains unknown brokers - unlike in add-partitions case,
            // this follows the existing logic in TopicCommand
            arguments.replicasAssignments.asScala.map { case (partitionId, replicas) =>
              (partitionId.intValue, replicas.asScala.map(_.intValue))
            }
          } else
            AdminUtils.assignReplicasToBrokers(brokers, arguments.numPartitions, arguments.replicationFactor)
        }
        trace(s"Assignments for topic $topic are $assignments ")

        createTopicPolicy match {
          case Some(policy) =>
            AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)

            // Use `null` for unset fields in the public API
            val numPartitions: java.lang.Integer =
              if (arguments.numPartitions == NO_NUM_PARTITIONS) null else arguments.numPartitions
            val replicationFactor: java.lang.Short =
              if (arguments.replicationFactor == NO_REPLICATION_FACTOR) null else arguments.replicationFactor
            val replicaAssignments = if (arguments.replicasAssignments.isEmpty) null else arguments.replicasAssignments

            policy.validate(new RequestMetadata(topic, numPartitions, replicationFactor, replicaAssignments,
              arguments.configs))

            if (!validateOnly)
              AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignments, configs, update = false)

          case None =>
            if (validateOnly)
              AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)
            else
              AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignments, configs, update = false)
        }
        CreatePartitionsMetadata(topic, assignments, ApiError.NONE)
      } catch {
        // Log client errors at a lower level than unexpected exceptions
        case e@ (_: PolicyViolationException | _: ApiException) =>
          info(s"Error processing create topic request for topic $topic with arguments $arguments", e)
          CreatePartitionsMetadata(topic, Map(), ApiError.fromThrowable(e))
        case e: Throwable =>
          error(s"Error processing create topic request for topic $topic with arguments $arguments", e)
          CreatePartitionsMetadata(topic, Map(), ApiError.fromThrowable(e))
      }
    }

    // 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
    if (timeout <= 0 || validateOnly || !metadata.exists(_.error.is(Errors.NONE))) {
      val results = metadata.map { createTopicMetadata =>
        // ignore topics that already have errors
        if (createTopicMetadata.error.isSuccess() && !validateOnly) {
          (createTopicMetadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
        } else {
          (createTopicMetadata.topic, createTopicMetadata.error)
        }
      }.toMap
      responseCallback(results)
    } else {
      // 3. else pass the assignments and errors to the delayed operation and set the keys
      val delayedCreate = new DelayedCreatePartitions(timeout, metadata.toSeq, this, responseCallback)
      val delayedCreateKeys = createInfo.keys.map(new TopicKey(_)).toSeq
      // try to complete the request immediately, otherwise put it into the purgatory
      topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys)
    }
  }

  /**
    * Delete topics and wait until the topics have been completely deleted.
    * The callback function will be triggered either when timeout, error or the topics are deleted.
    */
  def deleteTopics(timeout: Int,
                   topics: Set[String],
                   responseCallback: Map[String, Errors] => Unit) {

    // 1. map over topics calling the asynchronous delete
    val metadata = topics.map { topic =>
        try {
          AdminUtils.deleteTopic(zkUtils, topic)
          DeleteTopicMetadata(topic, Errors.NONE)
        } catch {
          case _: TopicAlreadyMarkedForDeletionException =>
            // swallow the exception, and still track deletion allowing multiple calls to wait for deletion
            DeleteTopicMetadata(topic, Errors.NONE)
          case e: Throwable =>
            error(s"Error processing delete topic request for topic $topic", e)
            DeleteTopicMetadata(topic, Errors.forException(e))
        }
    }

    // 2. if timeout <= 0 or no topics can proceed return immediately
    if (timeout <= 0 || !metadata.exists(_.error == Errors.NONE)) {
      val results = metadata.map { deleteTopicMetadata =>
        // ignore topics that already have errors
        if (deleteTopicMetadata.error == Errors.NONE) {
          (deleteTopicMetadata.topic, Errors.REQUEST_TIMED_OUT)
        } else {
          (deleteTopicMetadata.topic, deleteTopicMetadata.error)
        }
      }.toMap
      responseCallback(results)
    } else {
      // 3. else pass the topics and errors to the delayed operation and set the keys
      val delayedDelete = new DelayedDeleteTopics(timeout, metadata.toSeq, this, responseCallback)
      val delayedDeleteKeys = topics.map(new TopicKey(_)).toSeq
      // try to complete the request immediately, otherwise put it into the purgatory
      topicPurgatory.tryCompleteElseWatch(delayedDelete, delayedDeleteKeys)
    }
  }

  def createPartitions(timeout: Int,
                       newPartitions: Map[String, NewPartitions],
                       validateOnly: Boolean,
                       listenerName: ListenerName,
                       callback: Map[String, ApiError] => Unit): Unit = {

    val reassignPartitionsInProgress = zkUtils.pathExists(ZkUtils.ReassignPartitionsPath)
    val allBrokers = AdminUtils.getBrokerMetadatas(zkUtils)
    val allBrokerIds = allBrokers.map(_.id)

    // 1. map over topics creating assignment and calling AdminUtils
    val metadata = newPartitions.map { case (topic, newPartition) =>
      try {
        // We prevent addition partitions while a reassignment is in progress, since
        // during reassignment there is no meaningful notion of replication factor
        if (reassignPartitionsInProgress)
          throw new ReassignmentInProgressException("A partition reassignment is in progress.")

        val existingAssignment = zkUtils.getReplicaAssignmentForTopics(List(topic)).map {
          case (topicPartition, replicas) => topicPartition.partition -> replicas
        }
        if (existingAssignment.isEmpty)
          throw new UnknownTopicOrPartitionException(s"The topic '$topic' does not exist.")

        val oldNumPartitions = existingAssignment.size
        val newNumPartitions = newPartition.totalCount
        val numPartitionsIncrement = newNumPartitions - oldNumPartitions
        if (numPartitionsIncrement < 0) {
          throw new InvalidPartitionsException(
            s"Topic currently has $oldNumPartitions partitions, which is higher than the requested $newNumPartitions.")
        } else if (numPartitionsIncrement == 0) {
          throw new InvalidPartitionsException(s"Topic already has $oldNumPartitions partitions.")
        }

        val reassignment = Option(newPartition.assignments).map(_.asScala.map(_.asScala.map(_.toInt))).map { assignments =>
          val unknownBrokers = assignments.flatten.toSet -- allBrokerIds
          if (unknownBrokers.nonEmpty)
            throw new InvalidReplicaAssignmentException(
              s"Unknown broker(s) in replica assignment: ${unknownBrokers.mkString(", ")}.")

          if (assignments.size != numPartitionsIncrement)
            throw new InvalidReplicaAssignmentException(
              s"Increasing the number of partitions by $numPartitionsIncrement " +
                s"but ${assignments.size} assignments provided.")

          assignments.zipWithIndex.map { case (replicas, index) =>
            existingAssignment.size + index -> replicas
          }.toMap
        }

        val updatedReplicaAssignment = AdminUtils.addPartitions(zkUtils, topic, existingAssignment, allBrokers,
          newPartition.totalCount, reassignment, validateOnly = validateOnly)
        CreatePartitionsMetadata(topic, updatedReplicaAssignment, ApiError.NONE)
      } catch {
        case e: AdminOperationException =>
          CreatePartitionsMetadata(topic, Map.empty, ApiError.fromThrowable(e))
        case e: ApiException =>
          CreatePartitionsMetadata(topic, Map.empty, ApiError.fromThrowable(e))
      }
    }

    // 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
    if (timeout <= 0 || validateOnly || !metadata.exists(_.error.is(Errors.NONE))) {
      val results = metadata.map { createPartitionMetadata =>
        // ignore topics that already have errors
        if (createPartitionMetadata.error.isSuccess() && !validateOnly) {
          (createPartitionMetadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
        } else {
          (createPartitionMetadata.topic, createPartitionMetadata.error)
        }
      }.toMap
      callback(results)
    } else {
      // 3. else pass the assignments and errors to the delayed operation and set the keys
      val delayedCreate = new DelayedCreatePartitions(timeout, metadata.toSeq, this, callback)
      val delayedCreateKeys = newPartitions.keySet.map(new TopicKey(_)).toSeq
      // try to complete the request immediately, otherwise put it into the purgatory
      topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys)
    }
  }

  def describeConfigs(resourceToConfigNames: Map[Resource, Option[Set[String]]]): Map[Resource, DescribeConfigsResponse.Config] = {
    resourceToConfigNames.map { case (resource, configNames) =>

      def createResponseConfig(config: AbstractConfig, isReadOnly: Boolean, isDefault: String => Boolean): DescribeConfigsResponse.Config = {
        val filteredConfigPairs = config.values.asScala.filter { case (configName, _) =>
          /* Always returns true if configNames is None */
          configNames.map(_.contains(configName)).getOrElse(true)
        }.toIndexedSeq

        val configEntries = filteredConfigPairs.map { case (name, value) =>
          val configEntryType = config.typeOf(name)
          val isSensitive = configEntryType == ConfigDef.Type.PASSWORD
          val valueAsString =
            if (isSensitive) null
            else ConfigDef.convertToString(value, configEntryType)
          new DescribeConfigsResponse.ConfigEntry(name, valueAsString, isSensitive, isDefault(name), isReadOnly)
        }

        new DescribeConfigsResponse.Config(ApiError.NONE, configEntries.asJava)
      }

      try {
        val resourceConfig = resource.`type` match {

          case ResourceType.TOPIC =>
            val topic = resource.name
            Topic.validate(topic)
            // Consider optimizing this by caching the configs or retrieving them from the `Log` when possible
            val topicProps = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)
            val logConfig = LogConfig.fromProps(KafkaServer.copyKafkaConfigToLog(config), topicProps)
            createResponseConfig(logConfig, isReadOnly = false, name => !topicProps.containsKey(name))

          case ResourceType.BROKER =>
            val brokerId = try resource.name.toInt catch {
              case _: NumberFormatException =>
                throw new InvalidRequestException(s"Broker id must be an integer, but it is: ${resource.name}")
            }
            if (brokerId == config.brokerId)
              createResponseConfig(config, isReadOnly = true, name => !config.originals.containsKey(name))
            else
              throw new InvalidRequestException(s"Unexpected broker id, expected ${config.brokerId}, but received $brokerId")

          case resourceType => throw new InvalidRequestException(s"Unsupported resource type: $resourceType")
        }
        resource -> resourceConfig
      } catch {
        case e: Throwable =>
          // Log client errors at a lower level than unexpected exceptions
          val message = s"Error processing describe configs request for resource $resource"
          if (e.isInstanceOf[ApiException])
            info(message, e)
          else
            error(message, e)
          resource -> new DescribeConfigsResponse.Config(ApiError.fromThrowable(e), Collections.emptyList[DescribeConfigsResponse.ConfigEntry])
      }
    }.toMap
  }

  def alterConfigs(configs: Map[Resource, AlterConfigsRequest.Config], validateOnly: Boolean): Map[Resource, ApiError] = {
    configs.map { case (resource, config) =>
      try {
        resource.`type` match {
          case ResourceType.TOPIC =>
            val topic = resource.name

            val properties = new Properties
            config.entries.asScala.foreach { configEntry =>
              properties.setProperty(configEntry.name(), configEntry.value())
            }

            alterConfigPolicy match {
              case Some(policy) =>
                AdminUtils.validateTopicConfig(zkUtils, topic, properties)

                val configEntriesMap = config.entries.asScala.map(entry => (entry.name, entry.value)).toMap
                policy.validate(new AlterConfigPolicy.RequestMetadata(
                  new ConfigResource(ConfigResource.Type.TOPIC, resource.name), configEntriesMap.asJava))

                if (!validateOnly)
                  AdminUtils.changeTopicConfig(zkUtils, topic, properties)
              case None =>
                if (validateOnly)
                  AdminUtils.validateTopicConfig(zkUtils, topic, properties)
                else
                  AdminUtils.changeTopicConfig(zkUtils, topic, properties)
            }
            resource -> ApiError.NONE
          case resourceType =>
            throw new InvalidRequestException(s"AlterConfigs is only supported for topics, but resource type is $resourceType")
        }
      } catch {
        case e: ConfigException =>
          val message = s"Invalid config value for resource $resource: ${e.getMessage}"
          info(message)
          resource -> ApiError.fromThrowable(new InvalidRequestException(message, e))
        case e: Throwable =>
          // Log client errors at a lower level than unexpected exceptions
          val message = s"Error processing alter configs request for resource $resource, config $config"
          if (e.isInstanceOf[ApiException] || e.isInstanceOf[PolicyViolationException])
            info(message, e)
          else
            error(message, e)
          resource -> ApiError.fromThrowable(e)
      }
    }.toMap
  }

  def shutdown() {
    topicPurgatory.shutdown()
    CoreUtils.swallow(createTopicPolicy.foreach(_.close()))
    CoreUtils.swallow(alterConfigPolicy.foreach(_.close()))
  }
}
