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
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, ConfigResource}
import org.apache.kafka.common.errors.{ApiException, InvalidPartitionsException, InvalidReplicaAssignmentException, InvalidRequestException, ReassignmentInProgressException, UnknownTopicOrPartitionException, InvalidConfigurationException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.CreatePartitionsRequest.PartitionDetails
import org.apache.kafka.common.requests.CreateTopicsRequest._
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigSource
import org.apache.kafka.common.requests.{AlterConfigsRequest, ApiError, DescribeConfigsResponse}
import org.apache.kafka.server.policy.{AlterConfigPolicy, CreateTopicPolicy}
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata

import scala.collection._
import scala.collection.JavaConverters._

class AdminManager(val config: KafkaConfig,
                   val metrics: Metrics,
                   val metadataCache: MetadataCache,
                   val zkClient: KafkaZkClient) extends Logging with KafkaMetricsGroup {

  this.logIdent = "[Admin Manager on Broker " + config.brokerId + "]: "

  private val topicPurgatory = DelayedOperationPurgatory[DelayedOperation]("topic", config.brokerId)
  private val adminZkClient = new AdminZkClient(zkClient)

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
            adminZkClient.validateTopicCreate(topic, assignments, configs)

            // Use `null` for unset fields in the public API
            val numPartitions: java.lang.Integer =
              if (arguments.numPartitions == NO_NUM_PARTITIONS) null else arguments.numPartitions
            val replicationFactor: java.lang.Short =
              if (arguments.replicationFactor == NO_REPLICATION_FACTOR) null else arguments.replicationFactor
            val replicaAssignments = if (arguments.replicasAssignments.isEmpty) null else arguments.replicasAssignments

            policy.validate(new RequestMetadata(topic, numPartitions, replicationFactor, replicaAssignments,
              arguments.configs))

            if (!validateOnly)
              adminZkClient.createTopicWithAssignment(topic, configs, assignments)

          case None =>
            if (validateOnly)
              adminZkClient.validateTopicCreate(topic, assignments, configs)
            else
              adminZkClient.createTopicWithAssignment(topic, configs, assignments)
        }
        CreatePartitionsMetadata(topic, assignments, ApiError.NONE)
      } catch {
        // Log client errors at a lower level than unexpected exceptions
        case e: ApiException =>
          info(s"Error processing create topic request for topic $topic with arguments $arguments", e)
          CreatePartitionsMetadata(topic, Map(), ApiError.fromThrowable(e))
        case e: ConfigException =>
          info(s"Error processing create topic request for topic $topic with arguments $arguments", e)
          CreatePartitionsMetadata(topic, Map(), ApiError.fromThrowable(new InvalidConfigurationException(e.getMessage, e.getCause)))
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
          adminZkClient.deleteTopic(topic)
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
                       newPartitions: Map[String, PartitionDetails],
                       validateOnly: Boolean,
                       listenerName: ListenerName,
                       callback: Map[String, ApiError] => Unit): Unit = {

    val reassignPartitionsInProgress = zkClient.reassignPartitionsInProgress
    val allBrokers = adminZkClient.getBrokerMetadatas()
    val allBrokerIds = allBrokers.map(_.id)

    // 1. map over topics creating assignment and calling AdminUtils
    val metadata = newPartitions.map { case (topic, newPartition) =>
      try {
        // We prevent addition partitions while a reassignment is in progress, since
        // during reassignment there is no meaningful notion of replication factor
        if (reassignPartitionsInProgress)
          throw new ReassignmentInProgressException("A partition reassignment is in progress.")

        val existingAssignment = zkClient.getReplicaAssignmentForTopics(immutable.Set(topic)).map {
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

        val reassignment = Option(newPartition.newAssignments).map(_.asScala.map(_.asScala.map(_.toInt))).map { assignments =>
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

        val updatedReplicaAssignment = adminZkClient.addPartitions(topic, existingAssignment, allBrokers,
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

  def describeConfigs(resourceToConfigNames: Map[ConfigResource, Option[Set[String]]], includeSynonyms: Boolean): Map[ConfigResource, DescribeConfigsResponse.Config] = {
    resourceToConfigNames.map { case (resource, configNames) =>

      def allConfigs(config: AbstractConfig) = {
        config.originals.asScala.filter(_._2 != null) ++ config.values.asScala
      }
      def createResponseConfig(configs: Map[String, Any],
                               createConfigEntry: (String, Any) => DescribeConfigsResponse.ConfigEntry): DescribeConfigsResponse.Config = {
        val filteredConfigPairs = configs.filter { case (configName, _) =>
          /* Always returns true if configNames is None */
          configNames.forall(_.contains(configName))
        }.toIndexedSeq

        val configEntries = filteredConfigPairs.map { case (name, value) => createConfigEntry(name, value) }
        new DescribeConfigsResponse.Config(ApiError.NONE, configEntries.asJava)
      }

      try {
        val resourceConfig = resource.`type` match {

          case ConfigResource.Type.TOPIC =>
            val topic = resource.name
            Topic.validate(topic)
            if (metadataCache.contains(topic)) {
              // Consider optimizing this by caching the configs or retrieving them from the `Log` when possible
              val topicProps = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
              val logConfig = LogConfig.fromProps(KafkaServer.copyKafkaConfigToLog(config), topicProps)
              createResponseConfig(allConfigs(logConfig), createTopicConfigEntry(logConfig, topicProps, includeSynonyms))
            } else {
              new DescribeConfigsResponse.Config(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, null), Collections.emptyList[DescribeConfigsResponse.ConfigEntry])
            }

          case ConfigResource.Type.BROKER =>
            if (resource.name == null || resource.name.isEmpty)
              createResponseConfig(config.dynamicConfig.currentDynamicDefaultConfigs,
                createBrokerConfigEntry(perBrokerConfig = false, includeSynonyms))
            else if (resourceNameToBrokerId(resource.name) == config.brokerId)
              createResponseConfig(allConfigs(config),
                createBrokerConfigEntry(perBrokerConfig = true, includeSynonyms))
            else
              throw new InvalidRequestException(s"Unexpected broker id, expected ${config.brokerId} or empty string, but received $resource.name")

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

  def alterConfigs(configs: Map[ConfigResource, AlterConfigsRequest.Config], validateOnly: Boolean): Map[ConfigResource, ApiError] = {
    configs.map { case (resource, config) =>

      def validateConfigPolicy(resourceType: ConfigResource.Type): Unit = {
        alterConfigPolicy match {
          case Some(policy) =>
            val configEntriesMap = config.entries.asScala.map(entry => (entry.name, entry.value)).toMap
            policy.validate(new AlterConfigPolicy.RequestMetadata(
              new ConfigResource(resourceType, resource.name), configEntriesMap.asJava))
          case None =>
        }
      }
      try {
        resource.`type` match {
          case ConfigResource.Type.TOPIC =>
            val topic = resource.name

            val properties = new Properties
            config.entries.asScala.foreach { configEntry =>
              properties.setProperty(configEntry.name, configEntry.value)
            }

            adminZkClient.validateTopicConfig(topic, properties)
            validateConfigPolicy(ConfigResource.Type.TOPIC)
            if (!validateOnly) {
              info(s"Updating topic $topic with new configuration $config")
              adminZkClient.changeTopicConfig(topic, properties)
            }

            resource -> ApiError.NONE

          case ConfigResource.Type.BROKER =>
            val brokerId = if (resource.name == null || resource.name.isEmpty)
              None
            else {
              val id = resourceNameToBrokerId(resource.name)
              if (id != this.config.brokerId)
                throw new InvalidRequestException(s"Unexpected broker id, expected ${this.config.brokerId}, but received $resource.name")
              Some(id)
            }
            val configProps = new Properties
            config.entries.asScala.foreach { configEntry =>
              configProps.setProperty(configEntry.name, configEntry.value)
            }

            val perBrokerConfig = brokerId.nonEmpty
            this.config.dynamicConfig.validate(configProps, perBrokerConfig)
            validateConfigPolicy(ConfigResource.Type.BROKER)
            if (!validateOnly) {
              if (perBrokerConfig)
                this.config.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(configProps)
              adminZkClient.changeBrokerConfig(brokerId,
                this.config.dynamicConfig.toPersistentProps(configProps, perBrokerConfig))
            }

            resource -> ApiError.NONE
          case resourceType =>
            throw new InvalidRequestException(s"AlterConfigs is only supported for topics and brokers, but resource type is $resourceType")
        }
      } catch {
        case e @ (_: ConfigException | _: IllegalArgumentException) =>
          val message = s"Invalid config value for resource $resource: ${e.getMessage}"
          info(message)
          resource -> ApiError.fromThrowable(new InvalidRequestException(message, e))
        case e: Throwable =>
          // Log client errors at a lower level than unexpected exceptions
          val message = s"Error processing alter configs request for resource $resource, config $config"
          if (e.isInstanceOf[ApiException])
            info(message, e)
          else
            error(message, e)
          resource -> ApiError.fromThrowable(e)
      }
    }.toMap
  }

  def shutdown() {
    topicPurgatory.shutdown()
    CoreUtils.swallow(createTopicPolicy.foreach(_.close()), this)
    CoreUtils.swallow(alterConfigPolicy.foreach(_.close()), this)
  }

  private def resourceNameToBrokerId(resourceName: String): Int = {
    try resourceName.toInt catch {
      case _: NumberFormatException =>
        throw new InvalidRequestException(s"Broker id must be an integer, but it is: $resourceName")
    }
  }

  private def brokerSynonyms(name: String): List[String] = {
    DynamicBrokerConfig.brokerConfigSynonyms(name, matchListenerOverride = true)
  }

  private def configType(name: String, synonyms: List[String]): ConfigDef.Type = {
    val configType = config.typeOf(name)
    if (configType != null)
      configType
    else
      synonyms.iterator.map(config.typeOf).find(_ != null).orNull
  }

  private def configSynonyms(name: String, synonyms: List[String], isSensitive: Boolean): List[DescribeConfigsResponse.ConfigSynonym] = {
    val dynamicConfig = config.dynamicConfig
    val allSynonyms = mutable.Buffer[DescribeConfigsResponse.ConfigSynonym]()

    def maybeAddSynonym(map: Map[String, String], source: ConfigSource)(name: String): Unit = {
      map.get(name).map { value =>
        val configValue = if (isSensitive) null else value
        allSynonyms += new DescribeConfigsResponse.ConfigSynonym(name, configValue, source)
      }
    }

    synonyms.foreach(maybeAddSynonym(dynamicConfig.currentDynamicBrokerConfigs, ConfigSource.DYNAMIC_BROKER_CONFIG))
    synonyms.foreach(maybeAddSynonym(dynamicConfig.currentDynamicDefaultConfigs, ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG))
    synonyms.foreach(maybeAddSynonym(dynamicConfig.staticBrokerConfigs, ConfigSource.STATIC_BROKER_CONFIG))
    synonyms.foreach(maybeAddSynonym(dynamicConfig.staticDefaultConfigs, ConfigSource.DEFAULT_CONFIG))
    allSynonyms.dropWhile(s => s.name != name).toList // e.g. drop listener overrides when describing base config
  }

  private def createTopicConfigEntry(logConfig: LogConfig, topicProps: Properties, includeSynonyms: Boolean)
                                    (name: String, value: Any): DescribeConfigsResponse.ConfigEntry = {
    val configEntryType = logConfig.typeOf(name)
    val isSensitive = configEntryType == ConfigDef.Type.PASSWORD
    val valueAsString = if (isSensitive) null else ConfigDef.convertToString(value, configEntryType)
    val allSynonyms = {
      val list = LogConfig.TopicConfigSynonyms.get(name)
        .map(s => configSynonyms(s, brokerSynonyms(s), isSensitive))
        .getOrElse(List.empty)
      if (!topicProps.containsKey(name))
        list
      else
        new DescribeConfigsResponse.ConfigSynonym(name, valueAsString, ConfigSource.TOPIC_CONFIG) +: list
    }
    val source = if (allSynonyms.isEmpty) ConfigSource.DEFAULT_CONFIG else allSynonyms.head.source
    val synonyms = if (!includeSynonyms) List.empty else allSynonyms
    new DescribeConfigsResponse.ConfigEntry(name, valueAsString, source, isSensitive, false, synonyms.asJava)
  }

  private def createBrokerConfigEntry(perBrokerConfig: Boolean, includeSynonyms: Boolean)
                                     (name: String, value: Any): DescribeConfigsResponse.ConfigEntry = {
    val allNames = brokerSynonyms(name)
    val configEntryType = configType(name, allNames)
    // If we can't determine the config entry type, treat it as a sensitive config to be safe
    val isSensitive = configEntryType == ConfigDef.Type.PASSWORD || configEntryType == null
    val valueAsString = if (isSensitive)
      null
    else value match {
      case v: String => v
      case _ => ConfigDef.convertToString(value, configEntryType)
    }
    val allSynonyms = configSynonyms(name, allNames, isSensitive)
        .filter(perBrokerConfig || _.source == ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
    val synonyms = if (!includeSynonyms) List.empty else allSynonyms
    val source = if (allSynonyms.isEmpty) ConfigSource.DEFAULT_CONFIG else allSynonyms.head.source
    val readOnly = !allNames.exists(DynamicBrokerConfig.AllDynamicConfigs.contains)
    new DescribeConfigsResponse.ConfigEntry(name, valueAsString, source, isSensitive, readOnly, synonyms.asJava)
  }
}
