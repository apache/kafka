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
import kafka.utils.Log4jController
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.config.ConfigDef.ConfigKey
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, ConfigResource, LogLevelConfig}
import org.apache.kafka.common.errors.{ApiException, InvalidConfigurationException, InvalidPartitionsException, InvalidReplicaAssignmentException, InvalidRequestException, ReassignmentInProgressException, TopicExistsException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData.{CreatableTopicConfigs, CreatableTopicResult}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.CreatePartitionsRequest.PartitionDetails
import org.apache.kafka.common.requests.CreateTopicsRequest._
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigSource
import org.apache.kafka.common.requests.{AlterConfigsRequest, ApiError, DescribeConfigsResponse}
import org.apache.kafka.server.policy.{AlterConfigPolicy, CreateTopicPolicy}
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata

import scala.collection.{Map, mutable, _}
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

  def hasDelayedTopicOperations = topicPurgatory.numDelayed != 0

  private val defaultNumPartitions = config.numPartitions.intValue()
  private val defaultReplicationFactor = config.defaultReplicationFactor.shortValue()

  /**
    * Try to complete delayed topic operations with the request key
    */
  def tryCompleteDelayedTopicOperations(topic: String): Unit = {
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
                   toCreate: Map[String, CreatableTopic],
                   includeConfigsAndMetatadata: Map[String, CreatableTopicResult],
                   responseCallback: Map[String, ApiError] => Unit): Unit = {

    // 1. map over topics creating assignment and calling zookeeper
    val brokers = metadataCache.getAliveBrokers.map { b => kafka.admin.BrokerMetadata(b.id, b.rack) }
    val metadata = toCreate.values.map(topic =>
      try {
        if (metadataCache.contains(topic.name))
          throw new TopicExistsException(s"Topic '${topic.name}' already exists.")

        val configs = new Properties()
        topic.configs.asScala.foreach { entry =>
          configs.setProperty(entry.name, entry.value)
        }
        LogConfig.validate(configs)

        if ((topic.numPartitions != NO_NUM_PARTITIONS || topic.replicationFactor != NO_REPLICATION_FACTOR)
            && !topic.assignments().isEmpty) {
          throw new InvalidRequestException("Both numPartitions or replicationFactor and replicasAssignments were set. " +
            "Both cannot be used at the same time.")
        }

        val resolvedNumPartitions = if (topic.numPartitions == NO_NUM_PARTITIONS)
          defaultNumPartitions else topic.numPartitions
        val resolvedReplicationFactor = if (topic.replicationFactor == NO_REPLICATION_FACTOR)
          defaultReplicationFactor else topic.replicationFactor

        val assignments = if (topic.assignments().isEmpty) {
          AdminUtils.assignReplicasToBrokers(
            brokers, resolvedNumPartitions, resolvedReplicationFactor)
        } else {
          val assignments = new mutable.HashMap[Int, Seq[Int]]
          // Note: we don't check that replicaAssignment contains unknown brokers - unlike in add-partitions case,
          // this follows the existing logic in TopicCommand
          topic.assignments.asScala.foreach {
            case assignment => assignments(assignment.partitionIndex()) =
              assignment.brokerIds().asScala.map(a => a: Int)
          }
          assignments
        }
        trace(s"Assignments for topic $topic are $assignments ")

        createTopicPolicy match {
          case Some(policy) =>
            adminZkClient.validateTopicCreate(topic.name(), assignments, configs)

            // Use `null` for unset fields in the public API
            val numPartitions: java.lang.Integer =
              if (topic.assignments().isEmpty) resolvedNumPartitions else null
            val replicationFactor: java.lang.Short =
              if (topic.assignments().isEmpty) resolvedReplicationFactor else null
            val javaAssignments = if (topic.assignments().isEmpty) {
              null
            } else {
              assignments.map { case (k, v) =>
                (k: java.lang.Integer) -> v.map(i => i: java.lang.Integer).asJava
              }.asJava
            }
            val javaConfigs = new java.util.HashMap[String, String]
            topic.configs().asScala.foreach(config => javaConfigs.put(config.name(), config.value()))
            policy.validate(new RequestMetadata(topic.name, numPartitions, replicationFactor,
              javaAssignments, javaConfigs))

            if (!validateOnly)
              adminZkClient.createTopicWithAssignment(topic.name, configs, assignments)

          case None =>
            if (validateOnly)
              adminZkClient.validateTopicCreate(topic.name, assignments, configs)
            else
              adminZkClient.createTopicWithAssignment(topic.name, configs, assignments)
        }

        // For responses with DescribeConfigs permission, populate metadata and configs
        includeConfigsAndMetatadata.get(topic.name).foreach { result =>
          val logConfig = LogConfig.fromProps(KafkaServer.copyKafkaConfigToLog(config), configs)
          val createEntry = createTopicConfigEntry(logConfig, configs, includeSynonyms = false)(_, _)
          val topicConfigs = logConfig.values.asScala.map { case (k, v) =>
            val entry = createEntry(k, v)
            val source = ConfigSource.values.indices.map(_.toByte)
              .find(i => ConfigSource.forId(i.toByte) == entry.source)
              .getOrElse(0.toByte)
            new CreatableTopicConfigs()
                .setName(k)
                .setValue(entry.value)
                .setIsSensitive(entry.isSensitive)
                .setReadOnly(entry.isReadOnly)
                .setConfigSource(source)
          }.toList.asJava
          result.setConfigs(topicConfigs)
          result.setNumPartitions(assignments.size)
          result.setReplicationFactor(assignments(0).size.toShort)
        }
        CreatePartitionsMetadata(topic.name, assignments, ApiError.NONE)
      } catch {
        // Log client errors at a lower level than unexpected exceptions
        case e: ApiException =>
          info(s"Error processing create topic request $topic", e)
          CreatePartitionsMetadata(topic.name, Map(), ApiError.fromThrowable(e))
        case e: ConfigException =>
          info(s"Error processing create topic request $topic", e)
          CreatePartitionsMetadata(topic.name, Map(), ApiError.fromThrowable(new InvalidConfigurationException(e.getMessage, e.getCause)))
        case e: Throwable =>
          error(s"Error processing create topic request $topic", e)
          CreatePartitionsMetadata(topic.name, Map(), ApiError.fromThrowable(e))
      }).toBuffer

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
      val delayedCreate = new DelayedCreatePartitions(timeout, metadata, this, responseCallback)
      val delayedCreateKeys = toCreate.values.map(topic => new TopicKey(topic.name)).toBuffer
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
                   responseCallback: Map[String, Errors] => Unit): Unit = {

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

    val allBrokers = adminZkClient.getBrokerMetadatas()
    val allBrokerIds = allBrokers.map(_.id)

    // 1. map over topics creating assignment and calling AdminUtils
    val metadata = newPartitions.map { case (topic, newPartition) =>
      try {
        val existingAssignment = zkClient.getFullReplicaAssignmentForTopics(immutable.Set(topic)).map {
          case (topicPartition, assignment) =>
            if (assignment.isBeingReassigned) {
              // We prevent adding partitions while topic reassignment is in progress, to protect from a race condition
              // between the controller thread processing reassignment update and createPartitions(this) request.
              throw new ReassignmentInProgressException(s"A partition reassignment is in progress for the topic '$topic'.")
            }
            topicPartition.partition -> assignment
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

        val newPartitionsAssignment = Option(newPartition.newAssignments).map(_.asScala.map(_.asScala.map(_.toInt))).map { assignments =>
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
          newPartition.totalCount, newPartitionsAssignment, validateOnly = validateOnly)
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
        }.toBuffer

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
              throw new InvalidRequestException(s"Unexpected broker id, expected ${config.brokerId} or empty string, but received ${resource.name}")

          case ConfigResource.Type.BROKER_LOGGER =>
            if (resource.name == null || resource.name.isEmpty)
              throw new InvalidRequestException("Broker id must not be empty")
            else if (resourceNameToBrokerId(resource.name) != config.brokerId)
              throw new InvalidRequestException(s"Unexpected broker id, expected ${config.brokerId} but received ${resource.name}")
            else
              createResponseConfig(Log4jController.loggers,
                (name, value) => new DescribeConfigsResponse.ConfigEntry(name, value.toString, ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG, false, false, List.empty.asJava))
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

      try {
        val configEntriesMap = config.entries.asScala.map(entry => (entry.name, entry.value)).toMap

        val configProps = new Properties
        config.entries.asScala.foreach { configEntry =>
          configProps.setProperty(configEntry.name, configEntry.value)
        }

        resource.`type` match {
          case ConfigResource.Type.TOPIC => alterTopicConfigs(resource, validateOnly, configProps, configEntriesMap)
          case ConfigResource.Type.BROKER => alterBrokerConfigs(resource, validateOnly, configProps, configEntriesMap)
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

  private def alterTopicConfigs(resource: ConfigResource, validateOnly: Boolean,
                                configProps: Properties, configEntriesMap: Map[String, String]): (ConfigResource, ApiError) = {
    val topic = resource.name
    adminZkClient.validateTopicConfig(topic, configProps)
    validateConfigPolicy(resource, configEntriesMap)
    if (!validateOnly) {
      info(s"Updating topic $topic with new configuration $config")
      adminZkClient.changeTopicConfig(topic, configProps)
    }

    resource -> ApiError.NONE
  }

  private def alterBrokerConfigs(resource: ConfigResource, validateOnly: Boolean,
                                 configProps: Properties, configEntriesMap: Map[String, String]): (ConfigResource, ApiError) = {
    val brokerId = getBrokerId(resource)
    val perBrokerConfig = brokerId.nonEmpty
    this.config.dynamicConfig.validate(configProps, perBrokerConfig)
    validateConfigPolicy(resource, configEntriesMap)
    if (!validateOnly) {
      if (perBrokerConfig)
        this.config.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(configProps)
      adminZkClient.changeBrokerConfig(brokerId,
        this.config.dynamicConfig.toPersistentProps(configProps, perBrokerConfig))
    }

    resource -> ApiError.NONE
  }

  private def alterLogLevelConfigs(alterConfigOps: List[AlterConfigOp]): Unit = {
    alterConfigOps.foreach { alterConfigOp =>
      val loggerName = alterConfigOp.configEntry().name()
      val logLevel = alterConfigOp.configEntry().value()
      alterConfigOp.opType() match {
        case OpType.SET => Log4jController.logLevel(loggerName, logLevel)
        case OpType.DELETE => Log4jController.unsetLogLevel(loggerName)
      }
    }
  }

  private def getBrokerId(resource: ConfigResource) = {
    if (resource.name == null || resource.name.isEmpty)
      None
    else {
      val id = resourceNameToBrokerId(resource.name)
      if (id != this.config.brokerId)
        throw new InvalidRequestException(s"Unexpected broker id, expected ${this.config.brokerId}, but received ${resource.name}")
      Some(id)
    }
  }

  private def validateConfigPolicy(resource: ConfigResource, configEntriesMap: Map[String, String]): Unit = {
    alterConfigPolicy match {
      case Some(policy) =>
        policy.validate(new AlterConfigPolicy.RequestMetadata(
          new ConfigResource(resource.`type`(), resource.name), configEntriesMap.asJava))
      case None =>
    }
  }

  def incrementalAlterConfigs(configs: Map[ConfigResource, List[AlterConfigOp]], validateOnly: Boolean): Map[ConfigResource, ApiError] = {
    configs.map { case (resource, alterConfigOps) =>
      try {
        // throw InvalidRequestException if any duplicate keys
        val duplicateKeys = alterConfigOps.groupBy(config => config.configEntry().name())
          .mapValues(_.size).filter(_._2 > 1).keys.toSet
        if (duplicateKeys.nonEmpty)
          throw new InvalidRequestException(s"Error due to duplicate config keys : ${duplicateKeys.mkString(",")}")

        val configEntriesMap = alterConfigOps.map(entry => (entry.configEntry().name(), entry.configEntry().value())).toMap

        resource.`type` match {
          case ConfigResource.Type.TOPIC =>
            val configProps = adminZkClient.fetchEntityConfig(ConfigType.Topic, resource.name)
            prepareIncrementalConfigs(alterConfigOps, configProps, LogConfig.configKeys)
            alterTopicConfigs(resource, validateOnly, configProps, configEntriesMap)

          case ConfigResource.Type.BROKER =>
            val brokerId = getBrokerId(resource)
            val perBrokerConfig = brokerId.nonEmpty

            val persistentProps = if (perBrokerConfig) adminZkClient.fetchEntityConfig(ConfigType.Broker, brokerId.get.toString)
            else adminZkClient.fetchEntityConfig(ConfigType.Broker, ConfigEntityName.Default)

            val configProps = this.config.dynamicConfig.fromPersistentProps(persistentProps, perBrokerConfig)
            prepareIncrementalConfigs(alterConfigOps, configProps, KafkaConfig.configKeys)
            alterBrokerConfigs(resource, validateOnly, configProps, configEntriesMap)

          case ConfigResource.Type.BROKER_LOGGER =>
            getBrokerId(resource)
            validateLogLevelConfigs(alterConfigOps)

            if (!validateOnly)
              alterLogLevelConfigs(alterConfigOps)
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
          val message = s"Error processing alter configs request for resource $resource, config $alterConfigOps"
          if (e.isInstanceOf[ApiException])
            info(message, e)
          else
            error(message, e)
          resource -> ApiError.fromThrowable(e)
      }
    }.toMap
  }

  private def validateLogLevelConfigs(alterConfigOps: List[AlterConfigOp]): Unit = {
    def validateLoggerNameExists(loggerName: String): Unit = {
      if (!Log4jController.loggerExists(loggerName))
        throw new ConfigException(s"Logger $loggerName does not exist!")
    }

    alterConfigOps.foreach { alterConfigOp =>
      val loggerName = alterConfigOp.configEntry().name()
      alterConfigOp.opType() match {
        case OpType.SET =>
          validateLoggerNameExists(loggerName)
          val logLevel = alterConfigOp.configEntry().value()
          if (!LogLevelConfig.VALID_LOG_LEVELS.contains(logLevel)) {
            val validLevelsStr = LogLevelConfig.VALID_LOG_LEVELS.asScala.mkString(", ")
            throw new ConfigException(
              s"Cannot set the log level of $loggerName to $logLevel as it is not a supported log level. " +
              s"Valid log levels are $validLevelsStr"
            )
          }
        case OpType.DELETE =>
          validateLoggerNameExists(loggerName)
          if (loggerName == Log4jController.ROOT_LOGGER)
            throw new InvalidRequestException(s"Removing the log level of the ${Log4jController.ROOT_LOGGER} logger is not allowed")
        case OpType.APPEND => throw new InvalidRequestException(s"${OpType.APPEND} operation is not allowed for the ${ConfigResource.Type.BROKER_LOGGER} resource")
        case OpType.SUBTRACT => throw new InvalidRequestException(s"${OpType.SUBTRACT} operation is not allowed for the ${ConfigResource.Type.BROKER_LOGGER} resource")
      }
    }
  }

  private def prepareIncrementalConfigs(alterConfigOps: List[AlterConfigOp], configProps: Properties, configKeys: Map[String, ConfigKey]): Unit = {

    def listType(configName: String, configKeys: Map[String, ConfigKey]): Boolean = {
      val configKey = configKeys(configName)
      if (configKey == null)
        throw new InvalidConfigurationException(s"Unknown topic config name: $configName")
      configKey.`type` == ConfigDef.Type.LIST
    }

    alterConfigOps.foreach { alterConfigOp =>
      alterConfigOp.opType() match {
        case OpType.SET => configProps.setProperty(alterConfigOp.configEntry().name(), alterConfigOp.configEntry().value())
        case OpType.DELETE => configProps.remove(alterConfigOp.configEntry().name())
        case OpType.APPEND => {
          if (!listType(alterConfigOp.configEntry().name(), configKeys))
            throw new InvalidRequestException(s"Config value append is not allowed for config key: ${alterConfigOp.configEntry().name()}")
          val oldValueList = configProps.getProperty(alterConfigOp.configEntry().name()).split(",").toList
          val newValueList = oldValueList ::: alterConfigOp.configEntry().value().split(",").toList
          configProps.setProperty(alterConfigOp.configEntry().name(), newValueList.mkString(","))
        }
        case OpType.SUBTRACT => {
          if (!listType(alterConfigOp.configEntry().name(), configKeys))
            throw new InvalidRequestException(s"Config value subtract is not allowed for config key: ${alterConfigOp.configEntry().name()}")
          val oldValueList = configProps.getProperty(alterConfigOp.configEntry().name()).split(",").toList
          val newValueList = oldValueList.diff(alterConfigOp.configEntry().value().split(",").toList)
          configProps.setProperty(alterConfigOp.configEntry().name(), newValueList.mkString(","))
        }
      }
    }
  }

  def shutdown(): Unit = {
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
    val readOnly = !DynamicBrokerConfig.AllDynamicConfigs.contains(name)
    new DescribeConfigsResponse.ConfigEntry(name, valueAsString, source, isSensitive, readOnly, synonyms.asJava)
  }
}
