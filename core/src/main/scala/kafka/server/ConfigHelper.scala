/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.log.LogConfig
import kafka.server.metadata.ConfigRepository
import kafka.utils.{Log4jController, Logging}
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, ConfigResource, LogLevelConfig}
import org.apache.kafka.common.config.ConfigDef.ConfigKey
import org.apache.kafka.common.errors.{ApiException, InvalidConfigurationException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource
import org.apache.kafka.common.message.DescribeConfigsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ApiError, DescribeConfigsResponse, IncrementalAlterConfigsRequest}
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigSource

import scala.collection.{Map, mutable, Seq}
import scala.jdk.CollectionConverters._

class ConfigHelper(metadataCache: MetadataCache, config: KafkaConfig, configRepository: ConfigRepository) extends Logging {

  def getBrokerId(resource: ConfigResource) = {
    if (resource.name == null || resource.name.isEmpty)
      None
    else {
      Some(resourceNameToBrokerId(resource.name))
    }
  }

  def getAndValidateBrokerId(resource: ConfigResource) = {
    val id = getBrokerId(resource)
    if (id.nonEmpty && (id.get != this.config.brokerId))
      throw new InvalidRequestException(s"Unexpected broker id, expected ${this.config.brokerId}, but received ${resource.name}")
    id
  }

  def prepareIncrementalConfigs(alterConfigOps: Seq[AlterConfigOp], configProps: Properties, configKeys: Map[String, ConfigKey]): Unit = {

    def listType(configName: String, configKeys: Map[String, ConfigKey]): Boolean = {
      val configKey = configKeys(configName)
      if (configKey == null)
        throw new InvalidConfigurationException(s"Unknown topic config name: $configName")
      configKey.`type` == ConfigDef.Type.LIST
    }

    alterConfigOps.foreach { alterConfigOp =>
      val configPropName = alterConfigOp.configEntry.name
      alterConfigOp.opType() match {
        case OpType.SET => configProps.setProperty(alterConfigOp.configEntry.name, alterConfigOp.configEntry.value)
        case OpType.DELETE => configProps.remove(alterConfigOp.configEntry.name)
        case OpType.APPEND => {
          if (!listType(alterConfigOp.configEntry.name, configKeys))
            throw new InvalidRequestException(s"Config value append is not allowed for config key: ${alterConfigOp.configEntry.name}")
          val oldValueList = Option(configProps.getProperty(alterConfigOp.configEntry.name))
            .orElse(Option(ConfigDef.convertToString(configKeys(configPropName).defaultValue, ConfigDef.Type.LIST)))
            .getOrElse("")
            .split(",").toList
          val newValueList = oldValueList ::: alterConfigOp.configEntry.value.split(",").toList
          configProps.setProperty(alterConfigOp.configEntry.name, newValueList.mkString(","))
        }
        case OpType.SUBTRACT => {
          if (!listType(alterConfigOp.configEntry.name, configKeys))
            throw new InvalidRequestException(s"Config value subtract is not allowed for config key: ${alterConfigOp.configEntry.name}")
          val oldValueList = Option(configProps.getProperty(alterConfigOp.configEntry.name))
            .orElse(Option(ConfigDef.convertToString(configKeys(configPropName).defaultValue, ConfigDef.Type.LIST)))
            .getOrElse("")
            .split(",").toList
          val newValueList = oldValueList.diff(alterConfigOp.configEntry.value.split(",").toList)
          configProps.setProperty(alterConfigOp.configEntry.name, newValueList.mkString(","))
        }
      }
    }
  }

  def validateLogLevelConfigs(alterConfigOps: Seq[AlterConfigOp]): Unit = {
    def validateLoggerNameExists(loggerName: String): Unit = {
      if (!Log4jController.loggerExists(loggerName))
        throw new ConfigException(s"Logger $loggerName does not exist!")
    }

    alterConfigOps.foreach { alterConfigOp =>
      val loggerName = alterConfigOp.configEntry.name
      alterConfigOp.opType() match {
        case OpType.SET =>
          validateLoggerNameExists(loggerName)
          val logLevel = alterConfigOp.configEntry.value
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

  def toLoggableProps(resource: ConfigResource, configProps: Properties): Map[String, String] = {
    configProps.asScala.map {
      case (key, value) => (key, KafkaConfig.loggableValue(resource.`type`, key, value))
    }
  }

  def validateBrokerConfigs(resource: ConfigResource, 
                            validateOnly: Boolean, 
                            configProps: Properties): (ConfigResource, ApiError) = {
    val brokerId = getAndValidateBrokerId(resource)
    val perBrokerConfig = brokerId.nonEmpty
    // Validate and process the reconfiguration
    this.config.dynamicConfig.validate(configProps, perBrokerConfig)
    if (!validateOnly) {
      if (perBrokerConfig)
        this.config.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(configProps)
    }

    resource -> ApiError.NONE
  }

  def unmarshalIncrementalRequest(incrementalRequest: IncrementalAlterConfigsRequest): (Map[ConfigResource, Seq[AlterConfigOp]], Boolean) = {
    (incrementalRequest.data.resources.iterator.asScala.map { alterConfigResource =>
      val configResource = new ConfigResource(ConfigResource.Type.forId(alterConfigResource.resourceType),
        alterConfigResource.resourceName)
      configResource -> alterConfigResource.configs.iterator.asScala.map {
        alterConfig => new AlterConfigOp(new ConfigEntry(alterConfig.name, alterConfig.value),
          OpType.forId(alterConfig.configOperation))
      }.toBuffer
    }.toMap, incrementalRequest.data.validateOnly)
  }

  def handleValidateAlterConfigsError(exception: Throwable, resource: ConfigResource, configProps: Option[Properties], alterOps: Option[Seq[AlterConfigOp]]): (ConfigResource, ApiError) = {
    exception match {
      case e @ (_: ConfigException | _: IllegalArgumentException) =>
        val message = s"Invalid config value for resource $resource: ${e.getMessage}"
        info(message)
        resource -> ApiError.fromThrowable(new InvalidRequestException(message, e))
      case e: Throwable =>
        // Log client errors at a lower level than unexpected exceptions
        val configs = if (configProps.nonEmpty) toLoggableProps(resource, configProps.get).mkString(",") else alterOps.getOrElse("No incremental alter config operations define")
        val message = s"Error processing alter configs request for resource $resource, config $configs"
        if (e.isInstanceOf[ApiException])
          info(message, e)
        else
          error(message, e)
        resource -> ApiError.fromThrowable(e)
    }
  }

  def describeConfigs(resourceToConfigNames: List[DescribeConfigsResource],
                      includeSynonyms: Boolean,
                      includeDocumentation: Boolean): List[DescribeConfigsResponseData.DescribeConfigsResult] = {
    resourceToConfigNames.map { case resource =>

      def allConfigs(config: AbstractConfig) = {
        config.originals.asScala.filter(_._2 != null) ++ config.nonInternalValues.asScala
      }

      def createResponseConfig(configs: Map[String, Any],
                               createConfigEntry: (String, Any) => DescribeConfigsResponseData.DescribeConfigsResourceResult): DescribeConfigsResponseData.DescribeConfigsResult = {
        val filteredConfigPairs = if (resource.configurationKeys == null || resource.configurationKeys.isEmpty)
          configs.toBuffer
        else
          configs.filter { case (configName, _) =>
            resource.configurationKeys.asScala.contains(configName)
          }.toBuffer

        val configEntries = filteredConfigPairs.map { case (name, value) => createConfigEntry(name, value) }
        new DescribeConfigsResponseData.DescribeConfigsResult().setErrorCode(Errors.NONE.code)
          .setConfigs(configEntries.asJava)
      }

      try {
        val configResult = ConfigResource.Type.forId(resource.resourceType) match {
          case ConfigResource.Type.TOPIC =>
            val topic = resource.resourceName
            Topic.validate(topic)
            if (metadataCache.contains(topic)) {
              val topicProps = configRepository.topicConfig(topic)
              val logConfig = LogConfig.fromProps(LogConfig.extractLogConfigMap(config), topicProps)
              createResponseConfig(allConfigs(logConfig), createTopicConfigEntry(logConfig, topicProps, includeSynonyms, includeDocumentation))
            } else {
              new DescribeConfigsResponseData.DescribeConfigsResult().setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                .setConfigs(Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult])
            }

          case ConfigResource.Type.BROKER =>
            if (resource.resourceName == null || resource.resourceName.isEmpty)
              createResponseConfig(config.dynamicConfig.currentDynamicDefaultConfigs,
                createBrokerConfigEntry(perBrokerConfig = false, includeSynonyms, includeDocumentation))
            else if (resourceNameToBrokerId(resource.resourceName) == config.brokerId)
              createResponseConfig(allConfigs(config),
                createBrokerConfigEntry(perBrokerConfig = true, includeSynonyms, includeDocumentation))
            else
              throw new InvalidRequestException(s"Unexpected broker id, expected ${config.brokerId} or empty string, but received ${resource.resourceName}")

          case ConfigResource.Type.BROKER_LOGGER =>
            if (resource.resourceName == null || resource.resourceName.isEmpty)
              throw new InvalidRequestException("Broker id must not be empty")
            else if (resourceNameToBrokerId(resource.resourceName) != config.brokerId)
              throw new InvalidRequestException(s"Unexpected broker id, expected ${config.brokerId} but received ${resource.resourceName}")
            else
              createResponseConfig(Log4jController.loggers,
                (name, value) => new DescribeConfigsResponseData.DescribeConfigsResourceResult().setName(name)
                  .setValue(value.toString).setConfigSource(ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG.id)
                  .setIsSensitive(false).setReadOnly(false).setSynonyms(List.empty.asJava))
          case resourceType => throw new InvalidRequestException(s"Unsupported resource type: $resourceType")
        }
        configResult.setResourceName(resource.resourceName).setResourceType(resource.resourceType)
      } catch {
        case e: Throwable =>
          // Log client errors at a lower level than unexpected exceptions
          val message = s"Error processing describe configs request for resource $resource"
          if (e.isInstanceOf[ApiException])
            info(message, e)
          else
            error(message, e)
          val err = ApiError.fromThrowable(e)
          new DescribeConfigsResponseData.DescribeConfigsResult()
            .setResourceName(resource.resourceName)
            .setResourceType(resource.resourceType)
            .setErrorMessage(err.message)
            .setErrorCode(err.error.code)
            .setConfigs(Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult])
      }
    }
  }

  def createTopicConfigEntry(logConfig: LogConfig, topicProps: Properties, includeSynonyms: Boolean, includeDocumentation: Boolean)
                            (name: String, value: Any): DescribeConfigsResponseData.DescribeConfigsResourceResult = {
    val configEntryType = LogConfig.configType(name)
    val isSensitive = KafkaConfig.maybeSensitive(configEntryType)
    val valueAsString = if (isSensitive) null else ConfigDef.convertToString(value, configEntryType.orNull)
    val allSynonyms = {
      val list = LogConfig.TopicConfigSynonyms.get(name)
        .map(s => configSynonyms(s, brokerSynonyms(s), isSensitive))
        .getOrElse(List.empty)
      if (!topicProps.containsKey(name))
        list
      else
        new DescribeConfigsResponseData.DescribeConfigsSynonym().setName(name).setValue(valueAsString)
          .setSource(ConfigSource.TOPIC_CONFIG.id) +: list
    }
    val source = if (allSynonyms.isEmpty) ConfigSource.DEFAULT_CONFIG.id else allSynonyms.head.source
    val synonyms = if (!includeSynonyms) List.empty else allSynonyms
    val dataType = configResponseType(configEntryType)
    val configDocumentation = if (includeDocumentation) logConfig.documentationOf(name) else null
    new DescribeConfigsResponseData.DescribeConfigsResourceResult()
      .setName(name).setValue(valueAsString).setConfigSource(source)
      .setIsSensitive(isSensitive).setReadOnly(false).setSynonyms(synonyms.asJava)
      .setDocumentation(configDocumentation).setConfigType(dataType.id)
  }

  private def createBrokerConfigEntry(perBrokerConfig: Boolean, includeSynonyms: Boolean, includeDocumentation: Boolean)
                                     (name: String, value: Any): DescribeConfigsResponseData.DescribeConfigsResourceResult = {
    val allNames = brokerSynonyms(name)
    val configEntryType = KafkaConfig.configType(name)
    val isSensitive = KafkaConfig.maybeSensitive(configEntryType)
    val valueAsString = if (isSensitive)
      null
    else value match {
      case v: String => v
      case _ => ConfigDef.convertToString(value, configEntryType.orNull)
    }
    val allSynonyms = configSynonyms(name, allNames, isSensitive)
      .filter(perBrokerConfig || _.source == ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG.id)
    val synonyms = if (!includeSynonyms) List.empty else allSynonyms
    val source = if (allSynonyms.isEmpty) ConfigSource.DEFAULT_CONFIG.id else allSynonyms.head.source
    val readOnly = !DynamicBrokerConfig.AllDynamicConfigs.contains(name)

    val dataType = configResponseType(configEntryType)
    val configDocumentation = if (includeDocumentation) brokerDocumentation(name) else null
    new DescribeConfigsResponseData.DescribeConfigsResourceResult().setName(name).setValue(valueAsString).setConfigSource(source)
      .setIsSensitive(isSensitive).setReadOnly(readOnly).setSynonyms(synonyms.asJava)
      .setDocumentation(configDocumentation).setConfigType(dataType.id)
  }

  private def configSynonyms(name: String, synonyms: List[String], isSensitive: Boolean): List[DescribeConfigsResponseData.DescribeConfigsSynonym] = {
    val dynamicConfig = config.dynamicConfig
    val allSynonyms = mutable.Buffer[DescribeConfigsResponseData.DescribeConfigsSynonym]()

    def maybeAddSynonym(map: Map[String, String], source: ConfigSource)(name: String): Unit = {
      map.get(name).map { value =>
        val configValue = if (isSensitive) null else value
        allSynonyms += new DescribeConfigsResponseData.DescribeConfigsSynonym().setName(name).setValue(configValue).setSource(source.id)
      }
    }

    synonyms.foreach(maybeAddSynonym(dynamicConfig.currentDynamicBrokerConfigs, ConfigSource.DYNAMIC_BROKER_CONFIG))
    synonyms.foreach(maybeAddSynonym(dynamicConfig.currentDynamicDefaultConfigs, ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG))
    synonyms.foreach(maybeAddSynonym(dynamicConfig.staticBrokerConfigs, ConfigSource.STATIC_BROKER_CONFIG))
    synonyms.foreach(maybeAddSynonym(dynamicConfig.staticDefaultConfigs, ConfigSource.DEFAULT_CONFIG))
    allSynonyms.dropWhile(s => s.name != name).toList // e.g. drop listener overrides when describing base config
  }

  private def brokerSynonyms(name: String): List[String] = {
    DynamicBrokerConfig.brokerConfigSynonyms(name, matchListenerOverride = true)
  }

  private def brokerDocumentation(name: String): String = {
    config.documentationOf(name)
  }

  private def configResponseType(configType: Option[ConfigDef.Type]): DescribeConfigsResponse.ConfigType = {
    if (configType.isEmpty)
      DescribeConfigsResponse.ConfigType.UNKNOWN
    else configType.get match {
      case ConfigDef.Type.BOOLEAN => DescribeConfigsResponse.ConfigType.BOOLEAN
      case ConfigDef.Type.STRING => DescribeConfigsResponse.ConfigType.STRING
      case ConfigDef.Type.INT => DescribeConfigsResponse.ConfigType.INT
      case ConfigDef.Type.SHORT => DescribeConfigsResponse.ConfigType.SHORT
      case ConfigDef.Type.LONG => DescribeConfigsResponse.ConfigType.LONG
      case ConfigDef.Type.DOUBLE => DescribeConfigsResponse.ConfigType.DOUBLE
      case ConfigDef.Type.LIST => DescribeConfigsResponse.ConfigType.LIST
      case ConfigDef.Type.CLASS => DescribeConfigsResponse.ConfigType.CLASS
      case ConfigDef.Type.PASSWORD => DescribeConfigsResponse.ConfigType.PASSWORD
      case _ => DescribeConfigsResponse.ConfigType.UNKNOWN
    }
  }

  private def resourceNameToBrokerId(resourceName: String): Int = {
    try resourceName.toInt catch {
      case _: NumberFormatException =>
        throw new InvalidRequestException(s"Broker id must be an integer, but it is: $resourceName")
    }
  }
}
