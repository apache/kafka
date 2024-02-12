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

import kafka.network.RequestChannel

import java.util.{Collections, Properties}
import kafka.server.metadata.ConfigRepository
import kafka.utils.{Log4jController, Logging}
import org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigResource}
import org.apache.kafka.common.errors.{ApiException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource
import org.apache.kafka.common.message.DescribeConfigsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ApiError, DescribeConfigsRequest, DescribeConfigsResponse}
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigSource
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, TOPIC}
import org.apache.kafka.server.config.ServerTopicConfigSynonyms
import org.apache.kafka.storage.internals.log.LogConfig

import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

class ConfigHelper(metadataCache: MetadataCache, config: KafkaConfig, configRepository: ConfigRepository) extends Logging {

  def allConfigs(config: AbstractConfig): mutable.Map[String, Any] = {
    config.originals.asScala.filter(_._2 != null) ++ config.nonInternalValues.asScala
  }

  def handleDescribeConfigsRequest(
    request: RequestChannel.Request,
    authHelper: AuthHelper
  ): DescribeConfigsResponseData = {
    val describeConfigsRequest = request.body[DescribeConfigsRequest]
    val (authorizedResources, unauthorizedResources) = describeConfigsRequest.data.resources.asScala.partition { resource =>
      ConfigResource.Type.forId(resource.resourceType) match {
        case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER | ConfigResource.Type.CLIENT_METRICS =>
          authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)
        case ConfigResource.Type.TOPIC =>
          authHelper.authorize(request.context, DESCRIBE_CONFIGS, TOPIC, resource.resourceName)
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.resourceName}")
      }
    }
    val authorizedConfigs = describeConfigs(authorizedResources.toList, describeConfigsRequest.data.includeSynonyms, describeConfigsRequest.data.includeDocumentation)
    val unauthorizedConfigs = unauthorizedResources.map { resource =>
      val error = ConfigResource.Type.forId(resource.resourceType) match {
        case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER | ConfigResource.Type.CLIENT_METRICS => Errors.CLUSTER_AUTHORIZATION_FAILED
        case ConfigResource.Type.TOPIC => Errors.TOPIC_AUTHORIZATION_FAILED
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.resourceName}")
      }
      new DescribeConfigsResponseData.DescribeConfigsResult().setErrorCode(error.code)
        .setErrorMessage(error.message)
        .setConfigs(Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult])
        .setResourceName(resource.resourceName)
        .setResourceType(resource.resourceType)
    }
    new DescribeConfigsResponseData().setResults((authorizedConfigs ++ unauthorizedConfigs).asJava)
  }

  def describeConfigs(resourceToConfigNames: List[DescribeConfigsResource],
                      includeSynonyms: Boolean,
                      includeDocumentation: Boolean): List[DescribeConfigsResponseData.DescribeConfigsResult] = {
    resourceToConfigNames.map { resource =>

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
              val logConfig = LogConfig.fromProps(config.extractLogConfigMap, topicProps)
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

          case ConfigResource.Type.CLIENT_METRICS =>
            val subscriptionName = resource.resourceName
            if (subscriptionName == null || subscriptionName.isEmpty) {
              throw new InvalidRequestException("Client metrics subscription name must not be empty")
            } else {
              val entityProps = configRepository.config(new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName))
              val configEntries = new  ListBuffer[DescribeConfigsResponseData.DescribeConfigsResourceResult]()
              entityProps.forEach((name, value) => {
                configEntries += new DescribeConfigsResponseData.DescribeConfigsResourceResult().setName(name.toString)
                  .setValue(value.toString).setConfigSource(ConfigSource.CLIENT_METRICS_CONFIG.id())
                  .setIsSensitive(false).setReadOnly(false).setSynonyms(List.empty.asJava)
              })

              new DescribeConfigsResponseData.DescribeConfigsResult()
                .setErrorCode(Errors.NONE.code)
                .setConfigs(configEntries.asJava)
            }

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
    val configEntryType = LogConfig.configType(name).asScala
    val isSensitive = KafkaConfig.maybeSensitive(configEntryType)
    val valueAsString = if (isSensitive) null else ConfigDef.convertToString(value, configEntryType.orNull)
    val allSynonyms = {
      val list = Option(ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.get(name))
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
