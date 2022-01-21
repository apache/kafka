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

import java.util
import java.util.Properties

import kafka.server.metadata.ConfigRepository
import kafka.utils.Log4jController
import kafka.utils._
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.config.ConfigDef.ConfigKey
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER, TOPIC}
import org.apache.kafka.common.config.{ConfigDef, ConfigResource, LogLevelConfig}
import org.apache.kafka.common.errors.{ApiException, ClusterAuthorizationException, InvalidConfigurationException, InvalidRequestException}
import org.apache.kafka.common.message.{AlterConfigsRequestData, AlterConfigsResponseData, IncrementalAlterConfigsRequestData, IncrementalAlterConfigsResponseData}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => LAlterConfigsResource}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => LAlterConfigsResourceResponse}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource => IAlterConfigsResource, AlterableConfig => IAlterableConfig}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.{AlterConfigsResourceResponse => IAlterConfigsResourceResponse}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors.{INVALID_REQUEST, UNKNOWN_SERVER_ERROR}
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.resource.{Resource, ResourceType}
import org.slf4j.LoggerFactory

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

/**
 * Manages dynamic configuration operations on the broker.
 *
 * There are two RPCs that alter KIP-226 dynamic configurations: alterConfigs, and
 * incrementalAlterConfigs. The main difference between the two is that alterConfigs sets
 * all configurations related to a specific config resource, whereas
 * incrementalAlterConfigs makes only designated changes.
 *
 * The original, non-incremental AlterConfigs is deprecated because there are inherent
 * race conditions when multiple clients use it. It deletes any resource configuration
 * keys that are not specified. This leads to clients trying to do read-modify-write
 * cycles when they only want to change one config key. (But even read-modify-write doesn't
 * work correctly, since "sensitive" configurations are omitted when read.)
 *
 * KIP-412 added support for changing log4j log levels via IncrementalAlterConfigs, but
 * not via the original AlterConfigs. In retrospect, this would have been better off as a
 * separate RPC, since the semantics are quite different. In particular, KIP-226 configs
 * are stored durably (in ZK or KRaft) and persist across broker restarts, but KIP-412
 * log4j levels do not. However, we have to handle it here now in order to maintain
 * compatibility.
 *
 * Configuration processing is split into two parts.
 * - The first step, called "preprocessing," handles setting KIP-412 log levels, validating
 * BROKER configurations. We also filter out some other things here like UNKNOWN resource
 * types, etc.
 * - The second step is "persistence," and handles storing the configurations durably to our
 * metadata store.
 *
 * When KIP-590 forwarding is active (such as in KRaft mode), preprocessing will happen
 * on the broker, while persistence will happen on the active controller. (If KIP-590
 * forwarding is not active, then both steps are done on the same broker.)
 *
 * In KRaft mode, the active controller performs its own configuration validation step in
 * {@link kafka.server.ControllerConfigurationValidator}. This is mainly important for
 * TOPIC resources, since we already validated changes to BROKER resources on the
 * forwarding broker. The KRaft controller is also responsible for enforcing the configured
 * {@link org.apache.kafka.server.policy.AlterConfigPolicy}.
 */
class ConfigAdminManager(nodeId: Int,
                         conf: KafkaConfig,
                         configRepository: ConfigRepository) extends Logging {
  import ConfigAdminManager._

  this.logIdent = "[ConfigAdminManager[nodeId=" + nodeId + "]: "

  /**
   * Preprocess an incremental configuration operation on the broker. This step handles
   * setting log4j levels, as well as filtering out some invalid resource requests that
   * should not be forwarded to the controller.
   *
   * @param request     The request data.
   * @param authorize   A callback which is invoked when we need to authorize an operation.
   *                    Currently, we only use this for log4j operations. Other types of
   *                    operations are authorized in the persistence step. The arguments
   *                    are the type and name of the resource to be authorized.
   *
   * @return            A map from resources to errors. If a resource appears in this map,
   *                    it has been preprocessed and does not need further processing.
   */
  def preprocess(
    request: IncrementalAlterConfigsRequestData,
    authorize: (ResourceType, String) => Boolean
  ): util.IdentityHashMap[IAlterConfigsResource, ApiError] = {
    val results = new util.IdentityHashMap[IAlterConfigsResource, ApiError]()
    val resourceIds = new util.HashMap[(Byte, String), IAlterConfigsResource]
    request.resources().forEach(resource => {
      val preexisting = resourceIds.put((resource.resourceType(), resource.resourceName()), resource)
      if (preexisting != null) {
        Seq(preexisting, resource).foreach(
          r => results.put(r, new ApiError(INVALID_REQUEST, "Each resource must appear at most once.")))
      }
    })
    request.resources().forEach(resource => {
      if (!results.containsKey(resource)) {
        val resourceType = ConfigResource.Type.forId(resource.resourceType())
        val configResource = new ConfigResource(resourceType, resource.resourceName())
        try {
          if (containsDuplicates(resource.configs().asScala.map(_.name()))) {
            throw new InvalidRequestException("Error due to duplicate config keys")
          }
          val nullUpdates = new util.ArrayList[String]()
          resource.configs().forEach { config =>
            if (config.configOperation() != AlterConfigOp.OpType.DELETE.id() &&
              config.value() == null) {
              nullUpdates.add(config.name())
            }
          }
          if (!nullUpdates.isEmpty()) {
            throw new InvalidRequestException("Null value not supported for : " +
              String.join(", ", nullUpdates))
          }
          resourceType match {
            case BROKER_LOGGER =>
              if (!authorize(ResourceType.CLUSTER, Resource.CLUSTER_NAME)) {
                throw new ClusterAuthorizationException(Errors.CLUSTER_AUTHORIZATION_FAILED.message())
              }
              validateResourceNameIsCurrentNodeId(resource.resourceName())
              validateLogLevelConfigs(resource.configs())
              if (!request.validateOnly()) {
                alterLogLevelConfigs(resource.configs())
              }
              results.put(resource, ApiError.NONE)
            case BROKER =>
              // The resource name must be either blank (if setting a cluster config) or
              // the ID of this specific broker.
              if (!configResource.name().isEmpty) {
                validateResourceNameIsCurrentNodeId(resource.resourceName())
              }
              validateBrokerConfigChange(resource, configResource)
            case TOPIC =>
            // Nothing to do.
            case _ =>
              throw new InvalidRequestException(s"Unknown resource type ${resource.resourceType().toInt}")
          }
        } catch {
          case t: Throwable => {
            val err = ApiError.fromThrowable(t)
            info(s"Error preprocessing incrementalAlterConfigs request on ${configResource}", t)
            results.put(resource, err)
          }
        }
      }
    })
    results
  }

  def validateBrokerConfigChange(
    resource: IAlterConfigsResource,
    configResource: ConfigResource
  ): Unit = {
    val perBrokerConfig = !configResource.name().isEmpty
    val persistentProps = configRepository.config(configResource)
    val configProps = conf.dynamicConfig.fromPersistentProps(persistentProps, perBrokerConfig)
    val alterConfigOps = resource.configs().asScala.map {
      case config =>
        val opType = AlterConfigOp.OpType.forId(config.configOperation())
        if (opType == null) {
          throw new InvalidRequestException(s"Unknown operations type ${config.configOperation}")
        }
        new AlterConfigOp(new ConfigEntry(config.name(), config.value()), opType)
    }.toSeq
    prepareIncrementalConfigs(alterConfigOps, configProps, KafkaConfig.configKeys)
    try {
      validateBrokerConfigChange(configProps, configResource)
    } catch {
      case t: Throwable => error(s"validation of configProps ${configProps} for ${configResource} failed with exception", t)
        throw t
    }
  }

  def validateBrokerConfigChange(
    props: Properties,
    configResource: ConfigResource
  ): Unit = {
    try {
      conf.dynamicConfig.validate(props, !configResource.name().isEmpty)
    } catch {
      case e: ApiException => throw e
      //KAFKA-13609: InvalidRequestException is not really the right exception here if the
      // configuration fails validation. The configuration is still well-formed, but just
      // can't be applied. It should probably throw InvalidConfigurationException. However,
      // we should probably only change this in a KIP since it has compatibility implications.
      case e: Throwable => throw new InvalidRequestException(e.getMessage)
    }
 }

  /**
   * Preprocess a legacy configuration operation on the broker.
   *
   * @param request     The request data.
   *
   * @return
   */
  def preprocess(
    request: AlterConfigsRequestData,
  ): util.IdentityHashMap[LAlterConfigsResource, ApiError] = {
    val results = new util.IdentityHashMap[LAlterConfigsResource, ApiError]()
    val resourceIds = new util.HashMap[(Byte, String), LAlterConfigsResource]
    request.resources().forEach(resource => {
      val preexisting = resourceIds.put((resource.resourceType(), resource.resourceName()), resource)
      if (preexisting != null) {
        Seq(preexisting, resource).foreach(
          r => results.put(r, new ApiError(INVALID_REQUEST, "Each resource must appear at most once.")))
      }
    })
    request.resources().forEach(resource => {
      if (!results.containsKey(resource)) {
        val resourceType = ConfigResource.Type.forId(resource.resourceType())
        val configResource = new ConfigResource(resourceType, resource.resourceName())
        try {
          if (containsDuplicates(resource.configs().asScala.map(_.name()))) {
            throw new InvalidRequestException("Error due to duplicate config keys")
          }
          val nullUpdates = new util.ArrayList[String]()
          resource.configs().forEach { config =>
            if (config.value() == null) {
              nullUpdates.add(config.name())
            }
          }
          if (!nullUpdates.isEmpty()) {
            throw new InvalidRequestException("Null value not supported for : " +
              String.join(", ", nullUpdates))
          }
          resourceType match {
            case BROKER =>
              if (!configResource.name().isEmpty) {
                validateResourceNameIsCurrentNodeId(resource.resourceName())
              }
              validateBrokerConfigChange(resource, configResource)
            case TOPIC =>
            // Nothing to do.
            case _ =>
              // Since legacy AlterConfigs does not support BROKER_LOGGER, any attempt to use it
              // gets caught by this clause.
              throw new InvalidRequestException(s"Unknown resource type ${resource.resourceType().toInt}")
          }
        } catch {
          case t: Throwable => {
            val err = ApiError.fromThrowable(t)
            info(s"Error preprocessing alterConfigs request on ${configResource}: ${err}")
            results.put(resource, err)
          }
        }
      }
    })
    results
  }

  def validateBrokerConfigChange(
    resource: LAlterConfigsResource,
    configResource: ConfigResource
  ): Unit = {
    val props = new Properties()
    resource.configs().forEach {
      config => props.setProperty(config.name(), config.value())
    }
    validateBrokerConfigChange(props, configResource)
  }

  def validateResourceNameIsCurrentNodeId(name: String): Unit = {
    val id = try name.toInt catch {
      case _: NumberFormatException =>
        throw new InvalidRequestException(s"Node id must be an integer, but it is: $name")
    }
    if (id != nodeId) {
      throw new InvalidRequestException(s"Unexpected broker id, expected ${nodeId}, but received ${name}")
    }
  }

  def validateLogLevelConfigs(ops: util.Collection[IAlterableConfig]): Unit = {
    def validateLoggerNameExists(loggerName: String): Unit = {
      if (!Log4jController.loggerExists(loggerName)) {
        throw new InvalidConfigurationException(s"Logger $loggerName does not exist!")
      }
    }
    ops.forEach { op =>
      val loggerName = op.name
      OpType.forId(op.configOperation()) match {
        case OpType.SET =>
          validateLoggerNameExists(loggerName)
          val logLevel = op.value()
          if (!LogLevelConfig.VALID_LOG_LEVELS.contains(logLevel)) {
            val validLevelsStr = LogLevelConfig.VALID_LOG_LEVELS.asScala.mkString(", ")
            throw new InvalidConfigurationException(
              s"Cannot set the log level of $loggerName to $logLevel as it is not a supported log level. " +
                s"Valid log levels are $validLevelsStr"
            )
          }
        case OpType.DELETE =>
          validateLoggerNameExists(loggerName)
          if (loggerName == Log4jController.ROOT_LOGGER)
            throw new InvalidRequestException(s"Removing the log level of the ${Log4jController.ROOT_LOGGER} logger is not allowed")
        case OpType.APPEND => throw new InvalidRequestException(s"${OpType.APPEND} " +
          s"operation is not allowed for the ${BROKER_LOGGER} resource")
        case OpType.SUBTRACT => throw new InvalidRequestException(s"${OpType.SUBTRACT} " +
          s"operation is not allowed for the ${BROKER_LOGGER} resource")
        case _ => throw new InvalidRequestException(s"Unknown operation type ${op.configOperation()} " +
          s"is not allowed for the ${BROKER_LOGGER} resource")
      }
    }
  }

  def alterLogLevelConfigs(ops: util.Collection[IAlterableConfig]): Unit = {
    ops.forEach { op =>
      val loggerName = op.name()
      val logLevel = op.value()
      OpType.forId(op.configOperation()) match {
        case OpType.SET =>
          info(s"Updating the log level of $loggerName to $logLevel")
          Log4jController.logLevel(loggerName, logLevel)
        case OpType.DELETE =>
          info(s"Unset the log level of $loggerName")
          Log4jController.unsetLogLevel(loggerName)
        case _ => throw new IllegalArgumentException(
          s"Invalid log4j configOperation: ${op.configOperation()}")
      }
    }
  }
}

object ConfigAdminManager {
  val log = LoggerFactory.getLogger(classOf[ConfigAdminManager])

  /**
   * Copy the incremental configs request data without any already-processed elements.
   *
   * @param request   The input request. Will not be modified.
   * @param processed A map containing the resources that have already been processed.
   * @return          A new request object.
   */
  def copyWithoutPreprocessed(
    request: IncrementalAlterConfigsRequestData,
    processed: util.IdentityHashMap[IAlterConfigsResource, ApiError]
  ): IncrementalAlterConfigsRequestData = {
    val copy = new IncrementalAlterConfigsRequestData().
      setValidateOnly(request.validateOnly())
    request.resources().forEach(resource => {
      if (!processed.containsKey(resource)) {
        copy.resources().mustAdd(resource.duplicate())
      }
    })
    copy
  }

  /**
   * Copy the legacy alter configs request data without any already-processed elements.
   *
   * @param request   The input request. Will not be modified.
   * @param processed A map containing the resources that have already been processed.
   * @return          A new request object.
   */
  def copyWithoutPreprocessed(
    request: AlterConfigsRequestData,
    processed: util.IdentityHashMap[LAlterConfigsResource, ApiError]
  ): AlterConfigsRequestData = {
    val copy = new AlterConfigsRequestData().
      setValidateOnly(request.validateOnly())
    request.resources().forEach(resource => {
      if (!processed.containsKey(resource)) {
        copy.resources().mustAdd(resource.duplicate())
      }
    })
    copy
  }

  def reassembleIncrementalResponse(
    original: IncrementalAlterConfigsRequestData,
    preprocessingResponses: util.IdentityHashMap[IAlterConfigsResource, ApiError],
    persistentResponses: IncrementalAlterConfigsResponseData
  ): IncrementalAlterConfigsResponseData = {
    val response = new IncrementalAlterConfigsResponseData()
    val responsesByResource = persistentResponses.responses().iterator().asScala.map {
      case r => (r.resourceName(), r.resourceType()) -> new ApiError(r.errorCode(), r.errorMessage())
    }.toMap
    original.resources().forEach(r => {
      val err = Option(preprocessingResponses.get(r)) match {
        case None =>
          responsesByResource.get((r.resourceName(), r.resourceType())) match {
            case None => log.error("The controller returned fewer results than we " +
              s"expected. No response found for ${r}.")
              new ApiError(UNKNOWN_SERVER_ERROR)
            case Some(err) => err
          }
        case Some(err) => err
      }
      response.responses().add(new IAlterConfigsResourceResponse().
        setResourceName(r.resourceName()).
        setResourceType(r.resourceType()).
        setErrorCode(err.error().code()).
        setErrorMessage(err.message()))
    })
    response
  }

  def reassembleLegacyResponse(
    original: AlterConfigsRequestData,
    preprocessingResponses: util.IdentityHashMap[LAlterConfigsResource, ApiError],
    persistentResponses: AlterConfigsResponseData
  ): AlterConfigsResponseData = {
    val response = new AlterConfigsResponseData()
    val responsesByResource = persistentResponses.responses().iterator().asScala.map {
      case r => (r.resourceName(), r.resourceType()) -> new ApiError(r.errorCode(), r.errorMessage())
    }.toMap
    original.resources().forEach(r => {
      val err = Option(preprocessingResponses.get(r)) match {
        case None =>
          responsesByResource.get((r.resourceName(), r.resourceType())) match {
            case None => log.error("The controller returned fewer results than we " +
              s"expected. No response found for ${r}.")
              new ApiError(UNKNOWN_SERVER_ERROR)
            case Some(err) => err
          }
        case Some(err) => err
      }
      response.responses().add(new LAlterConfigsResourceResponse().
        setResourceName(r.resourceName()).
        setResourceType(r.resourceType()).
        setErrorCode(err.error().code()).
        setErrorMessage(err.message()))
    })
    response
  }

  def containsDuplicates[T](
    iterable: Iterable[T]
  ): Boolean = {
    val previous = new util.HashSet[T]()
    !iterable.forall(previous.add(_))
  }

  /**
   * Convert the configuration properties for an object (broker, topic, etc.) to a Scala
   * map. Sensitive configurations will be redacted, so that the output is suitable for
   * logging.
   *
   * @param resource      The configuration resource.
   * @param configProps   The configuration as a Properties object.
   * @return              A map containing all the configuration keys and values, as they
   *                      should be logged.
   */
  def toLoggableProps(resource: ConfigResource, configProps: Properties): Map[String, String] = {
    configProps.asScala.map {
      case (key, value) => (key, KafkaConfig.loggableValue(resource.`type`, key, value))
    }
  }

  /**
   * Apply a series of incremental configuration operations to a set of resource properties.
   *
   * @param alterConfigOps  The incremental configuration operations to apply.
   * @param configProps     The resource properties. This will be modified by this function.
   * @param configKeys      Information about configuration key types.
   */
  def prepareIncrementalConfigs(
    alterConfigOps: Seq[AlterConfigOp],
    configProps: Properties,
    configKeys: Map[String, ConfigKey]
  ): Unit = {
    def listType(configName: String, configKeys: Map[String, ConfigKey]): Boolean = {
      val configKey = configKeys(configName)
      if (configKey == null)
        throw new InvalidConfigurationException(s"Unknown config name: $configName")
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
}
