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

import kafka.log.LogConfig
import kafka.server.DynamicConfigManager.prepareIncrementalConfigs
import kafka.utils.Log4jController
import kafka.utils._
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER, TOPIC}
import org.apache.kafka.common.config.{ConfigResource, LogLevelConfig}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, InvalidConfigurationException, InvalidRequestException}
import org.apache.kafka.common.message.{AlterConfigsRequestData, AlterConfigsResponseData, IncrementalAlterConfigsRequestData, IncrementalAlterConfigsResponseData}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => LAlterConfigsResource}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => LAlterConfigsResourceResponse}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource => IAlterConfigsResource, AlterableConfig => IAlterableConfig}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.{AlterConfigsResourceResponse => IAlterConfigsResourceResponse}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors.{INVALID_REQUEST, UNKNOWN_SERVER_ERROR}
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.resource.{Resource, ResourceType}

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
                         getResourceConfig: ConfigResource => Properties) extends Logging {
  this.logIdent = "[ConfigAdminManager[nodeId=" + nodeId + "]: "

  /**
   * Preprocess an incremental configuration operation on the broker.
   *
   * @param request     The request data.
   * @param authorize   A callback which is invoked when we need to authorize an operation.
   *                    Currently, we only use this for log4j operations. Other types of
   *                    operations are authorized in the persistence step.
   * @return
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
    val props = getResourceConfig(configResource)
    val alterConfigOps = resource.configs().asScala.map {
      case config =>
        val opType = AlterConfigOp.OpType.forId(config.configOperation())
        if (opType == null) {
          throw new InvalidRequestException(s"Unknown operations type ${config.configOperation}")
        }
        new AlterConfigOp(new ConfigEntry(config.name(), config.value()), opType)
    }.toSeq
    prepareIncrementalConfigs(alterConfigOps, props, LogConfig.configKeys)
    conf.dynamicConfig.validate(props, !configResource.name().isEmpty)
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
          resourceType match {
            case BROKER =>
              validateResourceNameIsCurrentNodeId(resource.resourceName())
            case TOPIC =>
            // Nothing to do.
            case _ =>
              // Since legacy AlterConfigs does not support BROKER_LOGGER, any attempt to use it
              // gets caught by this clause.
              throw new InvalidRequestException(s"Unknown resource type ${resource.resourceType().toInt}")
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
    val iterator = persistentResponses.responses().iterator()
    original.resources().forEach(resource => {
      val error = Option(preprocessingResponses.get(resource)) match {
        case None => if (!iterator.hasNext) {
          new ApiError(UNKNOWN_SERVER_ERROR)
        } else {
          val resourceResponse = iterator.next()
          new ApiError(Errors.forCode(resourceResponse.errorCode()), resourceResponse.errorMessage())
        }
        case Some(error) => error
      }
      response.responses().add(new IAlterConfigsResourceResponse().
        setResourceName(resource.resourceName()).
        setResourceType(resource.resourceType()).
        setErrorCode(error.error().code()).
        setErrorMessage(error.message()))
    })
    response
  }

  def reassembleLegacyResponse(
    original: AlterConfigsRequestData,
    preprocessingResponses: util.IdentityHashMap[LAlterConfigsResource, ApiError],
    persistentResponses: AlterConfigsResponseData
  ): AlterConfigsResponseData = {
    val response = new AlterConfigsResponseData()
    val iterator = persistentResponses.responses().iterator()
    original.resources().forEach(resource => {
      val error = Option(preprocessingResponses.get(resource)) match {
        case None => if (!iterator.hasNext) {
          new ApiError(UNKNOWN_SERVER_ERROR)
        } else {
          val resourceResponse = iterator.next()
          new ApiError(Errors.forCode(resourceResponse.errorCode()), resourceResponse.errorMessage())
        }
        case Some(error) => error
      }
      response.responses().add(new LAlterConfigsResourceResponse().
        setResourceName(resource.resourceName()).
        setResourceType(resource.resourceType()).
        setErrorCode(error.error().code()).
        setErrorMessage(error.message()))
    })
    response
  }
}