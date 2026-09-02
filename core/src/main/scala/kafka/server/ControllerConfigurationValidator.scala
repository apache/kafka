/*
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
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, CLIENT_METRICS, GROUP, TOPIC}
import org.apache.kafka.controller.ConfigurationValidator
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.coordinator.group.GroupConfig
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.metrics.ClientMetricsConfigs
import org.apache.kafka.storage.internals.log.LogConfig

import scala.collection.mutable

/**
 * The validator that the controller uses for dynamic configuration changes.
 * It performs the generic validation, which can't be bypassed. If an AlterConfigPolicy
 * is configured, the controller will check that after verifying that this passes.
 *
 * For changes to BROKER resources, the forwarding broker performs an extra validation step
 * in {@link kafka.server.ConfigAdminManager#preprocess()} before sending the change to
 * the controller. Therefore, the validation here is just a kind of sanity check, which
 * should never fail under normal conditions.
 *
 * For changes to GROUP resources, the forwarding broker performs validation in
 * {@link kafka.server.ConfigAdminManager#preprocess()} before sending the change to
 * the controller, starting from metadata version 4.5-IV0. The validation here is only run
 * for requests that did not come through such a broker: requests sent directly to the
 * controller, and requests forwarded while the cluster may still contain brokers that
 * predate the broker-side validation.
 *
 * This validator does not handle changes to BROKER_LOGGER resources. Despite being bundled
 * in the same RPC, BROKER_LOGGER is not really a dynamic configuration in the same sense
 * as the others. It is not persisted to the metadata log.
 */
class ControllerConfigurationValidator(kafkaConfig: KafkaConfig) extends ConfigurationValidator {
  private def validateTopicName(
    name: String
  ): Unit = {
    if (name.isEmpty) {
      throw new InvalidRequestException("Default topic resources are not allowed.")
    }
    Topic.validate(name)
  }

  private def validateBrokerName(
    name: String
  ): Unit = {
    if (name.nonEmpty) {
      val brokerId = try {
        Integer.valueOf(name)
      } catch {
        case _: NumberFormatException =>
          throw new InvalidRequestException("Unable to parse broker name as a base 10 number.")
      }
      if (brokerId < 0) {
        throw new InvalidRequestException("Invalid negative broker ID.")
      }
    }
  }

  private def filterAndValidateNullConfigs(
    newConfigs: util.Map[String, String],
    resourceTypeName: String
  ): util.HashMap[String, String] = {
    val filteredConfigs = new util.HashMap[String, String]()
    val nullConfigs = new mutable.ArrayBuffer[String]()

    newConfigs.forEach((key, value) => {
      if (value == null) {
        nullConfigs += key
      } else {
        filteredConfigs.put(key, value)
      }
    })
    if (nullConfigs.nonEmpty) {
      throw new InvalidConfigurationException(s"Null value not supported for $resourceTypeName configs: " +
        nullConfigs.mkString(","))
    }

    filteredConfigs
  }

  private def throwExceptionForUnknownResourceType(
    resource: ConfigResource
  ): Unit = {
    // Note: we should never handle BROKER_LOGGER resources here, since changes to
    // those resources are not persisted in the metadata.
    throw new InvalidRequestException(s"Unknown resource type ${resource.`type`}")
  }

  override def validate(
    resource: ConfigResource
  ): Unit = {
    resource.`type`() match {
      case TOPIC => validateTopicName(resource.name())
      case BROKER => validateBrokerName(resource.name())
      case _ => throwExceptionForUnknownResourceType(resource)
    }
  }

  override def validate(
    resource: ConfigResource,
    newConfigs: util.Map[String, String],
    oldConfigs: util.Map[String, String],
    metadataVersion: MetadataVersion,
    forwarded: Boolean
  ): Unit = {
    resource.`type`() match {
      case TOPIC =>
        validateTopicName(resource.name())
        val filteredConfigs = filterAndValidateNullConfigs(newConfigs, "topic")
        LogConfig.validate(oldConfigs, filteredConfigs, kafkaConfig.extractLogConfigMap,
          kafkaConfig.remoteLogManagerConfig.isRemoteStorageSystemEnabled())
      case BROKER => validateBrokerName(resource.name())
      case CLIENT_METRICS =>
        val filteredConfigs = filterAndValidateNullConfigs(newConfigs, "client metrics")
        ClientMetricsConfigs.validate(resource.name(), filteredConfigs)
      case GROUP =>
        ConfigAdminManager.validateGroupResourceName(resource.name())
        // From IBP_4_5_IV0 on, the forwarding broker has already validated the change, so only
        // validate here when the request did not come through such a broker. The metadata version
        // check can be removed once upgrading directly from a pre-{@link MetadataVersion#IBP_4_5_IV0}
        // cluster is no longer supported.
        val validatedOnBroker = forwarded && metadataVersion.isAtLeast(MetadataVersion.IBP_4_5_IV0)
        if (!validatedOnBroker) {
          val filteredConfigs = filterAndValidateNullConfigs(newConfigs, "group")
          GroupConfig.validateOnController(filteredConfigs, oldConfigs, kafkaConfig.groupCoordinatorConfig, kafkaConfig.shareGroupConfig)
        }
      case _ => throwExceptionForUnknownResourceType(resource)
    }
  }
}
