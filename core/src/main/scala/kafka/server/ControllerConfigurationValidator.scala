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
import java.util.Properties

import kafka.log.LogConfig
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, TOPIC}
import org.apache.kafka.controller.ConfigurationValidator
import org.apache.kafka.common.errors.InvalidRequestException

import scala.collection.mutable

class ControllerConfigurationValidator extends ConfigurationValidator {
  override def validate(resource: ConfigResource, config: util.Map[String, String]): Unit = {
    resource.`type`() match {
      case TOPIC =>
        val properties = new Properties()
        val nullTopicConfigs = new mutable.ArrayBuffer[String]()
        config.entrySet().forEach(e => {
          if (e.getValue() == null) {
            nullTopicConfigs += e.getKey()
          } else {
            properties.setProperty(e.getKey(), e.getValue())
          }
        })
        if (nullTopicConfigs.nonEmpty) {
          throw new InvalidRequestException("Null value not supported for topic configs : " +
            nullTopicConfigs.mkString(","))
        }
        LogConfig.validate(properties)
      case BROKER =>
        // TODO: add broker configuration validation
      case _ =>
        // Note: we should never handle BROKER_LOGGER resources here, since changes to
        // those resources are not persisted in the metadata.
        throw new InvalidRequestException(s"Unknown resource type ${resource.`type`}")
    }
  }
}