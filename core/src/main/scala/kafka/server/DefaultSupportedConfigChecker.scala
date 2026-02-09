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
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.coordinator.group.GroupConfig
import org.apache.kafka.metadata.SupportedConfigChecker
import org.apache.kafka.server.config.DynamicConfig
import org.apache.kafka.server.metrics.ClientMetricsConfigs
import org.apache.kafka.storage.internals.log.LogConfig
import scala.jdk.CollectionConverters._

/**
 * Default implementation of SupportedConfigChecker that checks if a configuration name
 * is supported for a given resource type based on the actual config definitions.
 *
 * This class maintains a whitelist of valid configuration names per resource type:
 * - TOPIC: Configurations defined in LogConfig
 * - BROKER: Configurations defined in DynamicConfig.Broker
 * - CLIENT_METRICS: Configurations defined in ClientMetricsConfigs
 * - GROUP: Configurations defined in GroupConfig
 */
class DefaultSupportedConfigChecker extends SupportedConfigChecker {
  private val validConfigsByType: Map[ConfigResource.Type, util.Set[String]] = {
    val topicConfigs = LogConfig.nonInternalConfigNames.asScala.toSet
    val brokerConfigs = DynamicConfig.Broker.names.asScala.toSet
    val clientMetricsConfigs = ClientMetricsConfigs.configDef().names.asScala.toSet
    val groupConfigs = GroupConfig.configDef().names.asScala.toSet

    Map(
      ConfigResource.Type.TOPIC -> topicConfigs.asJava,
      ConfigResource.Type.BROKER -> brokerConfigs.asJava,
      ConfigResource.Type.CLIENT_METRICS -> clientMetricsConfigs.asJava,
      ConfigResource.Type.GROUP -> groupConfigs.asJava
    )
  }

  override def isSupported(resourceType: ConfigResource.Type, configName: String): Boolean = {
    validConfigsByType.get(resourceType) match {
      case Some(configs) => configs.contains(configName)
      case None => false
    }
  }
}

