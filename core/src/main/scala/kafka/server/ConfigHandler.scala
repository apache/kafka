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

import java.util.Properties

import kafka.api.ApiVersion
import kafka.log.{LogConfig, LogManager}
import kafka.utils.Logging
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.protocol.ApiKeys

import scala.collection.Map
import scala.collection.JavaConverters._

/**
 * The ConfigHandler is used to process config change notifications received by the DynamicConfigManager
 */
trait ConfigHandler {
  def processConfigChanges(entityName: String, value: Properties)
}

/**
 * The TopicConfigHandler will process topic config changes in ZK.
 * The callback provides the topic name and the full properties set read from ZK
 */
class TopicConfigHandler(private val logManager: LogManager, kafkaConfig: KafkaConfig) extends ConfigHandler with Logging {

  def processConfigChanges(topic: String, topicConfig: Properties) {
    // Validate the compatibility of message format version.
    val configNameToExclude = Option(topicConfig.getProperty(LogConfig.MessageFormatVersionProp)).flatMap { versionString =>
      if (kafkaConfig.interBrokerProtocolVersion < ApiVersion(versionString)) {
        warn(s"Log configuration ${LogConfig.MessageFormatVersionProp} is ignored for `$topic` because `$versionString` " +
          s"is not compatible with Kafka inter-broker protocol version `${kafkaConfig.interBrokerProtocolVersionString}`")
        Some(LogConfig.MessageFormatVersionProp)
      } else
        None
    }

    val logs = logManager.logsByTopicPartition.filterKeys(_.topic == topic).values.toBuffer
    if (logs.nonEmpty) {
      /* combine the default properties with the overrides in zk to create the new LogConfig */
      val props = new Properties()
      props.putAll(logManager.defaultConfig.originals)
      topicConfig.asScala.foreach { case (key, value) =>
        if (key != configNameToExclude) props.put(key, value)
      }
      val logConfig = LogConfig(props)
      logs.foreach(_.config = logConfig)
    }
  }
}

object ClientConfigOverride {
  val ProducerOverride = "producer_byte_rate"
  val ConsumerOverride = "consumer_byte_rate"
}

/**
 * The QuotaConfigHandler will process clientId or user principal quota config changes in ZK.
 * The callback provides the clientId or base64-encoded user principal and the full properties set read from ZK.
 * This implementation reports the overrides to the respective ClientQuotaManager objects
 */
class QuotaConfigHandler(private val quotaType: String, private val quotaManagers: Map[Short, ClientQuotaManager]) extends ConfigHandler {

  def processConfigChanges(sanitizedQuotaId: String, quotaConfig: Properties) = {
    val quotaId = QuotaId.fromSanitizedId(quotaType, sanitizedQuotaId)
    if (quotaConfig.containsKey(ClientConfigOverride.ProducerOverride)) {
      quotaManagers(ApiKeys.PRODUCE.id).updateQuota(quotaId,
        new Quota(quotaConfig.getProperty(ClientConfigOverride.ProducerOverride).toLong, true))
    } else
      quotaManagers(ApiKeys.PRODUCE.id).removeQuota(quotaId)

    if (quotaConfig.containsKey(ClientConfigOverride.ConsumerOverride)) {
      quotaManagers(ApiKeys.FETCH.id).updateQuota(quotaId,
        new Quota(quotaConfig.getProperty(ClientConfigOverride.ConsumerOverride).toLong, true))
    } else
      quotaManagers(ApiKeys.FETCH.id).removeQuota(quotaId)
  }
}
