/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
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
import kafka.server.Constants._
import kafka.server.KafkaConfig._
import kafka.server.QuotaFactory.{QuotaManagers}
import kafka.utils.Logging
import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.metrics.Quota._
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
class TopicConfigHandler(private val logManager: LogManager, kafkaConfig: KafkaConfig, val quotas: QuotaManagers) extends ConfigHandler with Logging  {

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

    val brokerId: Int = kafkaConfig.brokerId

    if (topicConfig.containsKey(LogConfig.ThrottledReplicasListProp)) {
      val partitions = parseThrottledPartitions(topicConfig, brokerId)
      quotas.leader.markReplicasAsThrottled(topic, partitions)
      quotas.follower.markReplicasAsThrottled(topic, partitions)
      logger.info(s"Setting throttled partitions on broker $brokerId to ${partitions.map(_.toString)}")
    }
  }

  def parseThrottledPartitions(topicConfig: Properties, brokerId: Int): Seq[Int] = {
    val configValue = topicConfig.get(LogConfig.ThrottledReplicasListProp).toString.trim
    ThrottledReplicaValidator.ensureValid(LogConfig.ThrottledReplicasListProp, configValue)
    if (configValue.trim == "*")
      allReplicas
    else if (configValue.trim == "")
      Seq()
    else {
      configValue.trim
        .split(":")
        .map(_.split("-"))
        .filter(_(1).toInt == brokerId //Filter this replica
        )
        .map(_(0).toInt).toSeq //convert to list of partition ids
    }
  }
}

object ClientConfigOverride {
  val ProducerOverride = "producer_byte_rate"
  val ConsumerOverride = "consumer_byte_rate"
}

/**
  * The ClientIdConfigHandler will process clientId config changes in ZK.
  * The callback provides the clientId and the full properties set read from ZK.
  * This implementation reports the overrides to the respective ClientQuotaManager objects
  */
class ClientIdConfigHandler(private val quotaManagers: QuotaManagers) extends ConfigHandler {
  //TODO - this may require special support for config being removed. Check before merge.
  def processConfigChanges(clientId: String, clientConfig: Properties) {
    if (clientConfig.containsKey(ClientConfigOverride.ProducerOverride)) {
      quotaManagers.produce.updateQuota(clientId,
        new Quota(clientConfig.getProperty(ClientConfigOverride.ProducerOverride).toLong, true))
    } else {
      quotaManagers.fetch.resetQuota(clientId)
    }

    if (clientConfig.containsKey(ClientConfigOverride.ConsumerOverride)) {
      quotaManagers.fetch.updateQuota(clientId,
        new Quota(clientConfig.getProperty(ClientConfigOverride.ConsumerOverride).toLong, true))
    } else {
      quotaManagers.produce.resetQuota(clientId)
    }
  }
}

/**
  * The BrokerConfigHandler will process individual broker config changes in ZK.
  * The callback provides the brokerId and the full properties set read from ZK.
  * This implementation reports the overrides to the respective ReplicationQuotaManager objects
  */
class BrokerConfigHandler(private val brokerConfig: KafkaConfig, private val quotaManagers: QuotaManagers) extends ConfigHandler with Logging {
  def processConfigChanges(brokerId: String, properties: Properties) {
    //TODO - this may require special support for config being removed. Check before merge.
    if (brokerConfig.brokerId == brokerId.trim.toInt) {
      //Alter throttle limit:
      if (properties.containsKey(ThrottledReplicationRateLimitProp)) {
        val limit = properties.getProperty(ThrottledReplicationRateLimitProp).toLong
        brokerConfig.mutateConfig(ThrottledReplicationRateLimitProp, limit)
        quotaManagers.leader.updateQuota(upperBound(limit))
        quotaManagers.follower.updateQuota(upperBound(limit))
      }
    }
  }
}

object ThrottledReplicaValidator extends Validator {
  override def ensureValid(name: String, value: scala.Any): Unit = {
    if (!value.isInstanceOf[String])
      throw new ConfigException(name, value, s"$name  must be a string")
    if (!isValid(value.toString)) {
      throw new ConfigException(name, value, s"$name  must match for format [number]-[number]:[number]-[number]:[number]-[number] etc")
    }
  }

  private def isValid(value: String): Boolean = {
    val proposed = value.trim
    proposed.equals("*") || proposed.matches("([0-9]+-[0-9]+)?(:[0-9]+-[0-9]+)*")
  }
}