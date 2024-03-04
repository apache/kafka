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

import java.net.{InetAddress, UnknownHostException}
import java.util.{Collections, Properties}
import DynamicConfig.Broker._
import kafka.controller.KafkaController
import kafka.log.UnifiedLog
import kafka.network.ConnectionQuotas
import kafka.security.CredentialProvider
import kafka.server.Constants._
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Implicits._
import kafka.utils.Logging
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.metrics.Quota._
import org.apache.kafka.common.utils.Sanitizer
import org.apache.kafka.server.ClientMetricsManager
import org.apache.kafka.server.config.ConfigEntityName
import org.apache.kafka.storage.internals.log.{LogConfig, ThrottledReplicaListValidator}
import org.apache.kafka.storage.internals.log.LogConfig.MessageFormatVersion

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.util.Try

/**
  * The ConfigHandler is used to process broker configuration change notifications.
  */
trait ConfigHandler {
  def processConfigChanges(entityName: String, value: Properties): Unit
}

/**
  * The TopicConfigHandler will process topic config changes from ZooKeeper or the metadata log.
  * The callback provides the topic name and the full properties set.
  */
class TopicConfigHandler(private val replicaManager: ReplicaManager,
                         kafkaConfig: KafkaConfig,
                         val quotas: QuotaManagers,
                         kafkaController: Option[KafkaController]) extends ConfigHandler with Logging  {

  private def updateLogConfig(topic: String,
                              topicConfig: Properties): Unit = {
    val logManager = replicaManager.logManager
    // Validate the configurations.
    val configNamesToExclude = excludedConfigs(topic, topicConfig)
    val props = new Properties()
    topicConfig.asScala.forKeyValue { (key, value) =>
      if (!configNamesToExclude.contains(key)) props.put(key, value)
    }

    val logs = logManager.logsByTopic(topic)
    val wasRemoteLogEnabledBeforeUpdate = logs.exists(_.remoteLogEnabled())

    logManager.updateTopicConfig(topic, props, kafkaConfig.isRemoteLogStorageSystemEnabled)
    maybeBootstrapRemoteLogComponents(topic, logs, wasRemoteLogEnabledBeforeUpdate)
  }

  private[server] def maybeBootstrapRemoteLogComponents(topic: String,
                                                        logs: Seq[UnifiedLog],
                                                        wasRemoteLogEnabledBeforeUpdate: Boolean): Unit = {
    val isRemoteLogEnabled = logs.exists(_.remoteLogEnabled())
    // Topic configs gets updated incrementally. This check is added to prevent redundant updates.
    if (!wasRemoteLogEnabledBeforeUpdate && isRemoteLogEnabled) {
      val (leaderPartitions, followerPartitions) =
        logs.flatMap(log => replicaManager.onlinePartition(log.topicPartition)).partition(_.isLeader)
      val topicIds = Collections.singletonMap(topic, replicaManager.metadataCache.getTopicId(topic))
      replicaManager.remoteLogManager.foreach(rlm =>
        rlm.onLeadershipChange(leaderPartitions.toSet.asJava, followerPartitions.toSet.asJava, topicIds))
    } else if (wasRemoteLogEnabledBeforeUpdate && !isRemoteLogEnabled) {
      warn(s"Disabling remote log on the topic: $topic is not supported.")
    }
  }

  def processConfigChanges(topic: String, topicConfig: Properties): Unit = {
    updateLogConfig(topic, topicConfig)

    def updateThrottledList(prop: String, quotaManager: ReplicationQuotaManager): Unit = {
      if (topicConfig.containsKey(prop) && topicConfig.getProperty(prop).nonEmpty) {
        val partitions = parseThrottledPartitions(topicConfig, kafkaConfig.brokerId, prop)
        quotaManager.markThrottled(topic, partitions)
        debug(s"Setting $prop on broker ${kafkaConfig.brokerId} for topic: $topic and partitions $partitions")
      } else {
        quotaManager.removeThrottle(topic)
        debug(s"Removing $prop from broker ${kafkaConfig.brokerId} for topic $topic")
      }
    }
    updateThrottledList(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, quotas.leader)
    updateThrottledList(LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, quotas.follower)

    if (Try(topicConfig.getProperty(KafkaConfig.UncleanLeaderElectionEnableProp).toBoolean).getOrElse(false)) {
      kafkaController.foreach(_.enableTopicUncleanLeaderElection(topic))
    }
  }

  def parseThrottledPartitions(topicConfig: Properties, brokerId: Int, prop: String): Seq[Int] = {
    val configValue = topicConfig.get(prop).toString.trim
    ThrottledReplicaListValidator.ensureValidString(prop, configValue)
    configValue match {
      case "" => Seq()
      case "*" => AllReplicas
      case _ => configValue.trim
        .split(",")
        .map(_.split(":"))
        .filter(_ (1).toInt == brokerId) //Filter this replica
        .map(_ (0).toInt).toSeq //convert to list of partition ids
    }
  }

  @nowarn("cat=deprecation")
  private def excludedConfigs(topic: String, topicConfig: Properties): Set[String] = {
    // Verify message format version
    Option(topicConfig.getProperty(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)).flatMap { versionString =>
      val messageFormatVersion = new MessageFormatVersion(versionString, kafkaConfig.interBrokerProtocolVersion.version)
      if (messageFormatVersion.shouldIgnore) {
        if (messageFormatVersion.shouldWarn)
          warn(messageFormatVersion.topicWarningMessage(topic))
        Some(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)
      } else if (kafkaConfig.interBrokerProtocolVersion.isLessThan(messageFormatVersion.messageFormatVersion)) {
        warn(s"Topic configuration ${TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG} is ignored for `$topic` because `$versionString` " +
          s"is higher than what is allowed by the inter-broker protocol version `${kafkaConfig.interBrokerProtocolVersionString}`")
        Some(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)
      } else
        None
    }.toSet
  }
}


/**
 * Handles <client-id>, <user> or <user, client-id> quota config updates in ZK.
 * This implementation reports the overrides to the respective ClientQuotaManager objects
 */
class QuotaConfigHandler(private val quotaManagers: QuotaManagers) {

  def updateQuotaConfig(sanitizedUser: Option[String], sanitizedClientId: Option[String], config: Properties): Unit = {
    val clientId = sanitizedClientId.map(Sanitizer.desanitize)
    val producerQuota =
      if (config.containsKey(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG))
        Some(new Quota(config.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG).toLong.toDouble, true))
      else
        None
    quotaManagers.produce.updateQuota(sanitizedUser, clientId, sanitizedClientId, producerQuota)
    val consumerQuota =
      if (config.containsKey(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG))
        Some(new Quota(config.getProperty(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG).toLong.toDouble, true))
      else
        None
    quotaManagers.fetch.updateQuota(sanitizedUser, clientId, sanitizedClientId, consumerQuota)
    val requestQuota =
      if (config.containsKey(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG))
        Some(new Quota(config.getProperty(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG).toDouble, true))
      else
        None
    quotaManagers.request.updateQuota(sanitizedUser, clientId, sanitizedClientId, requestQuota)
    val controllerMutationQuota =
      if (config.containsKey(QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG))
        Some(new Quota(config.getProperty(QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG).toDouble, true))
      else
        None
    quotaManagers.controllerMutation.updateQuota(sanitizedUser, clientId, sanitizedClientId, controllerMutationQuota)
  }
}

/**
 * The ClientIdConfigHandler will process clientId config changes in ZK.
 * The callback provides the clientId and the full properties set read from ZK.
 */
class ClientIdConfigHandler(private val quotaManagers: QuotaManagers) extends QuotaConfigHandler(quotaManagers) with ConfigHandler {

  def processConfigChanges(sanitizedClientId: String, clientConfig: Properties): Unit = {
    updateQuotaConfig(None, Some(sanitizedClientId), clientConfig)
  }
}

/**
 * The UserConfigHandler will process <user> and <user, client-id> quota changes in ZK.
 * The callback provides the node name containing sanitized user principal, sanitized client-id if this is
 * a <user, client-id> update and the full properties set read from ZK.
 */
class UserConfigHandler(private val quotaManagers: QuotaManagers, val credentialProvider: CredentialProvider) extends QuotaConfigHandler(quotaManagers) with ConfigHandler {

  def processConfigChanges(quotaEntityPath: String, config: Properties): Unit = {
    // Entity path is <user> or <user>/clients/<client>
    val entities = quotaEntityPath.split("/")
    if (entities.length != 1 && entities.length != 3)
      throw new IllegalArgumentException("Invalid quota entity path: " + quotaEntityPath)
    val sanitizedUser = entities(0)
    val sanitizedClientId = if (entities.length == 3) Some(entities(2)) else None
    updateQuotaConfig(Some(sanitizedUser), sanitizedClientId, config)
    if (sanitizedClientId.isEmpty && sanitizedUser != ConfigEntityName.DEFAULT)
      credentialProvider.updateCredentials(Sanitizer.desanitize(sanitizedUser), config)
  }
}

class IpConfigHandler(private val connectionQuotas: ConnectionQuotas) extends ConfigHandler with Logging {

  def processConfigChanges(ip: String, config: Properties): Unit = {
    val ipConnectionRateQuota = Option(config.getProperty(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG)).map(_.toInt)
    val updatedIp = {
      if (ip != ConfigEntityName.DEFAULT) {
        try {
          Some(InetAddress.getByName(ip))
        } catch {
          case _: UnknownHostException => throw new IllegalArgumentException(s"Unable to resolve address $ip")
        }
      } else
        None
    }
    connectionQuotas.updateIpConnectionRateQuota(updatedIp, ipConnectionRateQuota)
  }
}

/**
  * The BrokerConfigHandler will process individual broker config changes in ZK.
  * The callback provides the brokerId and the full properties set read from ZK.
  * This implementation reports the overrides to the respective ReplicationQuotaManager objects
  */
class BrokerConfigHandler(private val brokerConfig: KafkaConfig,
                          private val quotaManagers: QuotaManagers) extends ConfigHandler with Logging {

  def processConfigChanges(brokerId: String, properties: Properties): Unit = {
    def getOrDefault(prop: String): Long = {
      if (properties.containsKey(prop))
        properties.getProperty(prop).toLong
      else
        DefaultReplicationThrottledRate
    }
    if (brokerId == ConfigEntityName.DEFAULT)
      brokerConfig.dynamicConfig.updateDefaultConfig(properties)
    else if (brokerConfig.brokerId == brokerId.trim.toInt) {
      brokerConfig.dynamicConfig.updateBrokerConfig(brokerConfig.brokerId, properties)
      quotaManagers.leader.updateQuota(upperBound(getOrDefault(LeaderReplicationThrottledRateProp).toDouble))
      quotaManagers.follower.updateQuota(upperBound(getOrDefault(FollowerReplicationThrottledRateProp).toDouble))
      quotaManagers.alterLogDirs.updateQuota(upperBound(getOrDefault(ReplicaAlterLogDirsIoMaxBytesPerSecondProp).toDouble))
    }
  }
}

/**
 * The ClientMetricsConfigHandler will process individual client metrics subscription changes.
 */
class ClientMetricsConfigHandler(private val clientMetricsManager: ClientMetricsManager) extends ConfigHandler with Logging {
  def processConfigChanges(subscriptionGroupId: String, properties: Properties): Unit = {
    clientMetricsManager.updateSubscription(subscriptionGroupId, properties)
  }
}
