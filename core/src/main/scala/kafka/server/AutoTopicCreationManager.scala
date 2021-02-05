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

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import kafka.cluster.Broker
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME}
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreateableTopicConfig, CreateableTopicConfigCollection}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ApiError, CreateTopicsRequest}
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, Set, mutable}

trait AutoTopicCreationManager {

  def createTopics(
    topicNames: Set[String],
    controllerMutationQuota: ControllerMutationQuota
  ): Seq[MetadataResponseTopic]

  def start(): Unit

  def shutdown(): Unit
}

object AutoTopicCreationManager {

  def apply(
    config: KafkaConfig,
    metadataCache: MetadataCache,
    time: Time,
    metrics: Metrics,
    threadNamePrefix: Option[String],
    adminManager: ZkAdminManager,
    controller: KafkaController,
    groupCoordinator: GroupCoordinator,
    txnCoordinator: TransactionCoordinator,
    enableForwarding: Boolean
  ): AutoTopicCreationManager = {

    val channelManager =
      if (enableForwarding)
        Some(new BrokerToControllerChannelManagerImpl(
          controllerNodeProvider = MetadataCacheControllerNodeProvider(
            config, metadataCache),
          time = time,
          metrics = metrics,
          config = config,
          channelName = "autoTopicCreationChannel",
          threadNamePrefix = threadNamePrefix,
          retryTimeoutMs = config.requestTimeoutMs.longValue
        ))
      else
        None
    new DefaultAutoTopicCreationManager(config, metadataCache, channelManager, adminManager,
      controller, groupCoordinator, txnCoordinator)
  }
}

class DefaultAutoTopicCreationManager(
  config: KafkaConfig,
  metadataCache: MetadataCache,
  channelManager: Option[BrokerToControllerChannelManager],
  adminManager: ZkAdminManager,
  controller: KafkaController,
  groupCoordinator: GroupCoordinator,
  txnCoordinator: TransactionCoordinator
) extends AutoTopicCreationManager with Logging {

  private val inflightTopics = new ConcurrentHashMap[String, Boolean]()

  override def start(): Unit = {
    channelManager.foreach(_.start())
  }

  override def shutdown(): Unit = {
    channelManager.foreach(_.shutdown())
    inflightTopics.clear()
  }

  override def createTopics(
    topics: Set[String],
    controllerMutationQuota: ControllerMutationQuota
  ): Seq[MetadataResponseTopic] = {
    val (topicResponses, creatableTopics) = filterCreatableTopics(topics)

    if (creatableTopics.nonEmpty) {
      if (!controller.isActive && channelManager.isDefined) {
        val topicsToCreate = new CreateTopicsRequestData.CreatableTopicCollection
        creatableTopics.foreach(config => topicsToCreate.add(config._2))
        val createTopicsRequest = new CreateTopicsRequest.Builder(
          new CreateTopicsRequestData()
            .setTimeoutMs(config.requestTimeoutMs)
            .setTopics(topicsToCreate)
        )

        channelManager.get.sendRequest(createTopicsRequest, new ControllerRequestCompletionHandler {
          override def onTimeout(): Unit = {
            debug(s"Auto topic creation timed out for $topics.")
            clearInflightRequests(creatableTopics)
          }

          override def onComplete(response: ClientResponse): Unit = {
            debug(s"Auto topic creation completed for $topics.")
            clearInflightRequests(creatableTopics)
          }
        })
        info(s"Sent auto-creation request for $topics to the active controller.")
      } else {
        try {
          def creationCallback(topicErrors: Map[String, ApiError]): Unit = {
            info(s"Auto-creation of topics $topics returned with errors: $topicErrors")
          }

          // Note that we use timeout = 0 since we do not need to wait for metadata propagation
          adminManager.createTopics(
            timeout = 0,
            validateOnly = false,
            creatableTopics,
            Map.empty,
            controllerMutationQuota,
            creationCallback
          )
        } finally {
          clearInflightRequests(creatableTopics)
        }
      }
    }
    topicResponses
  }

  private def clearInflightRequests(creatableTopics: Map[String, CreatableTopic]): Unit = {
    creatableTopics.keySet.foreach(inflightTopics.remove)
    debug(s"Cleared inflight topic creation state for $creatableTopics")
  }

  private def creatableTopic(topic: String): CreatableTopic = {
    topic match {
      case GROUP_METADATA_TOPIC_NAME =>
        new CreatableTopic()
          .setName(topic)
          .setNumPartitions(config.offsetsTopicPartitions)
          .setReplicationFactor(config.offsetsTopicReplicationFactor)
          .setConfigs(convertToTopicConfigCollections(groupCoordinator.offsetsTopicConfigs))
      case TRANSACTION_STATE_TOPIC_NAME =>
        new CreatableTopic()
          .setName(topic)
          .setNumPartitions(config.transactionTopicPartitions)
          .setReplicationFactor(config.transactionTopicReplicationFactor)
          .setConfigs(convertToTopicConfigCollections(
            txnCoordinator.transactionTopicConfigs))
      case topicName =>
        new CreatableTopic()
          .setName(topicName)
          .setNumPartitions(config.numPartitions)
          .setReplicationFactor(config.defaultReplicationFactor.shortValue)
    }
  }

  private def convertToTopicConfigCollections(config: Properties): CreateableTopicConfigCollection = {
    val topicConfigs = new CreateableTopicConfigCollection()
    config.forEach {
      case (name, value) =>
        topicConfigs.add(new CreateableTopicConfig()
          .setName(name.toString)
          .setValue(value.toString))
    }
    topicConfigs
  }

  private def filterCreatableTopics(
    topics: Set[String]
  ): (Seq[MetadataResponseTopic], Map[String, CreatableTopic]) = {

    val aliveBrokers = metadataCache.getAliveBrokers
    val creatableTopics = mutable.Map.empty[String, CreatableTopic]
    val topicResponses = topics.toSeq.map { topic =>
      val error = if (hasEnoughLiveBrokers(topic, aliveBrokers)) {
        val alreadyPresent = inflightTopics.putIfAbsent(topic, true)
        if (!alreadyPresent) {
          creatableTopics.put(topic, creatableTopic(topic))
        }

        Errors.UNKNOWN_TOPIC_OR_PARTITION
      } else {
        Errors.INVALID_REPLICATION_FACTOR
      }

      new MetadataResponseTopic()
        .setErrorCode(error.code)
        .setName(topic)
        .setIsInternal(Topic.isInternal(topic))
    }

    (topicResponses, creatableTopics)
  }

  private def hasEnoughLiveBrokers(
    topicName: String,
    aliveBrokers: Seq[Broker],
  ): Boolean = {
    val (replicationFactor, replicationFactorConfig) = topicName match {
      case GROUP_METADATA_TOPIC_NAME =>
        (config.offsetsTopicReplicationFactor.intValue, KafkaConfig.OffsetsTopicReplicationFactorProp)

      case TRANSACTION_STATE_TOPIC_NAME =>
        (config.transactionTopicReplicationFactor.intValue, KafkaConfig.TransactionsTopicReplicationFactorProp)

      case _ =>
        (config.defaultReplicationFactor, KafkaConfig.DefaultReplicationFactorProp)
    }

    if (aliveBrokers.size < replicationFactor) {
      error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
        s"'$replicationFactor' for auto creation of topic '$topicName' which is configured by $replicationFactorConfig. " +
        "This error can be ignored if the cluster is starting up and not all brokers are up yet.")
      false
    } else {
      true
    }
  }

}
