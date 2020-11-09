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

import java.util
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
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, Set}

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
    new AutoTopicCreationManagerImpl(config, metadataCache, channelManager, adminManager,
      controller, groupCoordinator, txnCoordinator)
  }
}

class AutoTopicCreationManagerImpl(
  config: KafkaConfig,
  metadataCache: MetadataCache,
  channelManager: Option[BrokerToControllerChannelManager],
  adminManager: ZkAdminManager,
  controller: KafkaController,
  groupCoordinator: GroupCoordinator,
  txnCoordinator: TransactionCoordinator
) extends AutoTopicCreationManager with Logging {

  private val inflightTopics = new ConcurrentHashMap[String, CreatableTopic]()

  override def start(): Unit = {
    channelManager.foreach(_.start())
  }

  override def shutdown(): Unit = {
    channelManager.foreach(_.shutdown())
    inflightTopics.clear()
  }

  override def createTopics(topics: Set[String],
                            controllerMutationQuota: ControllerMutationQuota): Seq[MetadataResponseTopic] = {
    val topicResponses = topics.map(topic =>
      metadataResponseTopic(
      if (!hasEnoughAliveBrokers(topic))
        Errors.INVALID_REPLICATION_FACTOR
      else
        Errors.UNKNOWN_TOPIC_OR_PARTITION,
      topic, Topic.isInternal(topic), util.Collections.emptyList()))

    val topicConfigs = topicResponses
      .filter(topicResponse => shouldCreate(topicResponse))
      .map(topicResponse => getTopicConfigs(topicResponse.name()))
      .map(topic => (topic.name, topic)).toMap

    if (topicConfigs.nonEmpty) {
      if (!controller.isActive && channelManager.isDefined) {
        // Mark the topics as inflight during auto creation through forwarding.
        topicConfigs.foreach(config => inflightTopics.put(config._1, config._2))

        val topicsToCreate = new CreateTopicsRequestData.CreatableTopicCollection
        topicConfigs.foreach(config => topicsToCreate.add(config._2))
        val createTopicsRequest = new CreateTopicsRequest.Builder(
          new CreateTopicsRequestData()
            .setTimeoutMs(config.requestTimeoutMs)
            .setTopics(topicsToCreate)
        )

        channelManager.get.sendRequest(createTopicsRequest, new ControllerRequestCompletionHandler {
          override def onTimeout(): Unit = {
            debug(s"Auto topic creation timed out for $topics.")
            clearInflightRequests(topicConfigs)
          }

          override def onComplete(response: ClientResponse): Unit = {
            debug(s"Auto topic creation completed for $topics.")
            clearInflightRequests(topicConfigs)
          }
        })
        info(s"Sent $topics to the active controller for auto creation.")
      } else {
        adminManager.createTopics(
          config.requestTimeoutMs,
          validateOnly = false,
          topicConfigs,
          Map.empty,
          controllerMutationQuota,
          _ => ())
        info(s"Topics $topics are being created asynchronously.")
      }
    }
    topicResponses.toSeq
  }

  private def shouldCreate(topicResponse: MetadataResponseTopic): Boolean = {
    Errors.forCode(topicResponse.errorCode()) == Errors.UNKNOWN_TOPIC_OR_PARTITION &&
      !inflightTopics.containsKey(topicResponse.name)
  }

  private def clearInflightRequests(topicConfigs: Map[String, CreatableTopic]): Unit = {
    topicConfigs.foreach(config => inflightTopics.remove(config._1))
    debug(s"Cleared pending topic creation states for $topicConfigs")
  }

  private def getTopicConfigs(topic: String): CreatableTopic = {
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

  private def hasEnoughAliveBrokers(topic: String): Boolean = {
    if (topic == null)
      throw new IllegalArgumentException("topic must not be null")

    val aliveBrokers = metadataCache.getAliveBrokers

    topic match {
      case GROUP_METADATA_TOPIC_NAME =>
        checkEnoughLiveBrokers(aliveBrokers, config.offsetsTopicReplicationFactor.intValue(), topic, None)
      case TRANSACTION_STATE_TOPIC_NAME =>
        checkEnoughLiveBrokers(aliveBrokers, config.transactionTopicReplicationFactor.intValue(), topic, None)
      case _ =>
        checkEnoughLiveBrokers(aliveBrokers, config.defaultReplicationFactor, topic, None)
    }
  }

  private def checkEnoughLiveBrokers(aliveBrokers: Seq[Broker],
                                     replicationFactor: Int,
                                     topicName: String,
                                     configName: Option[String]): Boolean = {
    if (aliveBrokers.size < replicationFactor) {
      val configHint = configName match {
        case Some(config) => s", whose replication factor is configured via $config"
        case None => ""
      }
      error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
        s"'$replicationFactor' for auto creation of topic '$topicName'$configHint. " +
        s"This error can be ignored if the cluster is starting up " +
        s"and not all brokers are up yet.")
      false
    } else
      true
  }

  private def metadataResponseTopic(error: Errors,
                                    topic: String,
                                    isInternal: Boolean,
                                    partitionData: util.List[MetadataResponsePartition]): MetadataResponseTopic = {
    new MetadataResponseTopic()
      .setErrorCode(error.code)
      .setName(topic)
      .setIsInternal(isInternal)
      .setPartitions(partitionData)
  }
}
