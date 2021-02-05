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

import java.util.{Collections, Properties}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import kafka.cluster.Broker
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.errors.InvalidTopicException
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
import scala.jdk.CollectionConverters._

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

  private val inflightTopics = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())

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
    val (creatableTopics, uncreatableTopicResponses) = filterCreatableTopics(topics)

    val creatableTopicResponses = if (creatableTopics.isEmpty) {
      Seq.empty
    } else if (!controller.isActive && channelManager.isDefined) {
      sendCreateTopicRequest(creatableTopics)
    } else {
      createTopicsInZk(creatableTopics, controllerMutationQuota)
    }

    uncreatableTopicResponses ++ creatableTopicResponses
  }

  private def createTopicsInZk(
    creatableTopics: Map[String, CreatableTopic],
    controllerMutationQuota: ControllerMutationQuota
  ): Seq[MetadataResponseTopic] = {
    val topicErrors = new AtomicReference[Map[String, ApiError]]()

    // Note that we use timeout = 0 since we do not need to wait for metadata propagation
    // and we want to get the response error immediately.
    adminManager.createTopics(
      timeout = 0,
      validateOnly = false,
      creatableTopics,
      Map.empty,
      controllerMutationQuota,
      topicErrors.set
    )

    val creatableTopicResponses = Option(topicErrors.get) match {
      case Some(errors) =>
        errors.toSeq.map { case (topic, apiError) =>
          val error = apiError.error match {
            case Errors.TOPIC_ALREADY_EXISTS | Errors.REQUEST_TIMED_OUT =>
              // The timeout error is expected because we set timeout=0. This
              // nevertheless indicates that the topic metadata was created
              // successfully, so we return LEADER_NOT_AVAILABLE.
              Errors.LEADER_NOT_AVAILABLE
            case error => error
          }

          new MetadataResponseTopic()
            .setErrorCode(error.code)
            .setName(topic)
            .setIsInternal(Topic.isInternal(topic))
        }

      case None =>
        creatableTopics.keySet.toSeq.map { topic =>
          new MetadataResponseTopic()
            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            .setName(topic)
            .setIsInternal(Topic.isInternal(topic))
        }
    }

    creatableTopicResponses
  }

  private def sendCreateTopicRequest(
    creatableTopics: Map[String, CreatableTopic]
  ): Seq[MetadataResponseTopic] = {
    val topicsToCreate = new CreateTopicsRequestData.CreatableTopicCollection(creatableTopics.size)
    topicsToCreate.addAll(creatableTopics.values.asJavaCollection)

    val createTopicsRequest = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData()
        .setTimeoutMs(config.requestTimeoutMs)
        .setTopics(topicsToCreate)
    )

    channelManager.get.sendRequest(createTopicsRequest, new ControllerRequestCompletionHandler {
      override def onTimeout(): Unit = {
        debug(s"Auto topic creation timed out for ${creatableTopics.keys}.")
        clearInflightRequests(creatableTopics)
      }

      override def onComplete(response: ClientResponse): Unit = {
        debug(s"Auto topic creation completed for ${creatableTopics.keys}.")
        clearInflightRequests(creatableTopics)
      }
    })

    val creatableTopicResponses = creatableTopics.keySet.toSeq.map { topic =>
      new MetadataResponseTopic()
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        .setName(topic)
        .setIsInternal(Topic.isInternal(topic))
    }

    info(s"Sent auto-creation request for ${creatableTopics.keys} to the active controller.")
    creatableTopicResponses
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

  private def isValidTopicName(topic: String): Boolean = {
    try {
      Topic.validate(topic)
      true
    } catch {
      case e: InvalidTopicException =>
        false
    }
  }

  private def filterCreatableTopics(
    topics: Set[String]
  ): (Map[String, CreatableTopic], Seq[MetadataResponseTopic]) = {

    val aliveBrokers = metadataCache.getAliveBrokers
    val creatableTopics = mutable.Map.empty[String, CreatableTopic]
    val uncreatableTopics = mutable.Buffer.empty[MetadataResponseTopic]

    topics.foreach { topic =>
      // Attempt basic topic validation before sending any requests to the controller.
      val validationError: Option[Errors] = if (!isValidTopicName(topic)) {
        Some(Errors.INVALID_TOPIC_EXCEPTION)
      } else if (!hasEnoughLiveBrokers(topic, aliveBrokers)) {
        Some(Errors.INVALID_REPLICATION_FACTOR)
      } else if (!inflightTopics.add(topic)) {
        Some(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      } else {
        None
      }

      validationError match {
        case Some(error) =>
          uncreatableTopics.addOne(new MetadataResponseTopic()
            .setErrorCode(error.code)
            .setName(topic)
            .setIsInternal(Topic.isInternal(topic))
          )
        case None =>
          creatableTopics.put(topic, creatableTopic(topic))
      }
    }

    (creatableTopics, uncreatableTopics)
  }

  private def hasEnoughLiveBrokers(
    topicName: String,
    aliveBrokers: Seq[Broker]
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
