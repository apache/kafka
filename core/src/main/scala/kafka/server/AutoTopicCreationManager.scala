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

import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, Properties}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, SHARE_GROUP_STATE_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME}
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicConfig, CreatableTopicConfigCollection}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{CreateTopicsRequest, RequestContext, RequestHeader}
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.coordinator.share.ShareCoordinator
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}

import scala.collection.{Map, Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional

trait AutoTopicCreationManager {

  def createTopics(
    topicNames: Set[String],
    controllerMutationQuota: ControllerMutationQuota,
    metadataRequestContext: Option[RequestContext]
  ): Seq[MetadataResponseTopic]

  def createStreamsInternalTopics(
    topics: Map[String, CreatableTopic],
    requestContext: RequestContext
  ): Unit

}

class DefaultAutoTopicCreationManager(
  config: KafkaConfig,
  channelManager: NodeToControllerChannelManager,
  groupCoordinator: GroupCoordinator,
  txnCoordinator: TransactionCoordinator,
  shareCoordinator: ShareCoordinator
) extends AutoTopicCreationManager with Logging {

  private val inflightTopics = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())

  /**
   * Initiate auto topic creation for the given topics.
   *
   * @param topics the topics to create
   * @param controllerMutationQuota the controller mutation quota for topic creation
   * @param metadataRequestContext defined when creating topics on behalf of the client. The goal here is to preserve
   *                               original client principal for auditing, thus needing to wrap a plain CreateTopicsRequest
   *                               inside Envelope to send to the controller when forwarding is enabled.
   * @return auto created topic metadata responses
   */
  override def createTopics(
    topics: Set[String],
    controllerMutationQuota: ControllerMutationQuota,
    metadataRequestContext: Option[RequestContext]
  ): Seq[MetadataResponseTopic] = {
    val (creatableTopics, uncreatableTopicResponses) = filterCreatableTopics(topics)

    val creatableTopicResponses = if (creatableTopics.isEmpty) {
      Seq.empty
    } else {
      sendCreateTopicRequest(creatableTopics, metadataRequestContext)
    }

    uncreatableTopicResponses ++ creatableTopicResponses
  }

  override def createStreamsInternalTopics(
    topics: Map[String, CreatableTopic],
    requestContext: RequestContext
  ): Unit = {

    for ((_, creatableTopic) <- topics) {
      if (creatableTopic.numPartitions() == -1) {
        creatableTopic
          .setNumPartitions(config.numPartitions)
      }
      if (creatableTopic.replicationFactor() == -1) {
        creatableTopic
          .setReplicationFactor(config.defaultReplicationFactor.shortValue)
      }
    }

    if (topics.nonEmpty) {
      sendCreateTopicRequest(topics, Some(requestContext))
    }
  }

  private def sendCreateTopicRequest(
    creatableTopics: Map[String, CreatableTopic],
    requestContext: Option[RequestContext]
  ): Seq[MetadataResponseTopic] = {
    val topicsToCreate = new CreateTopicsRequestData.CreatableTopicCollection(creatableTopics.size)
    topicsToCreate.addAll(creatableTopics.values.asJavaCollection)

    val createTopicsRequest = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData()
        .setTimeoutMs(config.requestTimeoutMs)
        .setTopics(topicsToCreate)
    )

    val requestCompletionHandler = new ControllerRequestCompletionHandler {
      override def onTimeout(): Unit = {
        clearInflightRequests(creatableTopics)
        debug(s"Auto topic creation timed out for ${creatableTopics.keys}.")
      }

      override def onComplete(response: ClientResponse): Unit = {
        clearInflightRequests(creatableTopics)
        if (response.authenticationException() != null) {
          warn(s"Auto topic creation failed for ${creatableTopics.keys} with authentication exception")
        } else if (response.versionMismatch() != null) {
          warn(s"Auto topic creation failed for ${creatableTopics.keys} with invalid version exception")
        } else {
          debug(s"Auto topic creation completed for ${creatableTopics.keys} with response ${response.responseBody}.")
        }
      }
    }

    val request = requestContext.map { context =>
      val requestVersion =
        channelManager.controllerApiVersions.toScala match {
          case None =>
            // We will rely on the Metadata request to be retried in the case
            // that the latest version is not usable by the controller.
            ApiKeys.CREATE_TOPICS.latestVersion()
          case Some(nodeApiVersions) =>
            nodeApiVersions.latestUsableVersion(ApiKeys.CREATE_TOPICS)
        }

      // Borrow client information such as client id and correlation id from the original request,
      // in order to correlate the create request with the original metadata request.
      val requestHeader = new RequestHeader(ApiKeys.CREATE_TOPICS,
        requestVersion,
        context.clientId,
        context.correlationId)
      ForwardingManager.buildEnvelopeRequest(context,
        createTopicsRequest.build(requestVersion).serializeWithHeader(requestHeader))
    }.getOrElse(createTopicsRequest)

    channelManager.sendRequest(request, requestCompletionHandler)

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
          .setNumPartitions(config.groupCoordinatorConfig.offsetsTopicPartitions)
          .setReplicationFactor(config.groupCoordinatorConfig.offsetsTopicReplicationFactor)
          .setConfigs(convertToTopicConfigCollections(groupCoordinator.groupMetadataTopicConfigs))
      case TRANSACTION_STATE_TOPIC_NAME =>
        val transactionLogConfig = new TransactionLogConfig(config)
        new CreatableTopic()
          .setName(topic)
          .setNumPartitions(transactionLogConfig.transactionTopicPartitions)
          .setReplicationFactor(transactionLogConfig.transactionTopicReplicationFactor)
          .setConfigs(convertToTopicConfigCollections(
            txnCoordinator.transactionTopicConfigs))
      case SHARE_GROUP_STATE_TOPIC_NAME =>
        new CreatableTopic()
          .setName(topic)
          .setNumPartitions(config.shareCoordinatorConfig.shareCoordinatorStateTopicNumPartitions())
          .setReplicationFactor(config.shareCoordinatorConfig.shareCoordinatorStateTopicReplicationFactor())
          .setConfigs(convertToTopicConfigCollections(shareCoordinator.shareGroupStateTopicConfigs()))
      case topicName =>
        new CreatableTopic()
          .setName(topicName)
          .setNumPartitions(config.numPartitions)
          .setReplicationFactor(config.defaultReplicationFactor.shortValue)
    }
  }

  private def convertToTopicConfigCollections(config: Properties): CreatableTopicConfigCollection = {
    val topicConfigs = new CreatableTopicConfigCollection()
    config.forEach {
      case (name, value) =>
        topicConfigs.add(new CreatableTopicConfig()
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
      case _: InvalidTopicException =>
        false
    }
  }

  private def filterCreatableTopics(
    topics: Set[String]
  ): (Map[String, CreatableTopic], Seq[MetadataResponseTopic]) = {

    val creatableTopics = mutable.Map.empty[String, CreatableTopic]
    val uncreatableTopics = mutable.Buffer.empty[MetadataResponseTopic]

    topics.foreach { topic =>
      // Attempt basic topic validation before sending any requests to the controller.
      val validationError: Option[Errors] = if (!isValidTopicName(topic)) {
        Some(Errors.INVALID_TOPIC_EXCEPTION)
      } else if (!inflightTopics.add(topic)) {
        Some(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      } else {
        None
      }

      validationError match {
        case Some(error) =>
          uncreatableTopics += new MetadataResponseTopic()
            .setErrorCode(error.code)
            .setName(topic)
            .setIsInternal(Topic.isInternal(topic))
        case None =>
          creatableTopics.put(topic, creatableTopic(topic))
      }
    }

    (creatableTopics, uncreatableTopics)
  }
}
