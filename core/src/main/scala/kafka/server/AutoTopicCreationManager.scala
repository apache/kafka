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
import java.util.concurrent.atomic.AtomicReference
import java.util.{Collections, Properties}
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
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiError, CreateTopicsRequest, RequestContext, RequestHeader}

import scala.collection.{Map, Seq, Set, mutable}
import scala.jdk.CollectionConverters._

abstract class AutoTopicCreationManager(config: KafkaConfig,
                                        groupCoordinator: GroupCoordinator,
                                        txnCoordinator: TransactionCoordinator) extends Logging {

  private[server] val inflightTopics = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())

  private[server] def doCreateTopics(createableTopics: Map[String, CreatableTopic],
                                     controllerMutationQuota: ControllerMutationQuota,
                                     metadataRequestContext: Option[RequestContext]): Seq[MetadataResponseTopic]

  def createTopics(
                    topicNames: Set[String],
                    controllerMutationQuota: ControllerMutationQuota,
                    metadataRequestContext: Option[RequestContext]
                  ): Seq[MetadataResponseTopic] = {
    val (creatableTopics, uncreatableTopicResponses) = filterCreatableTopics(topicNames)
    val creatableTopicResponses = if (creatableTopics.isEmpty) {
      Seq.empty
    } else {
      doCreateTopics(creatableTopics, controllerMutationQuota, metadataRequestContext)
    }
    uncreatableTopicResponses ++ creatableTopicResponses
  }

  def creatableTopic(topic: String): CreatableTopic = {
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

  def convertToTopicConfigCollections(config: Properties): CreateableTopicConfigCollection = {
    val topicConfigs = new CreateableTopicConfigCollection()
    config.forEach {
      case (name, value) =>
        topicConfigs.add(new CreateableTopicConfig()
          .setName(name.toString)
          .setValue(value.toString))
    }
    topicConfigs
  }

  def isValidTopicName(topic: String): Boolean = {
    try {
      Topic.validate(topic)
      true
    } catch {
      case _: InvalidTopicException =>
        false
    }
  }

  def filterCreatableTopics(topics: Set[String]): (Map[String, CreatableTopic], Seq[MetadataResponseTopic]) = {

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

  private[server] def clearInflightRequests(creatableTopics: Map[String, CreatableTopic]): Unit = {
    creatableTopics.keySet.foreach(inflightTopics.remove)
    debug(s"Cleared inflight topic creation state for $creatableTopics")
  }
}

object AutoTopicCreationManager {
  def apply(
    config: KafkaConfig,
    channelManager: BrokerToControllerChannelManager,
    metadataSupport: MetadataSupport,
    groupCoordinator: GroupCoordinator,
    txnCoordinator: TransactionCoordinator,
  ): AutoTopicCreationManager = {
    metadataSupport match {
      case zk: ZkSupport => new ZkAutoTopicCreationManager(config, zk, groupCoordinator, txnCoordinator)
      case _: RaftSupport => new DefaultAutoTopicCreationManager(config, channelManager, groupCoordinator, txnCoordinator)
    }
  }
}


class ZkAutoTopicCreationManager(config: KafkaConfig,
                                 zkSupport: ZkSupport,
                                 groupCoordinator: GroupCoordinator,
                                 txnCoordinator: TransactionCoordinator)
  extends AutoTopicCreationManager(config, groupCoordinator, txnCoordinator) {

  override def doCreateTopics(creatableTopics: Map[String, CreatableTopic],
                              controllerMutationQuota: ControllerMutationQuota,
                              metadataRequestContext: Option[RequestContext]): Seq[MetadataResponseTopic] = {
    val topicErrors = new AtomicReference[Map[String, ApiError]]()
    try {
      // Note that we use timeout = 0 since we do not need to wait for metadata propagation
      // and we want to get the response error immediately.
      zkSupport.adminManager.createTopics(
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
    } finally {
      clearInflightRequests(creatableTopics)
    }
  }
}

class DefaultAutoTopicCreationManager(config: KafkaConfig,
                                      channelManager: BrokerToControllerChannelManager,
                                      groupCoordinator: GroupCoordinator,
                                      txnCoordinator: TransactionCoordinator)
  extends AutoTopicCreationManager(config, groupCoordinator, txnCoordinator) {

  override def doCreateTopics(creatableTopics: Map[String, CreatableTopic],
                              controllerMutationQuota: ControllerMutationQuota,
                              metadataRequestContext: Option[RequestContext]): Seq[MetadataResponseTopic] = {
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

    val request = metadataRequestContext.map { context =>
      val requestVersion =
        channelManager.controllerApiVersions() match {
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
}

