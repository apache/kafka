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
import java.util.concurrent.locks.ReentrantLock
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
import org.apache.kafka.common.requests.{AbstractResponse, CreateTopicsRequest, CreateTopicsResponse, EnvelopeResponse, RequestContext, RequestHeader}
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.coordinator.share.ShareCoordinator
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.quota.ControllerMutationQuota
import org.apache.kafka.common.utils.Time

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
    requestContext: RequestContext,
    timeoutMs: Long
  ): Unit

  def getStreamsInternalTopicCreationErrors(
    topicNames: Set[String],
    currentTimeMs: Long
  ): Map[String, String]

  def close(): Unit = {}

}

/**
 * Thread-safe cache that stores topic creation errors with per-entry expiration.
 * - Expiration: maintained by a min-heap (priority queue) on expiration time
 * - Capacity: enforced by evicting entries with earliest expiration time (not LRU)
 * - Updates: old entries remain in queue but are ignored via reference equality check
 */
private[server] class ExpiringErrorCache(maxSize: Int, time: Time) {

  private case class Entry(topicName: String, errorMessage: String, expirationTimeMs: Long)

  private val byTopic = new ConcurrentHashMap[String, Entry]()
  private val expiryQueue = new java.util.PriorityQueue[Entry](11, new java.util.Comparator[Entry] {
    override def compare(a: Entry, b: Entry): Int = java.lang.Long.compare(a.expirationTimeMs, b.expirationTimeMs)
  })
  private val lock = new ReentrantLock()

  def put(topicName: String, errorMessage: String, ttlMs: Long): Unit = {
    lock.lock()
    try {
      val currentTimeMs = time.milliseconds()
      val expirationTimeMs = currentTimeMs + ttlMs
      val entry = Entry(topicName, errorMessage, expirationTimeMs)
      byTopic.put(topicName, entry)
      expiryQueue.add(entry)

      // Clean up expired entries and enforce capacity
      while (!expiryQueue.isEmpty && 
             (expiryQueue.peek().expirationTimeMs <= currentTimeMs || byTopic.size() > maxSize)) {
        val evicted = expiryQueue.poll()
        val current = byTopic.get(evicted.topicName)
        if (current != null && (current eq evicted)) {
          byTopic.remove(evicted.topicName)
        }
      }
    } finally {
      lock.unlock()
    }
  }

  def hasError(topicName: String, currentTimeMs: Long): Boolean = {
    val entry = byTopic.get(topicName)
    entry != null && entry.expirationTimeMs > currentTimeMs
  }

  def getErrorsForTopics(topicNames: Set[String], currentTimeMs: Long): Map[String, String] = {
    val result = mutable.Map.empty[String, String]
    topicNames.foreach { topicName =>
      val entry = byTopic.get(topicName)
      if (entry != null && entry.expirationTimeMs > currentTimeMs) {
        result.put(topicName, entry.errorMessage)
      }
    }
    result.toMap
  }

  private[server] def clear(): Unit = {
    lock.lock()
    try {
      byTopic.clear()
      expiryQueue.clear()
    } finally {
      lock.unlock()
    }
  }
}


class DefaultAutoTopicCreationManager(
  config: KafkaConfig,
  channelManager: NodeToControllerChannelManager,
  groupCoordinator: GroupCoordinator,
  txnCoordinator: TransactionCoordinator,
  shareCoordinator: ShareCoordinator,
  time: Time,
  topicErrorCacheCapacity: Int = 1000
) extends AutoTopicCreationManager with Logging {

  private val inflightTopics = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())

  // Hardcoded default capacity; can be overridden in tests via constructor param
  private val topicCreationErrorCache = new ExpiringErrorCache(topicErrorCacheCapacity, time)

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
    requestContext: RequestContext,
    timeoutMs: Long
  ): Unit = {
    if (topics.isEmpty) {
      return
    }

    val currentTimeMs = time.milliseconds()

    // Filter out topics that are:
    // 1. Already in error cache (back-off period)
    // 2. Already in-flight (concurrent request)
    val topicsToCreate = topics.filter { case (topicName, _) =>
      !topicCreationErrorCache.hasError(topicName, currentTimeMs) &&
      inflightTopics.add(topicName)
    }

    if (topicsToCreate.nonEmpty) {
      sendCreateTopicRequestWithErrorCaching(topicsToCreate, Some(requestContext), timeoutMs)
    }
  }

  override def getStreamsInternalTopicCreationErrors(
    topicNames: Set[String],
    currentTimeMs: Long
  ): Map[String, String] = {
    topicCreationErrorCache.getErrorsForTopics(topicNames, currentTimeMs)
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

    // Capture request header information for proper envelope response parsing
    val requestHeaderForParsing = requestContext.map { context =>
      val requestVersion =
        channelManager.controllerApiVersions.toScala match {
          case None =>
            ApiKeys.CREATE_TOPICS.latestVersion()
          case Some(nodeApiVersions) =>
            nodeApiVersions.latestUsableVersion(ApiKeys.CREATE_TOPICS)
        }

      new RequestHeader(ApiKeys.CREATE_TOPICS,
        requestVersion,
        context.clientId,
        context.correlationId)
    }

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
          if (response.hasResponse) {
            response.responseBody() match {
              case envelopeResponse: EnvelopeResponse =>
                // Unwrap the envelope response to get the actual CreateTopicsResponse
                val envelopeError = envelopeResponse.error()
                if (envelopeError != Errors.NONE) {
                  warn(s"Auto topic creation failed for ${creatableTopics.keys} with envelope error: ${envelopeError}")
                } else {
                  requestHeaderForParsing match {
                    case Some(requestHeader) =>
                      try {
                        // Use the captured request header for proper envelope response parsing
                        val createTopicsResponse = AbstractResponse.parseResponse(
                          envelopeResponse.responseData(), requestHeader).asInstanceOf[CreateTopicsResponse]

                        createTopicsResponse.data().topics().forEach(topicResult => {
                          val error = Errors.forCode(topicResult.errorCode)
                          if (error != Errors.NONE) {
                            warn(s"Auto topic creation failed for ${topicResult.name} with error '${error.name}': ${topicResult.errorMessage}")
                          }
                        })
                      } catch {
                        case e: Exception =>
                          warn(s"Failed to parse envelope response for auto topic creation of ${creatableTopics.keys}", e)
                      }
                    case None =>
                      warn(s"Cannot parse envelope response without original request header information")
                  }
                }
              case createTopicsResponse: CreateTopicsResponse =>
                createTopicsResponse.data().topics().forEach(topicResult => {
                  val error = Errors.forCode(topicResult.errorCode)
                  if (error != Errors.NONE) {
                    warn(s"Auto topic creation failed for ${topicResult.name} with error '${error.name}': ${topicResult.errorMessage}")
                  }
                })
              case other =>
                warn(s"Auto topic creation request received unexpected response type: ${other.getClass.getSimpleName}")
            }
          }
          debug(s"Auto topic creation completed for ${creatableTopics.keys} with response ${response.responseBody}.")
        }
      }
    }

    val request = (requestContext, requestHeaderForParsing) match {
      case (Some(context), Some(requestHeader)) =>
        ForwardingManager.buildEnvelopeRequest(context,
          createTopicsRequest.build(requestHeader.apiVersion()).serializeWithHeader(requestHeader))
      case _ =>
        createTopicsRequest
    }

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

  private def sendCreateTopicRequestWithErrorCaching(
    creatableTopics: Map[String, CreatableTopic],
    requestContext: Option[RequestContext],
    timeoutMs: Long
  ): Seq[MetadataResponseTopic] = {
    val topicsToCreate = new CreateTopicsRequestData.CreatableTopicCollection(creatableTopics.size)
    topicsToCreate.addAll(creatableTopics.values.asJavaCollection)

    val createTopicsRequest = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData()
        .setTimeoutMs(config.requestTimeoutMs)
        .setTopics(topicsToCreate)
    )

    // Capture request header information for proper envelope response parsing
    val requestHeaderForParsing = requestContext.map { context =>
      val requestVersion =
        channelManager.controllerApiVersions.toScala match {
          case None =>
            ApiKeys.CREATE_TOPICS.latestVersion()
          case Some(nodeApiVersions) =>
            nodeApiVersions.latestUsableVersion(ApiKeys.CREATE_TOPICS)
        }

      new RequestHeader(ApiKeys.CREATE_TOPICS,
        requestVersion,
        context.clientId,
        context.correlationId)
    }

    val requestCompletionHandler = new ControllerRequestCompletionHandler {
      override def onTimeout(): Unit = {
        clearInflightRequests(creatableTopics)
        debug(s"Auto topic creation timed out for ${creatableTopics.keys}.")
        cacheTopicCreationErrors(creatableTopics.keys.toSet, "Auto topic creation timed out.", timeoutMs)
      }

      override def onComplete(response: ClientResponse): Unit = {
        clearInflightRequests(creatableTopics)
        if (response.authenticationException() != null) {
          val authException = response.authenticationException()
          warn(s"Auto topic creation failed for ${creatableTopics.keys} with authentication exception: ${authException.getMessage}")
          cacheTopicCreationErrors(creatableTopics.keys.toSet, authException.getMessage, timeoutMs)
        } else if (response.versionMismatch() != null) {
          val versionException = response.versionMismatch()
          warn(s"Auto topic creation failed for ${creatableTopics.keys} with version mismatch exception: ${versionException.getMessage}")
          cacheTopicCreationErrors(creatableTopics.keys.toSet, versionException.getMessage, timeoutMs)
        } else {
          if (response.hasResponse) {
            response.responseBody() match {
              case envelopeResponse: EnvelopeResponse =>
                // Unwrap the envelope response to get the actual CreateTopicsResponse
                val envelopeError = envelopeResponse.error()
                if (envelopeError != Errors.NONE) {
                  warn(s"Auto topic creation failed for ${creatableTopics.keys} with envelope error: ${envelopeError}")
                  cacheTopicCreationErrors(creatableTopics.keys.toSet, s"Envelope error: ${envelopeError}", timeoutMs)
                } else {
                  requestHeaderForParsing match {
                    case Some(requestHeader) =>
                      try {
                        // Use the captured request header for proper envelope response parsing
                        val createTopicsResponse = AbstractResponse.parseResponse(
                          envelopeResponse.responseData(), requestHeader).asInstanceOf[CreateTopicsResponse]

                        cacheTopicCreationErrorsFromResponse(createTopicsResponse, timeoutMs)
                      } catch {
                        case e: Exception =>
                          warn(s"Failed to parse envelope response for auto topic creation of ${creatableTopics.keys}", e)
                          cacheTopicCreationErrors(creatableTopics.keys.toSet, s"Response parsing error: ${e.getMessage}", timeoutMs)
                      }
                    case None =>
                      warn(s"Cannot parse envelope response without original request header information")
                      cacheTopicCreationErrors(creatableTopics.keys.toSet, "Missing request header for envelope parsing", timeoutMs)
                  }
                }
              case createTopicsResponse: CreateTopicsResponse =>
                cacheTopicCreationErrorsFromResponse(createTopicsResponse, timeoutMs)
              case unexpectedResponse =>
                warn(s"Auto topic creation request received unexpected response type: ${unexpectedResponse.getClass.getSimpleName}")
                cacheTopicCreationErrors(creatableTopics.keys.toSet, s"Unexpected response type: ${unexpectedResponse.getClass.getSimpleName}", timeoutMs)
            }
            debug(s"Auto topic creation completed for ${creatableTopics.keys} with response ${response.responseBody}.")
          }
        }
      }
    }

    val request = (requestContext, requestHeaderForParsing) match {
      case (Some(context), Some(requestHeader)) =>
        ForwardingManager.buildEnvelopeRequest(context,
          createTopicsRequest.build(requestHeader.apiVersion()).serializeWithHeader(requestHeader))
      case _ =>
        createTopicsRequest
    }

    channelManager.sendRequest(request, requestCompletionHandler)

    val creatableTopicResponses = creatableTopics.keySet.toSeq.map { topic =>
      new MetadataResponseTopic()
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        .setName(topic)
        .setIsInternal(Topic.isInternal(topic))
    }

    creatableTopicResponses
  }

  private def cacheTopicCreationErrors(topicNames: Set[String], errorMessage: String, ttlMs: Long): Unit = {
    topicNames.foreach { topicName =>
      topicCreationErrorCache.put(topicName, errorMessage, ttlMs)
    }
  }

  private def cacheTopicCreationErrorsFromResponse(response: CreateTopicsResponse, ttlMs: Long): Unit = {
    response.data().topics().forEach { topicResult =>
      if (topicResult.errorCode() != Errors.NONE.code()) {
        val errorMessage = Option(topicResult.errorMessage())
          .filter(_.nonEmpty)
          .getOrElse(Errors.forCode(topicResult.errorCode()).message())
        topicCreationErrorCache.put(topicResult.name(), errorMessage, ttlMs)
        debug(s"Cached topic creation error for ${topicResult.name()}: $errorMessage")
      }
    }
  }

  override def close(): Unit = {
    topicCreationErrorCache.clear()
  }
}
