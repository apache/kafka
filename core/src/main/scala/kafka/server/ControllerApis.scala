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
import java.util.Collections
import java.util.concurrent.ExecutionException

import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.common.Uuid.ZERO_UUID
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.acl.AclOperation.{ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, DELETE, DESCRIBE}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{ApiException, ClusterAuthorizationException, InvalidRequestException, TopicDeletionDisabledException}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DeleteTopicsResponseData.{DeletableTopicResult, DeletableTopicResultCollection}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.message.{BeginQuorumEpochResponseData, BrokerHeartbeatResponseData, BrokerRegistrationResponseData, CreateTopicsRequestData, CreateTopicsResponseData, DeleteTopicsRequestData, DeleteTopicsResponseData, DescribeQuorumResponseData, EndQuorumEpochResponseData, FetchResponseData, MetadataResponseData, SaslAuthenticateResponseData, SaslHandshakeResponseData, UnregisterBrokerResponseData, VoteResponseData}
import org.apache.kafka.common.protocol.Errors.{INVALID_REQUEST, TOPIC_AUTHORIZATION_FAILED}
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, TOPIC}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.controller.Controller
import org.apache.kafka.metadata.{ApiMessageAndVersion, BrokerHeartbeatReply, BrokerRegistrationReply, VersionRange}
import org.apache.kafka.server.authorizer.Authorizer

import scala.jdk.CollectionConverters._


/**
 * Request handler for Controller APIs
 */
class ControllerApis(val requestChannel: RequestChannel,
                     val authorizer: Option[Authorizer],
                     val quotas: QuotaManagers,
                     val time: Time,
                     val supportedFeatures: Map[String, VersionRange],
                     val controller: Controller,
                     val raftManager: RaftManager[ApiMessageAndVersion],
                     val config: KafkaConfig,
                     val metaProperties: MetaProperties,
                     val controllerNodes: Seq[Node],
                     val apiVersionManager: ApiVersionManager) extends ApiRequestHandler with Logging {

  val authHelper = new AuthHelper(authorizer)
  val requestHelper = new RequestHandlerHelper(requestChannel, quotas, time)

  override def handle(request: RequestChannel.Request): Unit = {
    try {
      request.header.apiKey match {
        case ApiKeys.FETCH => handleFetch(request)
        case ApiKeys.METADATA => handleMetadataRequest(request)
        case ApiKeys.CREATE_TOPICS => handleCreateTopics(request)
        case ApiKeys.DELETE_TOPICS => handleDeleteTopics(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.VOTE => handleVote(request)
        case ApiKeys.BEGIN_QUORUM_EPOCH => handleBeginQuorumEpoch(request)
        case ApiKeys.END_QUORUM_EPOCH => handleEndQuorumEpoch(request)
        case ApiKeys.DESCRIBE_QUORUM => handleDescribeQuorum(request)
        case ApiKeys.ALTER_ISR => handleAlterIsrRequest(request)
        case ApiKeys.BROKER_REGISTRATION => handleBrokerRegistration(request)
        case ApiKeys.BROKER_HEARTBEAT => handleBrokerHeartBeatRequest(request)
        case ApiKeys.UNREGISTER_BROKER => handleUnregisterBroker(request)
        case ApiKeys.ALTER_CLIENT_QUOTAS => handleAlterClientQuotas(request)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => handleIncrementalAlterConfigs(request)
        case ApiKeys.ENVELOPE => handleEnvelopeRequest(request)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: ExecutionException => requestHelper.handleError(request, e.getCause)
      case e: Throwable => requestHelper.handleError(request, e)
    }
  }

  def handleEnvelopeRequest(request: RequestChannel.Request): Unit = {
    if (!authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new ClusterAuthorizationException(
        s"Principal ${request.context.principal} does not have required CLUSTER_ACTION for envelope"))
    } else {
      EnvelopeUtils.handleEnvelopeRequest(request, requestChannel.metrics, handle)
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslHandshakeResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslHandshakeResponse(responseData))
  }

  def handleSaslAuthenticateRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslAuthenticateResponseData()
      .setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
      .setErrorMessage("SaslAuthenticate request received after successful authentication")
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslAuthenticateResponse(responseData))
  }

  private def handleFetch(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new FetchResponse(response.asInstanceOf[FetchResponseData]))
  }

  def handleMetadataRequest(request: RequestChannel.Request): Unit = {
    val metadataRequest = request.body[MetadataRequest]
    def createResponseCallback(requestThrottleMs: Int): MetadataResponse = {
      val metadataResponseData = new MetadataResponseData()
      metadataResponseData.setThrottleTimeMs(requestThrottleMs)
      controllerNodes.foreach { node =>
        metadataResponseData.brokers.add(new MetadataResponseBroker()
          .setHost(node.host)
          .setNodeId(node.id)
          .setPort(node.port)
          .setRack(node.rack))
      }
      metadataResponseData.setClusterId(metaProperties.clusterId.toString)
      if (controller.isActive) {
        metadataResponseData.setControllerId(config.nodeId)
      } else {
        metadataResponseData.setControllerId(MetadataResponse.NO_CONTROLLER_ID)
      }
      val clusterAuthorizedOperations = if (metadataRequest.data.includeClusterAuthorizedOperations) {
        if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
          authHelper.authorizedOperations(request, Resource.CLUSTER)
        } else {
          0
        }
      } else {
        Int.MinValue
      }
      // TODO: fill in information about the metadata topic
      metadataResponseData.setClusterAuthorizedOperations(clusterAuthorizedOperations)
      new MetadataResponse(metadataResponseData, request.header.apiVersion)
    }
    requestHelper.sendResponseMaybeThrottle(request,
      requestThrottleMs => createResponseCallback(requestThrottleMs))
  }

  def handleDeleteTopics(request: RequestChannel.Request): Unit = {
    val responses = deleteTopics(request.body[DeleteTopicsRequest].data,
      request.context.apiVersion,
      authHelper.authorize(request.context, DELETE, CLUSTER, CLUSTER_NAME),
      names => authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC, names)(n => n),
      names => authHelper.filterByAuthorized(request.context, DELETE, TOPIC, names)(n => n))
    requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => {
      val responseData = new DeleteTopicsResponseData().
        setResponses(new DeletableTopicResultCollection(responses.iterator)).
        setThrottleTimeMs(throttleTimeMs)
      new DeleteTopicsResponse(responseData)
    })
  }

  def deleteTopics(request: DeleteTopicsRequestData,
                   apiVersion: Int,
                   hasClusterAuth: Boolean,
                   getDescribableTopics: Iterable[String] => Set[String],
                   getDeletableTopics: Iterable[String] => Set[String]): util.List[DeletableTopicResult] = {
    // Check if topic deletion is enabled at all.
    if (!config.deleteTopicEnable) {
      if (apiVersion < 3) {
        throw new InvalidRequestException("Topic deletion is disabled.")
      } else {
        throw new TopicDeletionDisabledException()
      }
    }
    // The first step is to load up the names and IDs that have been provided by the
    // request.  This is a bit messy because we support multiple ways of referring to
    // topics (both by name and by id) and because we need to check for duplicates or
    // other invalid inputs.
    val responses = new util.ArrayList[DeletableTopicResult]
    def appendResponse(name: String, id: Uuid, error: ApiError): Unit = {
      responses.add(new DeletableTopicResult().
        setName(name).
        setTopicId(id).
        setErrorCode(error.error.code).
        setErrorMessage(error.message))
    }
    val providedNames = new util.HashSet[String]
    val duplicateProvidedNames = new util.HashSet[String]
    val providedIds = new util.HashSet[Uuid]
    val duplicateProvidedIds = new util.HashSet[Uuid]
    def addProvidedName(name: String): Unit = {
      if (duplicateProvidedNames.contains(name) || !providedNames.add(name)) {
        duplicateProvidedNames.add(name)
        providedNames.remove(name)
      }
    }
    request.topicNames.forEach(addProvidedName)
    request.topics.forEach {
      topic => if (topic.name == null) {
        if (topic.topicId.equals(ZERO_UUID)) {
          appendResponse(null, ZERO_UUID, new ApiError(INVALID_REQUEST,
            "Neither topic name nor id were specified."))
        } else if (duplicateProvidedIds.contains(topic.topicId) || !providedIds.add(topic.topicId)) {
          duplicateProvidedIds.add(topic.topicId)
          providedIds.remove(topic.topicId)
        }
      } else {
        if (topic.topicId.equals(ZERO_UUID)) {
          addProvidedName(topic.name)
        } else {
          appendResponse(topic.name, topic.topicId, new ApiError(INVALID_REQUEST,
            "You may not specify both topic name and topic id."))
        }
      }
    }
    // Create error responses for duplicates.
    duplicateProvidedNames.forEach(name => appendResponse(name, ZERO_UUID,
      new ApiError(INVALID_REQUEST, "Duplicate topic name.")))
    duplicateProvidedIds.forEach(id => appendResponse(null, id,
      new ApiError(INVALID_REQUEST, "Duplicate topic id.")))
    // At this point we have all the valid names and IDs that have been provided.
    // However, the Authorizer needs topic names as inputs, not topic IDs.  So
    // we need to resolve all IDs to names.
    val toAuthenticate = new util.HashSet[String]
    toAuthenticate.addAll(providedNames)
    val idToName = new util.HashMap[Uuid, String]
    controller.findTopicNames(providedIds).get().forEach { (id, nameOrError) =>
      if (nameOrError.isError) {
        appendResponse(null, id, nameOrError.error())
      } else {
        toAuthenticate.add(nameOrError.result())
        idToName.put(id, nameOrError.result())
      }
    }
    // Get the list of deletable topics (those we can delete) and the list of describeable
    // topics.  If a topic can't be deleted or described, we have to act like it doesn't
    // exist, even when it does.
    val topicsToAuthenticate = toAuthenticate.asScala
    val (describeable, deletable) = if (hasClusterAuth) {
      (topicsToAuthenticate.toSet, topicsToAuthenticate.toSet)
    } else {
      (getDescribableTopics(topicsToAuthenticate), getDeletableTopics(topicsToAuthenticate))
    }
    // For each topic that was provided by ID, check if authentication failed.
    // If so, remove it from the idToName map and create an error response for it.
    val iterator = idToName.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val id = entry.getKey
      val name = entry.getValue
      if (!deletable.contains(name)) {
        if (describeable.contains(name)) {
          appendResponse(name, id, new ApiError(TOPIC_AUTHORIZATION_FAILED))
        } else {
          appendResponse(null, id, new ApiError(TOPIC_AUTHORIZATION_FAILED))
        }
        iterator.remove()
      }
    }
    // For each topic that was provided by name, check if authentication failed.
    // If so, create an error response for it.  Otherwise, add it to the idToName map.
    controller.findTopicIds(providedNames).get().forEach { (name, idOrError) =>
      if (!describeable.contains(name)) {
        appendResponse(name, ZERO_UUID, new ApiError(TOPIC_AUTHORIZATION_FAILED))
      } else if (idOrError.isError) {
        appendResponse(name, ZERO_UUID, idOrError.error)
      } else if (deletable.contains(name)) {
        val id = idOrError.result()
        if (duplicateProvidedIds.contains(id) || idToName.put(id, name) != null) {
          // This is kind of a weird case: what if we supply topic ID X and also a name
          // that maps to ID X?  In that case, _if authorization succeeds_, we end up
          // here.  If authorization doesn't succeed, we refrain from commenting on the
          // situation since it would reveal topic ID mappings.
          duplicateProvidedIds.add(id)
          idToName.remove(id)
          appendResponse(name, id, new ApiError(INVALID_REQUEST,
            "The provided topic name maps to an ID that was already supplied."))
        }
      } else {
        appendResponse(name, ZERO_UUID, new ApiError(TOPIC_AUTHORIZATION_FAILED))
      }
    }
    // Finally, the idToName map contains all the topics that we are authorized to delete.
    // Perform the deletion and create responses for each one.
    val idToError = controller.deleteTopics(idToName.keySet).get()
    idToError.forEach { (id, error) =>
        appendResponse(idToName.get(id), id, error)
    }
    // Shuffle the responses so that users can not use patterns in their positions to
    // distinguish between absent topics and topics we are not permitted to see.
    Collections.shuffle(responses)
    responses
  }

  def handleCreateTopics(request: RequestChannel.Request): Unit = {
    val responseData = createTopics(request.body[CreateTopicsRequest].data(),
        authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME),
        names => authHelper.filterByAuthorized(request.context, CREATE, TOPIC, names)(identity))
    requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => {
      responseData.setThrottleTimeMs(throttleTimeMs)
      new CreateTopicsResponse(responseData)
    })
  }

  def createTopics(request: CreateTopicsRequestData,
                   hasClusterAuth: Boolean,
                   getCreatableTopics: Iterable[String] => Set[String]): CreateTopicsResponseData = {
    val topicNames = new util.HashSet[String]()
    val duplicateTopicNames = new util.HashSet[String]()
    request.topics().forEach { topicData =>
      if (!duplicateTopicNames.contains(topicData.name())) {
        if (!topicNames.add(topicData.name())) {
          topicNames.remove(topicData.name())
          duplicateTopicNames.add(topicData.name())
        }
      }
    }
    val authorizedTopicNames = if (hasClusterAuth) {
      topicNames.asScala
    } else {
      getCreatableTopics.apply(topicNames.asScala)
    }
    val effectiveRequest = request.duplicate()
    val iterator = effectiveRequest.topics().iterator()
    while (iterator.hasNext) {
      val creatableTopic = iterator.next()
      if (duplicateTopicNames.contains(creatableTopic.name()) ||
          !authorizedTopicNames.contains(creatableTopic.name())) {
        iterator.remove()
      }
    }
    val response = controller.createTopics(effectiveRequest).get()
    duplicateTopicNames.forEach { name =>
      response.topics().add(new CreatableTopicResult().
        setName(name).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Found multiple entries for this topic."))
    }
    topicNames.forEach { name =>
      if (!authorizedTopicNames.contains(name)) {
        response.topics().add(new CreatableTopicResult().
          setName(name).
          setErrorCode(TOPIC_AUTHORIZATION_FAILED.code()))
      }
    }
    response
  }

  def handleApiVersionsRequest(request: RequestChannel.Request): Unit = {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on an SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    def createResponseCallback(requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      } else if (!apiVersionRequest.isValid) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, INVALID_REQUEST.exception)
      } else {
        apiVersionManager.apiVersionResponse(requestThrottleMs)
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponseCallback)
  }

  private def handleVote(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new VoteResponse(response.asInstanceOf[VoteResponseData]))
  }

  private def handleBeginQuorumEpoch(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new BeginQuorumEpochResponse(response.asInstanceOf[BeginQuorumEpochResponseData]))
  }

  private def handleEndQuorumEpoch(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new EndQuorumEpochResponse(response.asInstanceOf[EndQuorumEpochResponseData]))
  }

  private def handleDescribeQuorum(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    handleRaftRequest(request, response => new DescribeQuorumResponse(response.asInstanceOf[DescribeQuorumResponseData]))
  }

  def handleAlterIsrRequest(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val alterIsrRequest = request.body[AlterIsrRequest]
    val future = controller.alterIsr(alterIsrRequest.data)
    future.whenComplete((result, exception) => {
      val response = if (exception != null) {
        alterIsrRequest.getErrorResponse(exception)
      } else {
        new AlterIsrResponse(result)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    })
  }

  def handleBrokerHeartBeatRequest(request: RequestChannel.Request): Unit = {
    val heartbeatRequest = request.body[BrokerHeartbeatRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    controller.processBrokerHeartbeat(heartbeatRequest.data).handle[Unit]((reply, e) => {
      def createResponseCallback(requestThrottleMs: Int,
                                 reply: BrokerHeartbeatReply,
                                 e: Throwable): BrokerHeartbeatResponse = {
        if (e != null) {
          new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.forException(e).code))
        } else {
          new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.NONE.code).
            setIsCaughtUp(reply.isCaughtUp).
            setIsFenced(reply.isFenced).
            setShouldShutDown(reply.shouldShutDown))
        }
      }
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, reply, e))
    })
  }

  def handleUnregisterBroker(request: RequestChannel.Request): Unit = {
    val decommissionRequest = request.body[UnregisterBrokerRequest]
    authHelper.authorizeClusterOperation(request, ALTER)

    controller.unregisterBroker(decommissionRequest.data().brokerId()).handle[Unit]((_, e) => {
      def createResponseCallback(requestThrottleMs: Int,
                                 e: Throwable): UnregisterBrokerResponse = {
        if (e != null) {
          new UnregisterBrokerResponse(new UnregisterBrokerResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.forException(e).code))
        } else {
          new UnregisterBrokerResponse(new UnregisterBrokerResponseData().
            setThrottleTimeMs(requestThrottleMs))
        }
      }
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, e))
    })
  }

  def handleBrokerRegistration(request: RequestChannel.Request): Unit = {
    val registrationRequest = request.body[BrokerRegistrationRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    controller.registerBroker(registrationRequest.data).handle[Unit]((reply, e) => {
      def createResponseCallback(requestThrottleMs: Int,
                                 reply: BrokerRegistrationReply,
                                 e: Throwable): BrokerRegistrationResponse = {
        if (e != null) {
          new BrokerRegistrationResponse(new BrokerRegistrationResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.forException(e).code()))
        } else {
          new BrokerRegistrationResponse(new BrokerRegistrationResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.NONE.code).
            setBrokerEpoch(reply.epoch))
        }
      }
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, reply, e))
    })
  }

  private def handleRaftRequest(request: RequestChannel.Request,
                                buildResponse: ApiMessage => AbstractResponse): Unit = {
    val requestBody = request.body[AbstractRequest]
    val future = raftManager.handleRequest(request.header, requestBody.data, time.milliseconds())

    future.whenComplete((responseData, exception) => {
      val response = if (exception != null) {
        requestBody.getErrorResponse(exception)
      } else {
        buildResponse(responseData)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    })
  }

  def handleAlterClientQuotas(request: RequestChannel.Request): Unit = {
    val quotaRequest = request.body[AlterClientQuotasRequest]
    authHelper.authorize(request.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)

    controller.alterClientQuotas(quotaRequest.entries, quotaRequest.validateOnly)
      .whenComplete((results, exception) => {
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            AlterClientQuotasResponse.fromQuotaEntities(results, requestThrottleMs))
        }
      })
  }

  def handleIncrementalAlterConfigs(request: RequestChannel.Request): Unit = {
    val alterConfigsRequest = request.body[IncrementalAlterConfigsRequest]
    authHelper.authorize(request.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)
    val configChanges = new util.HashMap[ConfigResource, util.Map[String, util.Map.Entry[AlterConfigOp.OpType, String]]]()
    alterConfigsRequest.data.resources.forEach { resource =>
      val configResource = new ConfigResource(ConfigResource.Type.forId(resource.resourceType), resource.resourceName())
      val altersByName = new util.HashMap[String, util.Map.Entry[AlterConfigOp.OpType, String]]()
      resource.configs.forEach { config =>
        altersByName.put(config.name, new util.AbstractMap.SimpleEntry[AlterConfigOp.OpType, String](
          AlterConfigOp.OpType.forId(config.configOperation), config.value))
      }
      configChanges.put(configResource, altersByName)
    }
    controller.incrementalAlterConfigs(configChanges, alterConfigsRequest.data.validateOnly)
      .whenComplete((results, exception) => {
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new IncrementalAlterConfigsResponse(requestThrottleMs, results))
        }
      })
  }
}
