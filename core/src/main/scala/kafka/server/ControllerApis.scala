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

import java.{lang, util}
import java.nio.ByteBuffer
import java.util.{Collections, OptionalLong}
import java.util.Map.Entry
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.logger.RuntimeLoggerManager
import kafka.server.metadata.KRaftMetadataCache
import kafka.utils.Logging
import org.apache.kafka.clients.admin.{AlterConfigOp, EndpointType}
import org.apache.kafka.common.Uuid.ZERO_UUID
import org.apache.kafka.common.acl.AclOperation.{ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, CREATE_TOKENS, DELETE, DESCRIBE, DESCRIBE_CONFIGS}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{ApiException, ClusterAuthorizationException, InvalidRequestException, TopicDeletionDisabledException, UnsupportedVersionException}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => OldAlterConfigsResourceResponse}
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DeleteTopicsResponseData.{DeletableTopicResult, DeletableTopicResultCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse
import org.apache.kafka.common.message.{CreateTopicsRequestData, _}
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, TOPIC, USER}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.Uuid
import org.apache.kafka.controller.ControllerRequestContext.requestTimeoutMsToDeadlineNs
import org.apache.kafka.controller.{Controller, ControllerRequestContext}
import org.apache.kafka.image.publisher.ControllerRegistrationsPublisher
import org.apache.kafka.metadata.{BrokerHeartbeatReply, BrokerRegistrationReply}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.ApiMessageAndVersion

import scala.jdk.CollectionConverters._


/**
 * Request handler for Controller APIs
 */
class ControllerApis(
  val requestChannel: RequestChannel,
  val authorizer: Option[Authorizer],
  val quotas: QuotaManagers,
  val time: Time,
  val controller: Controller,
  val raftManager: RaftManager[ApiMessageAndVersion],
  val config: KafkaConfig,
  val clusterId: String,
  val registrationsPublisher: ControllerRegistrationsPublisher,
  val apiVersionManager: ApiVersionManager,
  val metadataCache: KRaftMetadataCache
) extends ApiRequestHandler with Logging {

  this.logIdent = s"[ControllerApis nodeId=${config.nodeId}] "
  val authHelper = new AuthHelper(authorizer)
  val configHelper = new ConfigHelper(metadataCache, config, metadataCache)
  val requestHelper = new RequestHandlerHelper(requestChannel, quotas, time)
  val runtimeLoggerManager = new RuntimeLoggerManager(config.nodeId, logger.underlying)
  private val aclApis = new AclApis(authHelper, authorizer, requestHelper, "controller", config)

  def isClosed: Boolean = aclApis.isClosed

  def close(): Unit = aclApis.close()

  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    try {
      val handlerFuture: CompletableFuture[Unit] = request.header.apiKey match {
        case ApiKeys.FETCH => handleFetch(request)
        case ApiKeys.FETCH_SNAPSHOT => handleFetchSnapshot(request)
        case ApiKeys.CREATE_TOPICS => handleCreateTopics(request)
        case ApiKeys.DELETE_TOPICS => handleDeleteTopics(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.ALTER_CONFIGS => handleLegacyAlterConfigs(request)
        case ApiKeys.VOTE => handleVote(request)
        case ApiKeys.BEGIN_QUORUM_EPOCH => handleBeginQuorumEpoch(request)
        case ApiKeys.END_QUORUM_EPOCH => handleEndQuorumEpoch(request)
        case ApiKeys.DESCRIBE_QUORUM => handleDescribeQuorum(request)
        case ApiKeys.ALTER_PARTITION => handleAlterPartitionRequest(request)
        case ApiKeys.BROKER_REGISTRATION => handleBrokerRegistration(request)
        case ApiKeys.BROKER_HEARTBEAT => handleBrokerHeartBeatRequest(request)
        case ApiKeys.UNREGISTER_BROKER => handleUnregisterBroker(request)
        case ApiKeys.ALTER_CLIENT_QUOTAS => handleAlterClientQuotas(request)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => handleIncrementalAlterConfigs(request)
        case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => handleAlterPartitionReassignments(request)
        case ApiKeys.LIST_PARTITION_REASSIGNMENTS => handleListPartitionReassignments(request)
        case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => handleAlterUserScramCredentials(request)
        case ApiKeys.CREATE_DELEGATION_TOKEN => handleCreateDelegationTokenRequest(request)
        case ApiKeys.RENEW_DELEGATION_TOKEN => handleRenewDelegationTokenRequest(request)
        case ApiKeys.EXPIRE_DELEGATION_TOKEN => handleExpireDelegationTokenRequest(request)
        case ApiKeys.ENVELOPE => handleEnvelopeRequest(request, requestLocal)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.ALLOCATE_PRODUCER_IDS => handleAllocateProducerIdsRequest(request)
        case ApiKeys.CREATE_PARTITIONS => handleCreatePartitions(request)
        case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
        case ApiKeys.DESCRIBE_ACLS => aclApis.handleDescribeAcls(request)
        case ApiKeys.CREATE_ACLS => aclApis.handleCreateAcls(request)
        case ApiKeys.DELETE_ACLS => aclApis.handleDeleteAcls(request)
        case ApiKeys.ELECT_LEADERS => handleElectLeaders(request)
        case ApiKeys.UPDATE_FEATURES => handleUpdateFeatures(request)
        case ApiKeys.DESCRIBE_CLUSTER => handleDescribeCluster(request)
        case ApiKeys.CONTROLLER_REGISTRATION => handleControllerRegistration(request)
        case ApiKeys.ASSIGN_REPLICAS_TO_DIRS => handleAssignReplicasToDirs(request)
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey}")
      }

      // This catches exceptions in the future and subsequent completion stages returned by the request handlers.
      handlerFuture.whenComplete { (_, exception) =>
        if (exception != null) {
          // CompletionException does not include the stack frames in its "cause" exception, so we need to
          // log the original exception here
          error(s"Unexpected error handling request ${request.requestDesc(true)} " +
            s"with context ${request.context}", exception)
          requestHelper.handleError(request, exception)
        }
      }
    } catch {
      case e: FatalExitError => throw e
      case t: Throwable =>
        // This catches exceptions in the blocking parts of the request handlers
        error(s"Unexpected error handling request ${request.requestDesc(true)} " +
          s"with context ${request.context}", t)
        requestHelper.handleError(request, t)
    } finally {
      // Only record local completion time if it is unset.
      if (request.apiLocalCompleteTimeNanos < 0) {
        request.apiLocalCompleteTimeNanos = time.nanoseconds
      }
    }
  }

  def handleEnvelopeRequest(request: RequestChannel.Request, requestLocal: RequestLocal): CompletableFuture[Unit] = {
    if (!authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new ClusterAuthorizationException(
        s"Principal ${request.context.principal} does not have required CLUSTER_ACTION for envelope"))
    } else {
      EnvelopeUtils.handleEnvelopeRequest(request, requestChannel.metrics, handle(_, requestLocal))
    }
    CompletableFuture.completedFuture[Unit](())
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val responseData = new SaslHandshakeResponseData().setErrorCode(ILLEGAL_SASL_STATE.code)
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslHandshakeResponse(responseData))
    CompletableFuture.completedFuture[Unit](())
  }

  def handleSaslAuthenticateRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val responseData = new SaslAuthenticateResponseData()
      .setErrorCode(ILLEGAL_SASL_STATE.code)
      .setErrorMessage("SaslAuthenticate request received after successful authentication")
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslAuthenticateResponse(responseData))
    CompletableFuture.completedFuture[Unit](())
  }

  def handleFetch(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new FetchResponse(response.asInstanceOf[FetchResponseData]))
  }

  def handleFetchSnapshot(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new FetchSnapshotResponse(response.asInstanceOf[FetchSnapshotResponseData]))
  }

  private def handleDeleteTopics(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val deleteTopicsRequest = request.body[DeleteTopicsRequest]
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 5)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, deleteTopicsRequest.data.timeoutMs),
      controllerMutationQuotaRecorderFor(controllerMutationQuota))
    val future = deleteTopics(context,
      deleteTopicsRequest.data,
      request.context.apiVersion,
      authHelper.authorize(request.context, DELETE, CLUSTER, CLUSTER_NAME, logIfDenied = false),
      names => authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC, names)(n => n),
      names => authHelper.filterByAuthorized(request.context, DELETE, TOPIC, names)(n => n))
    future.handle[Unit] { (results, exception) =>
      val response = if (exception != null) {
        deleteTopicsRequest.getErrorResponse(exception)
      } else {
        val responseData = new DeleteTopicsResponseData()
          .setResponses(new DeletableTopicResultCollection(results.iterator))
        new DeleteTopicsResponse(responseData)
      }
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, response)
    }
  }

  def deleteTopics(
    context: ControllerRequestContext,
    request: DeleteTopicsRequestData,
    apiVersion: Int,
    hasClusterAuth: Boolean,
    getDescribableTopics: Iterable[String] => Set[String],
    getDeletableTopics: Iterable[String] => Set[String]
  ): CompletableFuture[util.List[DeletableTopicResult]] = {
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
    controller.findTopicNames(context, providedIds).thenCompose { topicNames =>
      topicNames.forEach { (id, nameOrError) =>
        if (nameOrError.isError) {
          appendResponse(null, id, nameOrError.error())
        } else {
          toAuthenticate.add(nameOrError.result())
          idToName.put(id, nameOrError.result())
        }
      }
      // Get the list of deletable topics (those we can delete) and the list of describable
      // topics.
      val topicsToAuthenticate = toAuthenticate.asScala
      val (describable, deletable) = if (hasClusterAuth) {
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
          if (describable.contains(name)) {
            appendResponse(name, id, new ApiError(TOPIC_AUTHORIZATION_FAILED))
          } else {
            appendResponse(null, id, new ApiError(TOPIC_AUTHORIZATION_FAILED))
          }
          iterator.remove()
        }
      }
      // For each topic that was provided by name, check if authentication failed.
      // If so, create an error response for it. Otherwise, add it to the idToName map.
      controller.findTopicIds(context, providedNames).thenCompose { topicIds =>
        topicIds.forEach { (name, idOrError) =>
          if (!describable.contains(name)) {
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
        controller.deleteTopics(context, idToName.keySet).thenApply { idToError =>
          idToError.forEach { (id, error) =>
            appendResponse(idToName.get(id), id, error)
          }
          // Shuffle the responses so that users can not use patterns in their positions to
          // distinguish between absent topics and topics we are not permitted to see.
          Collections.shuffle(responses)
          responses
        }
      }
    }
  }

  private def handleCreateTopics(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val createTopicsRequest = request.body[CreateTopicsRequest]
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 6)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, createTopicsRequest.data.timeoutMs),
      controllerMutationQuotaRecorderFor(controllerMutationQuota))
    val future = createTopics(context,
        createTopicsRequest.data,
        authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME, logIfDenied = false),
        names => authHelper.filterByAuthorized(request.context, CREATE, TOPIC, names)(identity),
        names => authHelper.filterByAuthorized(request.context, DESCRIBE_CONFIGS, TOPIC,
            names, logIfDenied = false)(identity))
    future.handle[Unit] { (result, exception) =>
      val response = if (exception != null) {
        createTopicsRequest.getErrorResponse(exception)
      } else {
        new CreateTopicsResponse(result)
      }
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, response)
    }
  }

  private def controllerMutationQuotaRecorderFor(controllerMutationQuota: ControllerMutationQuota) = {
    new Consumer[lang.Integer]() {
      override def accept(permits: lang.Integer): Unit = controllerMutationQuota.record(permits.doubleValue())
    }
  }

  def createTopics(
    context: ControllerRequestContext,
    request: CreateTopicsRequestData,
    hasClusterAuth: Boolean,
    getCreatableTopics: Iterable[String] => Set[String],
    getDescribableTopics: Iterable[String] => Set[String]
  ): CompletableFuture[CreateTopicsResponseData] = {
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

    /* The cluster metadata topic is an internal topic with a different implementation. The user should not be
     * allowed to create it as a regular topic.
     */
    if (topicNames.contains(Topic.CLUSTER_METADATA_TOPIC_NAME)) {
      info(s"Rejecting creation of internal topic ${Topic.CLUSTER_METADATA_TOPIC_NAME}")
    }
    val allowedTopicNames = topicNames.asScala.diff(Set(Topic.CLUSTER_METADATA_TOPIC_NAME))

    val authorizedTopicNames = if (hasClusterAuth) {
      allowedTopicNames
    } else {
      getCreatableTopics.apply(allowedTopicNames)
    }
    val describableTopicNames = getDescribableTopics.apply(allowedTopicNames).asJava
    val effectiveRequest = request.duplicate()
    val iterator = effectiveRequest.topics().iterator()
    while (iterator.hasNext) {
      val creatableTopic = iterator.next()
      if (duplicateTopicNames.contains(creatableTopic.name()) ||
          !authorizedTopicNames.contains(creatableTopic.name())) {
        iterator.remove()
      }
    }
    controller.createTopics(context, effectiveRequest, describableTopicNames).thenApply { response =>
      duplicateTopicNames.forEach { name =>
        response.topics().add(new CreatableTopicResult().
          setName(name).
          setErrorCode(INVALID_REQUEST.code).
          setErrorMessage("Duplicate topic name."))
      }
      topicNames.forEach { name =>
        if (!authorizedTopicNames.contains(name)) {
          response.topics().add(new CreatableTopicResult().
            setName(name).
            setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
            setErrorMessage("Authorization failed."))
        }
      }
      response
    }
  }

  def handleApiVersionsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    // Note that controller returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on an SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the controller version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    val apiVersionRequest = request.body[ApiVersionsRequest]
    if (apiVersionRequest.hasUnsupportedRequestVersion) {
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => apiVersionRequest.getErrorResponse(requestThrottleMs, UNSUPPORTED_VERSION.exception))
    } else if (!apiVersionRequest.isValid) {
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => apiVersionRequest.getErrorResponse(requestThrottleMs, INVALID_REQUEST.exception))
    } else {
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => apiVersionManager.apiVersionResponse(requestThrottleMs))
    }
    CompletableFuture.completedFuture[Unit](())
  }

  private def authorizeAlterResource(requestContext: RequestContext,
                                     resource: ConfigResource): ApiError = {
    resource.`type` match {
      case ConfigResource.Type.BROKER | ConfigResource.Type.CLIENT_METRICS =>
        if (authHelper.authorize(requestContext, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)) {
          new ApiError(NONE)
        } else {
          new ApiError(CLUSTER_AUTHORIZATION_FAILED)
        }
      case ConfigResource.Type.TOPIC =>
        if (authHelper.authorize(requestContext, ALTER_CONFIGS, TOPIC, resource.name)) {
          new ApiError(NONE)
        } else {
          new ApiError(TOPIC_AUTHORIZATION_FAILED)
        }
      case rt => new ApiError(INVALID_REQUEST, s"Unexpected resource type $rt.")
    }
  }

  def handleLegacyAlterConfigs(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val response = new AlterConfigsResponseData()
    val alterConfigsRequest = request.body[AlterConfigsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal, OptionalLong.empty())
    val duplicateResources = new util.HashSet[ConfigResource]
    val configChanges = new util.HashMap[ConfigResource, util.Map[String, String]]()
    alterConfigsRequest.data.resources.forEach { resource =>
      val configResource = new ConfigResource(
        ConfigResource.Type.forId(resource.resourceType), resource.resourceName())
      if (configResource.`type`().equals(ConfigResource.Type.UNKNOWN)) {
        response.responses().add(new OldAlterConfigsResourceResponse().
          setErrorCode(UNSUPPORTED_VERSION.code()).
          setErrorMessage("Unknown resource type " + resource.resourceType() + ".").
          setResourceName(resource.resourceName()).
          setResourceType(resource.resourceType()))
      } else if (!duplicateResources.contains(configResource)) {
        val configs = new util.HashMap[String, String]()
        resource.configs().forEach(config => configs.put(config.name(), config.value()))
        if (configChanges.put(configResource, configs) != null) {
          duplicateResources.add(configResource)
          configChanges.remove(configResource)
          response.responses().add(new OldAlterConfigsResourceResponse().
            setErrorCode(INVALID_REQUEST.code()).
            setErrorMessage("Duplicate resource.").
            setResourceName(resource.resourceName()).
            setResourceType(resource.resourceType()))
        }
      }
    }
    val iterator = configChanges.keySet().iterator()
    while (iterator.hasNext) {
      val resource = iterator.next()
      val apiError = authorizeAlterResource(request.context, resource)
      if (apiError.isFailure) {
        response.responses().add(new OldAlterConfigsResourceResponse().
          setErrorCode(apiError.error().code()).
          setErrorMessage(apiError.message()).
          setResourceName(resource.name()).
          setResourceType(resource.`type`().id()))
        iterator.remove()
      }
    }
    controller.legacyAlterConfigs(context, configChanges, alterConfigsRequest.data.validateOnly)
      .handle[Unit] { (controllerResults, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          controllerResults.forEach((key, value) => response.responses().add(
            new OldAlterConfigsResourceResponse().
              setErrorCode(value.error().code()).
              setErrorMessage(value.message()).
              setResourceName(key.name()).
              setResourceType(key.`type`().id())))
          requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
            new AlterConfigsResponse(response.setThrottleTimeMs(throttleMs)))
        }
      }
  }

  def handleVote(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new VoteResponse(response.asInstanceOf[VoteResponseData]))
  }

  def handleBeginQuorumEpoch(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new BeginQuorumEpochResponse(response.asInstanceOf[BeginQuorumEpochResponseData]))
  }

  def handleEndQuorumEpoch(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new EndQuorumEpochResponse(response.asInstanceOf[EndQuorumEpochResponseData]))
  }

  def handleDescribeQuorum(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    handleRaftRequest(request, response => new DescribeQuorumResponse(response.asInstanceOf[DescribeQuorumResponseData]))
  }

  def handleElectLeaders(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, ALTER)
    val electLeadersRequest = request.body[ElectLeadersRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, electLeadersRequest.data.timeoutMs))
    val future = controller.electLeaders(context, electLeadersRequest.data)
    future.handle[Unit] { (responseData, exception) =>
      if (exception != null) {
        requestHelper.sendResponseMaybeThrottle(request, throttleMs => {
          electLeadersRequest.getErrorResponse(throttleMs, exception)
        })
      } else {
        requestHelper.sendResponseMaybeThrottle(request, throttleMs => {
          new ElectLeadersResponse(responseData.setThrottleTimeMs(throttleMs))
        })
      }
    }
  }

  def handleAlterPartitionRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val alterPartitionRequest = request.body[AlterPartitionRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val future = controller.alterPartition(context, alterPartitionRequest.data)
    future.handle[Unit] { (result, exception) =>
      val response = if (exception != null) {
        alterPartitionRequest.getErrorResponse(exception)
      } else {
        new AlterPartitionResponse(result)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    }
  }

  def handleBrokerHeartBeatRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val heartbeatRequest = request.body[BrokerHeartbeatRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, config.brokerHeartbeatIntervalMs / 2))
    controller.processBrokerHeartbeat(context, heartbeatRequest.data).handle[Unit] { (reply, e) =>
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
            setErrorCode(NONE.code).
            setIsCaughtUp(reply.isCaughtUp).
            setIsFenced(reply.isFenced).
            setShouldShutDown(reply.shouldShutDown))
        }
      }
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, reply, e))
    }
  }

  def handleUnregisterBroker(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val decommissionRequest = request.body[UnregisterBrokerRequest]
    authHelper.authorizeClusterOperation(request, ALTER)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())

    controller.unregisterBroker(context, decommissionRequest.data.brokerId).handle[Unit] { (_, e) =>
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
    }
  }

  private def handleBrokerRegistration(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val registrationRequest = request.body[BrokerRegistrationRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())

    controller.registerBroker(context, registrationRequest.data).handle[Unit] { (reply, e) =>
      def createResponseCallback(requestThrottleMs: Int,
                                 reply: BrokerRegistrationReply,
                                 e: Throwable): BrokerRegistrationResponse = {
        if (e != null) {
          new BrokerRegistrationResponse(new BrokerRegistrationResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.forException(e).code))
        } else {
          new BrokerRegistrationResponse(new BrokerRegistrationResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(NONE.code).
            setBrokerEpoch(reply.epoch))
        }
      }
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, reply, e))
    }
  }

  private def handleRaftRequest(request: RequestChannel.Request,
                                buildResponse: ApiMessage => AbstractResponse): CompletableFuture[Unit] = {
    val requestBody = request.body[AbstractRequest]
    val future = raftManager.handleRequest(request.header, requestBody.data, time.milliseconds())
    future.handle[Unit] { (responseData, exception) =>
      val response = if (exception != null) {
        requestBody.getErrorResponse(exception)
      } else {
        buildResponse(responseData)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    }
  }

  def handleAlterClientQuotas(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val quotaRequest = request.body[AlterClientQuotasRequest]
    authHelper.authorizeClusterOperation(request, ALTER_CONFIGS)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.alterClientQuotas(context, quotaRequest.entries, quotaRequest.validateOnly)
      .handle[Unit] { (results, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            AlterClientQuotasResponse.fromQuotaEntities(results, requestThrottleMs))
        }
      }
  }

  def handleIncrementalAlterConfigs(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val response = new IncrementalAlterConfigsResponseData()
    val alterConfigsRequest = request.body[IncrementalAlterConfigsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    val duplicateResources = new util.HashSet[ConfigResource]
    val configChanges = new util.HashMap[ConfigResource,
      util.Map[String, Entry[AlterConfigOp.OpType, String]]]()
    val brokerLoggerResponses = new util.ArrayList[AlterConfigsResourceResponse](1)
    alterConfigsRequest.data.resources.forEach { resource =>
      val configResource = new ConfigResource(
        ConfigResource.Type.forId(resource.resourceType), resource.resourceName())
      if (configResource.`type`().equals(ConfigResource.Type.BROKER_LOGGER)) {
        val apiError = try {
          runtimeLoggerManager.applyChangesForResource(
            authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME),
            alterConfigsRequest.data().validateOnly(),
            resource)
          ApiError.NONE
        } catch {
          case t: Throwable => ApiError.fromThrowable(t)
        }
        brokerLoggerResponses.add(new AlterConfigsResourceResponse().
          setResourceName(resource.resourceName()).
          setResourceType(resource.resourceType()).
          setErrorCode(apiError.error().code()).
          setErrorMessage(if (apiError.isFailure) apiError.messageWithFallback() else null))
      } else if (configResource.`type`().equals(ConfigResource.Type.UNKNOWN)) {
        response.responses().add(new AlterConfigsResourceResponse().
          setErrorCode(UNSUPPORTED_VERSION.code()).
          setErrorMessage("Unknown resource type " + resource.resourceType() + ".").
          setResourceName(resource.resourceName()).
          setResourceType(resource.resourceType()))
      } else if (!duplicateResources.contains(configResource)) {
        val altersByName = new util.HashMap[String, Entry[AlterConfigOp.OpType, String]]()
        resource.configs.forEach { config =>
          altersByName.put(config.name, new util.AbstractMap.SimpleEntry[AlterConfigOp.OpType, String](
            AlterConfigOp.OpType.forId(config.configOperation), config.value))
        }
        if (configChanges.put(configResource, altersByName) != null) {
          duplicateResources.add(configResource)
          configChanges.remove(configResource)
          response.responses().add(new AlterConfigsResourceResponse().
            setErrorCode(INVALID_REQUEST.code()).
            setErrorMessage("Duplicate resource.").
            setResourceName(resource.resourceName()).
            setResourceType(resource.resourceType()))
        }
      }
    }
    val iterator = configChanges.keySet().iterator()
    while (iterator.hasNext) {
      val resource = iterator.next()
      val apiError = authorizeAlterResource(request.context, resource)
      if (apiError.isFailure) {
        response.responses().add(new AlterConfigsResourceResponse().
          setErrorCode(apiError.error().code()).
          setErrorMessage(apiError.message()).
          setResourceName(resource.name()).
          setResourceType(resource.`type`().id()))
        iterator.remove()
      }
    }
    controller.incrementalAlterConfigs(context, configChanges, alterConfigsRequest.data.validateOnly)
      .handle[Unit] { (controllerResults, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          controllerResults.forEach((key, value) => response.responses().add(
            new AlterConfigsResourceResponse().
              setErrorCode(value.error().code()).
              setErrorMessage(value.message()).
              setResourceName(key.name()).
              setResourceType(key.`type`().id())))
          brokerLoggerResponses.forEach(r => response.responses().add(r))
          requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
            new IncrementalAlterConfigsResponse(response.setThrottleTimeMs(throttleMs)))
        }
      }
  }

  private def handleCreatePartitions(request: RequestChannel.Request): CompletableFuture[Unit] = {
    def filterAlterAuthorizedTopics(topics: Iterable[String]): Set[String] = {
      authHelper.filterByAuthorized(request.context, ALTER, TOPIC, topics)(n => n)
    }
    val createPartitionsRequest = request.body[CreatePartitionsRequest]
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 3)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, createPartitionsRequest.data.timeoutMs),
      controllerMutationQuotaRecorderFor(controllerMutationQuota))
    val future = createPartitions(context,
      createPartitionsRequest.data(),
      filterAlterAuthorizedTopics)
    future.handle[Unit] { (responses, exception) =>
      val response = if (exception != null) {
        createPartitionsRequest.getErrorResponse(exception)
      } else {
        val responseData = new CreatePartitionsResponseData().setResults(responses)
        new CreatePartitionsResponse(responseData)
      }
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, response)
    }
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val responseData = configHelper.handleDescribeConfigsRequest(request, authHelper)
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeConfigsResponse(responseData.setThrottleTimeMs(requestThrottleMs)))
    CompletableFuture.completedFuture[Unit](())
  }

  def createPartitions(
    context: ControllerRequestContext,
    request: CreatePartitionsRequestData,
    getAlterAuthorizedTopics: Iterable[String] => Set[String]
  ): CompletableFuture[util.List[CreatePartitionsTopicResult]] = {
    val responses = new util.ArrayList[CreatePartitionsTopicResult]()
    val duplicateTopicNames = new util.HashSet[String]()
    val topicNames = new util.HashSet[String]()
    request.topics().forEach {
      topic =>
        if (!topicNames.add(topic.name())) {
          duplicateTopicNames.add(topic.name())
        }
    }
    duplicateTopicNames.forEach { topicName =>
      responses.add(new CreatePartitionsTopicResult().
        setName(topicName).
        setErrorCode(INVALID_REQUEST.code).
        setErrorMessage("Duplicate topic name."))
        topicNames.remove(topicName)
    }
    val authorizedTopicNames = getAlterAuthorizedTopics(topicNames.asScala)
    val topics = new util.ArrayList[CreatePartitionsTopic]
    topicNames.forEach { topicName =>
      if (authorizedTopicNames.contains(topicName)) {
        topics.add(request.topics().find(topicName))
      } else {
        responses.add(new CreatePartitionsTopicResult().
          setName(topicName).
          setErrorCode(TOPIC_AUTHORIZATION_FAILED.code))
      }
    }
    controller.createPartitions(context, topics, request.validateOnly).thenApply { results =>
      results.forEach(response => responses.add(response))
      responses
    }
  }

  def handleControllerRegistration(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val registrationRequest = request.body[ControllerRegistrationRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())

    controller.registerController(context, registrationRequest.data)
      .thenApply[Unit] { _ =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new ControllerRegistrationResponse(new ControllerRegistrationResponseData().
            setThrottleTimeMs(requestThrottleMs)))
      }
  }

  def handleAlterPartitionReassignments(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val alterRequest = request.body[AlterPartitionReassignmentsRequest]
    authHelper.authorizeClusterOperation(request, ALTER)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, alterRequest.data.timeoutMs))
    controller.alterPartitionReassignments(context, alterRequest.data)
      .thenApply[Unit] { response =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new AlterPartitionReassignmentsResponse(response.setThrottleTimeMs(requestThrottleMs)))
      }
  }

  private def handleAlterUserScramCredentials(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val alterRequest = request.body[AlterUserScramCredentialsRequest]
    authHelper.authorizeClusterOperation(request, ALTER)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.alterUserScramCredentials(context, alterRequest.data)
      .thenApply[Unit] { response =>
         requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
           new AlterUserScramCredentialsResponse(response.setThrottleTimeMs(requestThrottleMs)))
      }
  }

  // The principal is carried through in the forwarded case.
  // The security protocol in the context is for the current connection (hop)
  // We need to always disallow a tokenAuthenticated principal
  // We need to allow special protocols but only in the forwarded case for testing.
  def allowTokenRequests(request: RequestChannel.Request): Boolean = {
    val protocol = request.context.securityProtocol
    if (request.context.principal.tokenAuthenticated ||
      // We allow forwarded requests to use PLAINTEXT for testing purposes
      (request.isForwarded == false && protocol == SecurityProtocol.PLAINTEXT) ||
      // disallow requests from 1-way SSL
      (request.isForwarded == false && protocol == SecurityProtocol.SSL && request.context.principal == KafkaPrincipal.ANONYMOUS))
      false
    else
      true
  }

  private def handleCreateDelegationTokenRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val createTokenRequest = request.body[CreateDelegationTokenRequest]

    val requester = request.context.principal
    val ownerPrincipalName = createTokenRequest.data.ownerPrincipalName
    val ownerPrincipalType = createTokenRequest.data.ownerPrincipalType
    val owner = if (ownerPrincipalName == null || ownerPrincipalName.isEmpty) {
      request.context.principal
    } else {
      new KafkaPrincipal(ownerPrincipalType, ownerPrincipalName)
    }

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, owner, requester))
      CompletableFuture.completedFuture[Unit](())
    } else if (!owner.equals(requester) && 
      !authHelper.authorize(request.context, CREATE_TOKENS, USER, owner.toString)) {
      // Requester is always allowed to create token for self
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, owner, requester))
        CompletableFuture.completedFuture[Unit](())
    } else {

      val context = new ControllerRequestContext(request.context.header.data,
        request.context.principal, OptionalLong.empty())

      // Copy the response data to a new response so we can apply the request version
      controller.createDelegationToken(context, createTokenRequest.data)
        .thenApply[Unit] { response =>
           requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
             CreateDelegationTokenResponse.prepareResponse(
               request.context.requestVersion,
               requestThrottleMs,
               Errors.forCode(response.errorCode()),
               new KafkaPrincipal(response.principalType(), response.principalName()),
               new KafkaPrincipal(response.tokenRequesterPrincipalType(), response.tokenRequesterPrincipalName()),
               response.issueTimestampMs(),
               response.expiryTimestampMs(),
               response.maxTimestampMs(),
               response.tokenId(),
               ByteBuffer.wrap(response.hmac())))
      }
    }
  }

  private def handleRenewDelegationTokenRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val renewTokenRequest = request.body[RenewDelegationTokenRequest]

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new RenewDelegationTokenResponse(
          new RenewDelegationTokenResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
              .setExpiryTimestampMs(DelegationTokenManager.ErrorTimestamp)))
        CompletableFuture.completedFuture[Unit](())
    } else {
      val context = new ControllerRequestContext(
        request.context.header.data,
        request.context.principal,
        OptionalLong.empty())
      controller.renewDelegationToken(context, renewTokenRequest.data)
        .thenApply[Unit] { response =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new RenewDelegationTokenResponse(response.setThrottleTimeMs(requestThrottleMs)))
      }
    }
  }

  private def handleExpireDelegationTokenRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val expireTokenRequest = request.body[ExpireDelegationTokenRequest]

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ExpireDelegationTokenResponse(
          new ExpireDelegationTokenResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
              .setExpiryTimestampMs(DelegationTokenManager.ErrorTimestamp)))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val context = new ControllerRequestContext(
        request.context.header.data,
        request.context.principal,
        OptionalLong.empty())
      controller.expireDelegationToken(context, expireTokenRequest.data)
        .thenApply[Unit] { response =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new ExpireDelegationTokenResponse(response.setThrottleTimeMs(requestThrottleMs)))
      }
    }
  }

  def handleListPartitionReassignments(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val listRequest = request.body[ListPartitionReassignmentsRequest]
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.listPartitionReassignments(context, listRequest.data)
      .thenApply[Unit] { response =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new ListPartitionReassignmentsResponse(response.setThrottleTimeMs(requestThrottleMs)))
      }
  }

  def handleAllocateProducerIdsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val allocatedProducerIdsRequest = request.body[AllocateProducerIdsRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
        OptionalLong.empty())
    controller.allocateProducerIds(context, allocatedProducerIdsRequest.data)
      .handle[Unit] { (results, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            results.setThrottleTimeMs(requestThrottleMs)
            new AllocateProducerIdsResponse(results)
          })
        }
      }
  }

  def handleUpdateFeatures(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val updateFeaturesRequest = request.body[UpdateFeaturesRequest]
    authHelper.authorizeClusterOperation(request, ALTER)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.updateFeatures(context, updateFeaturesRequest.data)
      .handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new UpdateFeaturesResponse(response.setThrottleTimeMs(requestThrottleMs)))
        }
      }
  }

  def handleDescribeCluster(request: RequestChannel.Request): CompletableFuture[Unit] = {
    // Nearly all RPCs should check MetadataVersion inside the QuorumController. However, this
    // RPC is consulting a cache which lives outside the QC. So we check MetadataVersion here.
    if (!apiVersionManager.features.metadataVersion().isControllerRegistrationSupported) {
      throw new UnsupportedVersionException("Direct-to-controller communication is not " +
        "supported with the current MetadataVersion.")
    }
    // Unlike on the broker, DESCRIBE_CLUSTER on the controller requires a high level of
    // permissions (ALTER on CLUSTER).
    authHelper.authorizeClusterOperation(request, ALTER)
    val response = authHelper.computeDescribeClusterResponse(
      request,
      EndpointType.CONTROLLER,
      clusterId,
      () => registrationsPublisher.describeClusterControllers(request.context.listenerName()),
      () => raftManager.leaderAndEpoch.leaderId().orElse(-1)
    )
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeClusterResponse(response.setThrottleTimeMs(requestThrottleMs)))
    CompletableFuture.completedFuture[Unit](())
  }

  private def handleAssignReplicasToDirs(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val assignReplicasToDirsRequest = request.body[AssignReplicasToDirsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.assignReplicasToDirs(context, assignReplicasToDirsRequest.data).thenApply { reply =>
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => new AssignReplicasToDirsResponse(reply.setThrottleTimeMs(requestThrottleMs)))
    }
  }
}
