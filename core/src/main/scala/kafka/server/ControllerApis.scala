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
import java.util.Map.Entry
import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}
import java.util.concurrent.{CompletableFuture, ExecutionException}

import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.common.Uuid.ZERO_UUID
import org.apache.kafka.common.acl.AclOperation.{ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, DELETE, DESCRIBE}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{ApiException, ClusterAuthorizationException, InvalidRequestException, TopicDeletionDisabledException}
import org.apache.kafka.common.internals.FatalExitError
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
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, TOPIC}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.controller.Controller
import org.apache.kafka.metadata.{BrokerHeartbeatReply, BrokerRegistrationReply, VersionRange}
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.ApiMessageAndVersion

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
  private val aclApis = new AclApis(authHelper, authorizer, requestHelper, "controller", config)

  def isClosed: Boolean = aclApis.isClosed

  def close(): Unit = aclApis.close()

  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    try {
      request.header.apiKey match {
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
        case ApiKeys.ENVELOPE => handleEnvelopeRequest(request, requestLocal)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.ALLOCATE_PRODUCER_IDS => handleAllocateProducerIdsRequest(request)
        case ApiKeys.CREATE_PARTITIONS => handleCreatePartitions(request)
        case ApiKeys.DESCRIBE_ACLS => aclApis.handleDescribeAcls(request)
        case ApiKeys.CREATE_ACLS => aclApis.handleCreateAcls(request)
        case ApiKeys.DELETE_ACLS => aclApis.handleDeleteAcls(request)
        case ApiKeys.ELECT_LEADERS => handleElectLeaders(request)
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => {
        val t = if (e.isInstanceOf[ExecutionException]) e.getCause() else e
        error(s"Unexpected error handling request ${request.requestDesc(true)} " +
          s"with context ${request.context}", t)
        requestHelper.handleError(request, t)
      }
    }
  }

  def handleEnvelopeRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    if (!authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new ClusterAuthorizationException(
        s"Principal ${request.context.principal} does not have required CLUSTER_ACTION for envelope"))
    } else {
      EnvelopeUtils.handleEnvelopeRequest(request, requestChannel.metrics, handle(_, requestLocal))
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslHandshakeResponseData().setErrorCode(ILLEGAL_SASL_STATE.code)
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslHandshakeResponse(responseData))
  }

  def handleSaslAuthenticateRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslAuthenticateResponseData()
      .setErrorCode(ILLEGAL_SASL_STATE.code)
      .setErrorMessage("SaslAuthenticate request received after successful authentication")
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslAuthenticateResponse(responseData))
  }

  def handleFetch(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new FetchResponse(response.asInstanceOf[FetchResponseData]))
  }

  def handleFetchSnapshot(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new FetchSnapshotResponse(response.asInstanceOf[FetchSnapshotResponseData]))
  }

  def handleDeleteTopics(request: RequestChannel.Request): Unit = {
    val deleteTopicsRequest = request.body[DeleteTopicsRequest]
    val future = deleteTopics(deleteTopicsRequest.data,
      request.context.apiVersion,
      authHelper.authorize(request.context, DELETE, CLUSTER, CLUSTER_NAME, logIfDenied = false),
      names => authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC, names)(n => n),
      names => authHelper.filterByAuthorized(request.context, DELETE, TOPIC, names)(n => n))
    future.whenComplete { (results, exception) =>
      requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => {
        if (exception != null) {
          deleteTopicsRequest.getErrorResponse(throttleTimeMs, exception)
        } else {
          val responseData = new DeleteTopicsResponseData().
            setResponses(new DeletableTopicResultCollection(results.iterator)).
            setThrottleTimeMs(throttleTimeMs)
          new DeleteTopicsResponse(responseData)
        }
      })
    }
  }

  def deleteTopics(request: DeleteTopicsRequestData,
                   apiVersion: Int,
                   hasClusterAuth: Boolean,
                   getDescribableTopics: Iterable[String] => Set[String],
                   getDeletableTopics: Iterable[String] => Set[String])
                   : CompletableFuture[util.List[DeletableTopicResult]] = {
    // Check if topic deletion is enabled at all.
    if (!config.deleteTopicEnable) {
      if (apiVersion < 3) {
        throw new InvalidRequestException("Topic deletion is disabled.")
      } else {
        throw new TopicDeletionDisabledException()
      }
    }
    val deadlineNs = time.nanoseconds() + NANOSECONDS.convert(request.timeoutMs, MILLISECONDS);
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
    controller.findTopicNames(deadlineNs, providedIds).thenCompose { topicNames =>
      topicNames.forEach { (id, nameOrError) =>
        if (nameOrError.isError) {
          appendResponse(null, id, nameOrError.error())
        } else {
          toAuthenticate.add(nameOrError.result())
          idToName.put(id, nameOrError.result())
        }
      }
      // Get the list of deletable topics (those we can delete) and the list of describeable
      // topics.
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
      // If so, create an error response for it. Otherwise, add it to the idToName map.
      controller.findTopicIds(deadlineNs, providedNames).thenCompose { topicIds =>
        topicIds.forEach { (name, idOrError) =>
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
        controller.deleteTopics(deadlineNs, idToName.keySet).thenApply { idToError =>
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

  def handleCreateTopics(request: RequestChannel.Request): Unit = {
    val createTopicsRequest = request.body[CreateTopicsRequest]
    val future = createTopics(createTopicsRequest.data(),
        authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME, logIfDenied = false),
        names => authHelper.filterByAuthorized(request.context, CREATE, TOPIC, names)(identity))
    future.whenComplete { (result, exception) =>
      requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => {
        if (exception != null) {
          createTopicsRequest.getErrorResponse(throttleTimeMs, exception)
        } else {
          result.setThrottleTimeMs(throttleTimeMs)
          new CreateTopicsResponse(result)
        }
      })
    }
  }

  def createTopics(request: CreateTopicsRequestData,
                   hasClusterAuth: Boolean,
                   getCreatableTopics: Iterable[String] => Set[String])
                   : CompletableFuture[CreateTopicsResponseData] = {
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
    controller.createTopics(effectiveRequest).thenApply { response =>
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
            setErrorCode(TOPIC_AUTHORIZATION_FAILED.code))
        }
      }
      response
    }
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
        apiVersionRequest.getErrorResponse(requestThrottleMs, UNSUPPORTED_VERSION.exception)
      } else if (!apiVersionRequest.isValid) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, INVALID_REQUEST.exception)
      } else {
        apiVersionManager.apiVersionResponse(requestThrottleMs)
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponseCallback)
  }

  def authorizeAlterResource(requestContext: RequestContext,
                             resource: ConfigResource): ApiError = {
    resource.`type` match {
      case ConfigResource.Type.BROKER =>
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

  def handleLegacyAlterConfigs(request: RequestChannel.Request): Unit = {
    val response = new AlterConfigsResponseData()
    val alterConfigsRequest = request.body[AlterConfigsRequest]
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
    controller.legacyAlterConfigs(configChanges, alterConfigsRequest.data.validateOnly)
      .whenComplete { (controllerResults, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          controllerResults.entrySet().forEach(entry => response.responses().add(
            new OldAlterConfigsResourceResponse().
              setErrorCode(entry.getValue.error().code()).
              setErrorMessage(entry.getValue.message()).
              setResourceName(entry.getKey.name()).
              setResourceType(entry.getKey.`type`().id())))
          requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
            new AlterConfigsResponse(response.setThrottleTimeMs(throttleMs)))
        }
      }
  }

  def handleVote(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new VoteResponse(response.asInstanceOf[VoteResponseData]))
  }

  def handleBeginQuorumEpoch(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new BeginQuorumEpochResponse(response.asInstanceOf[BeginQuorumEpochResponseData]))
  }

  def handleEndQuorumEpoch(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new EndQuorumEpochResponse(response.asInstanceOf[EndQuorumEpochResponseData]))
  }

  def handleDescribeQuorum(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    handleRaftRequest(request, response => new DescribeQuorumResponse(response.asInstanceOf[DescribeQuorumResponseData]))
  }

  def handleElectLeaders(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, ALTER)

    val electLeadersRequest = request.body[ElectLeadersRequest]
    val future = controller.electLeaders(electLeadersRequest.data)
    future.whenComplete { (responseData, exception) =>
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

  def handleAlterPartitionRequest(request: RequestChannel.Request): Unit = {
    val alterPartitionRequest = request.body[AlterPartitionRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val future = controller.alterPartition(alterPartitionRequest.data)
    future.whenComplete { (result, exception) =>
      val response = if (exception != null) {
        alterPartitionRequest.getErrorResponse(exception)
      } else {
        new AlterPartitionResponse(result)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    }
  }

  def handleBrokerHeartBeatRequest(request: RequestChannel.Request): Unit = {
    val heartbeatRequest = request.body[BrokerHeartbeatRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    controller.processBrokerHeartbeat(heartbeatRequest.data).handle[Unit] { (reply, e) =>
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

  def handleUnregisterBroker(request: RequestChannel.Request): Unit = {
    val decommissionRequest = request.body[UnregisterBrokerRequest]
    authHelper.authorizeClusterOperation(request, ALTER)

    controller.unregisterBroker(decommissionRequest.data().brokerId()).handle[Unit] { (_, e) =>
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

  def handleBrokerRegistration(request: RequestChannel.Request): Unit = {
    val registrationRequest = request.body[BrokerRegistrationRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    controller.registerBroker(registrationRequest.data).handle[Unit] { (reply, e) =>
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
                                buildResponse: ApiMessage => AbstractResponse): Unit = {
    val requestBody = request.body[AbstractRequest]
    val future = raftManager.handleRequest(request.header, requestBody.data, time.milliseconds())

    future.whenComplete { (responseData, exception) =>
      val response = if (exception != null) {
        requestBody.getErrorResponse(exception)
      } else {
        buildResponse(responseData)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    }
  }

  def handleAlterClientQuotas(request: RequestChannel.Request): Unit = {
    val quotaRequest = request.body[AlterClientQuotasRequest]
    authHelper.authorizeClusterOperation(request, ALTER_CONFIGS)
    controller.alterClientQuotas(quotaRequest.entries, quotaRequest.validateOnly)
      .whenComplete { (results, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            AlterClientQuotasResponse.fromQuotaEntities(results, requestThrottleMs))
        }
      }
  }

  def handleIncrementalAlterConfigs(request: RequestChannel.Request): Unit = {
    val response = new IncrementalAlterConfigsResponseData()
    val alterConfigsRequest = request.body[IncrementalAlterConfigsRequest]
    val duplicateResources = new util.HashSet[ConfigResource]
    val configChanges = new util.HashMap[ConfigResource,
      util.Map[String, Entry[AlterConfigOp.OpType, String]]]()
    alterConfigsRequest.data.resources.forEach { resource =>
      val configResource = new ConfigResource(
        ConfigResource.Type.forId(resource.resourceType), resource.resourceName())
      if (configResource.`type`().equals(ConfigResource.Type.UNKNOWN)) {
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
    controller.incrementalAlterConfigs(configChanges, alterConfigsRequest.data.validateOnly)
      .whenComplete { (controllerResults, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          controllerResults.entrySet().forEach(entry => response.responses().add(
            new AlterConfigsResourceResponse().
              setErrorCode(entry.getValue.error().code()).
              setErrorMessage(entry.getValue.message()).
              setResourceName(entry.getKey.name()).
              setResourceType(entry.getKey.`type`().id())))
          requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
            new IncrementalAlterConfigsResponse(response.setThrottleTimeMs(throttleMs)))
        }
      }
  }

  def handleCreatePartitions(request: RequestChannel.Request): Unit = {
    def filterAlterAuthorizedTopics(topics: Iterable[String]): Set[String] = {
      authHelper.filterByAuthorized(request.context, ALTER, TOPIC, topics)(n => n)
    }

    val future = createPartitions(
      request.body[CreatePartitionsRequest].data,
      filterAlterAuthorizedTopics
    )

    future.whenComplete { (responses, exception) =>
      if (exception != null) {
        requestHelper.handleError(request, exception)
      } else {
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
          val responseData = new CreatePartitionsResponseData().
            setResults(responses).
            setThrottleTimeMs(requestThrottleMs)
          new CreatePartitionsResponse(responseData)
        })
      }
    }
  }

  def createPartitions(
    request: CreatePartitionsRequestData,
    getAlterAuthorizedTopics: Iterable[String] => Set[String]
  ): CompletableFuture[util.List[CreatePartitionsTopicResult]] = {
    val deadlineNs = time.nanoseconds() + NANOSECONDS.convert(request.timeoutMs, MILLISECONDS);
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
    controller.createPartitions(deadlineNs, topics).thenApply { results =>
      results.forEach(response => responses.add(response))
      responses
    }
  }

  def handleAlterPartitionReassignments(request: RequestChannel.Request): Unit = {
    val alterRequest = request.body[AlterPartitionReassignmentsRequest]
    authHelper.authorizeClusterOperation(request, ALTER)
    val response = controller.alterPartitionReassignments(alterRequest.data()).get()
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new AlterPartitionReassignmentsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleListPartitionReassignments(request: RequestChannel.Request): Unit = {
    val listRequest = request.body[ListPartitionReassignmentsRequest]
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    val response = controller.listPartitionReassignments(listRequest.data()).get()
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new ListPartitionReassignmentsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleAllocateProducerIdsRequest(request: RequestChannel.Request): Unit = {
    val allocatedProducerIdsRequest = request.body[AllocateProducerIdsRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    controller.allocateProducerIds(allocatedProducerIdsRequest.data)
      .whenComplete((results, exception) => {
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            results.setThrottleTimeMs(requestThrottleMs)
            new AllocateProducerIdsResponse(results)
          })
        }
      })
  }
}
