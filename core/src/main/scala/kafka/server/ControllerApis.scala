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

import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.common.Node
import org.apache.kafka.common.acl.AclOperation.{ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, DESCRIBE}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{ApiException, ClusterAuthorizationException}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.message.{BeginQuorumEpochResponseData, BrokerHeartbeatResponseData, BrokerRegistrationResponseData, CreateTopicsResponseData, DescribeQuorumResponseData, EndQuorumEpochResponseData, FetchResponseData, MetadataResponseData, SaslAuthenticateResponseData, SaslHandshakeResponseData, UnregisterBrokerResponseData, VoteResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, TOPIC}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.controller.Controller
import org.apache.kafka.metadata.{ApiMessageAndVersion, BrokerHeartbeatReply, BrokerRegistrationReply, VersionRange}
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.mutable
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
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey()}")
      }
    } catch {
      case e: FatalExitError => throw e
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
        metadataResponseData.brokers().add(new MetadataResponseBroker()
          .setHost(node.host)
          .setNodeId(node.id)
          .setPort(node.port)
          .setRack(node.rack))
      }
      metadataResponseData.setClusterId(metaProperties.clusterId.toString)
      if (controller.isActive()) {
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

  def handleCreateTopics(request: RequestChannel.Request): Unit = {
    val createTopicRequest = request.body[CreateTopicsRequest]
    val (authorizedCreateRequest, unauthorizedTopics) =
      if (authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME)) {
        (createTopicRequest.data, Seq.empty)
      } else {
        val duplicate = createTopicRequest.data.duplicate()
        val authorizedTopics = new CreatableTopicCollection()
        val unauthorizedTopics = mutable.Buffer.empty[String]

        createTopicRequest.data.topics.asScala.foreach { topicData =>
          if (authHelper.authorize(request.context, CREATE, TOPIC, topicData.name)) {
            authorizedTopics.add(topicData)
          } else {
            unauthorizedTopics += topicData.name
          }
        }
        (duplicate.setTopics(authorizedTopics), unauthorizedTopics)
      }

    def sendResponse(response: CreateTopicsResponseData): Unit = {
      unauthorizedTopics.foreach { topic =>
        val result = new CreatableTopicResult()
          .setName(topic)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        response.topics.add(result)
      }

      requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => {
        response.setThrottleTimeMs(throttleTimeMs)
        new CreateTopicsResponse(response)
      })
    }

    if (authorizedCreateRequest.topics.isEmpty) {
      sendResponse(new CreateTopicsResponseData())
    } else {
      val future = controller.createTopics(authorizedCreateRequest)
      future.whenComplete((responseData, exception) => {
        val response = if (exception != null) {
          createTopicRequest.getErrorResponse(exception).asInstanceOf[CreateTopicsResponse].data
        } else {
          responseData
        }
        sendResponse(response)
      })
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
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      } else if (!apiVersionRequest.isValid) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
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
    val future = controller.alterIsr(alterIsrRequest.data())
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
            setErrorCode(Errors.forException(e).code()))
        } else {
          new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.NONE.code()).
            setIsCaughtUp(reply.isCaughtUp()).
            setIsFenced(reply.isFenced()).
            setShouldShutDown(reply.shouldShutDown()))
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
            setErrorCode(Errors.forException(e).code()))
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
            setErrorCode(Errors.NONE.code()).
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

    controller.alterClientQuotas(quotaRequest.entries(), quotaRequest.validateOnly())
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
      val configResource = new ConfigResource(ConfigResource.Type.forId(resource.resourceType()), resource.resourceName())
      val altersByName = new util.HashMap[String, util.Map.Entry[AlterConfigOp.OpType, String]]()
      resource.configs.forEach { config =>
        altersByName.put(config.name(), new util.AbstractMap.SimpleEntry[AlterConfigOp.OpType, String](
          AlterConfigOp.OpType.forId(config.configOperation()), config.value()))
      }
      configChanges.put(configResource, altersByName)
    }
    controller.incrementalAlterConfigs(configChanges, alterConfigsRequest.data().validateOnly())
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
