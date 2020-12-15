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

import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.acl.AclOperation.{CLUSTER_ACTION, CREATE, DESCRIBE}
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.ApiVersionsResponseData.{ApiVersionsResponseKey, FinalizedFeatureKey, SupportedFeatureKey}
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.message.{AlterIsrResponseData, ApiVersionsResponseData, BeginQuorumEpochResponseData, BrokerHeartbeatResponseData, BrokerRegistrationResponseData, CreateTopicsResponseData, DescribeQuorumResponseData, EndQuorumEpochResponseData, FetchResponseData, MetadataResponseData, VoteResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.record.BaseRecords
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, TOPIC}
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.controller.ClusterControlManager.{HeartbeatReply, RegistrationReply}
import org.apache.kafka.controller.{Controller, LeaderAndIsr}
import org.apache.kafka.metadata.{FeatureManager, VersionRange}
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.mutable
import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/**
 * Request handler for Controller APIs
 */
class ControllerApis(val requestChannel: RequestChannel,
                     val authorizer: Option[Authorizer],
                     val quotas: QuotaManagers,
                     val time: Time,
                     val supportedFeatures: Map[String, VersionRange],
                     val controller: Controller,
                     val raftManager: RaftManager,
                     val config: KafkaConfig,
                     val metaProperties: MetaProperties,
                     val controllerNodes: Seq[Node]) extends ApiRequestHandler with Logging {

  val apisUtils = new ApisUtils(new LogContext(s"[ControllerApis id=${config.controllerId}] "),
    requestChannel, authorizer, quotas, time)

  var supportedApiKeys = Set(
    ApiKeys.FETCH,
    ApiKeys.METADATA,
    //ApiKeys.SASL_HANDSHAKE
    ApiKeys.API_VERSIONS,
    ApiKeys.CREATE_TOPICS,
    //ApiKeys.DELETE_TOPICS,
    //ApiKeys.DESCRIBE_ACLS,
    //ApiKeys.CREATE_ACLS,
    //ApiKeys.DELETE_ACLS,
    //ApiKeys.DESCRIBE_CONFIGS,
    //ApiKeys.ALTER_CONFIGS,
    //ApiKeys.SASL_AUTHENTICATE,
    //ApiKeys.CREATE_PARTITIONS,
    //ApiKeys.CREATE_DELEGATION_TOKEN
    //ApiKeys.RENEW_DELEGATION_TOKEN
    //ApiKeys.EXPIRE_DELEGATION_TOKEN
    //ApiKeys.DESCRIBE_DELEGATION_TOKEN
    //ApiKeys.ELECT_LEADERS
    //ApiKeys.INCREMENTAL_ALTER_CONFIGS
    //ApiKeys.ALTER_PARTITION_REASSIGNMENTS
    //ApiKeys.LIST_PARTITION_REASSIGNMENTS
    //ApiKeys.DESCRIBE_CLIENT_QUOTAS
    //ApiKeys.ALTER_CLIENT_QUOTAS
    //ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS
    //ApiKeys.ALTER_USER_SCRAM_CREDENTIALS
    //ApiKeys.UPDATE_FEATURES
    ApiKeys.VOTE,
    ApiKeys.BEGIN_QUORUM_EPOCH,
    ApiKeys.END_QUORUM_EPOCH,
    ApiKeys.DESCRIBE_QUORUM,
    ApiKeys.ALTER_ISR,
    ApiKeys.BROKER_REGISTRATION,
    ApiKeys.BROKER_HEARTBEAT,
  )

  private def maybeHandleInvalidEnvelope(
    envelope: RequestChannel.Request,
    forwardedApiKey: ApiKeys
  ): Boolean = {
    def sendEnvelopeError(error: Errors): Unit = {
      apisUtils.sendErrorResponseMaybeThrottle(envelope, error.exception)
    }

    if (!apisUtils.authorize(envelope.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      // Forwarding request must have CLUSTER_ACTION authorization to reduce the risk of impersonation.
      sendEnvelopeError(Errors.CLUSTER_AUTHORIZATION_FAILED)
      true
    } else if (!forwardedApiKey.forwardable) {
      sendEnvelopeError(Errors.INVALID_REQUEST)
      true
    } else {
      false
    }
  }

  override def handle(request: RequestChannel.Request): Unit = {
    try {
      val handled = request.envelope.exists(envelope => {
        maybeHandleInvalidEnvelope(envelope, request.header.apiKey)
      })

      if (handled)
        return

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
        case ApiKeys.BROKER_HEARTBEAT => handleBrokerHeartBeatRequest(request)
        case ApiKeys.BROKER_REGISTRATION => handleBrokerRegistration(request)
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey()}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => apisUtils.handleError(request, e)
    }
  }

  private def handleFetch(request: RequestChannel.Request): Unit = {
    apisUtils.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new FetchResponse[BaseRecords](response.asInstanceOf[FetchResponseData]))
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
      if (controller.curClaimEpoch() > 0) {
        metadataResponseData.setControllerId(config.controllerId)
      } else {
        metadataResponseData.setControllerId(MetadataResponse.NO_CONTROLLER_ID)
      }
      val clusterAuthorizedOperations = if (metadataRequest.data.includeClusterAuthorizedOperations) {
        if (apisUtils.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
          apisUtils.authorizedOperations(request, Resource.CLUSTER)
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
    apisUtils.sendResponseMaybeThrottle(request,
      requestThrottleMs => createResponseCallback(requestThrottleMs))
  }

  def handleCreateTopics(request: RequestChannel.Request): Unit = {
    val createTopicRequest = request.body[CreateTopicsRequest]
    val (authorizedCreateRequest, unauthorizedTopics) =
      if (apisUtils.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME)) {
        (createTopicRequest.data, Seq.empty)
      } else {
        val duplicate = createTopicRequest.data.duplicate()
        val authorizedTopics = new CreatableTopicCollection()
        val unauthorizedTopics = mutable.Buffer.empty[String]

        createTopicRequest.data.topics.asScala.foreach { topicData =>
          if (apisUtils.authorize(request.context, CREATE, TOPIC, topicData.name)) {
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

      apisUtils.sendResponseMaybeThrottle(request, throttleTimeMs => {
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
          createTopicRequest.getErrorResponse(exception).data
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
    def createResponseCallback(features: FeatureManager.FinalizedFeaturesAndEpoch,
                               requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion)
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      else if (!apiVersionRequest.isValid)
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
      else {
        val data = new ApiVersionsResponseData().
          setErrorCode(0.toShort).
          setThrottleTimeMs(requestThrottleMs).
          setFinalizedFeaturesEpoch(features.epoch())
        supportedFeatures.foreach {
          case (k, v) => data.supportedFeatures().add(new SupportedFeatureKey().
            setName(k).setMaxVersion(v.max()).setMinVersion(v.min()))
        }
        features.finalizedFeatures().asScala.foreach {
          case (k, v) => data.finalizedFeatures().add(new FinalizedFeatureKey().
            setName(k).setMaxVersionLevel(v.max()).setMinVersionLevel(v.min()))
        }
        ApiKeys.enabledApis().asScala.foreach {
          key =>
            if (supportedApiKeys.contains(key)) {
              data.apiKeys().add(new ApiVersionsResponseKey().
                setApiKey(key.id).
                setMaxVersion(key.latestVersion()).
                setMinVersion(key.oldestVersion()))
            }
        }
        new ApiVersionsResponse(data)
      }
    }
    FutureConverters.toScala(controller.finalizedFeatures()).onComplete {
      case Success(features) =>
        apisUtils.sendResponseMaybeThrottle(request,
          requestThrottleMs => createResponseCallback(features, requestThrottleMs))
      case Failure(e) => apisUtils.handleError(request, e)
    }
  }

  private def handleVote(request: RequestChannel.Request): Unit = {
    apisUtils.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new VoteResponse(response.asInstanceOf[VoteResponseData]))
  }

  private def handleBeginQuorumEpoch(request: RequestChannel.Request): Unit = {
    apisUtils.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new BeginQuorumEpochResponse(response.asInstanceOf[BeginQuorumEpochResponseData]))
  }

  private def handleEndQuorumEpoch(request: RequestChannel.Request): Unit = {
    apisUtils.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new EndQuorumEpochResponse(response.asInstanceOf[EndQuorumEpochResponseData]))
  }

  private def handleDescribeQuorum(request: RequestChannel.Request): Unit = {
    apisUtils.authorizeClusterOperation(request, DESCRIBE)
    handleRaftRequest(request, response => new DescribeQuorumResponse(response.asInstanceOf[DescribeQuorumResponseData]))
  }

  def handleAlterIsrRequest(request: RequestChannel.Request): Unit = {
    apisUtils.authorizeClusterOperation(request, CLUSTER_ACTION)

    val alterIsrRequest = request.body[AlterIsrRequest]
    val isrsToAlter = mutable.Map[TopicPartition, LeaderAndIsr]()
    alterIsrRequest.data.topics.forEach { topicReq =>
      topicReq.partitions.forEach { partitionReq =>
        val tp = new TopicPartition(topicReq.name, partitionReq.partitionIndex)
        val newIsr = partitionReq.newIsr()
        isrsToAlter.put(tp, new LeaderAndIsr(
          alterIsrRequest.data.brokerId,
          partitionReq.leaderEpoch,
          newIsr,
          partitionReq.currentIsrVersion))
      }
    }

    val future = controller.alterIsr(
      alterIsrRequest.data.brokerId,
      alterIsrRequest.data.brokerEpoch,
      isrsToAlter.asJava
    )

    future.whenComplete((responses, exception) => {
      val response = if (exception != null) {
        alterIsrRequest.getErrorResponse(exception)
      } else {
        val responseData = new AlterIsrResponseData()
          .setErrorCode(Errors.NONE.code)

        responses.forEach { (topicPartition, error) =>
          var topicData = responseData.topics.find(topicPartition.topic)
          if (topicData == null) {
            topicData = new AlterIsrResponseData.TopicData()
              .setName(topicPartition.topic)
            responseData.topics.add(topicData)
          }

          // TODO: Partition response data expects ISR info
          val partitionResult = new AlterIsrResponseData.PartitionData()
            .setPartitionIndex(topicPartition.partition)
            .setErrorCode(error.code)
          topicData.partitions.add(partitionResult)
        }

        new AlterIsrResponse(responseData)
      }
      apisUtils.sendResponseExemptThrottle(request, response)
    })
  }

  def handleBrokerHeartBeatRequest(request: RequestChannel.Request): Unit = {
    val heartbeatRequest = request.body[BrokerHeartbeatRequest]
    apisUtils.authorizeClusterOperation(request, CLUSTER_ACTION)

    controller.processBrokerHeartbeat(heartbeatRequest.data).handle[Unit]((reply, e) => {
      def createResponseCallback(requestThrottleMs: Int,
                                 reply: HeartbeatReply,
                                 e: Throwable): BrokerHeartbeatResponse = {
        if (e != null) {
          new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.forException(e).code()))
        } else {
          new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.NONE.code()).
            setIsCaughtUp(reply.isCaughtUp).
            setIsFenced(reply.isFenced))
        }
      }
      apisUtils.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, reply, e))
    })
  }

  def handleBrokerRegistration(request: RequestChannel.Request): Unit = {
    val registrationRequest = request.body[BrokerRegistrationRequest]
    apisUtils.authorizeClusterOperation(request, CLUSTER_ACTION)

    controller.registerBroker(registrationRequest.data).handle[Unit]((reply, e) => {
      def createResponseCallback(requestThrottleMs: Int,
                                 reply: RegistrationReply,
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
      apisUtils.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, reply, e))
    })
  }

  private def handleRaftRequest(request: RequestChannel.Request,
                                buildResponse: ApiMessage => AbstractResponse): Unit = {
    val requestBody = request.body[AbstractRequest]
    val future = raftManager.handleRequest(request.header, requestBody.data)

    future.whenComplete((responseData, exception) => {
      val response = if (exception != null) {
        requestBody.getErrorResponse(exception)
      } else {
        buildResponse(responseData)
      }
      apisUtils.sendResponseExemptThrottle(request, response)
    })
  }
}
