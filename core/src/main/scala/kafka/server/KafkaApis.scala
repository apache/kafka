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

import kafka.api.ElectLeadersRequestOps
import kafka.controller.ReplicaAssignment
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.server.handlers.DescribeTopicPartitionsRequestHandler
import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache}
import kafka.utils.Implicits._
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.admin.AdminUtils
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry, EndpointType}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, isInternal}
import org.apache.kafka.common.internals.{FatalExitError, Topic}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.{AddPartitionsToTxnResult, AddPartitionsToTxnResultCollection}
import org.apache.kafka.common.message.AlterConfigsResponseData.AlterConfigsResourceResponse
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.{ReassignablePartitionResponse, ReassignableTopicResponse}
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData.{CreatableTopicResult, CreatableTopicResultCollection}
import org.apache.kafka.common.message.DeleteRecordsResponseData.{DeleteRecordsPartitionResult, DeleteRecordsTopicResult}
import org.apache.kafka.common.message.DeleteTopicsResponseData.{DeletableTopicResult, DeletableTopicResultCollection}
import org.apache.kafka.common.message.ElectLeadersResponseData.{PartitionResult, ReplicaElectionResult}
import org.apache.kafka.common.message.ListClientMetricsResourcesResponseData.ClientMetricsResource
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult, OffsetForLeaderTopicResultCollection}
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ListenerName, NetworkSend, Send}
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{Resource, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{ProducerIdAndEpoch, Time}
import org.apache.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.server.ClientMetricsManager
import org.apache.kafka.server.authorizer._
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_0_11_0_IV0, IBP_2_3_IV0}
import org.apache.kafka.server.record.BrokerCompressionType
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchIsolation, FetchParams, FetchPartitionData}

import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import java.util.{Collections, Optional, OptionalInt}
import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val metadataSupport: MetadataSupport,
                val replicaManager: ReplicaManager,
                val groupCoordinator: GroupCoordinator,
                val txnCoordinator: TransactionCoordinator,
                val autoTopicCreationManager: AutoTopicCreationManager,
                val brokerId: Int,
                val config: KafkaConfig,
                val configRepository: ConfigRepository,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizer: Option[Authorizer],
                val quotas: QuotaManagers,
                val fetchManager: FetchManager,
                brokerTopicStats: BrokerTopicStats,
                val clusterId: String,
                time: Time,
                val tokenManager: DelegationTokenManager,
                val apiVersionManager: ApiVersionManager,
                val clientMetricsManager: Option[ClientMetricsManager]
) extends ApiRequestHandler with Logging {

  type FetchResponseStats = Map[TopicPartition, RecordValidationStats]
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  val configHelper = new ConfigHelper(metadataCache, config, configRepository)
  val authHelper = new AuthHelper(authorizer)
  val requestHelper = new RequestHandlerHelper(requestChannel, quotas, time)
  val aclApis = new AclApis(authHelper, authorizer, requestHelper, "broker", config)
  val configManager = new ConfigAdminManager(brokerId, config, configRepository)
  val describeTopicPartitionsRequestHandler : Option[DescribeTopicPartitionsRequestHandler] = metadataCache match {
    case kRaftMetadataCache: KRaftMetadataCache =>
      Some(new DescribeTopicPartitionsRequestHandler(kRaftMetadataCache, authHelper, config))
    case _ => None
  }

  def close(): Unit = {
    aclApis.close()
    info("Shutdown complete.")
  }

  private def isForwardingEnabled(request: RequestChannel.Request): Boolean = {
    metadataSupport.forwardingManager.isDefined && request.context.principalSerde.isPresent
  }

  private def maybeForwardToController(
    request: RequestChannel.Request,
    handler: RequestChannel.Request => Unit
  ): Unit = {
    def responseCallback(responseOpt: Option[AbstractResponse]): Unit = {
      responseOpt match {
        case Some(response) => requestHelper.sendForwardedResponse(request, response)
        case None => handleInvalidVersionsDuringForwarding(request)
      }
    }
    metadataSupport.maybeForward(request, handler, responseCallback)
  }

  private def handleInvalidVersionsDuringForwarding(request: RequestChannel.Request): Unit = {
    info(s"The client connection will be closed due to controller responded " +
      s"unsupported version exception during $request forwarding. " +
      s"This could happen when the controller changed after the connection was established.")
    requestChannel.closeConnection(request, Collections.emptyMap())
  }

  private def forwardToControllerOrFail(
    request: RequestChannel.Request
  ): Unit = {
    def errorHandler(request: RequestChannel.Request): Unit = {
      throw new IllegalStateException(s"Unable to forward $request to the controller")
    }

    maybeForwardToController(request, errorHandler)
  }

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    def handleError(e: Throwable): Unit = {
      error(s"Unexpected error handling request ${request.requestDesc(true)} " +
        s"with context ${request.context}", e)
      requestHelper.handleError(request, e)
    }

    try {
      trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
        s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")

      if (!apiVersionManager.isApiEnabled(request.header.apiKey, request.header.apiVersion)) {
        // The socket server will reject APIs which are not exposed in this scope and close the connection
        // before handing them to the request handler, so this path should not be exercised in practice
        throw new IllegalStateException(s"API ${request.header.apiKey} with version ${request.header.apiVersion} is not enabled")
      }

      request.header.apiKey match {
        case ApiKeys.PRODUCE => handleProduceRequest(request, requestLocal)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
        case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
        case ApiKeys.UPDATE_METADATA => handleUpdateMetadataRequest(request, requestLocal)
        case ApiKeys.CONTROLLED_SHUTDOWN => handleControlledShutdownRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request).exceptionally(handleError)
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request).exceptionally(handleError)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request).exceptionally(handleError)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupsRequest(request).exceptionally(handleError)
        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request).exceptionally(handleError)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.CREATE_TOPICS => maybeForwardToController(request, handleCreateTopicsRequest)
        case ApiKeys.DELETE_TOPICS => maybeForwardToController(request, handleDeleteTopicsRequest)
        case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
        case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request, requestLocal)
        case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
        case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionsToTxnRequest(request, requestLocal)
        case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request, requestLocal)
        case ApiKeys.END_TXN => handleEndTxnRequest(request, requestLocal)
        case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request, requestLocal)
        case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
        case ApiKeys.CREATE_ACLS => maybeForwardToController(request, handleCreateAcls)
        case ApiKeys.DELETE_ACLS => maybeForwardToController(request, handleDeleteAcls)
        case ApiKeys.ALTER_CONFIGS => handleAlterConfigsRequest(request)
        case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
        case ApiKeys.ALTER_REPLICA_LOG_DIRS => handleAlterReplicaLogDirsRequest(request)
        case ApiKeys.DESCRIBE_LOG_DIRS => handleDescribeLogDirsRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.CREATE_PARTITIONS => maybeForwardToController(request, handleCreatePartitionsRequest)
        // Create, renew and expire DelegationTokens must first validate that the connection
        // itself is not authenticated with a delegation token before maybeForwardToController.
        case ApiKeys.CREATE_DELEGATION_TOKEN => handleCreateTokenRequest(request)
        case ApiKeys.RENEW_DELEGATION_TOKEN => handleRenewTokenRequest(request)
        case ApiKeys.EXPIRE_DELEGATION_TOKEN => handleExpireTokenRequest(request)
        case ApiKeys.DESCRIBE_DELEGATION_TOKEN => handleDescribeTokensRequest(request)
        case ApiKeys.DELETE_GROUPS => handleDeleteGroupsRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.ELECT_LEADERS => maybeForwardToController(request, handleElectLeaders)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => handleIncrementalAlterConfigsRequest(request)
        case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => maybeForwardToController(request, handleAlterPartitionReassignmentsRequest)
        case ApiKeys.LIST_PARTITION_REASSIGNMENTS => maybeForwardToController(request, handleListPartitionReassignmentsRequest)
        case ApiKeys.OFFSET_DELETE => handleOffsetDeleteRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_CLIENT_QUOTAS => handleDescribeClientQuotasRequest(request)
        case ApiKeys.ALTER_CLIENT_QUOTAS => maybeForwardToController(request, handleAlterClientQuotasRequest)
        case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS => handleDescribeUserScramCredentialsRequest(request)
        case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => maybeForwardToController(request, handleAlterUserScramCredentialsRequest)
        case ApiKeys.ALTER_PARTITION => handleAlterPartitionRequest(request)
        case ApiKeys.UPDATE_FEATURES => maybeForwardToController(request, handleUpdateFeatures)
        case ApiKeys.ENVELOPE => handleEnvelope(request, requestLocal)
        case ApiKeys.DESCRIBE_CLUSTER => handleDescribeCluster(request)
        case ApiKeys.DESCRIBE_PRODUCERS => handleDescribeProducersRequest(request)
        case ApiKeys.UNREGISTER_BROKER => forwardToControllerOrFail(request)
        case ApiKeys.DESCRIBE_TRANSACTIONS => handleDescribeTransactionsRequest(request)
        case ApiKeys.LIST_TRANSACTIONS => handleListTransactionsRequest(request)
        case ApiKeys.ALLOCATE_PRODUCER_IDS => handleAllocateProducerIdsRequest(request)
        case ApiKeys.DESCRIBE_QUORUM => forwardToControllerOrFail(request)
        case ApiKeys.CONSUMER_GROUP_HEARTBEAT => handleConsumerGroupHeartbeat(request).exceptionally(handleError)
        case ApiKeys.CONSUMER_GROUP_DESCRIBE => handleConsumerGroupDescribe(request).exceptionally(handleError)
        case ApiKeys.DESCRIBE_TOPIC_PARTITIONS => handleDescribeTopicPartitionsRequest(request)
        case ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS => handleGetTelemetrySubscriptionsRequest(request)
        case ApiKeys.PUSH_TELEMETRY => handlePushTelemetryRequest(request)
        case ApiKeys.LIST_CLIENT_METRICS_RESOURCES => handleListClientMetricsResources(request)
        case _ => throw new IllegalStateException(s"No handler for request api key ${request.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => handleError(e)
    } finally {
      // try to complete delayed action. In order to avoid conflicting locking, the actions to complete delayed requests
      // are kept in a queue. We add the logic to check the ReplicaManager queue at the end of KafkaApis.handle() and the
      // expiration thread for certain delayed operations (e.g. DelayedJoin)
      // Delayed fetches are also completed by ReplicaFetcherThread.
      replicaManager.tryCompleteActions()
      // The local completion time may be set while processing the request. Only record it if it's unset.
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  override def tryCompleteActions(): Unit = {
    replicaManager.tryCompleteActions()
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val correlationId = request.header.correlationId
    val leaderAndIsrRequest = request.body[LeaderAndIsrRequest]

    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (zkSupport.isBrokerEpochStale(leaderAndIsrRequest.brokerEpoch, leaderAndIsrRequest.isKRaftController)) {
      // When the broker restarts very quickly, it is possible for this broker to receive request intended
      // for its previous generation so the broker should skip the stale request.
      info(s"Received LeaderAndIsr request with broker epoch ${leaderAndIsrRequest.brokerEpoch} " +
        s"smaller than the current broker epoch ${zkSupport.controller.brokerEpoch} from " +
        s"controller ${leaderAndIsrRequest.controllerId} with epoch ${leaderAndIsrRequest.controllerEpoch}.")
      requestHelper.sendResponseExemptThrottle(request, leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_BROKER_EPOCH.exception))
    } else {
      val response = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest,
        RequestHandlerHelper.onLeadershipChange(groupCoordinator, txnCoordinator, _, _))
      requestHelper.sendResponseExemptThrottle(request, response)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.body[StopReplicaRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (zkSupport.isBrokerEpochStale(stopReplicaRequest.brokerEpoch, stopReplicaRequest.isKRaftController)) {
      // When the broker restarts very quickly, it is possible for this broker to receive request intended
      // for its previous generation so the broker should skip the stale request.
      info(s"Received StopReplica request with broker epoch ${stopReplicaRequest.brokerEpoch} " +
        s"smaller than the current broker epoch ${zkSupport.controller.brokerEpoch} from " +
        s"controller ${stopReplicaRequest.controllerId} with epoch ${stopReplicaRequest.controllerEpoch}.")
      requestHelper.sendResponseExemptThrottle(request, new StopReplicaResponse(
        new StopReplicaResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code)))
    } else {
      val partitionStates = stopReplicaRequest.partitionStates().asScala
      val (result, error) = replicaManager.stopReplicas(
        request.context.correlationId,
        stopReplicaRequest.controllerId,
        stopReplicaRequest.controllerEpoch,
        partitionStates)
      // Clear the coordinator caches in case we were the leader. In the case of a reassignment, we
      // cannot rely on the LeaderAndIsr API for this since it is only sent to active replicas.
      result.forKeyValue { (topicPartition, error) =>
        if (error == Errors.NONE) {
          val partitionState = partitionStates(topicPartition)
          if (topicPartition.topic == GROUP_METADATA_TOPIC_NAME
              && partitionState.deletePartition) {
            val leaderEpoch = if (partitionState.leaderEpoch >= 0)
              OptionalInt.of(partitionState.leaderEpoch)
            else
              OptionalInt.empty
            groupCoordinator.onResignation(topicPartition.partition, leaderEpoch)
          } else if (topicPartition.topic == TRANSACTION_STATE_TOPIC_NAME
                     && partitionState.deletePartition) {
            val leaderEpoch = if (partitionState.leaderEpoch >= 0)
              Some(partitionState.leaderEpoch)
            else
              None
            txnCoordinator.onResignation(topicPartition.partition, coordinatorEpoch = leaderEpoch)
          }
        }
      }

      def toStopReplicaPartition(tp: TopicPartition, error: Errors) =
        new StopReplicaResponseData.StopReplicaPartitionError()
          .setTopicName(tp.topic)
          .setPartitionIndex(tp.partition)
          .setErrorCode(error.code)

      requestHelper.sendResponseExemptThrottle(request, new StopReplicaResponse(new StopReplicaResponseData()
        .setErrorCode(error.code)
        .setPartitionErrors(result.map {
          case (tp, error) => toStopReplicaPartition(tp, error)
        }.toBuffer.asJava)))
    }

    CoreUtils.swallow(replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads(), this)
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    val correlationId = request.header.correlationId
    val updateMetadataRequest = request.body[UpdateMetadataRequest]

    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (zkSupport.isBrokerEpochStale(updateMetadataRequest.brokerEpoch, updateMetadataRequest.isKRaftController)) {
      // When the broker restarts very quickly, it is possible for this broker to receive request intended
      // for its previous generation so the broker should skip the stale request.
      info(s"Received UpdateMetadata request with broker epoch ${updateMetadataRequest.brokerEpoch} " +
        s"smaller than the current broker epoch ${zkSupport.controller.brokerEpoch} from " +
        s"controller ${updateMetadataRequest.controllerId} with epoch ${updateMetadataRequest.controllerEpoch}.")
      requestHelper.sendResponseExemptThrottle(request,
        new UpdateMetadataResponse(new UpdateMetadataResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code)))
    } else {
      val deletedPartitions = replicaManager.maybeUpdateMetadataCache(correlationId, updateMetadataRequest)
      if (deletedPartitions.nonEmpty) {
        groupCoordinator.onPartitionsDeleted(deletedPartitions.asJava, requestLocal.bufferSupplier)
      }

      if (zkSupport.adminManager.hasDelayedTopicOperations) {
        updateMetadataRequest.partitionStates.forEach { partitionState =>
          zkSupport.adminManager.tryCompleteDelayedTopicOperations(partitionState.topicName)
        }
      }

      quotas.clientQuotaCallback.foreach { callback =>
        if (callback.updateClusterMetadata(metadataCache.getClusterMetadata(clusterId, request.context.listenerName))) {
          quotas.fetch.updateQuotaMetricConfigs()
          quotas.produce.updateQuotaMetricConfigs()
          quotas.request.updateQuotaMetricConfigs()
          quotas.controllerMutation.updateQuotaMetricConfigs()
        }
      }
      if (replicaManager.hasDelayedElectionOperations) {
        updateMetadataRequest.partitionStates.forEach { partitionState =>
          val tp = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
          replicaManager.tryCompleteElection(TopicPartitionOperationKey(tp))
        }
      }
      requestHelper.sendResponseExemptThrottle(request, new UpdateMetadataResponse(
        new UpdateMetadataResponseData().setErrorCode(Errors.NONE.code)))
    }
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.body[ControlledShutdownRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    def controlledShutdownCallback(controlledShutdownResult: Try[Set[TopicPartition]]): Unit = {
      val response = controlledShutdownResult match {
        case Success(partitionsRemaining) =>
         ControlledShutdownResponse.prepareResponse(Errors.NONE, partitionsRemaining.asJava)

        case Failure(throwable) =>
          controlledShutdownRequest.getErrorResponse(throwable)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    }
    zkSupport.controller.controlledShutdown(controlledShutdownRequest.data.brokerId, controlledShutdownRequest.data.brokerEpoch, controlledShutdownCallback)
  }

  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val offsetCommitRequest = request.body[OffsetCommitRequest]

    // Reject the request if not authorized to the group
    if (!authHelper.authorize(request.context, READ, GROUP, offsetCommitRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, offsetCommitRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (offsetCommitRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion.isLessThan(IBP_2_3_IV0)) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      requestHelper.sendMaybeThrottle(request, offsetCommitRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val authorizedTopics = authHelper.filterByAuthorized(
        request.context,
        READ,
        TOPIC,
        offsetCommitRequest.data.topics.asScala
      )(_.name)

      val responseBuilder = new OffsetCommitResponse.Builder()
      val authorizedTopicsRequest = new mutable.ArrayBuffer[OffsetCommitRequestData.OffsetCommitRequestTopic]()
      offsetCommitRequest.data.topics.forEach { topic =>
        if (!authorizedTopics.contains(topic.name)) {
          // If the topic is not authorized, we add the topic and all its partitions
          // to the response with TOPIC_AUTHORIZATION_FAILED.
          responseBuilder.addPartitions[OffsetCommitRequestData.OffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else if (!metadataCache.contains(topic.name)) {
          // If the topic is unknown, we add the topic and all its partitions
          // to the response with UNKNOWN_TOPIC_OR_PARTITION.
          responseBuilder.addPartitions[OffsetCommitRequestData.OffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          // Otherwise, we check all partitions to ensure that they all exist.
          val topicWithValidPartitions = new OffsetCommitRequestData.OffsetCommitRequestTopic().setName(topic.name)

          topic.partitions.forEach { partition =>
            if (metadataCache.getPartitionInfo(topic.name, partition.partitionIndex).nonEmpty) {
              topicWithValidPartitions.partitions.add(partition)
            } else {
              responseBuilder.addPartition(topic.name, partition.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          }

          if (!topicWithValidPartitions.partitions.isEmpty) {
            authorizedTopicsRequest += topicWithValidPartitions
          }
        }
      }

      if (authorizedTopicsRequest.isEmpty) {
        requestHelper.sendMaybeThrottle(request, responseBuilder.build())
        CompletableFuture.completedFuture(())
      } else if (request.header.apiVersion == 0) {
        // For version 0, always store offsets in ZK.
        commitOffsetsToZookeeper(
          request,
          offsetCommitRequest,
          authorizedTopicsRequest,
          responseBuilder
        )
      } else {
        // For version > 0, store offsets in Coordinator.
        commitOffsetsToCoordinator(
          request,
          offsetCommitRequest,
          authorizedTopicsRequest,
          responseBuilder,
          requestLocal
        )
      }
    }
  }

  private def commitOffsetsToZookeeper(
    request: RequestChannel.Request,
    offsetCommitRequest: OffsetCommitRequest,
    authorizedTopicsRequest: mutable.ArrayBuffer[OffsetCommitRequestData.OffsetCommitRequestTopic],
    responseBuilder: OffsetCommitResponse.Builder
  ): CompletableFuture[Unit] = {
    val zkSupport = metadataSupport.requireZkOrThrow(
      KafkaApis.unsupported("Version 0 offset commit requests"))

    authorizedTopicsRequest.foreach { topic =>
      topic.partitions.forEach { partition =>
        val error = try {
          if (partition.committedMetadata != null && partition.committedMetadata.length > config.offsetMetadataMaxSize) {
            Errors.OFFSET_METADATA_TOO_LARGE
          } else {
            zkSupport.zkClient.setOrCreateConsumerOffset(
              offsetCommitRequest.data.groupId,
              new TopicPartition(topic.name, partition.partitionIndex),
              partition.committedOffset
            )
            Errors.NONE
          }
        } catch {
          case e: Throwable =>
            Errors.forException(e)
        }

        responseBuilder.addPartition(topic.name, partition.partitionIndex, error)
      }
    }

    requestHelper.sendMaybeThrottle(request, responseBuilder.build())
    CompletableFuture.completedFuture[Unit](())
  }

  private def commitOffsetsToCoordinator(
    request: RequestChannel.Request,
    offsetCommitRequest: OffsetCommitRequest,
    authorizedTopicsRequest: mutable.ArrayBuffer[OffsetCommitRequestData.OffsetCommitRequestTopic],
    responseBuilder: OffsetCommitResponse.Builder,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val offsetCommitRequestData = new OffsetCommitRequestData()
      .setGroupId(offsetCommitRequest.data.groupId)
      .setMemberId(offsetCommitRequest.data.memberId)
      .setGenerationIdOrMemberEpoch(offsetCommitRequest.data.generationIdOrMemberEpoch)
      .setRetentionTimeMs(offsetCommitRequest.data.retentionTimeMs)
      .setGroupInstanceId(offsetCommitRequest.data.groupInstanceId)
      .setTopics(authorizedTopicsRequest.asJava)

    groupCoordinator.commitOffsets(
      request.context,
      offsetCommitRequestData,
      requestLocal.bufferSupplier
    ).handle[Unit] { (results, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, offsetCommitRequest.getErrorResponse(exception))
      } else {
        requestHelper.sendMaybeThrottle(request, responseBuilder.merge(results).build())
      }
    }
  }

  case class LeaderNode(leaderId: Int, leaderEpoch: Int, node: Option[Node])

  private def getCurrentLeader(tp: TopicPartition, ln: ListenerName): LeaderNode = {
    val partitionInfoOrError = replicaManager.getPartitionOrError(tp)
    val (leaderId, leaderEpoch) = partitionInfoOrError match {
      case Right(x) =>
        (x.leaderReplicaIdOpt.getOrElse(-1), x.getLeaderEpoch)
      case Left(x) =>
        debug(s"Unable to retrieve local leaderId and Epoch with error $x, falling back to metadata cache")
        metadataCache.getPartitionInfo(tp.topic, tp.partition) match {
          case Some(pinfo) => (pinfo.leader(), pinfo.leaderEpoch())
          case None => (-1, -1)
        }
    }
    LeaderNode(leaderId, leaderEpoch, metadataCache.getAliveBrokerNode(leaderId, ln))
  }

  /**
   * Handle a produce request
   */
  def handleProduceRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val produceRequest = request.body[ProduceRequest]

    if (RequestUtils.hasTransactionalRecords(produceRequest)) {
      val isAuthorizedTransactional = produceRequest.transactionalId != null &&
        authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, produceRequest.transactionalId)
      if (!isAuthorizedTransactional) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    }

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val invalidRequestResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val authorizedRequestInfo = mutable.Map[TopicPartition, MemoryRecords]()
    // cache the result to avoid redundant authorization calls
    val authorizedTopics = authHelper.filterByAuthorized(request.context, WRITE, TOPIC,
      produceRequest.data().topicData().asScala)(_.name())

    produceRequest.data.topicData.forEach(topic => topic.partitionData.forEach { partition =>
      val topicPartition = new TopicPartition(topic.name, partition.index)
      // This caller assumes the type is MemoryRecords and that is true on current serialization
      // We cast the type to avoid causing big change to code base.
      // https://issues.apache.org/jira/browse/KAFKA-10698
      val memoryRecords = partition.records.asInstanceOf[MemoryRecords]
      if (!authorizedTopics.contains(topicPartition.topic))
        unauthorizedTopicResponses += topicPartition -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        try {
          ProduceRequest.validateRecords(request.header.apiVersion, memoryRecords)
          authorizedRequestInfo += (topicPartition -> memoryRecords)
        } catch {
          case e: ApiException =>
            invalidRequestResponses += topicPartition -> new PartitionResponse(Errors.forException(e))
        }
    })

    // the callback for sending a produce response
    // The construction of ProduceResponse is able to accept auto-generated protocol data so
    // KafkaApis#handleProduceRequest should apply auto-generated protocol to avoid extra conversion.
    // https://issues.apache.org/jira/browse/KAFKA-10730
    @nowarn("cat=deprecation")
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ invalidRequestResponses
      var errorInResponse = false

      val nodeEndpoints = new mutable.HashMap[Int, Node]
      mergedResponseStatus.forKeyValue { (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            status.error.exceptionName))

          if (request.header.apiVersion >= 10) {
            status.error match {
              case Errors.NOT_LEADER_OR_FOLLOWER =>
                val leaderNode = getCurrentLeader(topicPartition, request.context.listenerName)
                leaderNode.node.foreach { node =>
                  nodeEndpoints.put(node.id(), node)
                }
                status.currentLeader
                  .setLeaderId(leaderNode.leaderId)
                  .setLeaderEpoch(leaderNode.leaderEpoch)
                case _ =>
            }
          }
        }
      }

      // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the quotas
      // have been violated. If both quotas have been violated, use the max throttle time between the two quotas. Note
      // that the request quota is not enforced if acks == 0.
      val timeMs = time.milliseconds()
      val requestSize = request.sizeInBytes
      val bandwidthThrottleTimeMs = quotas.produce.maybeRecordAndGetThrottleTimeMs(request, requestSize, timeMs)
      val requestThrottleTimeMs =
        if (produceRequest.acks == 0) 0
        else quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
      val maxThrottleTimeMs = Math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
      if (maxThrottleTimeMs > 0) {
        request.apiThrottleTimeMs = maxThrottleTimeMs
        if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
          requestHelper.throttle(quotas.produce, request, bandwidthThrottleTimeMs)
        } else {
          requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
        }
      }

      // Send the response immediately. In case of throttling, the channel has already been muted.
      if (produceRequest.acks == 0) {
        // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
        // the request, since no response is expected by the producer, the server will close socket server so that
        // the producer client will know that some error has happened and will refresh its metadata
        if (errorInResponse) {
          val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
            topicPartition -> status.error.exceptionName
          }.mkString(", ")
          info(
            s"Closing connection due to error during produce request with correlation id ${request.header.correlationId} " +
              s"from client id ${request.header.clientId} with ack=0\n" +
              s"Topic and partition to exceptions: $exceptionsSummary"
          )
          requestChannel.closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
        } else {
          // Note that although request throttling is exempt for acks == 0, the channel may be throttled due to
          // bandwidth quota violation.
          requestHelper.sendNoOpResponseExemptThrottle(request)
        }
      } else {
        requestChannel.sendResponse(request, new ProduceResponse(mergedResponseStatus.asJava, maxThrottleTimeMs, nodeEndpoints.values.toList.asJava), None)
      }
    }

    def processingStatsCallback(processingStats: FetchResponseStats): Unit = {
      processingStats.forKeyValue { (tp, info) =>
        updateRecordConversionStats(request, tp, info)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      val internalTopicsAllowed = request.header.clientId == AdminUtils.ADMIN_CLIENT_ID

      // call the replica manager to append messages to the replicas
      replicaManager.handleProduceAppend(
        timeout = produceRequest.timeout.toLong,
        requiredAcks = produceRequest.acks,
        internalTopicsAllowed = internalTopicsAllowed,
        transactionalId = produceRequest.transactionalId,
        entriesPerPartition = authorizedRequestInfo,
        responseCallback = sendResponseCallback,
        recordValidationStatsCallback = processingStatsCallback,
        requestLocal = requestLocal)

      // if the request is put into the purgatory, it will have a held reference and hence cannot be garbage collected;
      // hence we clear its data here in order to let GC reclaim its memory since it is already appended to log
      produceRequest.clearPartitionRecords()
    }
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request): Unit = {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val topicNames =
      if (fetchRequest.version() >= 13)
        metadataCache.topicIdsToNames()
      else
        Collections.emptyMap[Uuid, String]()

    val fetchData = fetchRequest.fetchData(topicNames)
    val forgottenTopics = fetchRequest.forgottenTopics(topicNames)

    val fetchContext = fetchManager.newContext(
      fetchRequest.version,
      fetchRequest.metadata,
      fetchRequest.isFromFollower,
      fetchData,
      forgottenTopics,
      topicNames)

    val erroneous = mutable.ArrayBuffer[(TopicIdPartition, FetchResponseData.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
        fetchContext.foreachPartition { (topicIdPartition, data) =>
          if (topicIdPartition.topic == null)
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
          else if (!metadataCache.contains(topicIdPartition.topicPartition))
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
          else
            interesting += topicIdPartition -> data
        }
      } else {
        fetchContext.foreachPartition { (topicIdPartition, _) =>
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        }
      }
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      val partitionDatas = new mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]
      fetchContext.foreachPartition { (topicIdPartition, partitionData) =>
        if (topicIdPartition.topic == null)
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
        else
          partitionDatas += topicIdPartition -> partitionData
      }
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, partitionDatas)(_._1.topicPartition.topic)
      partitionDatas.foreach { case (topicIdPartition, data) =>
        if (!authorizedTopics.contains(topicIdPartition.topic))
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicIdPartition.topicPartition))
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          interesting += topicIdPartition -> data
      }
    }

    def maybeDownConvertStorageError(error: Errors): Errors = {
      // If consumer sends FetchRequest V5 or earlier, the client library is not guaranteed to recognize the error code
      // for KafkaStorageException. In this case the client library will translate KafkaStorageException to
      // UnknownServerException which is not retriable. We can ensure that consumer will update metadata and retry
      // by converting the KafkaStorageException to NotLeaderOrFollowerException in the response if FetchRequest version <= 5
      if (error == Errors.KAFKA_STORAGE_ERROR && versionId <= 5) {
        Errors.NOT_LEADER_OR_FOLLOWER
      } else {
        error
      }
    }

    def maybeConvertFetchedData(tp: TopicIdPartition,
                                partitionData: FetchResponseData.PartitionData): FetchResponseData.PartitionData = {
      // We will never return a logConfig when the topic is unresolved and the name is null. This is ok since we won't have any records to convert.
      val logConfig = replicaManager.getLogConfig(tp.topicPartition)

      if (logConfig.exists(_.compressionType == BrokerCompressionType.ZSTD.name) && versionId < 10) {
        trace(s"Fetching messages is disabled for ZStandard compressed partition $tp. Sending unsupported version response to $clientId.")
        FetchResponse.partitionResponse(tp, Errors.UNSUPPORTED_COMPRESSION_TYPE)
      } else {
        // Down-conversion of fetched records is needed when the on-disk magic value is greater than what is
        // supported by the fetch request version.
        // If the inter-broker protocol version is `3.0` or higher, the log config message format version is
        // always `3.0` (i.e. magic value is `v2`). As a result, we always go through the down-conversion
        // path if the fetch version is 3 or lower (in rare cases the down-conversion may not be needed, but
        // it's not worth optimizing for them).
        // If the inter-broker protocol version is lower than `3.0`, we rely on the log config message format
        // version as a proxy for the on-disk magic value to maintain the long-standing behavior originally
        // introduced in Kafka 0.10.0. An important implication is that it's unsafe to downgrade the message
        // format version after a single message has been produced (the broker would return the message(s)
        // without down-conversion irrespective of the fetch version).
        val unconvertedRecords = FetchResponse.recordsOrFail(partitionData)
        val downConvertMagic =
          logConfig.map(_.recordVersion.value).flatMap { magic =>
            if (magic > RecordBatch.MAGIC_VALUE_V0 && versionId <= 1)
              Some(RecordBatch.MAGIC_VALUE_V0)
            else if (magic > RecordBatch.MAGIC_VALUE_V1 && versionId <= 3)
              Some(RecordBatch.MAGIC_VALUE_V1)
            else
              None
          }

        downConvertMagic match {
          case Some(magic) =>
            // For fetch requests from clients, check if down-conversion is disabled for the particular partition
            if (!fetchRequest.isFromFollower && !logConfig.forall(_.messageDownConversionEnable)) {
              trace(s"Conversion to message format ${downConvertMagic.get} is disabled for partition $tp. Sending unsupported version response to $clientId.")
              FetchResponse.partitionResponse(tp, Errors.UNSUPPORTED_VERSION)
            } else {
              try {
                trace(s"Down converting records from partition $tp to message format version $magic for fetch request from $clientId")
                // Because down-conversion is extremely memory intensive, we want to try and delay the down-conversion as much
                // as possible. With KIP-283, we have the ability to lazily down-convert in a chunked manner. The lazy, chunked
                // down-conversion always guarantees that at least one batch of messages is down-converted and sent out to the
                // client.
                new FetchResponseData.PartitionData()
                  .setPartitionIndex(tp.partition)
                  .setErrorCode(maybeDownConvertStorageError(Errors.forCode(partitionData.errorCode)).code)
                  .setHighWatermark(partitionData.highWatermark)
                  .setLastStableOffset(partitionData.lastStableOffset)
                  .setLogStartOffset(partitionData.logStartOffset)
                  .setAbortedTransactions(partitionData.abortedTransactions)
                  .setRecords(new LazyDownConversionRecords(tp.topicPartition, unconvertedRecords, magic, fetchContext.getFetchOffset(tp).get, time))
                  .setPreferredReadReplica(partitionData.preferredReadReplica())
              } catch {
                case e: UnsupportedCompressionTypeException =>
                  trace("Received unsupported compression type error during down-conversion", e)
                  FetchResponse.partitionResponse(tp, Errors.UNSUPPORTED_COMPRESSION_TYPE)
              }
            }
          case None =>
            new FetchResponseData.PartitionData()
              .setPartitionIndex(tp.partition)
              .setErrorCode(maybeDownConvertStorageError(Errors.forCode(partitionData.errorCode)).code)
              .setHighWatermark(partitionData.highWatermark)
              .setLastStableOffset(partitionData.lastStableOffset)
              .setLogStartOffset(partitionData.logStartOffset)
              .setAbortedTransactions(partitionData.abortedTransactions)
              .setRecords(unconvertedRecords)
              .setPreferredReadReplica(partitionData.preferredReadReplica)
              .setDivergingEpoch(partitionData.divergingEpoch)
              .setCurrentLeader(partitionData.currentLeader())
        }
      }
    }

    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      val partitions = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      val reassigningPartitions = mutable.Set[TopicIdPartition]()
      val nodeEndpoints = new mutable.HashMap[Int, Node]
      responsePartitionData.foreach { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.orElse(null)
        val lastStableOffset: Long = data.lastStableOffset.orElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        if (data.isReassignmentFetch) reassigningPartitions.add(tp)
        val partitionData = new FetchResponseData.PartitionData()
          .setPartitionIndex(tp.partition)
          .setErrorCode(maybeDownConvertStorageError(data.error).code)
          .setHighWatermark(data.highWatermark)
          .setLastStableOffset(lastStableOffset)
          .setLogStartOffset(data.logStartOffset)
          .setAbortedTransactions(abortedTransactions)
          .setRecords(data.records)
          .setPreferredReadReplica(data.preferredReadReplica.orElse(FetchResponse.INVALID_PREFERRED_REPLICA_ID))

        if (versionId >= 16) {
          data.error match {
            case Errors.NOT_LEADER_OR_FOLLOWER | Errors.FENCED_LEADER_EPOCH =>
              val leaderNode = getCurrentLeader(tp.topicPartition(), request.context.listenerName)
              leaderNode.node.foreach { node =>
                nodeEndpoints.put(node.id(), node)
              }
              partitionData.currentLeader()
                .setLeaderId(leaderNode.leaderId)
                .setLeaderEpoch(leaderNode.leaderEpoch)
            case _ =>
          }
        }

        data.divergingEpoch.ifPresent(partitionData.setDivergingEpoch(_))
        partitions.put(tp, partitionData)
      }
      erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

      var unconvertedFetchResponse: FetchResponse = null

      def createResponse(throttleTimeMs: Int): FetchResponse = {
        // Down-convert messages for each partition if required
        val convertedData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
        unconvertedFetchResponse.data().responses().forEach { topicResponse =>
          topicResponse.partitions().forEach { unconvertedPartitionData =>
            val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic, unconvertedPartitionData.partitionIndex()))
            val error = Errors.forCode(unconvertedPartitionData.errorCode)
            if (error != Errors.NONE)
              debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
                s"on partition $tp failed due to ${error.exceptionName}")
            convertedData.put(tp, maybeConvertFetchedData(tp, unconvertedPartitionData))
          }
        }

        // Prepare fetch response from converted data
        val response =
          FetchResponse.of(unconvertedFetchResponse.error, throttleTimeMs, unconvertedFetchResponse.sessionId, convertedData, nodeEndpoints.values.toList.asJava)
        // record the bytes out metrics only when the response is being sent
        response.data.responses.forEach { topicResponse =>
          topicResponse.partitions.forEach { data =>
            // If the topic name was not known, we will have no bytes out.
            if (topicResponse.topic != null) {
              val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic, data.partitionIndex))
              brokerTopicStats.updateBytesOut(tp.topic, fetchRequest.isFromFollower, reassigningPartitions.contains(tp), FetchResponse.recordsSize(data))
            }
          }
        }
        response
      }

      def updateConversionStats(send: Send): Unit = {
        send match {
          case send: MultiRecordsSend if send.recordConversionStats != null =>
            send.recordConversionStats.asScala.toMap.foreach {
              case (tp, stats) => updateRecordConversionStats(request, tp, stats)
            }
          case send: NetworkSend =>
            updateConversionStats(send.send())
          case _ =>
        }
      }

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
        val responseSize = KafkaApis.sizeOfThrottledPartitions(versionId, unconvertedFetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        val responsePartitionsSize = unconvertedFetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
        trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
          s"metadata=${unconvertedFetchResponse.sessionId}")
        requestHelper.sendResponseExemptThrottle(request, createResponse(0), Some(updateConversionStats))
      } else {
        // Fetch size used to determine throttle time is calculated before any down conversions.
        // This may be slightly different from the actual response size. But since down conversions
        // result in data being loaded into memory, we should do this only when we are not going to throttle.
        //
        // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the
        // quotas have been violated. If both quotas have been violated, use the max throttle time between the two
        // quotas. When throttled, we unrecord the recorded bandwidth quota value
        val responseSize = fetchContext.getResponseSize(partitions, versionId)
        val timeMs = time.milliseconds()
        val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
        val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request, responseSize, timeMs)

        val maxThrottleTimeMs = math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
        if (maxThrottleTimeMs > 0) {
          request.apiThrottleTimeMs = maxThrottleTimeMs
          // Even if we need to throttle for request quota violation, we should "unrecord" the already recorded value
          // from the fetch quota because we are going to return an empty response.
          quotas.fetch.unrecordQuotaSensor(request, responseSize, timeMs)
          if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
            requestHelper.throttle(quotas.fetch, request, bandwidthThrottleTimeMs)
          } else {
            requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
          }
          // If throttling is required, return an empty response.
          unconvertedFetchResponse = fetchContext.getThrottledResponse(maxThrottleTimeMs)
        } else {
          // Get the actual response. This will update the fetch context.
          unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
          val responsePartitionsSize = unconvertedFetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
          trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
            s"metadata=${unconvertedFetchResponse.sessionId}")
        }

        // Send the response immediately.
        requestChannel.sendResponse(request, createResponse(maxThrottleTimeMs), Some(updateConversionStats))
      }
    }

    if (interesting.isEmpty) {
      processResponseCallback(Seq.empty)
    } else {
      // for fetch from consumer, cap fetchMaxBytes to the maximum bytes that could be fetched without being throttled given
      // no bytes were recorded in the recent quota window
      // trying to fetch more bytes would result in a guaranteed throttling potentially blocking consumer progress
      val maxQuotaWindowBytes = if (fetchRequest.isFromFollower)
        Int.MaxValue
      else
        quotas.fetch.getMaxValueInQuotaWindow(request.session, clientId).toInt

      val fetchMaxBytes = Math.min(Math.min(fetchRequest.maxBytes, config.fetchMaxBytes), maxQuotaWindowBytes)
      val fetchMinBytes = Math.min(fetchRequest.minBytes, fetchMaxBytes)

      val clientMetadata: Optional[ClientMetadata] = if (versionId >= 11) {
        // Fetch API version 11 added preferred replica logic
        Optional.of(new DefaultClientMetadata(
          fetchRequest.rackId,
          clientId,
          request.context.clientAddress,
          request.context.principal,
          request.context.listenerName.value))
      } else {
        Optional.empty()
      }

      val params = new FetchParams(
        versionId,
        fetchRequest.replicaId,
        fetchRequest.replicaEpoch,
        fetchRequest.maxWait,
        fetchMinBytes,
        fetchMaxBytes,
        FetchIsolation.of(fetchRequest),
        clientMetadata
      )

      // call the replica manager to fetch messages from the local replica
      replicaManager.fetchMessages(
        params = params,
        fetchInfos = interesting,
        quota = replicationQuota(fetchRequest),
        responseCallback = processResponseCallback,
      )
    }
  }

  def replicationQuota(fetchRequest: FetchRequest): ReplicaQuota =
    if (fetchRequest.isFromFollower) quotas.leader else UnboundedQuota

  def handleListOffsetRequest(request: RequestChannel.Request): Unit = {
    val version = request.header.apiVersion

    val topics = if (version == 0)
      handleListOffsetRequestV0(request)
    else
      handleListOffsetRequestV1AndAbove(request)

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => new ListOffsetsResponse(new ListOffsetsResponseData()
      .setThrottleTimeMs(requestThrottleMs)
      .setTopics(topics.asJava)))
  }

  private def handleListOffsetRequestV0(request : RequestChannel.Request) : List[ListOffsetsTopicResponse] = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetsRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) = authHelper.partitionSeqByAuthorized(request.context,
        DESCRIBE, TOPIC, offsetRequest.topics.asScala.toSeq)(_.name)

    val unauthorizedResponseStatus = unauthorizedRequestInfo.map(topic =>
      new ListOffsetsTopicResponse()
        .setName(topic.name)
        .setPartitions(topic.partitions.asScala.map(partition =>
          new ListOffsetsPartitionResponse()
            .setPartitionIndex(partition.partitionIndex)
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)).asJava)
    )

    val responseTopics = authorizedRequestInfo.map { topic =>
      val responsePartitions = topic.partitions.asScala.map { partition =>
        val topicPartition = new TopicPartition(topic.name, partition.partitionIndex)

        try {
          val offsets = replicaManager.legacyFetchOffsetsForTimestamp(
            topicPartition = topicPartition,
            timestamp = partition.timestamp,
            maxNumOffsets = partition.maxNumOffsets,
            isFromConsumer = offsetRequest.replicaId == ListOffsetsRequest.CONSUMER_REPLICA_ID,
            fetchOnlyFromLeader = offsetRequest.replicaId != ListOffsetsRequest.DEBUGGING_REPLICA_ID)
          new ListOffsetsPartitionResponse()
            .setPartitionIndex(partition.partitionIndex)
            .setErrorCode(Errors.NONE.code)
            .setOldStyleOffsets(offsets.map(JLong.valueOf).asJava)
        } catch {
          // NOTE: UnknownTopicOrPartitionException and NotLeaderOrFollowerException are special cases since these error messages
          // are typically transient and there is no value in logging the entire stack trace for the same
          case e @ (_ : UnknownTopicOrPartitionException |
                    _ : NotLeaderOrFollowerException |
                    _ : KafkaStorageException) =>
            debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
              correlationId, clientId, topicPartition, e.getMessage))
            new ListOffsetsPartitionResponse()
              .setPartitionIndex(partition.partitionIndex)
              .setErrorCode(Errors.forException(e).code)
          case e: Throwable =>
            error("Error while responding to offset request", e)
            new ListOffsetsPartitionResponse()
              .setPartitionIndex(partition.partitionIndex)
              .setErrorCode(Errors.forException(e).code)
        }
      }
      new ListOffsetsTopicResponse().setName(topic.name).setPartitions(responsePartitions.asJava)
    }
    (responseTopics ++ unauthorizedResponseStatus).toList
  }

  private def handleListOffsetRequestV1AndAbove(request : RequestChannel.Request): List[ListOffsetsTopicResponse] = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetsRequest]
    val version = request.header.apiVersion

    def buildErrorResponse(e: Errors, partition: ListOffsetsPartition): ListOffsetsPartitionResponse = {
      new ListOffsetsPartitionResponse()
        .setPartitionIndex(partition.partitionIndex)
        .setErrorCode(e.code)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
    }

    val (authorizedRequestInfo, unauthorizedRequestInfo) = authHelper.partitionSeqByAuthorized(request.context,
        DESCRIBE, TOPIC, offsetRequest.topics.asScala.toSeq)(_.name)

    val unauthorizedResponseStatus = unauthorizedRequestInfo.map(topic =>
      new ListOffsetsTopicResponse()
        .setName(topic.name)
        .setPartitions(topic.partitions.asScala.map(partition =>
          buildErrorResponse(Errors.TOPIC_AUTHORIZATION_FAILED, partition)).asJava)
    )

    val responseTopics = authorizedRequestInfo.map { topic =>
      val responsePartitions = topic.partitions.asScala.map { partition =>
        val topicPartition = new TopicPartition(topic.name, partition.partitionIndex)
        if (offsetRequest.duplicatePartitions.contains(topicPartition)) {
          debug(s"OffsetRequest with correlation id $correlationId from client $clientId on partition $topicPartition " +
              s"failed because the partition is duplicated in the request.")
          buildErrorResponse(Errors.INVALID_REQUEST, partition)
        } else {
          try {
            val fetchOnlyFromLeader = offsetRequest.replicaId != ListOffsetsRequest.DEBUGGING_REPLICA_ID
            val isClientRequest = offsetRequest.replicaId == ListOffsetsRequest.CONSUMER_REPLICA_ID
            val isolationLevelOpt = if (isClientRequest)
              Some(offsetRequest.isolationLevel)
            else
              None

            val foundOpt = replicaManager.fetchOffsetForTimestamp(topicPartition,
              partition.timestamp,
              isolationLevelOpt,
              if (partition.currentLeaderEpoch == ListOffsetsResponse.UNKNOWN_EPOCH) Optional.empty() else Optional.of(partition.currentLeaderEpoch),
              fetchOnlyFromLeader)

            val response = foundOpt match {
              case Some(found) =>
                val partitionResponse = new ListOffsetsPartitionResponse()
                  .setPartitionIndex(partition.partitionIndex)
                  .setErrorCode(Errors.NONE.code)
                  .setTimestamp(found.timestamp)
                  .setOffset(found.offset)
                if (found.leaderEpoch.isPresent && version >= 4)
                  partitionResponse.setLeaderEpoch(found.leaderEpoch.get)
                partitionResponse
              case None =>
                buildErrorResponse(Errors.NONE, partition)
            }
            response
          } catch {
            // NOTE: These exceptions are special cases since these error messages are typically transient or the client
            // would have received a clear exception and there is no value in logging the entire stack trace for the same
            case e @ (_ : UnknownTopicOrPartitionException |
                      _ : NotLeaderOrFollowerException |
                      _ : UnknownLeaderEpochException |
                      _ : FencedLeaderEpochException |
                      _ : KafkaStorageException |
                      _ : UnsupportedForMessageFormatException) =>
              debug(s"Offset request with correlation id $correlationId from client $clientId on " +
                  s"partition $topicPartition failed due to ${e.getMessage}")
              buildErrorResponse(Errors.forException(e), partition)

            // Only V5 and newer ListOffset calls should get OFFSET_NOT_AVAILABLE
            case e: OffsetNotAvailableException =>
              if (request.header.apiVersion >= 5) {
                buildErrorResponse(Errors.forException(e), partition)
              } else {
                buildErrorResponse(Errors.LEADER_NOT_AVAILABLE, partition)
              }

            case e: Throwable =>
              error("Error while responding to offset request", e)
              buildErrorResponse(Errors.forException(e), partition)
          }
        }
      }
      new ListOffsetsTopicResponse().setName(topic.name).setPartitions(responsePartitions.asJava)
    }
    (responseTopics ++ unauthorizedResponseStatus).toList
  }

  private def metadataResponseTopic(error: Errors,
                                    topic: String,
                                    topicId: Uuid,
                                    isInternal: Boolean,
                                    partitionData: util.List[MetadataResponsePartition]): MetadataResponseTopic = {
    new MetadataResponseTopic()
      .setErrorCode(error.code)
      .setName(topic)
      .setTopicId(topicId)
      .setIsInternal(isInternal)
      .setPartitions(partitionData)
  }

  private def getTopicMetadata(
    request: RequestChannel.Request,
    fetchAllTopics: Boolean,
    allowAutoTopicCreation: Boolean,
    topics: Set[String],
    listenerName: ListenerName,
    errorUnavailableEndpoints: Boolean,
    errorUnavailableListeners: Boolean
  ): Seq[MetadataResponseTopic] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, listenerName,
      errorUnavailableEndpoints, errorUnavailableListeners)

    if (topics.isEmpty || topicResponses.size == topics.size || fetchAllTopics) {
      topicResponses
    } else {
      val nonExistingTopics = topics.diff(topicResponses.map(_.name).toSet)
      val nonExistingTopicResponses = if (allowAutoTopicCreation) {
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request)
        autoTopicCreationManager.createTopics(nonExistingTopics, controllerMutationQuota, Some(request.context))
      } else {
        nonExistingTopics.map { topic =>
          val error = try {
            Topic.validate(topic)
            Errors.UNKNOWN_TOPIC_OR_PARTITION
          } catch {
            case _: InvalidTopicException =>
              Errors.INVALID_TOPIC_EXCEPTION
          }

          metadataResponseTopic(
            error,
            topic,
            metadataCache.getTopicId(topic),
            Topic.isInternal(topic),
            util.Collections.emptyList()
          )
        }
      }

      topicResponses ++ nonExistingTopicResponses
    }
  }

  def handleTopicMetadataRequest(request: RequestChannel.Request): Unit = {
    val metadataRequest = request.body[MetadataRequest]
    val requestVersion = request.header.apiVersion

    // Topic IDs are not supported for versions 10 and 11. Topic names can not be null in these versions.
    if (!metadataRequest.isAllTopics) {
      metadataRequest.data.topics.forEach{ topic =>
        if (topic.name == null && metadataRequest.version < 12) {
          throw new InvalidRequestException(s"Topic name can not be null for version ${metadataRequest.version}")
        } else if (topic.topicId != Uuid.ZERO_UUID && metadataRequest.version < 12) {
          throw new InvalidRequestException(s"Topic IDs are not supported in requests for version ${metadataRequest.version}")
        }
      }
    }

    // Check if topicId is presented firstly.
    val topicIds = metadataRequest.topicIds.asScala.toSet.filterNot(_ == Uuid.ZERO_UUID)
    val useTopicId = topicIds.nonEmpty

    // Only get topicIds and topicNames when supporting topicId
    val unknownTopicIds = topicIds.filter(metadataCache.getTopicName(_).isEmpty)
    val knownTopicNames = topicIds.flatMap(metadataCache.getTopicName)

    val unknownTopicIdsTopicMetadata = unknownTopicIds.map(topicId =>
        metadataResponseTopic(Errors.UNKNOWN_TOPIC_ID, null, topicId, isInternal = false, util.Collections.emptyList())).toSeq

    val topics = if (metadataRequest.isAllTopics)
      metadataCache.getAllTopics()
    else if (useTopicId)
      knownTopicNames
    else
      metadataRequest.topics.asScala.toSet

    val authorizedForDescribeTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
      topics, logIfDenied = !metadataRequest.isAllTopics)(identity)
    var (authorizedTopics, unauthorizedForDescribeTopics) = topics.partition(authorizedForDescribeTopics.contains)
    var unauthorizedForCreateTopics = Set[String]()

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = authorizedTopics.filterNot(metadataCache.contains)
      if (metadataRequest.allowAutoTopicCreation && config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        if (!authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME, logIfDenied = false)) {
          val authorizedForCreateTopics = authHelper.filterByAuthorized(request.context, CREATE, TOPIC,
            nonExistingTopics)(identity)
          unauthorizedForCreateTopics = nonExistingTopics.diff(authorizedForCreateTopics)
          authorizedTopics = authorizedTopics.diff(unauthorizedForCreateTopics)
        }
      }
    }

    val unauthorizedForCreateTopicMetadata = unauthorizedForCreateTopics.map(topic =>
      // Set topicId to zero since we will never create topic which topicId
      metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, Uuid.ZERO_UUID, isInternal(topic), util.Collections.emptyList()))

    // do not disclose the existence of topics unauthorized for Describe, so we've not even checked if they exist or not
    val unauthorizedForDescribeTopicMetadata =
      // In case of all topics, don't include topics unauthorized for Describe
      if ((requestVersion == 0 && (metadataRequest.topics == null || metadataRequest.topics.isEmpty)) || metadataRequest.isAllTopics)
        Set.empty[MetadataResponseTopic]
      else if (useTopicId) {
        // Topic IDs are not considered sensitive information, so returning TOPIC_AUTHORIZATION_FAILED is OK
        unauthorizedForDescribeTopics.map(topic =>
          metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, null, metadataCache.getTopicId(topic), isInternal = false, util.Collections.emptyList()))
      } else {
        // We should not return topicId when on unauthorized error, so we return zero uuid.
        unauthorizedForDescribeTopics.map(topic =>
          metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, Uuid.ZERO_UUID, isInternal = false, util.Collections.emptyList()))
      }

    // In version 0, we returned an error when brokers with replicas were unavailable,
    // while in higher versions we simply don't include the broker in the returned broker list
    val errorUnavailableEndpoints = requestVersion == 0
    // In versions 5 and below, we returned LEADER_NOT_AVAILABLE if a matching listener was not found on the leader.
    // From version 6 onwards, we return LISTENER_NOT_FOUND to enable diagnosis of configuration errors.
    val errorUnavailableListeners = requestVersion >= 6

    val allowAutoCreation = config.autoCreateTopicsEnable && metadataRequest.allowAutoTopicCreation && !metadataRequest.isAllTopics
    val topicMetadata = getTopicMetadata(request, metadataRequest.isAllTopics, allowAutoCreation, authorizedTopics,
      request.context.listenerName, errorUnavailableEndpoints, errorUnavailableListeners)

    var clusterAuthorizedOperations = Int.MinValue // Default value in the schema
    if (requestVersion >= 8) {
      // get cluster authorized operations
      if (requestVersion <= 10) {
        if (metadataRequest.data.includeClusterAuthorizedOperations) {
          if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME))
            clusterAuthorizedOperations = authHelper.authorizedOperations(request, Resource.CLUSTER)
          else
            clusterAuthorizedOperations = 0
        }
      }

      // get topic authorized operations
      if (metadataRequest.data.includeTopicAuthorizedOperations) {
        def setTopicAuthorizedOperations(topicMetadata: Seq[MetadataResponseTopic]): Unit = {
          topicMetadata.foreach { topicData =>
            topicData.setTopicAuthorizedOperations(authHelper.authorizedOperations(request, new Resource(ResourceType.TOPIC, topicData.name)))
          }
        }
        setTopicAuthorizedOperations(topicMetadata)
      }
    }

    val completeTopicMetadata =  unknownTopicIdsTopicMetadata ++
      topicMetadata ++ unauthorizedForCreateTopicMetadata ++ unauthorizedForDescribeTopicMetadata

    val brokers = metadataCache.getAliveBrokerNodes(request.context.listenerName)

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))
    val controllerId = {
      metadataCache.getControllerId.flatMap {
        case ZkCachedControllerId(id) => Some(id)
        case KRaftCachedControllerId(_) => metadataCache.getRandomAliveBrokerId
      }
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
       MetadataResponse.prepareResponse(
         requestVersion,
         requestThrottleMs,
         brokers.toList.asJava,
         clusterId,
         controllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
         completeTopicMetadata.asJava,
         clusterAuthorizedOperations
      ))
  }

  def handleDescribeTopicPartitionsRequest(request: RequestChannel.Request): Unit = {
    describeTopicPartitionsRequestHandler match {
      case Some(handler) => {
        val response = handler.handleDescribeTopicPartitionsRequest(request)
        trace("Sending topic partitions metadata %s for correlation id %d to client %s".format(response.topics().asScala.mkString(","),
          request.header.correlationId, request.header.clientId))

        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
          response.setThrottleTimeMs(requestThrottleMs)
          new DescribeTopicPartitionsResponse(response)
        })
      }
      case None => throw new InvalidRequestException("ZK cluster does not handle DescribeTopicPartitions request")
    }
  }

  /**
   * Handle an offset fetch request
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val version = request.header.apiVersion
    if (version == 0) {
      handleOffsetFetchRequestFromZookeeper(request)
    } else {
      handleOffsetFetchRequestFromCoordinator(request)
    }
  }

  private def handleOffsetFetchRequestFromZookeeper(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val header = request.header
    val offsetFetchRequest = request.body[OffsetFetchRequest]

    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val offsetFetchResponse =
        // reject the request if not authorized to the group
        if (!authHelper.authorize(request.context, DESCRIBE, GROUP, offsetFetchRequest.groupId))
          offsetFetchRequest.getErrorResponse(requestThrottleMs, Errors.GROUP_AUTHORIZATION_FAILED)
        else {
          val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.unsupported("Version 0 offset fetch requests"))
          val (authorizedPartitions, unauthorizedPartitions) = partitionByAuthorized(
            offsetFetchRequest.partitions.asScala, request.context)

          // version 0 reads offsets from ZK
          val authorizedPartitionData = authorizedPartitions.map { topicPartition =>
            try {
              if (!metadataCache.contains(topicPartition))
                (topicPartition, OffsetFetchResponse.UNKNOWN_PARTITION)
              else {
                val payloadOpt = zkSupport.zkClient.getConsumerOffset(offsetFetchRequest.groupId, topicPartition)
                payloadOpt match {
                  case Some(payload) =>
                    (topicPartition, new OffsetFetchResponse.PartitionData(payload,
                      Optional.empty(), OffsetFetchResponse.NO_METADATA, Errors.NONE))
                  case None =>
                    (topicPartition, OffsetFetchResponse.UNKNOWN_PARTITION)
                }
              }
            } catch {
              case e: Throwable =>
                (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                  Optional.empty(), OffsetFetchResponse.NO_METADATA, Errors.forException(e)))
            }
          }.toMap

          val unauthorizedPartitionData = unauthorizedPartitions.map(_ -> OffsetFetchResponse.UNAUTHORIZED_PARTITION).toMap
          new OffsetFetchResponse(requestThrottleMs, Errors.NONE, (authorizedPartitionData ++ unauthorizedPartitionData).asJava)
        }
      trace(s"Sending offset fetch response $offsetFetchResponse for correlation id ${header.correlationId} to client ${header.clientId}.")
      offsetFetchResponse
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponse)
    CompletableFuture.completedFuture[Unit](())
  }

  private def handleOffsetFetchRequestFromCoordinator(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val offsetFetchRequest = request.body[OffsetFetchRequest]
    val groups = offsetFetchRequest.groups()
    val requireStable = offsetFetchRequest.requireStable()

    val futures = new mutable.ArrayBuffer[CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]](groups.size)
    groups.forEach { groupOffsetFetch =>
      val isAllPartitions = groupOffsetFetch.topics == null
      if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupOffsetFetch.groupId)) {
        futures += CompletableFuture.completedFuture(new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId(groupOffsetFetch.groupId)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code))
      } else if (isAllPartitions) {
        futures += fetchAllOffsetsForGroup(
          request.context,
          groupOffsetFetch,
          requireStable
        )
      } else {
        futures += fetchOffsetsForGroup(
          request.context,
          groupOffsetFetch,
          requireStable
        )
      }
    }

    CompletableFuture.allOf(futures.toArray: _*).handle[Unit] { (_, _) =>
      val groupResponses = new ArrayBuffer[OffsetFetchResponseData.OffsetFetchResponseGroup](futures.size)
      futures.foreach(future => groupResponses += future.get())
      requestHelper.sendMaybeThrottle(request, new OffsetFetchResponse(groupResponses.asJava, request.context.apiVersion))
    }
  }

  private def fetchAllOffsetsForGroup(
    requestContext: RequestContext,
    offsetFetchRequest: OffsetFetchRequestData.OffsetFetchRequestGroup,
    requireStable: Boolean
  ): CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup] = {
    groupCoordinator.fetchAllOffsets(
      requestContext,
      offsetFetchRequest,
      requireStable
    ).handle[OffsetFetchResponseData.OffsetFetchResponseGroup] { (offsetFetchResponse, exception) =>
      if (exception != null) {
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId(offsetFetchRequest.groupId)
          .setErrorCode(Errors.forException(exception).code)
      } else if (offsetFetchResponse.errorCode() != Errors.NONE.code) {
        offsetFetchResponse
      } else {
        // Clients are not allowed to see offsets for topics that are not authorized for Describe.
        val (authorizedOffsets, _) = authHelper.partitionSeqByAuthorized(
          requestContext,
          DESCRIBE,
          TOPIC,
          offsetFetchResponse.topics.asScala
        )(_.name)
        offsetFetchResponse.setTopics(authorizedOffsets.asJava)
      }
    }
  }

  private def fetchOffsetsForGroup(
    requestContext: RequestContext,
    offsetFetchRequest: OffsetFetchRequestData.OffsetFetchRequestGroup,
    requireStable: Boolean
  ): CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup] = {
    // Clients are not allowed to see offsets for topics that are not authorized for Describe.
    val (authorizedTopics, unauthorizedTopics) = authHelper.partitionSeqByAuthorized(
      requestContext,
      DESCRIBE,
      TOPIC,
      offsetFetchRequest.topics.asScala
    )(_.name)

    groupCoordinator.fetchOffsets(
      requestContext,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId(offsetFetchRequest.groupId)
        .setMemberId(offsetFetchRequest.memberId)
        .setMemberEpoch(offsetFetchRequest.memberEpoch)
        .setTopics(authorizedTopics.asJava),
      requireStable
    ).handle[OffsetFetchResponseData.OffsetFetchResponseGroup] { (offsetFetchResponse, exception) =>
      if (exception != null) {
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId(offsetFetchRequest.groupId)
          .setErrorCode(Errors.forException(exception).code)
      } else if (offsetFetchResponse.errorCode() != Errors.NONE.code) {
        offsetFetchResponse
      } else {
        val topics = new util.ArrayList[OffsetFetchResponseData.OffsetFetchResponseTopics](
          offsetFetchResponse.topics.size + unauthorizedTopics.size
        )
        topics.addAll(offsetFetchResponse.topics)
        unauthorizedTopics.foreach { topic =>
          val topicResponse = new OffsetFetchResponseData.OffsetFetchResponseTopics().setName(topic.name)
          topic.partitionIndexes.forEach { partitionIndex =>
            topicResponse.partitions.add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(partitionIndex)
              .setCommittedOffset(-1)
              .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code))
          }
          topics.add(topicResponse)
        }
        offsetFetchResponse.setTopics(topics)
      }
    }
  }

  private def partitionByAuthorized(
    seq: Seq[TopicPartition],
    context: RequestContext
  ): (Seq[TopicPartition], Seq[TopicPartition]) = {
    authHelper.partitionSeqByAuthorized(context, DESCRIBE, TOPIC, seq)(_.topic)
  }

  def handleFindCoordinatorRequest(request: RequestChannel.Request): Unit = {
    val version = request.header.apiVersion
    if (version < 4) {
      handleFindCoordinatorRequestLessThanV4(request)
    } else {
      handleFindCoordinatorRequestV4AndAbove(request)
    }
  }

  private def handleFindCoordinatorRequestV4AndAbove(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    val coordinators = findCoordinatorRequest.data.coordinatorKeys.asScala.map { key =>
      val (error, node) = getCoordinator(request, findCoordinatorRequest.data.keyType, key)
      new FindCoordinatorResponseData.Coordinator()
        .setKey(key)
        .setErrorCode(error.code)
        .setHost(node.host)
        .setNodeId(node.id)
        .setPort(node.port)
    }
    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val response = new FindCoordinatorResponse(
              new FindCoordinatorResponseData()
                .setCoordinators(coordinators.asJava)
                .setThrottleTimeMs(requestThrottleMs))
      trace("Sending FindCoordinator response %s for correlation id %d to client %s."
              .format(response, request.header.correlationId, request.header.clientId))
      response
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponse)
  }

  private def handleFindCoordinatorRequestLessThanV4(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    val (error, node) = getCoordinator(request, findCoordinatorRequest.data.keyType, findCoordinatorRequest.data.key)
    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val responseBody = new FindCoordinatorResponse(
          new FindCoordinatorResponseData()
            .setErrorCode(error.code)
            .setErrorMessage(error.message())
            .setNodeId(node.id)
            .setHost(node.host)
            .setPort(node.port)
            .setThrottleTimeMs(requestThrottleMs))
      trace("Sending FindCoordinator response %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      responseBody
    }
    if (error == Errors.NONE) {
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    } else {
      requestHelper.sendErrorResponseMaybeThrottle(request, error.exception)
    }
  }

  private def getCoordinator(request: RequestChannel.Request, keyType: Byte, key: String): (Errors, Node) = {
    if (keyType == CoordinatorType.GROUP.id &&
        !authHelper.authorize(request.context, DESCRIBE, GROUP, key))
      (Errors.GROUP_AUTHORIZATION_FAILED, Node.noNode)
    else if (keyType == CoordinatorType.TRANSACTION.id &&
        !authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, key))
      (Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, Node.noNode)
    else {
      val (partition, internalTopicName) = CoordinatorType.forId(keyType) match {
        case CoordinatorType.GROUP =>
          (groupCoordinator.partitionFor(key), GROUP_METADATA_TOPIC_NAME)

        case CoordinatorType.TRANSACTION =>
          (txnCoordinator.partitionFor(key), TRANSACTION_STATE_TOPIC_NAME)
      }

      val topicMetadata = metadataCache.getTopicMetadata(Set(internalTopicName), request.context.listenerName)

      if (topicMetadata.headOption.isEmpty) {
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request)
        autoTopicCreationManager.createTopics(Seq(internalTopicName).toSet, controllerMutationQuota, None)
        (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
      } else {
        if (topicMetadata.head.errorCode != Errors.NONE.code) {
          (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
        } else {
          val coordinatorEndpoint = topicMetadata.head.partitions.asScala
            .find(_.partitionIndex == partition)
            .filter(_.leaderId != MetadataResponse.NO_LEADER_ID)
            .flatMap(metadata => metadataCache.
                getAliveBrokerNode(metadata.leaderId, request.context.listenerName))

          coordinatorEndpoint match {
            case Some(endpoint) =>
              (Errors.NONE, endpoint)
            case _ =>
              (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
          }
        }
      }
    }
  }

  def handleDescribeGroupsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val describeRequest = request.body[DescribeGroupsRequest]
    val includeAuthorizedOperations = describeRequest.data.includeAuthorizedOperations
    val response = new DescribeGroupsResponseData()
    val authorizedGroups = new ArrayBuffer[String]()

    describeRequest.data.groups.forEach { groupId =>
      if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
        response.groups.add(DescribeGroupsResponse.groupError(
          groupId,
          Errors.GROUP_AUTHORIZATION_FAILED
        ))
      } else {
        authorizedGroups += groupId
      }
    }

    groupCoordinator.describeGroups(
      request.context,
      authorizedGroups.asJava
    ).handle[Unit] { (results, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, describeRequest.getErrorResponse(exception))
      } else {
        if (request.header.apiVersion >= 3 && includeAuthorizedOperations) {
          results.forEach { groupResult =>
            if (groupResult.errorCode == Errors.NONE.code) {
              groupResult.setAuthorizedOperations(authHelper.authorizedOperations(
                request,
                new Resource(ResourceType.GROUP, groupResult.groupId)
              ))
            }
          }
        }

        if (response.groups.isEmpty) {
          // If the response is empty, we can directly reuse the results.
          response.setGroups(results)
        } else {
          // Otherwise, we have to copy the results into the existing ones.
          response.groups.addAll(results)
        }

        requestHelper.sendMaybeThrottle(request, new DescribeGroupsResponse(response))
      }
    }
  }

  def handleListGroupsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val listGroupsRequest = request.body[ListGroupsRequest]
    val hasClusterDescribe = authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME, logIfDenied = false)

    groupCoordinator.listGroups(
      request.context,
      listGroupsRequest.data
    ).handle[Unit] { (response, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, listGroupsRequest.getErrorResponse(exception))
      } else {
        val listGroupsResponse = if (hasClusterDescribe) {
          // With describe cluster access all groups are returned. We keep this alternative for backward compatibility.
          new ListGroupsResponse(response)
        } else {
          // Otherwise, only groups with described group are returned.
          val authorizedGroups = response.groups.asScala.filter { group =>
            authHelper.authorize(request.context, DESCRIBE, GROUP, group.groupId, logIfDenied = false)
          }
          new ListGroupsResponse(response.setGroups(authorizedGroups.asJava))
        }
        requestHelper.sendMaybeThrottle(request, listGroupsResponse)
      }
    }
  }

  def handleJoinGroupRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val joinGroupRequest = request.body[JoinGroupRequest]

    if (joinGroupRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion.isLessThan(IBP_2_3_IV0)) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      requestHelper.sendMaybeThrottle(request, joinGroupRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, joinGroupRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, joinGroupRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.joinGroup(
        request.context,
        joinGroupRequest.data,
        requestLocal.bufferSupplier
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, joinGroupRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new JoinGroupResponse(response, request.context.apiVersion))
        }
      }
    }
  }

  def handleSyncGroupRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val syncGroupRequest = request.body[SyncGroupRequest]

    if (syncGroupRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion.isLessThan(IBP_2_3_IV0)) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!syncGroupRequest.areMandatoryProtocolTypeAndNamePresent()) {
      // Starting from version 5, ProtocolType and ProtocolName fields are mandatory.
      requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(Errors.INCONSISTENT_GROUP_PROTOCOL.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, syncGroupRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.syncGroup(
        request.context,
        syncGroupRequest.data,
        requestLocal.bufferSupplier
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new SyncGroupResponse(response))
        }
      }
    }
  }

  def handleDeleteGroupsRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val deleteGroupsRequest = request.body[DeleteGroupsRequest]
    val groups = deleteGroupsRequest.data.groupsNames.asScala.distinct

    val (authorizedGroups, unauthorizedGroups) =
      authHelper.partitionSeqByAuthorized(request.context, DELETE, GROUP, groups)(identity)

    groupCoordinator.deleteGroups(
      request.context,
      authorizedGroups.toList.asJava,
      requestLocal.bufferSupplier
    ).handle[Unit] { (results, exception) =>
      val response = new DeleteGroupsResponseData()

      if (exception != null) {
        val error = Errors.forException(exception)
        authorizedGroups.foreach { groupId =>
          response.results.add(new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId(groupId)
            .setErrorCode(error.code))
        }
      } else {
        response.setResults(results)
      }

      unauthorizedGroups.foreach { groupId =>
        response.results.add(new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId(groupId)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code))
      }

      requestHelper.sendMaybeThrottle(request, new DeleteGroupsResponse(response))
    }
  }

  def handleHeartbeatRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val heartbeatRequest = request.body[HeartbeatRequest]

    if (heartbeatRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion.isLessThan(IBP_2_3_IV0)) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      requestHelper.sendMaybeThrottle(request, heartbeatRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, heartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, heartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.heartbeat(
        request.context,
        heartbeatRequest.data
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, heartbeatRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new HeartbeatResponse(response))
        }
      }
    }
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val leaveGroupRequest = request.body[LeaveGroupRequest]

    if (!authHelper.authorize(request.context, READ, GROUP, leaveGroupRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, leaveGroupRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.leaveGroup(
        request.context,
        leaveGroupRequest.normalizedData()
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, leaveGroupRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new LeaveGroupResponse(response, leaveGroupRequest.version))
        }
      }
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

  def handleCreateTopicsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 6)

    def sendResponseCallback(results: CreatableTopicResultCollection): Unit = {
      val responseData = new CreateTopicsResponseData()
        .setTopics(results)
      val response = new CreateTopicsResponse(responseData)
      trace(s"Sending create topics response $responseData for correlation id " +
        s"${request.header.correlationId} to client ${request.header.clientId}.")
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, response)
    }

    val createTopicsRequest = request.body[CreateTopicsRequest]
    val results = new CreatableTopicResultCollection(createTopicsRequest.data.topics.size)
    if (!zkSupport.controller.isActive) {
      createTopicsRequest.data.topics.forEach { topic =>
        results.add(new CreatableTopicResult().setName(topic.name)
          .setErrorCode(Errors.NOT_CONTROLLER.code))
      }
      sendResponseCallback(results)
    } else {
      createTopicsRequest.data.topics.forEach { topic =>
        results.add(new CreatableTopicResult().setName(topic.name))
      }
      val hasClusterAuthorization = authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME,
        logIfDenied = false)

      val allowedTopicNames = {
        val topicNames = createTopicsRequest
          .data
          .topics
          .asScala
          .map(_.name)
          .toSet

          /* The cluster metadata topic is an internal topic with a different implementation. The user should not be
           * allowed to create it as a regular topic.
           */
          if (topicNames.contains(Topic.CLUSTER_METADATA_TOPIC_NAME)) {
            info(s"Rejecting creation of internal topic ${Topic.CLUSTER_METADATA_TOPIC_NAME}")
          }
          topicNames.diff(Set(Topic.CLUSTER_METADATA_TOPIC_NAME))
      }

      val authorizedTopics = if (hasClusterAuthorization) {
        allowedTopicNames
      } else {
        authHelper.filterByAuthorized(request.context, CREATE, TOPIC, allowedTopicNames)(identity)
      }
      val authorizedForDescribeConfigs = authHelper.filterByAuthorized(
        request.context,
        DESCRIBE_CONFIGS,
        TOPIC,
        allowedTopicNames,
        logIfDenied = false
      )(identity).map(name => name -> results.find(name)).toMap

      results.forEach { topic =>
        if (results.findAll(topic.name).size > 1) {
          topic.setErrorCode(Errors.INVALID_REQUEST.code)
          topic.setErrorMessage("Found multiple entries for this topic.")
        } else if (!authorizedTopics.contains(topic.name)) {
          topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          topic.setErrorMessage("Authorization failed.")
        }
        if (!authorizedForDescribeConfigs.contains(topic.name)) {
          topic.setTopicConfigErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        }
      }
      val toCreate = mutable.Map[String, CreatableTopic]()
      createTopicsRequest.data.topics.forEach { topic =>
        if (results.find(topic.name).errorCode == Errors.NONE.code) {
          toCreate += topic.name -> topic
        }
      }
      def handleCreateTopicsResults(errors: Map[String, ApiError]): Unit = {
        errors.foreach { case (topicName, error) =>
          val result = results.find(topicName)
          result.setErrorCode(error.error.code)
            .setErrorMessage(error.message)
          // Reset any configs in the response if Create failed
          if (error != ApiError.NONE) {
            result.setConfigs(List.empty.asJava)
              .setNumPartitions(-1)
              .setReplicationFactor(-1)
              .setTopicConfigErrorCode(Errors.NONE.code)
          }
        }
        sendResponseCallback(results)
      }
      zkSupport.adminManager.createTopics(
        createTopicsRequest.data.timeoutMs,
        createTopicsRequest.data.validateOnly,
        toCreate,
        authorizedForDescribeConfigs,
        controllerMutationQuota,
        handleCreateTopicsResults)
    }
  }

  def handleCreatePartitionsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val createPartitionsRequest = request.body[CreatePartitionsRequest]
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 3)

    def sendResponseCallback(results: Map[String, ApiError]): Unit = {
      val createPartitionsResults = results.map {
        case (topic, error) => new CreatePartitionsTopicResult()
          .setName(topic)
          .setErrorCode(error.error.code)
          .setErrorMessage(error.message)
      }.toSeq
      val response = new CreatePartitionsResponse(new CreatePartitionsResponseData()
        .setResults(createPartitionsResults.asJava))
      trace(s"Sending create partitions response $response for correlation id ${request.header.correlationId} to " +
        s"client ${request.header.clientId}.")
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, response)
    }

    if (!zkSupport.controller.isActive) {
      val result = createPartitionsRequest.data.topics.asScala.map { topic =>
        (topic.name, new ApiError(Errors.NOT_CONTROLLER, null))
      }.toMap
      sendResponseCallback(result)
    } else {
      // Special handling to add duplicate topics to the response
      val topics = createPartitionsRequest.data.topics.asScala.toSeq
      val dupes = topics.groupBy(_.name)
        .filter { _._2.size > 1 }
        .keySet
      val notDuped = topics.filterNot(topic => dupes.contains(topic.name))
      val (authorized, unauthorized) = authHelper.partitionSeqByAuthorized(request.context, ALTER, TOPIC,
        notDuped)(_.name)

      val (queuedForDeletion, valid) = authorized.partition { topic =>
        zkSupport.controller.isTopicQueuedForDeletion(topic.name)
      }

      val errors = dupes.map(_ -> new ApiError(Errors.INVALID_REQUEST, "Duplicate topic in request.")) ++
        unauthorized.map(_.name -> new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, "The topic authorization is failed.")) ++
        queuedForDeletion.map(_.name -> new ApiError(Errors.INVALID_TOPIC_EXCEPTION, "The topic is queued for deletion."))

      zkSupport.adminManager.createPartitions(
        createPartitionsRequest.data.timeoutMs,
        valid,
        createPartitionsRequest.data.validateOnly,
        controllerMutationQuota,
        result => sendResponseCallback(result ++ errors))
    }
  }

  def handleDeleteTopicsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 5)

    def sendResponseCallback(results: DeletableTopicResultCollection): Unit = {
      val responseData = new DeleteTopicsResponseData()
        .setResponses(results)
      val response = new DeleteTopicsResponse(responseData)
      trace(s"Sending delete topics response $response for correlation id ${request.header.correlationId} to client ${request.header.clientId}.")
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, response)
    }

    val deleteTopicRequest = request.body[DeleteTopicsRequest]
    val results = new DeletableTopicResultCollection(deleteTopicRequest.numberOfTopics())
    val toDelete = mutable.Set[String]()
    if (!zkSupport.controller.isActive) {
      deleteTopicRequest.topics().forEach { topic =>
        results.add(new DeletableTopicResult()
          .setName(topic.name())
          .setTopicId(topic.topicId())
          .setErrorCode(Errors.NOT_CONTROLLER.code))
      }
      sendResponseCallback(results)
    } else if (!config.deleteTopicEnable) {
      val error = if (request.context.apiVersion < 3) Errors.INVALID_REQUEST else Errors.TOPIC_DELETION_DISABLED
      deleteTopicRequest.topics().forEach { topic =>
        results.add(new DeletableTopicResult()
          .setName(topic.name())
          .setTopicId(topic.topicId())
          .setErrorCode(error.code))
      }
      sendResponseCallback(results)
    } else {
      val topicIdsFromRequest = deleteTopicRequest.topicIds().asScala.filter(topicId => topicId != Uuid.ZERO_UUID).toSet
      deleteTopicRequest.topics().forEach { topic =>
        if (topic.name() != null && topic.topicId() != Uuid.ZERO_UUID)
          throw new InvalidRequestException("Topic name and topic ID can not both be specified.")
        val name = if (topic.topicId() == Uuid.ZERO_UUID) topic.name()
        else zkSupport.controller.controllerContext.topicName(topic.topicId).orNull
        results.add(new DeletableTopicResult()
          .setName(name)
          .setTopicId(topic.topicId()))
      }
      val authorizedDescribeTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
        results.asScala.filter(result => result.name() != null))(_.name)
      val authorizedDeleteTopics = authHelper.filterByAuthorized(request.context, DELETE, TOPIC,
        results.asScala.filter(result => result.name() != null))(_.name)
      results.forEach { topic =>
        val unresolvedTopicId = topic.topicId() != Uuid.ZERO_UUID && topic.name() == null
        if (unresolvedTopicId) {
          topic.setErrorCode(Errors.UNKNOWN_TOPIC_ID.code)
        } else if (topicIdsFromRequest.contains(topic.topicId) && !authorizedDescribeTopics.contains(topic.name)) {

          // Because the client does not have Describe permission, the name should
          // not be returned in the response. Note, however, that we do not consider
          // the topicId itself to be sensitive, so there is no reason to obscure
          // this case with `UNKNOWN_TOPIC_ID`.
          topic.setName(null)
          topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        } else if (!authorizedDeleteTopics.contains(topic.name)) {
          topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        } else if (!metadataCache.contains(topic.name)) {
          topic.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        } else {
          toDelete += topic.name
        }
      }
      // If no authorized topics return immediately
      if (toDelete.isEmpty)
        sendResponseCallback(results)
      else {
        def handleDeleteTopicsResults(errors: Map[String, Errors]): Unit = {
          errors.foreach {
            case (topicName, error) =>
              results.find(topicName)
                .setErrorCode(error.code)
          }
          sendResponseCallback(results)
        }

        zkSupport.adminManager.deleteTopics(
          deleteTopicRequest.data.timeoutMs,
          toDelete,
          controllerMutationQuota,
          handleDeleteTopicsResults
        )
      }
    }
  }

  def handleDeleteRecordsRequest(request: RequestChannel.Request): Unit = {
    val deleteRecordsRequest = request.body[DeleteRecordsRequest]

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val authorizedForDeleteTopicOffsets = mutable.Map[TopicPartition, Long]()

    val topics = deleteRecordsRequest.data.topics.asScala
    val authorizedTopics = authHelper.filterByAuthorized(request.context, DELETE, TOPIC, topics)(_.name)
    val deleteTopicPartitions = topics.flatMap { deleteTopic =>
      deleteTopic.partitions.asScala.map { deletePartition =>
        new TopicPartition(deleteTopic.name, deletePartition.partitionIndex) -> deletePartition.offset
      }
    }
    for ((topicPartition, offset) <- deleteTopicPartitions) {
      if (!authorizedTopics.contains(topicPartition.topic))
        unauthorizedTopicResponses += topicPartition -> new DeleteRecordsPartitionResult()
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new DeleteRecordsPartitionResult()
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
      else
        authorizedForDeleteTopicOffsets += (topicPartition -> offset)
    }

    // the callback for sending a DeleteRecordsResponse
    def sendResponseCallback(authorizedTopicResponses: Map[TopicPartition, DeleteRecordsPartitionResult]): Unit = {
      val mergedResponseStatus = authorizedTopicResponses ++ unauthorizedTopicResponses ++ nonExistingTopicResponses
      mergedResponseStatus.forKeyValue { (topicPartition, status) =>
        if (status.errorCode != Errors.NONE.code) {
          debug("DeleteRecordsRequest with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            Errors.forCode(status.errorCode).exceptionName))
        }
      }

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DeleteRecordsResponse(new DeleteRecordsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection(mergedResponseStatus.groupBy(_._1.topic).map { case (topic, partitionMap) =>
            new DeleteRecordsTopicResult()
              .setName(topic)
              .setPartitions(new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(partitionMap.map { case (topicPartition, partitionResult) =>
                new DeleteRecordsPartitionResult().setPartitionIndex(topicPartition.partition)
                  .setLowWatermark(partitionResult.lowWatermark)
                  .setErrorCode(partitionResult.errorCode)
              }.toList.asJava.iterator()))
          }.toList.asJava.iterator()))))
    }

    if (authorizedForDeleteTopicOffsets.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // call the replica manager to append messages to the replicas
      replicaManager.deleteRecords(
        deleteRecordsRequest.data.timeoutMs.toLong,
        authorizedForDeleteTopicOffsets,
        sendResponseCallback)
    }
  }

  def handleInitProducerIdRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val initProducerIdRequest = request.body[InitProducerIdRequest]
    val transactionalId = initProducerIdRequest.data.transactionalId

    if (transactionalId != null) {
      if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    } else if (!authHelper.authorize(request.context, IDEMPOTENT_WRITE, CLUSTER, CLUSTER_NAME, true, false)
        && !authHelper.authorizeByResourceType(request.context, AclOperation.WRITE, ResourceType.TOPIC)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.CLUSTER_AUTHORIZATION_FAILED.exception)
      return
    }

    def sendResponseCallback(result: InitProducerIdResult): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val finalError =
          if (initProducerIdRequest.version < 4 && result.error == Errors.PRODUCER_FENCED) {
            // For older clients, they could not understand the new PRODUCER_FENCED error code,
            // so we need to return the INVALID_PRODUCER_EPOCH to have the same client handling logic.
            Errors.INVALID_PRODUCER_EPOCH
          } else {
            result.error
          }
        val responseData = new InitProducerIdResponseData()
          .setProducerId(result.producerId)
          .setProducerEpoch(result.producerEpoch)
          .setThrottleTimeMs(requestThrottleMs)
          .setErrorCode(finalError.code)
        val responseBody = new InitProducerIdResponse(responseData)
        trace(s"Completed $transactionalId's InitProducerIdRequest with result $result from client ${request.header.clientId}.")
        responseBody
      }
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    val producerIdAndEpoch = (initProducerIdRequest.data.producerId, initProducerIdRequest.data.producerEpoch) match {
      case (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH) => Right(None)
      case (RecordBatch.NO_PRODUCER_ID, _) | (_, RecordBatch.NO_PRODUCER_EPOCH) => Left(Errors.INVALID_REQUEST)
      case (_, _) => Right(Some(new ProducerIdAndEpoch(initProducerIdRequest.data.producerId, initProducerIdRequest.data.producerEpoch)))
    }

    producerIdAndEpoch match {
      case Right(producerIdAndEpoch) => txnCoordinator.handleInitProducerId(transactionalId, initProducerIdRequest.data.transactionTimeoutMs,
        producerIdAndEpoch, sendResponseCallback, requestLocal)
      case Left(error) => requestHelper.sendErrorResponseMaybeThrottle(request, error.exception)
    }
  }

  def handleEndTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    ensureInterBrokerVersion(IBP_0_11_0_IV0)
    val endTxnRequest = request.body[EndTxnRequest]
    val transactionalId = endTxnRequest.data.transactionalId

    if (authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
      def sendResponseCallback(error: Errors): Unit = {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val finalError =
            if (endTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
              // For older clients, they could not understand the new PRODUCER_FENCED error code,
              // so we need to return the INVALID_PRODUCER_EPOCH to have the same client handling logic.
              Errors.INVALID_PRODUCER_EPOCH
            } else {
              error
            }
          val responseBody = new EndTxnResponse(new EndTxnResponseData()
            .setErrorCode(finalError.code)
            .setThrottleTimeMs(requestThrottleMs))
          trace(s"Completed ${endTxnRequest.data.transactionalId}'s EndTxnRequest " +
            s"with committed: ${endTxnRequest.data.committed}, " +
            s"errors: $error from client ${request.header.clientId}.")
          responseBody
        }
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleEndTransaction(endTxnRequest.data.transactionalId,
        endTxnRequest.data.producerId,
        endTxnRequest.data.producerEpoch,
        endTxnRequest.result(),
        sendResponseCallback,
        requestLocal)
    } else
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new EndTxnResponse(new EndTxnResponseData()
            .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
            .setThrottleTimeMs(requestThrottleMs))
      )
  }

  def handleWriteTxnMarkersRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    ensureInterBrokerVersion(IBP_0_11_0_IV0)
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val writeTxnMarkersRequest = request.body[WriteTxnMarkersRequest]
    val errors = new ConcurrentHashMap[java.lang.Long, util.Map[TopicPartition, Errors]]()
    val markers = writeTxnMarkersRequest.markers
    val numAppends = new AtomicInteger(markers.size)

    if (numAppends.get == 0) {
      requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
      return
    }

    def updateErrors(producerId: Long, currentErrors: ConcurrentHashMap[TopicPartition, Errors]): Unit = {
      val previousErrors = errors.putIfAbsent(producerId, currentErrors)
      if (previousErrors != null)
        previousErrors.putAll(currentErrors)
    }

    /**
      * This is the call back invoked when a log append of transaction markers succeeds. This can be called multiple
      * times when handling a single WriteTxnMarkersRequest because there is one append per TransactionMarker in the
      * request, so there could be multiple appends of markers to the log. The final response will be sent only
      * after all appends have returned.
      */
    def maybeSendResponseCallback(producerId: Long, result: TransactionResult, currentErrors: ConcurrentHashMap[TopicPartition, Errors]): Unit = {
      trace(s"End transaction marker append for producer id $producerId completed with status: $currentErrors")
      updateErrors(producerId, currentErrors)

      if (!config.isNewGroupCoordinatorEnabled) {
        val successfulOffsetsPartitions = currentErrors.asScala.filter { case (topicPartition, error) =>
          topicPartition.topic == GROUP_METADATA_TOPIC_NAME && error == Errors.NONE
        }.keys

        if (successfulOffsetsPartitions.nonEmpty) {
          // as soon as the end transaction marker has been written for a transactional offset commit,
          // call to the group coordinator to materialize the offsets into the cache
          try {
            groupCoordinator.onTransactionCompleted(producerId, successfulOffsetsPartitions.asJava, result)
          } catch {
            case e: Exception =>
              error(s"Received an exception while trying to update the offsets cache on transaction marker append", e)
              val updatedErrors = new ConcurrentHashMap[TopicPartition, Errors]()
              successfulOffsetsPartitions.foreach(updatedErrors.put(_, Errors.UNKNOWN_SERVER_ERROR))
              updateErrors(producerId, updatedErrors)
          }
        }
      }

      if (numAppends.decrementAndGet() == 0)
        requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
    }

    // TODO: The current append API makes doing separate writes per producerId a little easier, but it would
    // be nice to have only one append to the log. This requires pushing the building of the control records
    // into Log so that we only append those having a valid producer epoch, and exposing a new appendControlRecord
    // API in ReplicaManager. For now, we've done the simpler approach
    var skippedMarkers = 0
    for (marker <- markers.asScala) {
      val producerId = marker.producerId
      val partitionsWithCompatibleMessageFormat = new mutable.ArrayBuffer[TopicPartition]

      val currentErrors = new ConcurrentHashMap[TopicPartition, Errors]()
      marker.partitions.forEach { partition =>
        replicaManager.getMagic(partition) match {
          case Some(magic) =>
            if (magic < RecordBatch.MAGIC_VALUE_V2)
              currentErrors.put(partition, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT)
            else
              partitionsWithCompatibleMessageFormat += partition
          case None =>
            currentErrors.put(partition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
      }

      if (!currentErrors.isEmpty)
        updateErrors(producerId, currentErrors)

      if (partitionsWithCompatibleMessageFormat.isEmpty) {
        numAppends.decrementAndGet()
        skippedMarkers += 1
      } else {
        val controlRecordType = marker.transactionResult match {
          case TransactionResult.COMMIT => ControlRecordType.COMMIT
          case TransactionResult.ABORT => ControlRecordType.ABORT
        }

        val markerResults = new ConcurrentHashMap[TopicPartition, Errors]()
        def maybeComplete(): Unit = {
          if (partitionsWithCompatibleMessageFormat.size == markerResults.size) {
            maybeSendResponseCallback(producerId, marker.transactionResult, markerResults)
          }
        }

        val controlRecords = mutable.Map.empty[TopicPartition, MemoryRecords]
        partitionsWithCompatibleMessageFormat.foreach { partition =>
          if (config.isNewGroupCoordinatorEnabled && partition.topic == GROUP_METADATA_TOPIC_NAME) {
            // When the new group coordinator is used, writing the end marker is fully delegated
            // to the group coordinator.
            groupCoordinator.completeTransaction(
              partition,
              marker.producerId,
              marker.producerEpoch,
              marker.coordinatorEpoch,
              marker.transactionResult,
              Duration.ofMillis(config.requestTimeoutMs.toLong)
            ).whenComplete { (_, exception) =>
              val error = if (exception == null) {
                Errors.NONE
              } else {
                Errors.forException(exception) match {
                  case Errors.COORDINATOR_NOT_AVAILABLE | Errors.COORDINATOR_LOAD_IN_PROGRESS | Errors.NOT_COORDINATOR =>
                    // The transaction coordinator does not expect those errors so we translate them
                    // to NOT_LEADER_OR_FOLLOWER to signal to it that the coordinator is not ready yet.
                    Errors.NOT_LEADER_OR_FOLLOWER
                  case error =>
                    error
                }
              }
              markerResults.put(partition, error)
              maybeComplete()
            }
          } else {
            // Otherwise, the regular appendRecords path is used for all the non __consumer_offsets
            // partitions or for all partitions when the new group coordinator is disabled.
            controlRecords += partition -> MemoryRecords.withEndTransactionMarker(
              producerId,
              marker.producerEpoch,
              new EndTransactionMarker(controlRecordType, marker.coordinatorEpoch)
            )
          }
        }

        replicaManager.appendRecords(
          timeout = config.requestTimeoutMs.toLong,
          requiredAcks = -1,
          internalTopicsAllowed = true,
          origin = AppendOrigin.COORDINATOR,
          entriesPerPartition = controlRecords,
          requestLocal = requestLocal,
          responseCallback = errors => {
            errors.forKeyValue { (tp, partitionResponse) =>
              markerResults.put(tp, partitionResponse.error)
            }
            maybeComplete()
          }
        )
      }
    }

    // No log appends were written as all partitions had incorrect log format
    // so we need to send the error response
    if (skippedMarkers == markers.size)
      requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
  }

  def ensureInterBrokerVersion(version: MetadataVersion): Unit = {
    if (config.interBrokerProtocolVersion.isLessThan(version))
      throw new UnsupportedVersionException(s"inter.broker.protocol.version: ${config.interBrokerProtocolVersion.version} is less than the required version: ${version.version}")
  }

  def handleAddPartitionsToTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    ensureInterBrokerVersion(IBP_0_11_0_IV0)
    val addPartitionsToTxnRequest =
      if (request.context.apiVersion() < 4)
        request.body[AddPartitionsToTxnRequest].normalizeRequest()
      else
        request.body[AddPartitionsToTxnRequest]
    val version = addPartitionsToTxnRequest.version
    val responses = new AddPartitionsToTxnResultCollection()
    val partitionsByTransaction = addPartitionsToTxnRequest.partitionsByTransaction()

    // Newer versions of the request should only come from other brokers.
    if (version >= 4) authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    // V4 requests introduced batches of transactions. We need all transactions to be handled before sending the
    // response so there are a few differences in handling errors and sending responses.
    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      if (version < 4) {
        // There will only be one response in data. Add it to the response data object.
        val data = new AddPartitionsToTxnResponseData()
        responses.forEach { result =>
          data.setResultsByTopicV3AndBelow(result.topicResults())
          data.setThrottleTimeMs(requestThrottleMs)
        }
        new AddPartitionsToTxnResponse(data)
      } else {
        new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData().setThrottleTimeMs(requestThrottleMs).setResultsByTransaction(responses))
      }
    }

    val txns = addPartitionsToTxnRequest.data.transactions
    def addResultAndMaybeSendResponse(result: AddPartitionsToTxnResult): Unit = {
      val canSend = responses.synchronized {
        responses.add(result)
        responses.size == txns.size
      }
      if (canSend) {
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }
    }

    txns.forEach { transaction =>
      val transactionalId = transaction.transactionalId

      if (transactionalId == null)
        throw new InvalidRequestException("Transactional ID can not be null in request.")

      val partitionsToAdd = partitionsByTransaction.get(transactionalId).asScala

      // Versions < 4 come from clients and must be authorized to write for the given transaction and for the given topics.
      if (version < 4 && !authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
        addResultAndMaybeSendResponse(addPartitionsToTxnRequest.errorResponseForTransaction(transactionalId, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
      } else {
        val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
        val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
        val authorizedPartitions = mutable.Set[TopicPartition]()

        // Only request versions less than 4 need write authorization since they come from clients.
        val authorizedTopics =
          if (version < 4)
            authHelper.filterByAuthorized(request.context, WRITE, TOPIC, partitionsToAdd.filterNot(tp => Topic.isInternal(tp.topic)))(_.topic)
          else
            partitionsToAdd.map(_.topic).toSet
        for (topicPartition <- partitionsToAdd) {
          if (!authorizedTopics.contains(topicPartition.topic))
            unauthorizedTopicErrors += topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED
          else if (!metadataCache.contains(topicPartition))
            nonExistingTopicErrors += topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION
          else
            authorizedPartitions.add(topicPartition)
        }

        if (unauthorizedTopicErrors.nonEmpty || nonExistingTopicErrors.nonEmpty) {
          // Any failed partition check causes the entire transaction to fail. We send the appropriate error codes for the
          // partitions which failed, and an 'OPERATION_NOT_ATTEMPTED' error code for the partitions which succeeded
          // the authorization check to indicate that they were not added to the transaction.
          val partitionErrors = unauthorizedTopicErrors ++ nonExistingTopicErrors ++
            authorizedPartitions.map(_ -> Errors.OPERATION_NOT_ATTEMPTED)
          addResultAndMaybeSendResponse(AddPartitionsToTxnResponse.resultForTransaction(transactionalId, partitionErrors.asJava))
        } else {
          def sendResponseCallback(error: Errors): Unit = {
            val finalError = {
              if (version < 2 && error == Errors.PRODUCER_FENCED) {
                // For older clients, they could not understand the new PRODUCER_FENCED error code,
                // so we need to return the old INVALID_PRODUCER_EPOCH to have the same client handling logic.
                Errors.INVALID_PRODUCER_EPOCH
              } else {
                error
              }
            }
            addResultAndMaybeSendResponse(addPartitionsToTxnRequest.errorResponseForTransaction(transactionalId, finalError))
          }

          if (!transaction.verifyOnly) {
            txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
              transaction.producerId,
              transaction.producerEpoch,
              authorizedPartitions,
              sendResponseCallback,
              requestLocal)
          } else {
            txnCoordinator.handleVerifyPartitionsInTransaction(transactionalId,
              transaction.producerId,
              transaction.producerEpoch,
              authorizedPartitions,
              addResultAndMaybeSendResponse)
          }
        }
      }
    }
  }

  def handleAddOffsetsToTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    ensureInterBrokerVersion(IBP_0_11_0_IV0)
    val addOffsetsToTxnRequest = request.body[AddOffsetsToTxnRequest]
    val transactionalId = addOffsetsToTxnRequest.data.transactionalId
    val groupId = addOffsetsToTxnRequest.data.groupId
    val offsetTopicPartition = new TopicPartition(GROUP_METADATA_TOPIC_NAME, groupCoordinator.partitionFor(groupId))

    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs)))
    else if (!authHelper.authorize(request.context, READ, GROUP, groupId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs))
      )
    else {
      def sendResponseCallback(error: Errors): Unit = {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val finalError =
            if (addOffsetsToTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
              // For older clients, they could not understand the new PRODUCER_FENCED error code,
              // so we need to return the old INVALID_PRODUCER_EPOCH to have the same client handling logic.
              Errors.INVALID_PRODUCER_EPOCH
            } else {
              error
            }

          val responseBody: AddOffsetsToTxnResponse = new AddOffsetsToTxnResponse(
            new AddOffsetsToTxnResponseData()
              .setErrorCode(finalError.code)
              .setThrottleTimeMs(requestThrottleMs))
          trace(s"Completed $transactionalId's AddOffsetsToTxnRequest for group $groupId on partition " +
            s"$offsetTopicPartition: errors: $error from client ${request.header.clientId}")
          responseBody
        }
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
        addOffsetsToTxnRequest.data.producerId,
        addOffsetsToTxnRequest.data.producerEpoch,
        Set(offsetTopicPartition),
        sendResponseCallback,
        requestLocal)
    }
  }

  def handleTxnOffsetCommitRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    ensureInterBrokerVersion(IBP_0_11_0_IV0)
    val txnOffsetCommitRequest = request.body[TxnOffsetCommitRequest]

    def sendResponse(response: TxnOffsetCommitResponse): Unit = {
      // We need to replace COORDINATOR_LOAD_IN_PROGRESS with COORDINATOR_NOT_AVAILABLE
      // for older producer client from 0.11 to prior 2.0, which could potentially crash due
      // to unexpected loading error. This bug is fixed later by KAFKA-7296. Clients using
      // txn commit protocol >= 2 (version 2.3 and onwards) are guaranteed to have
      // the fix to check for the loading error.
      if (txnOffsetCommitRequest.version < 2) {
        response.data.topics.forEach { topic =>
          topic.partitions.forEach { partition =>
            if (partition.errorCode == Errors.COORDINATOR_LOAD_IN_PROGRESS.code) {
              partition.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
            }
          }
        }
      }

      requestHelper.sendMaybeThrottle(request, response)
    }

    // authorize for the transactionalId and the consumer group. Note that we skip producerId authorization
    // since it is implied by transactionalId authorization
    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, txnOffsetCommitRequest.data.transactionalId)) {
      sendResponse(txnOffsetCommitRequest.getErrorResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, txnOffsetCommitRequest.data.groupId)) {
      sendResponse(txnOffsetCommitRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val authorizedTopics = authHelper.filterByAuthorized(
        request.context,
        READ,
        TOPIC,
        txnOffsetCommitRequest.data.topics.asScala
      )(_.name)

      val responseBuilder = new TxnOffsetCommitResponse.Builder()
      val authorizedTopicCommittedOffsets = new mutable.ArrayBuffer[TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic]()
      txnOffsetCommitRequest.data.topics.forEach { topic =>
        if (!authorizedTopics.contains(topic.name)) {
          // If the topic is not authorized, we add the topic and all its partitions
          // to the response with TOPIC_AUTHORIZATION_FAILED.
          responseBuilder.addPartitions[TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else if (!metadataCache.contains(topic.name)) {
          // If the topic is unknown, we add the topic and all its partitions
          // to the response with UNKNOWN_TOPIC_OR_PARTITION.
          responseBuilder.addPartitions[TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          // Otherwise, we check all partitions to ensure that they all exist.
          val topicWithValidPartitions = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName(topic.name)

          topic.partitions.forEach { partition =>
            if (metadataCache.getPartitionInfo(topic.name, partition.partitionIndex).nonEmpty) {
              topicWithValidPartitions.partitions.add(partition)
            } else {
              responseBuilder.addPartition(topic.name, partition.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          }

          if (!topicWithValidPartitions.partitions.isEmpty) {
            authorizedTopicCommittedOffsets += topicWithValidPartitions
          }
        }
      }

      if (authorizedTopicCommittedOffsets.isEmpty) {
        sendResponse(responseBuilder.build())
        CompletableFuture.completedFuture[Unit](())
      } else {
        val txnOffsetCommitRequestData = new TxnOffsetCommitRequestData()
          .setGroupId(txnOffsetCommitRequest.data.groupId)
          .setMemberId(txnOffsetCommitRequest.data.memberId)
          .setGenerationId(txnOffsetCommitRequest.data.generationId)
          .setGroupInstanceId(txnOffsetCommitRequest.data.groupInstanceId)
          .setProducerEpoch(txnOffsetCommitRequest.data.producerEpoch)
          .setProducerId(txnOffsetCommitRequest.data.producerId)
          .setTransactionalId(txnOffsetCommitRequest.data.transactionalId)
          .setTopics(authorizedTopicCommittedOffsets.asJava)

        groupCoordinator.commitTransactionalOffsets(
          request.context,
          txnOffsetCommitRequestData,
          requestLocal.bufferSupplier
        ).handle[Unit] { (response, exception) =>
          if (exception != null) {
            sendResponse(txnOffsetCommitRequest.getErrorResponse(exception))
          } else {
            sendResponse(responseBuilder.merge(response).build())
          }
        }
      }
    }
  }

  def handleDescribeAcls(request: RequestChannel.Request): Unit = {
    aclApis.handleDescribeAcls(request)
  }

  def handleCreateAcls(request: RequestChannel.Request): Unit = {
    metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    aclApis.handleCreateAcls(request)
  }

  def handleDeleteAcls(request: RequestChannel.Request): Unit = {
    metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    aclApis.handleDeleteAcls(request)
  }

  def handleOffsetForLeaderEpochRequest(request: RequestChannel.Request): Unit = {
    val offsetForLeaderEpoch = request.body[OffsetsForLeaderEpochRequest]
    val topics = offsetForLeaderEpoch.data.topics.asScala.toSeq

    // The OffsetsForLeaderEpoch API was initially only used for inter-broker communication and required
    // cluster permission. With KIP-320, the consumer now also uses this API to check for log truncation
    // following a leader change, so we also allow topic describe permission.
    val (authorizedTopics, unauthorizedTopics) =
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME, logIfDenied = false))
        (topics, Seq.empty[OffsetForLeaderTopic])
      else authHelper.partitionSeqByAuthorized(request.context, DESCRIBE, TOPIC, topics)(_.topic)

    val endOffsetsForAuthorizedPartitions = replicaManager.lastOffsetForLeaderEpoch(authorizedTopics)
    val endOffsetsForUnauthorizedPartitions = unauthorizedTopics.map { offsetForLeaderTopic =>
      val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
        new EpochEndOffset()
          .setPartition(offsetForLeaderPartition.partition)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
      }

      new OffsetForLeaderTopicResult()
        .setTopic(offsetForLeaderTopic.topic)
        .setPartitions(partitions.toList.asJava)
    }

    val endOffsetsForAllTopics = new OffsetForLeaderTopicResultCollection(
      (endOffsetsForAuthorizedPartitions ++ endOffsetsForUnauthorizedPartitions).asJava.iterator
    )

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new OffsetsForLeaderEpochResponse(new OffsetForLeaderEpochResponseData()
        .setThrottleTimeMs(requestThrottleMs)
        .setTopics(endOffsetsForAllTopics)))
  }

  def handleAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[AlterConfigsRequest]
    val preprocessingResponses = configManager.preprocess(original.data())
    val remaining = ConfigAdminManager.copyWithoutPreprocessed(original.data(), preprocessingResponses)
    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: AlterConfigsResponseData) =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new AlterConfigsResponse(ConfigAdminManager.reassembleLegacyResponse(
              original.data(),
              preprocessingResponses,
              result).setThrottleTimeMs(requestThrottleMs)))
        case _ => handleInvalidVersionsDuringForwarding(request)
      }
    }
    if (remaining.resources().isEmpty) {
      sendResponse(Some(new AlterConfigsResponseData()))
    } else if ((!request.isForwarded) && metadataSupport.canForward()) {
      metadataSupport.forwardingManager.get.forwardRequest(request,
        new AlterConfigsRequest(remaining, request.header.apiVersion()),
        response => sendResponse(response.map(_.data())))
    } else {
      sendResponse(Some(processLegacyAlterConfigsRequest(request, remaining)))
    }
  }

  def processLegacyAlterConfigsRequest(
    originalRequest: RequestChannel.Request,
    data: AlterConfigsRequestData
  ): AlterConfigsResponseData = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(originalRequest))
    val alterConfigsRequest = new AlterConfigsRequest(data, originalRequest.header.apiVersion())
    val (authorizedResources, unauthorizedResources) = alterConfigsRequest.configs.asScala.toMap.partition { case (resource, _) =>
      resource.`type` match {
        case ConfigResource.Type.BROKER_LOGGER =>
          throw new InvalidRequestException(s"AlterConfigs is deprecated and does not support the resource type ${ConfigResource.Type.BROKER_LOGGER}")
        case ConfigResource.Type.BROKER | ConfigResource.Type.CLIENT_METRICS =>
          authHelper.authorize(originalRequest.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)
        case ConfigResource.Type.TOPIC =>
          authHelper.authorize(originalRequest.context, ALTER_CONFIGS, TOPIC, resource.name)
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt")
      }
    }
    val authorizedResult = zkSupport.adminManager.alterConfigs(authorizedResources, alterConfigsRequest.validateOnly)
    val unauthorizedResult = unauthorizedResources.keys.map { resource =>
      resource -> configsAuthorizationApiError(resource)
    }
    val response = new AlterConfigsResponseData()
    (authorizedResult ++ unauthorizedResult).foreach { case (resource, error) =>
      response.responses().add(new AlterConfigsResourceResponse()
        .setErrorCode(error.error.code)
        .setErrorMessage(error.message)
        .setResourceName(resource.name)
        .setResourceType(resource.`type`.id))
    }
    response
  }

  def handleAlterPartitionReassignmentsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    authHelper.authorizeClusterOperation(request, ALTER)
    val alterPartitionReassignmentsRequest = request.body[AlterPartitionReassignmentsRequest]

    def sendResponseCallback(result: Either[Map[TopicPartition, ApiError], ApiError]): Unit = {
      val responseData = result match {
        case Right(topLevelError) =>
          new AlterPartitionReassignmentsResponseData().setErrorMessage(topLevelError.message).setErrorCode(topLevelError.error.code)

        case Left(assignments) =>
          val topicResponses = assignments.groupBy(_._1.topic).map {
            case (topic, reassignmentsByTp) =>
              val partitionResponses = reassignmentsByTp.map {
                case (topicPartition, error) =>
                  new ReassignablePartitionResponse().setPartitionIndex(topicPartition.partition)
                    .setErrorCode(error.error.code).setErrorMessage(error.message)
              }
              new ReassignableTopicResponse().setName(topic).setPartitions(partitionResponses.toList.asJava)
          }
          new AlterPartitionReassignmentsResponseData().setResponses(topicResponses.toList.asJava)
      }

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterPartitionReassignmentsResponse(responseData.setThrottleTimeMs(requestThrottleMs))
      )
    }

    val reassignments = alterPartitionReassignmentsRequest.data.topics.asScala.flatMap {
      reassignableTopic => reassignableTopic.partitions.asScala.map {
        reassignablePartition =>
          val tp = new TopicPartition(reassignableTopic.name, reassignablePartition.partitionIndex)
          if (reassignablePartition.replicas == null)
            tp -> None // revert call
          else
            tp -> Some(reassignablePartition.replicas.asScala.map(_.toInt))
      }
    }.toMap

    zkSupport.controller.alterPartitionReassignments(reassignments, sendResponseCallback)
  }

  def handleListPartitionReassignmentsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    val listPartitionReassignmentsRequest = request.body[ListPartitionReassignmentsRequest]

    def sendResponseCallback(result: Either[Map[TopicPartition, ReplicaAssignment], ApiError]): Unit = {
      val responseData = result match {
        case Right(error) => new ListPartitionReassignmentsResponseData().setErrorMessage(error.message).setErrorCode(error.error.code)

        case Left(assignments) =>
          val topicReassignments = assignments.groupBy(_._1.topic).map {
            case (topic, reassignmentsByTp) =>
              val partitionReassignments = reassignmentsByTp.map {
                case (topicPartition, assignment) =>
                  new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
                    .setPartitionIndex(topicPartition.partition)
                    .setAddingReplicas(assignment.addingReplicas.toList.asJava.asInstanceOf[java.util.List[java.lang.Integer]])
                    .setRemovingReplicas(assignment.removingReplicas.toList.asJava.asInstanceOf[java.util.List[java.lang.Integer]])
                    .setReplicas(assignment.replicas.toList.asJava.asInstanceOf[java.util.List[java.lang.Integer]])
              }.toList

              new ListPartitionReassignmentsResponseData.OngoingTopicReassignment().setName(topic)
                .setPartitions(partitionReassignments.asJava)
          }.toList

          new ListPartitionReassignmentsResponseData().setTopics(topicReassignments.asJava)
      }

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ListPartitionReassignmentsResponse(responseData.setThrottleTimeMs(requestThrottleMs))
      )
    }

    val partitionsOpt = Option(listPartitionReassignmentsRequest.data.topics).map { topics =>
      topics.iterator().asScala.flatMap { topic =>
        topic.partitionIndexes.iterator().asScala.map { partitionIndex =>
          new TopicPartition(topic.name(), partitionIndex)
        }
      }.toSet
    }

    zkSupport.controller.listPartitionReassignments(partitionsOpt, sendResponseCallback)
  }

  private def configsAuthorizationApiError(resource: ConfigResource): ApiError = {
    val error = resource.`type` match {
      case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER => Errors.CLUSTER_AUTHORIZATION_FAILED
      case ConfigResource.Type.TOPIC => Errors.TOPIC_AUTHORIZATION_FAILED
      case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.name}")
    }
    new ApiError(error, null)
  }

  def handleIncrementalAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[IncrementalAlterConfigsRequest]
    val preprocessingResponses = configManager.preprocess(original.data(),
      (rType, rName) => authHelper.authorize(request.context, ALTER_CONFIGS, rType, rName))
    val remaining = ConfigAdminManager.copyWithoutPreprocessed(original.data(), preprocessingResponses)

    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: IncrementalAlterConfigsResponseData) =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new IncrementalAlterConfigsResponse(ConfigAdminManager.reassembleIncrementalResponse(
              original.data(),
              preprocessingResponses,
              result).setThrottleTimeMs(requestThrottleMs)))
        case _ => handleInvalidVersionsDuringForwarding(request)
      }
    }

    if (remaining.resources().isEmpty) {
      sendResponse(Some(new IncrementalAlterConfigsResponseData()))
    } else if ((!request.isForwarded) && metadataSupport.canForward()) {
      metadataSupport.forwardingManager.get.forwardRequest(request,
        new IncrementalAlterConfigsRequest(remaining, request.header.apiVersion()),
        response => sendResponse(response.map(_.data())))
    } else {
      sendResponse(Some(processIncrementalAlterConfigsRequest(request, remaining)))
    }
  }

  def processIncrementalAlterConfigsRequest(
    originalRequest: RequestChannel.Request,
    data: IncrementalAlterConfigsRequestData
  ): IncrementalAlterConfigsResponseData = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(originalRequest))
    val configs = data.resources.iterator.asScala.map { alterConfigResource =>
      val configResource = new ConfigResource(ConfigResource.Type.forId(alterConfigResource.resourceType),
        alterConfigResource.resourceName)
      configResource -> alterConfigResource.configs.iterator.asScala.map {
        alterConfig => new AlterConfigOp(new ConfigEntry(alterConfig.name, alterConfig.value),
          OpType.forId(alterConfig.configOperation))
      }.toBuffer
    }.toMap

    val (authorizedResources, unauthorizedResources) = configs.partition { case (resource, _) =>
      resource.`type` match {
        case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER | ConfigResource.Type.CLIENT_METRICS =>
          authHelper.authorize(originalRequest.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)
        case ConfigResource.Type.TOPIC =>
          authHelper.authorize(originalRequest.context, ALTER_CONFIGS, TOPIC, resource.name)
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt")
      }
    }

    val authorizedResult = zkSupport.adminManager.incrementalAlterConfigs(authorizedResources, data.validateOnly)
    val unauthorizedResult = unauthorizedResources.keys.map { resource =>
      resource -> configsAuthorizationApiError(resource)
    }
    new IncrementalAlterConfigsResponse(0, (authorizedResult ++ unauthorizedResult).asJava).data()
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): Unit = {
    val responseData = configHelper.handleDescribeConfigsRequest(request, authHelper)
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeConfigsResponse(responseData.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleAlterReplicaLogDirsRequest(request: RequestChannel.Request): Unit = {
    val alterReplicaDirsRequest = request.body[AlterReplicaLogDirsRequest]
    if (authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      val result = replicaManager.alterReplicaLogDirs(alterReplicaDirsRequest.partitionDirs.asScala)
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterReplicaLogDirsResponse(new AlterReplicaLogDirsResponseData()
          .setResults(result.groupBy(_._1.topic).map {
            case (topic, errors) => new AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult()
              .setTopicName(topic)
              .setPartitions(errors.map {
                case (tp, error) => new AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult()
                  .setPartitionIndex(tp.partition)
                  .setErrorCode(error.code)
              }.toList.asJava)
          }.toList.asJava)
          .setThrottleTimeMs(requestThrottleMs)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterReplicaDirsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleDescribeLogDirsRequest(request: RequestChannel.Request): Unit = {
    val describeLogDirsDirRequest = request.body[DescribeLogDirsRequest]
    val (logDirInfos, error) = {
      if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
        val partitions =
          if (describeLogDirsDirRequest.isAllTopicPartitions)
            replicaManager.logManager.allLogs.map(_.topicPartition).toSet
          else
            describeLogDirsDirRequest.data.topics.asScala.flatMap(
              logDirTopic => logDirTopic.partitions.asScala.map(partitionIndex =>
                new TopicPartition(logDirTopic.topic, partitionIndex))).toSet

        (replicaManager.describeLogDirs(partitions), Errors.NONE)
      } else {
        (List.empty[DescribeLogDirsResponseData.DescribeLogDirsResult], Errors.CLUSTER_AUTHORIZATION_FAILED)
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => new DescribeLogDirsResponse(new DescribeLogDirsResponseData()
      .setThrottleTimeMs(throttleTimeMs)
      .setResults(logDirInfos.asJava)
      .setErrorCode(error.code)))
  }

  def handleCreateTokenRequest(request: RequestChannel.Request): Unit = {
    val createTokenRequest = request.body[CreateDelegationTokenRequest]

    val requester = request.context.principal
    val ownerPrincipalName = createTokenRequest.data.ownerPrincipalName
    val owner = if (ownerPrincipalName == null || ownerPrincipalName.isEmpty) {
      request.context.principal
    } else {
      new KafkaPrincipal(createTokenRequest.data.ownerPrincipalType, ownerPrincipalName)
    }
    val renewerList = createTokenRequest.data.renewers.asScala.toList.map(entry =>
      new KafkaPrincipal(entry.principalType, entry.principalName))

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, owner, requester))
    } else if (!owner.equals(requester) && !authHelper.authorize(request.context, CREATE_TOKENS, USER, owner.toString)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, owner, requester))
    } else if (renewerList.exists(principal => principal.getPrincipalType != KafkaPrincipal.USER_TYPE)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.INVALID_PRINCIPAL_TYPE, owner, requester))
    } else {
      maybeForwardToController(request, handleCreateTokenRequestZk)
    }
  }

  def handleCreateTokenRequestZk(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))

    val createTokenRequest = request.body[CreateDelegationTokenRequest]

    // the callback for sending a create token response
    def sendResponseCallback(createResult: CreateTokenResult): Unit = {
      trace(s"Sending create token response for correlation id ${request.header.correlationId} " +
        s"to client ${request.header.clientId}.")
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs, createResult.error, createResult.owner,
          createResult.tokenRequester, createResult.issueTimestamp, createResult.expiryTimestamp, createResult.maxTimestamp, createResult.tokenId,
          ByteBuffer.wrap(createResult.hmac)))
    }

    val ownerPrincipalName = createTokenRequest.data.ownerPrincipalName
    val owner = if (ownerPrincipalName == null || ownerPrincipalName.isEmpty) {
      request.context.principal
    } else {
      new KafkaPrincipal(createTokenRequest.data.ownerPrincipalType, ownerPrincipalName)
    }
    val requester = request.context.principal
    val renewerList = createTokenRequest.data.renewers.asScala.toList.map(entry =>
      new KafkaPrincipal(entry.principalType, entry.principalName))

    // DelegationToken changes only need to be executed on the controller during migration
    if (config.migrationEnabled && (!zkSupport.controller.isActive)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.NOT_CONTROLLER, owner, requester))
    } else {
      tokenManager.createToken(
        owner,
        requester,
        renewerList,
        createTokenRequest.data.maxLifetimeMs,
        sendResponseCallback)
    }
  }

  def handleRenewTokenRequest(request: RequestChannel.Request): Unit = {
    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new RenewDelegationTokenResponse(
          new RenewDelegationTokenResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
              .setExpiryTimestampMs(DelegationTokenManager.ErrorTimestamp)))
    } else {
      maybeForwardToController(request, handleRenewTokenRequestZk)
    }
  }

  def handleRenewTokenRequestZk(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))

    val renewTokenRequest = request.body[RenewDelegationTokenRequest]

    // the callback for sending a renew token response
    def sendResponseCallback(error: Errors, expiryTimestamp: Long): Unit = {
      trace("Sending renew token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new RenewDelegationTokenResponse(
             new RenewDelegationTokenResponseData()
               .setThrottleTimeMs(requestThrottleMs)
               .setErrorCode(error.code)
               .setExpiryTimestampMs(expiryTimestamp)))
    }
    // DelegationToken changes only need to be executed on the controller during migration
    if (config.migrationEnabled && (!zkSupport.controller.isActive)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new RenewDelegationTokenResponse(
          new RenewDelegationTokenResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(Errors.NOT_CONTROLLER.code)))
    } else {
      tokenManager.renewToken(
        request.context.principal,
        ByteBuffer.wrap(renewTokenRequest.data.hmac),
        renewTokenRequest.data.renewPeriodMs,
        sendResponseCallback
      )
    }
  }

  def handleExpireTokenRequest(request: RequestChannel.Request): Unit = {
    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ExpireDelegationTokenResponse(
          new ExpireDelegationTokenResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
              .setExpiryTimestampMs(DelegationTokenManager.ErrorTimestamp)))
    } else {
      maybeForwardToController(request, handleExpireTokenRequestZk)
    }
  }

  def handleExpireTokenRequestZk(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))

    val expireTokenRequest = request.body[ExpireDelegationTokenRequest]

    // the callback for sending a expire token response
    def sendResponseCallback(error: Errors, expiryTimestamp: Long): Unit = {
      trace("Sending expire token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ExpireDelegationTokenResponse(
            new ExpireDelegationTokenResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(error.code)
              .setExpiryTimestampMs(expiryTimestamp)))
    }
    // DelegationToken changes only need to be executed on the controller during migration
    if (config.migrationEnabled && (!zkSupport.controller.isActive)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ExpireDelegationTokenResponse(
          new ExpireDelegationTokenResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(Errors.NOT_CONTROLLER.code)))
    } else {
      tokenManager.expireToken(
        request.context.principal,
        expireTokenRequest.hmac(),
        expireTokenRequest.expiryTimePeriod(),
        sendResponseCallback
      )
    }
  }

  def handleDescribeTokensRequest(request: RequestChannel.Request): Unit = {
    val describeTokenRequest = request.body[DescribeDelegationTokenRequest]

    // the callback for sending a describe token response
    def sendResponseCallback(error: Errors, tokenDetails: List[DelegationToken]): Unit = {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeDelegationTokenResponse(request.context.requestVersion(), requestThrottleMs, error, tokenDetails.asJava))
      trace("Sending describe token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
    }

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, List.empty)
    else if (!config.tokenAuthEnabled)
      sendResponseCallback(Errors.DELEGATION_TOKEN_AUTH_DISABLED, List.empty)
    else {
      val requestPrincipal = request.context.principal

      if (describeTokenRequest.ownersListEmpty()) {
        sendResponseCallback(Errors.NONE, List())
      }
      else {
        val owners = if (describeTokenRequest.data.owners == null)
          None
        else
          Some(describeTokenRequest.data.owners.asScala.map(p => new KafkaPrincipal(p.principalType(), p.principalName)).toList)
        def authorizeToken(tokenId: String) = authHelper.authorize(request.context, DESCRIBE, DELEGATION_TOKEN, tokenId)
        def authorizeRequester(owner: KafkaPrincipal) = authHelper.authorize(request.context, DESCRIBE_TOKENS, USER, owner.toString)
        def eligible(token: TokenInformation) = DelegationTokenManager
          .filterToken(requestPrincipal, owners, token, authorizeToken, authorizeRequester)
        val tokens =  tokenManager.getTokens(eligible)
        sendResponseCallback(Errors.NONE, tokens)
      }
    }
  }

  def allowTokenRequests(request: RequestChannel.Request): Boolean = {
    val protocol = request.context.securityProtocol
    if (request.context.principal.tokenAuthenticated ||
      protocol == SecurityProtocol.PLAINTEXT ||
      // disallow requests from 1-way SSL
      (protocol == SecurityProtocol.SSL && request.context.principal == KafkaPrincipal.ANONYMOUS))
      false
    else
      true
  }

  def handleElectLeaders(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val electionRequest = request.body[ElectLeadersRequest]

    def sendResponseCallback(
      error: ApiError
    )(
      results: Map[TopicPartition, ApiError]
    ): Unit = {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        val adjustedResults = if (electionRequest.data.topicPartitions == null) {
          /* When performing elections across all of the partitions we should only return
           * partitions for which there was an election or resulted in an error. In other
           * words, partitions that didn't need election because they ready have the correct
           * leader are not returned to the client.
           */
          results.filter { case (_, error) =>
            error.error != Errors.ELECTION_NOT_NEEDED
          }
        } else results

        val electionResults = new util.ArrayList[ReplicaElectionResult]()
        adjustedResults
          .groupBy { case (tp, _) => tp.topic }
          .forKeyValue { (topic, ps) =>
            val electionResult = new ReplicaElectionResult()

            electionResult.setTopic(topic)
            ps.forKeyValue { (topicPartition, error) =>
              val partitionResult = new PartitionResult()
              partitionResult.setPartitionId(topicPartition.partition)
              partitionResult.setErrorCode(error.error.code)
              partitionResult.setErrorMessage(error.message)
              electionResult.partitionResult.add(partitionResult)
            }

            electionResults.add(electionResult)
          }

        new ElectLeadersResponse(
          requestThrottleMs,
          error.error.code,
          electionResults,
          electionRequest.version
        )
      })
    }

    if (!authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      val error = new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, null)
      val partitionErrors: Map[TopicPartition, ApiError] =
        electionRequest.topicPartitions.iterator.map(partition => partition -> error).toMap

      sendResponseCallback(error)(partitionErrors)
    } else {
      val partitions = if (electionRequest.data.topicPartitions == null) {
        metadataCache.getAllTopics().flatMap(metadataCache.getTopicPartitions)
      } else {
        electionRequest.topicPartitions
      }

      replicaManager.electLeaders(
        zkSupport.controller,
        partitions,
        electionRequest.electionType,
        sendResponseCallback(ApiError.NONE),
        electionRequest.data.timeoutMs
      )
    }
  }

  def handleOffsetDeleteRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val offsetDeleteRequest = request.body[OffsetDeleteRequest]

    if (!authHelper.authorize(request.context, DELETE, GROUP, offsetDeleteRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, offsetDeleteRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val authorizedTopics = authHelper.filterByAuthorized(
        request.context,
        READ,
        TOPIC,
        offsetDeleteRequest.data.topics.asScala
      )(_.name)

      val responseBuilder = new OffsetDeleteResponse.Builder
      val authorizedTopicPartitions = new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection()
      offsetDeleteRequest.data.topics.forEach { topic =>
        if (!authorizedTopics.contains(topic.name)) {
          // If the topic is not authorized, we add the topic and all its partitions
          // to the response with TOPIC_AUTHORIZATION_FAILED.
          responseBuilder.addPartitions[OffsetDeleteRequestData.OffsetDeleteRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else if (!metadataCache.contains(topic.name)) {
          // If the topic is unknown, we add the topic and all its partitions
          // to the response with UNKNOWN_TOPIC_OR_PARTITION.
          responseBuilder.addPartitions[OffsetDeleteRequestData.OffsetDeleteRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          // Otherwise, we check all partitions to ensure that they all exist.
          val topicWithValidPartitions = new OffsetDeleteRequestData.OffsetDeleteRequestTopic().setName(topic.name)

          topic.partitions.forEach { partition =>
            if (metadataCache.getPartitionInfo(topic.name, partition.partitionIndex).nonEmpty) {
              topicWithValidPartitions.partitions.add(partition)
            } else {
              responseBuilder.addPartition(topic.name, partition.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          }

          if (!topicWithValidPartitions.partitions.isEmpty) {
            authorizedTopicPartitions.add(topicWithValidPartitions)
          }
        }
      }

      val offsetDeleteRequestData = new OffsetDeleteRequestData()
        .setGroupId(offsetDeleteRequest.data.groupId)
        .setTopics(authorizedTopicPartitions)

      groupCoordinator.deleteOffsets(
        request.context,
        offsetDeleteRequestData,
        requestLocal.bufferSupplier
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, offsetDeleteRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, responseBuilder.merge(response).build())
        }
      }
    }
  }

  def handleDescribeClientQuotasRequest(request: RequestChannel.Request): Unit = {
    val describeClientQuotasRequest = request.body[DescribeClientQuotasRequest]

    if (!authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeClientQuotasRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      metadataSupport match {
        case ZkSupport(adminManager, controller, zkClient, forwardingManager, metadataCache, _) =>
          val result = adminManager.describeClientQuotas(describeClientQuotasRequest.filter)

          val entriesData = result.iterator.map { case (quotaEntity, quotaValues) =>
            val entityData = quotaEntity.entries.asScala.iterator.map { case (entityType, entityName) =>
              new DescribeClientQuotasResponseData.EntityData()
                .setEntityType(entityType)
                .setEntityName(entityName)
            }.toBuffer

            val valueData = quotaValues.iterator.map { case (key, value) =>
              new DescribeClientQuotasResponseData.ValueData()
                .setKey(key)
                .setValue(value)
            }.toBuffer

            new DescribeClientQuotasResponseData.EntryData()
              .setEntity(entityData.asJava)
              .setValues(valueData.asJava)
          }.toBuffer

          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new DescribeClientQuotasResponse(new DescribeClientQuotasResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setEntries(entriesData.asJava)))
        case RaftSupport(_, metadataCache) =>
          val result = metadataCache.describeClientQuotas(describeClientQuotasRequest.data())
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            result.setThrottleTimeMs(requestThrottleMs)
            new DescribeClientQuotasResponse(result)
          })
      }
    }
  }

  def handleAlterClientQuotasRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val alterClientQuotasRequest = request.body[AlterClientQuotasRequest]

    if (authHelper.authorize(request.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      val result = zkSupport.adminManager.alterClientQuotas(alterClientQuotasRequest.entries.asScala,
        alterClientQuotasRequest.validateOnly)

      val entriesData = result.iterator.map { case (quotaEntity, apiError) =>
        val entityData = quotaEntity.entries.asScala.iterator.map { case (key, value) =>
          new AlterClientQuotasResponseData.EntityData()
            .setEntityType(key)
            .setEntityName(value)
        }.toBuffer

        new AlterClientQuotasResponseData.EntryData()
          .setErrorCode(apiError.error.code)
          .setErrorMessage(apiError.message)
          .setEntity(entityData.asJava)
      }.toBuffer

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterClientQuotasResponse(new AlterClientQuotasResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setEntries(entriesData.asJava)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterClientQuotasRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleDescribeUserScramCredentialsRequest(request: RequestChannel.Request): Unit = {
    val describeUserScramCredentialsRequest = request.body[DescribeUserScramCredentialsRequest]

    if (!authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      metadataSupport match {
        case ZkSupport(adminManager, controller, zkClient, forwardingManager, metadataCache, _) =>
          val result = adminManager.describeUserScramCredentials(
            Option(describeUserScramCredentialsRequest.data.users).map(_.asScala.map(_.name).toList))
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new DescribeUserScramCredentialsResponse(result.setThrottleTimeMs(requestThrottleMs)))
        case RaftSupport(_, metadataCache) =>
          val result = metadataCache.describeScramCredentials(describeUserScramCredentialsRequest.data())
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new DescribeUserScramCredentialsResponse(result.setThrottleTimeMs(requestThrottleMs)))
      }
    }
  }

  def handleAlterUserScramCredentialsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val alterUserScramCredentialsRequest = request.body[AlterUserScramCredentialsRequest]

    if (!zkSupport.controller.isActive) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.NOT_CONTROLLER.exception))
    } else if (authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      val result = zkSupport.adminManager.alterUserScramCredentials(
        alterUserScramCredentialsRequest.data.upsertions().asScala, alterUserScramCredentialsRequest.data.deletions().asScala)
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterUserScramCredentialsResponse(result.setThrottleTimeMs(requestThrottleMs)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleAlterPartitionRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    val alterPartitionRequest = request.body[AlterPartitionRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    if (!zkSupport.controller.isActive)
      requestHelper.sendResponseExemptThrottle(request, alterPartitionRequest.getErrorResponse(
        AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.NOT_CONTROLLER.exception))
    else
      zkSupport.controller.alterPartitions(alterPartitionRequest.data, request.context.apiVersion, alterPartitionResp =>
        requestHelper.sendResponseExemptThrottle(request, new AlterPartitionResponse(alterPartitionResp)))
  }

  def handleUpdateFeatures(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val updateFeaturesRequest = request.body[UpdateFeaturesRequest]

    def sendResponseCallback(errors: Either[ApiError, Map[String, ApiError]]): Unit = {
      def createResponse(throttleTimeMs: Int): UpdateFeaturesResponse = {
        errors match {
          case Left(topLevelError) =>
            UpdateFeaturesResponse.createWithErrors(
              topLevelError,
              Collections.emptyMap(),
              throttleTimeMs)
          case Right(featureUpdateErrors) =>
            UpdateFeaturesResponse.createWithErrors(
              ApiError.NONE,
              featureUpdateErrors.asJava,
              throttleTimeMs)
        }
      }
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => createResponse(requestThrottleMs))
    }

    if (!authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      sendResponseCallback(Left(new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED)))
    } else if (!zkSupport.controller.isActive) {
      sendResponseCallback(Left(new ApiError(Errors.NOT_CONTROLLER)))
    } else if (!config.isFeatureVersioningSupported) {
      sendResponseCallback(Left(new ApiError(Errors.INVALID_REQUEST, "Feature versioning system is disabled.")))
    } else {
      zkSupport.controller.updateFeatures(updateFeaturesRequest, sendResponseCallback)
    }
  }

  def handleDescribeCluster(request: RequestChannel.Request): Unit = {
    val response = authHelper.computeDescribeClusterResponse(
      request,
      EndpointType.BROKER,
      clusterId,
      () => {
        val brokers = new DescribeClusterResponseData.DescribeClusterBrokerCollection()
        metadataCache.getAliveBrokerNodes(request.context.listenerName).foreach { node =>
          brokers.add(new DescribeClusterResponseData.DescribeClusterBroker().
            setBrokerId(node.id).
            setHost(node.host).
            setPort(node.port).
            setRack(node.rack))
        }
        brokers
      },
      () => {
        metadataCache.getControllerId match {
          case Some(value) =>
            value match {
              case ZkCachedControllerId (id) => id
              case KRaftCachedControllerId (_) => metadataCache.getRandomAliveBrokerId.getOrElse(- 1)
            }
          case None => -1
        }
      }
    )
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeClusterResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleEnvelope(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))

    // If forwarding is not yet enabled or this request has been received on an invalid endpoint,
    // then we treat the request as unparsable and close the connection.
    if (!isForwardingEnabled(request)) {
      info(s"Closing connection ${request.context.connectionId} because it sent an `Envelope` " +
        "request even though forwarding has not been enabled")
      requestChannel.closeConnection(request, Collections.emptyMap())
      return
    } else if (!request.context.fromPrivilegedListener) {
      info(s"Closing connection ${request.context.connectionId} from listener ${request.context.listenerName} " +
        s"because it sent an `Envelope` request, which is only accepted on the inter-broker listener " +
        s"${config.interBrokerListenerName}.")
      requestChannel.closeConnection(request, Collections.emptyMap())
      return
    } else if (!authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new ClusterAuthorizationException(
        s"Principal ${request.context.principal} does not have required CLUSTER_ACTION for envelope"))
      return
    } else if (!zkSupport.controller.isActive) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new NotControllerException(
        s"Broker $brokerId is not the active controller"))
      return
    }

    EnvelopeUtils.handleEnvelopeRequest(request, requestChannel.metrics, handle(_, requestLocal))
  }

  def handleDescribeProducersRequest(request: RequestChannel.Request): Unit = {
    val describeProducersRequest = request.body[DescribeProducersRequest]

    def partitionError(
      topicPartition: TopicPartition,
      apiError: ApiError
    ): DescribeProducersResponseData.PartitionResponse = {
      new DescribeProducersResponseData.PartitionResponse()
        .setPartitionIndex(topicPartition.partition)
        .setErrorCode(apiError.error.code)
        .setErrorMessage(apiError.message)
    }

    val response = new DescribeProducersResponseData()
    describeProducersRequest.data.topics.forEach { topicRequest =>
      val topicResponse = new DescribeProducersResponseData.TopicResponse()
        .setName(topicRequest.name)

      val invalidTopicError = checkValidTopic(topicRequest.name)

      val topicError = invalidTopicError.orElse {
        if (!authHelper.authorize(request.context, READ, TOPIC, topicRequest.name)) {
          Some(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED))
        } else if (!metadataCache.contains(topicRequest.name))
          Some(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION))
        else {
          None
        }
      }

      topicRequest.partitionIndexes.forEach { partitionId =>
        val topicPartition = new TopicPartition(topicRequest.name, partitionId)
        val partitionResponse = topicError match {
          case Some(error) => partitionError(topicPartition, error)
          case None => replicaManager.activeProducerState(topicPartition)
        }
        topicResponse.partitions.add(partitionResponse)
      }

      response.topics.add(topicResponse)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeProducersResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  private def checkValidTopic(topic: String): Option[ApiError] = {
    try {
      Topic.validate(topic)
      None
    } catch {
      case e: Throwable => Some(ApiError.fromThrowable(e))
    }
  }

  def handleDescribeTransactionsRequest(request: RequestChannel.Request): Unit = {
    val describeTransactionsRequest = request.body[DescribeTransactionsRequest]
    val response = new DescribeTransactionsResponseData()

    describeTransactionsRequest.data.transactionalIds.forEach { transactionalId =>
      val transactionState = if (!authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, transactionalId)) {
        new DescribeTransactionsResponseData.TransactionState()
          .setTransactionalId(transactionalId)
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
      } else {
        txnCoordinator.handleDescribeTransactions(transactionalId)
      }

      // Include only partitions which the principal is authorized to describe
      val topicIter = transactionState.topics.iterator()
      while (topicIter.hasNext) {
        val topic = topicIter.next().topic
        if (!authHelper.authorize(request.context, DESCRIBE, TOPIC, topic)) {
          topicIter.remove()
        }
      }
      response.transactionStates.add(transactionState)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeTransactionsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleListTransactionsRequest(request: RequestChannel.Request): Unit = {
    val listTransactionsRequest = request.body[ListTransactionsRequest]
    val filteredProducerIds = listTransactionsRequest.data.producerIdFilters.asScala.map(Long.unbox).toSet
    val filteredStates = listTransactionsRequest.data.stateFilters.asScala.toSet
    val durationFilter = listTransactionsRequest.data.durationFilter()
    val response = txnCoordinator.handleListTransactions(filteredProducerIds, filteredStates, durationFilter)

    // The response should contain only transactionalIds that the principal
    // has `Describe` permission to access.
    val transactionStateIter = response.transactionStates.iterator()
    while (transactionStateIter.hasNext) {
      val transactionState = transactionStateIter.next()
      if (!authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, transactionState.transactionalId)) {
        transactionStateIter.remove()
      }
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new ListTransactionsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleAllocateProducerIdsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    val allocateProducerIdsRequest = request.body[AllocateProducerIdsRequest]

    if (!zkSupport.controller.isActive)
      requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs =>
        allocateProducerIdsRequest.getErrorResponse(throttleTimeMs, Errors.NOT_CONTROLLER.exception))
    else
      zkSupport.controller.allocateProducerIds(allocateProducerIdsRequest.data, producerIdsResponse =>
        requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs =>
          new AllocateProducerIdsResponse(producerIdsResponse.setThrottleTimeMs(throttleTimeMs)))
      )
  }

  def handleConsumerGroupHeartbeat(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val consumerGroupHeartbeatRequest = request.body[ConsumerGroupHeartbeatRequest]

    if (!config.isNewGroupCoordinatorEnabled) {
      // The API is not supported by the "old" group coordinator (the default). If the
      // new one is not enabled, we fail directly here.
      requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, consumerGroupHeartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.consumerGroupHeartbeat(
        request.context,
        consumerGroupHeartbeatRequest.data,
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new ConsumerGroupHeartbeatResponse(response))
        }
      }
    }
  }

  def handleConsumerGroupDescribe(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val consumerGroupDescribeRequest = request.body[ConsumerGroupDescribeRequest]

    if (!config.isNewGroupCoordinatorEnabled) {
      // The API is not supported by the "old" group coordinator (the default). If the
      // new one is not enabled, we fail directly here.
      requestHelper.sendMaybeThrottle(request, request.body[ConsumerGroupDescribeRequest].getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val response = new ConsumerGroupDescribeResponseData()

      val authorizedGroups = new ArrayBuffer[String]()
      consumerGroupDescribeRequest.data.groupIds.forEach { groupId =>
        if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
          response.groups.add(new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          )
        } else {
          authorizedGroups += groupId
        }
      }

      groupCoordinator.consumerGroupDescribe(
        request.context,
        authorizedGroups.asJava
      ).handle[Unit] { (results, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, consumerGroupDescribeRequest.getErrorResponse(exception))
        } else {
          if (response.groups.isEmpty) {
            // If the response is empty, we can directly reuse the results.
            response.setGroups(results)
          } else {
            // Otherwise, we have to copy the results into the existing ones.
            response.groups.addAll(results)
          }

          requestHelper.sendMaybeThrottle(request, new ConsumerGroupDescribeResponse(response))
        }
      }
    }

  }

  def handleGetTelemetrySubscriptionsRequest(request: RequestChannel.Request): Unit = {
    val subscriptionRequest = request.body[GetTelemetrySubscriptionsRequest]

    clientMetricsManager match {
      case Some(metricsManager) =>
        try {
          requestHelper.sendMaybeThrottle(request, metricsManager.processGetTelemetrySubscriptionRequest(subscriptionRequest, request.context))
        } catch {
          case _: Exception =>
            requestHelper.sendMaybeThrottle(request, subscriptionRequest.getErrorResponse(Errors.INVALID_REQUEST.exception))
        }
      case None =>
        info("Received get telemetry client request for zookeeper based cluster")
        requestHelper.sendMaybeThrottle(request, subscriptionRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
    }
  }

  def handlePushTelemetryRequest(request: RequestChannel.Request): Unit = {
    val pushTelemetryRequest = request.body[PushTelemetryRequest]

    clientMetricsManager match {
      case Some(metricsManager) =>
        try {
          requestHelper.sendMaybeThrottle(request, metricsManager.processPushTelemetryRequest(pushTelemetryRequest, request.context))
        } catch {
          case _: Exception =>
            requestHelper.sendMaybeThrottle(request, pushTelemetryRequest.getErrorResponse(Errors.INVALID_REQUEST.exception))
        }
      case None =>
        info("Received push telemetry client request for zookeeper based cluster")
        requestHelper.sendMaybeThrottle(request, pushTelemetryRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
    }
  }

  def handleListClientMetricsResources(request: RequestChannel.Request): Unit = {
    val listClientMetricsResourcesRequest = request.body[ListClientMetricsResourcesRequest]

    if (!authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendMaybeThrottle(request, listClientMetricsResourcesRequest.getErrorResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      clientMetricsManager match {
        case Some(metricsManager) =>
          val data = new ListClientMetricsResourcesResponseData().setClientMetricsResources(
            metricsManager.listClientMetricsResources.asScala.map(
              name => new ClientMetricsResource().setName(name)).toList.asJava)
          requestHelper.sendMaybeThrottle(request, new ListClientMetricsResourcesResponse(data))
        case None =>
          // This should never happen as ZK based cluster calls should get rejected earlier itself,
          // but we should handle it gracefully.
          info("Received list client metrics resources request for zookeeper based cluster")
          requestHelper.sendMaybeThrottle(request, listClientMetricsResourcesRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      }
    }
  }

  private def updateRecordConversionStats(request: RequestChannel.Request,
                                          tp: TopicPartition,
                                          conversionStats: RecordValidationStats): Unit = {
    val conversionCount = conversionStats.numRecordsConverted
    if (conversionCount > 0) {
      request.header.apiKey match {
        case ApiKeys.PRODUCE =>
          brokerTopicStats.topicStats(tp.topic).produceMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.produceMessageConversionsRate.mark(conversionCount)
        case ApiKeys.FETCH =>
          brokerTopicStats.topicStats(tp.topic).fetchMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.fetchMessageConversionsRate.mark(conversionCount)
        case _ =>
          throw new IllegalStateException("Message conversion info is recorded only for Produce/Fetch requests")
      }
      request.messageConversionsTimeNanos = conversionStats.conversionTimeNanos
    }
    request.temporaryMemoryBytes = conversionStats.temporaryMemoryBytes
  }
}

object KafkaApis {
  // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
  // traffic doesn't exceed quota.
  // TODO: remove resolvedResponseData method when sizeOf can take a data object.
  private[server] def sizeOfThrottledPartitions(versionId: Short,
                                                unconvertedResponse: FetchResponse,
                                                quota: ReplicationQuotaManager): Int = {
    val responseData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    unconvertedResponse.data.responses().forEach(topicResponse =>
      topicResponse.partitions().forEach(partition =>
        responseData.put(new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic(), partition.partitionIndex)), partition)))
    FetchResponse.sizeOf(versionId, responseData.entrySet
      .iterator.asScala.filter(element => element.getKey.topicPartition.topic != null && quota.isThrottled(element.getKey.topicPartition)).asJava)
  }

  // visible for testing
  private[server] def shouldNeverReceive(request: RequestChannel.Request): Exception = {
    new UnsupportedVersionException(s"Should never receive when using a Raft-based metadata quorum: ${request.header.apiKey()}")
  }

  // visible for testing
  private[server] def shouldAlwaysForward(request: RequestChannel.Request): Exception = {
    new UnsupportedVersionException(s"Should always be forwarded to the Active Controller when using a Raft-based metadata quorum: ${request.header.apiKey}")
  }

  private def unsupported(text: String): Exception = {
    new UnsupportedVersionException(s"Unsupported when using a Raft-based metadata quorum: $text")
  }
}
