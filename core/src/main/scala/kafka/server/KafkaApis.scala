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

import java.lang.{Long => JLong}
import java.net.{InetAddress, UnknownHostException}
import java.nio.ByteBuffer
import java.util
import java.util.{Collections, Optional}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.api.{ApiVersion, ElectLeadersRequestOps, KAFKA_0_11_0_IV0, KAFKA_2_3_IV0}
import kafka.common.OffsetAndMetadata
import kafka.controller.{KafkaController, ReplicaAssignment}
import kafka.coordinator.group.{GroupCoordinator, JoinGroupResult, LeaveGroupResult, SyncGroupResult}
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.log.AppendOrigin
import kafka.message.ZStdCompressionCodec
import kafka.network.RequestChannel
import kafka.security.authorizer.AuthorizerUtils
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.acl.{AclBinding, AclOperation}
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.{FatalExitError, Topic}
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, isInternal}
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.message.{AddOffsetsToTxnResponseData, AlterClientQuotasResponseData, AlterConfigsResponseData, AlterPartitionReassignmentsResponseData, AlterReplicaLogDirsResponseData, ApiVersionsResponseData, CreateAclsResponseData, CreatePartitionsResponseData, CreateTopicsResponseData, DeleteAclsResponseData, DeleteGroupsResponseData, DeleteRecordsResponseData, DeleteTopicsResponseData, DescribeAclsResponseData, DescribeClientQuotasResponseData, DescribeClusterResponseData, DescribeConfigsResponseData, DescribeGroupsResponseData, DescribeLogDirsResponseData, DescribeProducersResponseData, EndTxnResponseData, ExpireDelegationTokenResponseData, FindCoordinatorResponseData, HeartbeatResponseData, InitProducerIdResponseData, JoinGroupResponseData, LeaveGroupResponseData, ListGroupsResponseData, ListOffsetsResponseData, ListPartitionReassignmentsResponseData, MetadataResponseData, OffsetCommitRequestData, OffsetCommitResponseData, OffsetDeleteResponseData, OffsetForLeaderEpochResponseData, RenewDelegationTokenResponseData, SaslAuthenticateResponseData, SaslHandshakeResponseData, StopReplicaResponseData, SyncGroupResponseData, UpdateMetadataResponseData}
import org.apache.kafka.common.message.CreateTopicsResponseData.{CreatableTopicResult, CreatableTopicResultCollection}
import org.apache.kafka.common.message.DeleteGroupsResponseData.{DeletableGroupResult, DeletableGroupResultCollection}
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.{ReassignablePartitionResponse, ReassignableTopicResponse}
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult
import org.apache.kafka.common.message.DeleteTopicsResponseData.{DeletableTopicResult, DeletableTopicResultCollection}
import org.apache.kafka.common.message.DeleteRecordsResponseData.{DeleteRecordsPartitionResult, DeleteRecordsTopicResult}
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult, OffsetForLeaderTopicResultCollection}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName, Send}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
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
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import org.apache.kafka.common.message.AlterConfigsResponseData.AlterConfigsResourceResponse
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.server.authorizer._

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq, Set, immutable, mutable}
import scala.util.{Failure, Success, Try}
import kafka.coordinator.group.GroupOverview

import scala.annotation.nowarn

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val adminManager: ZkAdminManager,
                val groupCoordinator: GroupCoordinator,
                val txnCoordinator: TransactionCoordinator,
                val controller: KafkaController,
                val forwardingManager: Option[ForwardingManager],
                val zkClient: KafkaZkClient,
                val brokerId: Int,
                val config: KafkaConfig,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizer: Option[Authorizer],
                val quotas: QuotaManagers,
                val fetchManager: FetchManager,
                brokerTopicStats: BrokerTopicStats,
                val clusterId: String,
                time: Time,
                val tokenManager: DelegationTokenManager,
                val brokerFeatures: BrokerFeatures,
                val finalizedFeatureCache: FinalizedFeatureCache) extends ApiRequestHandler with Logging {

  type FetchResponseStats = Map[TopicPartition, RecordConversionStats]
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  val adminZkClient = new AdminZkClient(zkClient)
  private val alterAclsPurgatory = new DelayedFuturePurgatory(purgatoryName = "AlterAcls", brokerId = config.brokerId)

  val authHelper = new AuthHelper(authorizer)
  val requestHelper = new RequestHandlerHelper(requestChannel, quotas, time, logIdent)

  def close(): Unit = {
    alterAclsPurgatory.shutdown()
    info("Shutdown complete.")
  }

  private def isForwardingEnabled(request: RequestChannel.Request): Boolean = {
    forwardingManager.isDefined && request.context.principalSerde.isPresent
  }

  private def maybeForwardToController(
    request: RequestChannel.Request,
    handler: RequestChannel.Request => Unit
  ): Unit = {
    def responseCallback(responseOpt: Option[AbstractResponse]): Unit = {
      responseOpt match {
        case Some(response) => requestHelper.sendForwardedResponse(request, response)
        case None =>
          info(s"The client connection will be closed due to controller responded " +
            s"unsupported version exception during $request forwarding. " +
            s"This could happen when the controller changed after the connection was established.")
          requestHelper.closeConnection(request, Collections.emptyMap())
      }
    }

    forwardingManager match {
      case Some(mgr) if !request.isForwarded && !controller.isActive =>
        mgr.forwardRequest(request, responseCallback)

      case _ =>
        handler(request)
    }
  }

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  override def handle(request: RequestChannel.Request): Unit = {
    try {
      trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
        s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")

      request.header.apiKey match {
        case ApiKeys.PRODUCE => handleProduceRequest(request)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
        case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
        case ApiKeys.UPDATE_METADATA => handleUpdateMetadataRequest(request)
        case ApiKeys.CONTROLLED_SHUTDOWN => handleControlledShutdownRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.CREATE_TOPICS => maybeForwardToController(request, handleCreateTopicsRequest)
        case ApiKeys.DELETE_TOPICS => maybeForwardToController(request, handleDeleteTopicsRequest)
        case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
        case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request)
        case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
        case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionToTxnRequest(request)
        case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request)
        case ApiKeys.END_TXN => handleEndTxnRequest(request)
        case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request)
        case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request)
        case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
        case ApiKeys.CREATE_ACLS => maybeForwardToController(request, handleCreateAcls)
        case ApiKeys.DELETE_ACLS => maybeForwardToController(request, handleDeleteAcls)
        case ApiKeys.ALTER_CONFIGS => maybeForwardToController(request, handleAlterConfigsRequest)
        case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
        case ApiKeys.ALTER_REPLICA_LOG_DIRS => handleAlterReplicaLogDirsRequest(request)
        case ApiKeys.DESCRIBE_LOG_DIRS => handleDescribeLogDirsRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.CREATE_PARTITIONS => maybeForwardToController(request, handleCreatePartitionsRequest)
        case ApiKeys.CREATE_DELEGATION_TOKEN => maybeForwardToController(request, handleCreateTokenRequest)
        case ApiKeys.RENEW_DELEGATION_TOKEN => maybeForwardToController(request, handleRenewTokenRequest)
        case ApiKeys.EXPIRE_DELEGATION_TOKEN => maybeForwardToController(request, handleExpireTokenRequest)
        case ApiKeys.DESCRIBE_DELEGATION_TOKEN => handleDescribeTokensRequest(request)
        case ApiKeys.DELETE_GROUPS => handleDeleteGroupsRequest(request)
        case ApiKeys.ELECT_LEADERS => handleElectReplicaLeader(request)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => maybeForwardToController(request, handleIncrementalAlterConfigsRequest)
        case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => maybeForwardToController(request, handleAlterPartitionReassignmentsRequest)
        case ApiKeys.LIST_PARTITION_REASSIGNMENTS => handleListPartitionReassignmentsRequest(request)
        case ApiKeys.OFFSET_DELETE => handleOffsetDeleteRequest(request)
        case ApiKeys.DESCRIBE_CLIENT_QUOTAS => handleDescribeClientQuotasRequest(request)
        case ApiKeys.ALTER_CLIENT_QUOTAS => maybeForwardToController(request, handleAlterClientQuotasRequest)
        case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS => handleDescribeUserScramCredentialsRequest(request)
        case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => maybeForwardToController(request, handleAlterUserScramCredentialsRequest)
        case ApiKeys.ALTER_ISR => handleAlterIsrRequest(request)
        case ApiKeys.UPDATE_FEATURES => maybeForwardToController(request, handleUpdateFeatures)
        case ApiKeys.ENVELOPE => handleEnvelope(request)
        case ApiKeys.DESCRIBE_CLUSTER => handleDescribeCluster(request)
        case ApiKeys.DESCRIBE_PRODUCERS => handleDescribeProducersRequest(request)

        // Until we are ready to integrate the Raft layer, these APIs are treated as
        // unexpected and we just close the connection.
        case ApiKeys.VOTE => requestHelper.closeConnection(request, util.Collections.emptyMap())
        case ApiKeys.BEGIN_QUORUM_EPOCH => requestHelper.closeConnection(request, util.Collections.emptyMap())
        case ApiKeys.END_QUORUM_EPOCH => requestHelper.closeConnection(request, util.Collections.emptyMap())
        case ApiKeys.DESCRIBE_QUORUM => requestHelper.closeConnection(request, util.Collections.emptyMap())
        case ApiKeys.FETCH_SNAPSHOT => requestHelper.closeConnection(request, util.Collections.emptyMap())

      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => requestHelper.handleError(request, e)
    } finally {
      // try to complete delayed action. In order to avoid conflicting locking, the actions to complete delayed requests
      // are kept in a queue. We add the logic to check the ReplicaManager queue at the end of KafkaApis.handle() and the
      // expiration thread for certain delayed operations (e.g. DelayedJoin)
      replicaManager.tryCompleteActions()
      // The local completion time may be set while processing the request. Only record it if it's unset.
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request): Unit = {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val correlationId = request.header.correlationId
    val leaderAndIsrRequest = request.body[LeaderAndIsrRequest]

    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (isBrokerEpochStale(leaderAndIsrRequest.brokerEpoch)) {
      // When the broker restarts very quickly, it is possible for this broker to receive request intended
      // for its previous generation so the broker should skip the stale request.
      info("Received LeaderAndIsr request with broker epoch " +
        s"${leaderAndIsrRequest.brokerEpoch} smaller than the current broker epoch ${controller.brokerEpoch}")
      requestHelper.sendResponseExemptThrottle(request, leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_BROKER_EPOCH.exception))
    } else {
      val response = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest,
        RequestHandlerHelper.onLeadershipChange(groupCoordinator, txnCoordinator, _, _))
      requestHelper.sendResponseExemptThrottle(request, response)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request): Unit = {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.body[StopReplicaRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (isBrokerEpochStale(stopReplicaRequest.brokerEpoch)) {
      // When the broker restarts very quickly, it is possible for this broker to receive request intended
      // for its previous generation so the broker should skip the stale request.
      info("Received StopReplica request with broker epoch " +
        s"${stopReplicaRequest.brokerEpoch} smaller than the current broker epoch ${controller.brokerEpoch}")
      requestHelper.sendResponseExemptThrottle(request, new StopReplicaResponse(
        new StopReplicaResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code)))
    } else {
      val partitionStates = stopReplicaRequest.partitionStates().asScala
      val (result, error) = replicaManager.stopReplicas(
        request.context.correlationId,
        stopReplicaRequest.controllerId,
        stopReplicaRequest.controllerEpoch,
        stopReplicaRequest.brokerEpoch,
        partitionStates)
      // Clear the coordinator caches in case we were the leader. In the case of a reassignment, we
      // cannot rely on the LeaderAndIsr API for this since it is only sent to active replicas.
      result.forKeyValue { (topicPartition, error) =>
        if (error == Errors.NONE) {
          if (topicPartition.topic == GROUP_METADATA_TOPIC_NAME
              && partitionStates(topicPartition).deletePartition) {
            groupCoordinator.onResignation(topicPartition.partition)
          } else if (topicPartition.topic == TRANSACTION_STATE_TOPIC_NAME
                     && partitionStates(topicPartition).deletePartition) {
            val partitionState = partitionStates(topicPartition)
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

  def handleUpdateMetadataRequest(request: RequestChannel.Request): Unit = {
    val correlationId = request.header.correlationId
    val updateMetadataRequest = request.body[UpdateMetadataRequest]

    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (isBrokerEpochStale(updateMetadataRequest.brokerEpoch)) {
      // When the broker restarts very quickly, it is possible for this broker to receive request intended
      // for its previous generation so the broker should skip the stale request.
      info("Received update metadata request with broker epoch " +
        s"${updateMetadataRequest.brokerEpoch} smaller than the current broker epoch ${controller.brokerEpoch}")
      requestHelper.sendResponseExemptThrottle(request,
        new UpdateMetadataResponse(new UpdateMetadataResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code)))
    } else {
      val deletedPartitions = replicaManager.maybeUpdateMetadataCache(correlationId, updateMetadataRequest)
      if (deletedPartitions.nonEmpty)
        groupCoordinator.handleDeletedPartitions(deletedPartitions)

      if (adminManager.hasDelayedTopicOperations) {
        updateMetadataRequest.partitionStates.forEach { partitionState =>
          adminManager.tryCompleteDelayedTopicOperations(partitionState.topicName)
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
    controller.controlledShutdown(controlledShutdownRequest.data.brokerId, controlledShutdownRequest.data.brokerEpoch, controlledShutdownCallback)
  }

  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(request: RequestChannel.Request): Unit = {
    val header = request.header
    val offsetCommitRequest = request.body[OffsetCommitRequest]

    val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
    val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
    // the callback for sending an offset commit response
    def sendResponseCallback(commitStatus: Map[TopicPartition, Errors]): Unit = {
      val combinedCommitStatus = commitStatus ++ unauthorizedTopicErrors ++ nonExistingTopicErrors
      if (isDebugEnabled)
        combinedCommitStatus.forKeyValue { (topicPartition, error) =>
          if (error != Errors.NONE) {
            debug(s"Offset commit request with correlation id ${header.correlationId} from client ${header.clientId} " +
              s"on partition $topicPartition failed due to ${error.exceptionName}")
          }
        }
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new OffsetCommitResponse(requestThrottleMs, combinedCommitStatus.asJava))
    }

    // reject the request if not authorized to the group
    if (!authHelper.authorize(request.context, READ, GROUP, offsetCommitRequest.data.groupId)) {
      val error = Errors.GROUP_AUTHORIZATION_FAILED
      val responseTopicList = OffsetCommitRequest.getErrorResponseTopics(
        offsetCommitRequest.data.topics,
        error)

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => new OffsetCommitResponse(
        new OffsetCommitResponseData()
            .setTopics(responseTopicList)
            .setThrottleTimeMs(requestThrottleMs)
      ))
    } else if (offsetCommitRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      val errorMap = new mutable.HashMap[TopicPartition, Errors]
      for (topicData <- offsetCommitRequest.data.topics.asScala) {
        for (partitionData <- topicData.partitions.asScala) {
          val topicPartition = new TopicPartition(topicData.name, partitionData.partitionIndex)
          errorMap += topicPartition -> Errors.UNSUPPORTED_VERSION
        }
      }
      sendResponseCallback(errorMap.toMap)
    } else {
      val authorizedTopicRequestInfoBldr = immutable.Map.newBuilder[TopicPartition, OffsetCommitRequestData.OffsetCommitRequestPartition]

      val topics = offsetCommitRequest.data.topics.asScala
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, topics)(_.name)
      for (topicData <- topics) {
        for (partitionData <- topicData.partitions.asScala) {
          val topicPartition = new TopicPartition(topicData.name, partitionData.partitionIndex)
          if (!authorizedTopics.contains(topicData.name))
            unauthorizedTopicErrors += (topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED)
          else if (!metadataCache.contains(topicPartition))
            nonExistingTopicErrors += (topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION)
          else
            authorizedTopicRequestInfoBldr += (topicPartition -> partitionData)
        }
      }

      val authorizedTopicRequestInfo = authorizedTopicRequestInfoBldr.result()

      if (authorizedTopicRequestInfo.isEmpty)
        sendResponseCallback(Map.empty)
      else if (header.apiVersion == 0) {
        // for version 0 always store offsets to ZK
        val responseInfo = authorizedTopicRequestInfo.map {
          case (topicPartition, partitionData) =>
            try {
              if (partitionData.committedMetadata() != null
                && partitionData.committedMetadata().length > config.offsetMetadataMaxSize)
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE)
              else {
                zkClient.setOrCreateConsumerOffset(
                  offsetCommitRequest.data.groupId,
                  topicPartition,
                  partitionData.committedOffset)
                (topicPartition, Errors.NONE)
              }
            } catch {
              case e: Throwable => (topicPartition, Errors.forException(e))
            }
        }
        sendResponseCallback(responseInfo)
      } else {
        // for version 1 and beyond store offsets in offset manager

        // "default" expiration timestamp is now + retention (and retention may be overridden if v2)
        // expire timestamp is computed differently for v1 and v2.
        //   - If v1 and no explicit commit timestamp is provided we treat it the same as v5.
        //   - If v1 and explicit retention time is provided we calculate expiration timestamp based on that
        //   - If v2/v3/v4 (no explicit commit timestamp) we treat it the same as v5.
        //   - For v5 and beyond there is no per partition expiration timestamp, so this field is no longer in effect
        val currentTimestamp = time.milliseconds
        val partitionData = authorizedTopicRequestInfo.map { case (k, partitionData) =>
          val metadata = if (partitionData.committedMetadata == null)
            OffsetAndMetadata.NoMetadata
          else
            partitionData.committedMetadata

          val leaderEpochOpt = if (partitionData.committedLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH)
            Optional.empty[Integer]
          else
            Optional.of[Integer](partitionData.committedLeaderEpoch)

          k -> new OffsetAndMetadata(
            offset = partitionData.committedOffset,
            leaderEpoch = leaderEpochOpt,
            metadata = metadata,
            commitTimestamp = partitionData.commitTimestamp match {
              case OffsetCommitRequest.DEFAULT_TIMESTAMP => currentTimestamp
              case customTimestamp => customTimestamp
            },
            expireTimestamp = offsetCommitRequest.data.retentionTimeMs match {
              case OffsetCommitRequest.DEFAULT_RETENTION_TIME => None
              case retentionTime => Some(currentTimestamp + retentionTime)
            }
          )
        }

        // call coordinator to handle commit offset
        groupCoordinator.handleCommitOffsets(
          offsetCommitRequest.data.groupId,
          offsetCommitRequest.data.memberId,
          Option(offsetCommitRequest.data.groupInstanceId),
          offsetCommitRequest.data.generationId,
          partitionData,
          sendResponseCallback)
      }
    }
  }

  /**
   * Handle a produce request
   */
  def handleProduceRequest(request: RequestChannel.Request): Unit = {
    val produceRequest = request.body[ProduceRequest]
    val requestSize = request.sizeInBytes

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

      mergedResponseStatus.forKeyValue { (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            status.error.exceptionName))
        }
      }

      // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the quotas
      // have been violated. If both quotas have been violated, use the max throttle time between the two quotas. Note
      // that the request quota is not enforced if acks == 0.
      val timeMs = time.milliseconds()
      val bandwidthThrottleTimeMs = quotas.produce.maybeRecordAndGetThrottleTimeMs(request, requestSize, timeMs)
      val requestThrottleTimeMs =
        if (produceRequest.acks == 0) 0
        else quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
      val maxThrottleTimeMs = Math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
      if (maxThrottleTimeMs > 0) {
        request.apiThrottleTimeMs = maxThrottleTimeMs
        if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
          quotas.produce.throttle(request, bandwidthThrottleTimeMs, requestChannel.sendResponse)
        } else {
          quotas.request.throttle(request, requestThrottleTimeMs, requestChannel.sendResponse)
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
          requestHelper.closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
        } else {
          // Note that although request throttling is exempt for acks == 0, the channel may be throttled due to
          // bandwidth quota violation.
          requestHelper.sendNoOpResponseExemptThrottle(request)
        }
      } else {
        requestHelper.sendResponse(request, Some(new ProduceResponse(mergedResponseStatus.asJava, maxThrottleTimeMs)), None)
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
      val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId

      // call the replica manager to append messages to the replicas
      replicaManager.appendRecords(
        timeout = produceRequest.timeout.toLong,
        requiredAcks = produceRequest.acks,
        internalTopicsAllowed = internalTopicsAllowed,
        origin = AppendOrigin.Client,
        entriesPerPartition = authorizedRequestInfo,
        responseCallback = sendResponseCallback,
        recordConversionStatsCallback = processingStatsCallback)

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
    val fetchContext = fetchManager.newContext(
      fetchRequest.metadata,
      fetchRequest.fetchData,
      fetchRequest.toForget,
      fetchRequest.isFromFollower)

    val clientMetadata: Option[ClientMetadata] = if (versionId >= 11) {
      // Fetch API version 11 added preferred replica logic
      Some(new DefaultClientMetadata(
        fetchRequest.rackId,
        clientId,
        request.context.clientAddress,
        request.context.principal,
        request.context.listenerName.value))
    } else {
      None
    }

    def errorResponse[T >: MemoryRecords <: BaseRecords](error: Errors): FetchResponse.PartitionData[T] = {
      new FetchResponse.PartitionData[T](error, FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
        FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
    }

    val erroneous = mutable.ArrayBuffer[(TopicPartition, FetchResponse.PartitionData[Records])]()
    val interesting = mutable.ArrayBuffer[(TopicPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
        fetchContext.foreachPartition { (topicPartition, data) =>
          if (!metadataCache.contains(topicPartition))
            erroneous += topicPartition -> errorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
          else
            interesting += (topicPartition -> data)
        }
      } else {
        fetchContext.foreachPartition { (part, _) =>
          erroneous += part -> errorResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
        }
      }
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      val partitionDatas = new mutable.ArrayBuffer[(TopicPartition, FetchRequest.PartitionData)]
      fetchContext.foreachPartition { (topicPartition, partitionData) =>
        partitionDatas += topicPartition -> partitionData
      }
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, partitionDatas)(_._1.topic)
      partitionDatas.foreach { case (topicPartition, data) =>
        if (!authorizedTopics.contains(topicPartition.topic))
          erroneous += topicPartition -> errorResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicPartition))
          erroneous += topicPartition -> errorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          interesting += (topicPartition -> data)
      }
    }

    def maybeDownConvertStorageError(error: Errors, version: Short): Errors = {
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

    def maybeConvertFetchedData(tp: TopicPartition,
                                partitionData: FetchResponse.PartitionData[Records]): FetchResponse.PartitionData[BaseRecords] = {
      val logConfig = replicaManager.getLogConfig(tp)

      if (logConfig.exists(_.compressionType == ZStdCompressionCodec.name) && versionId < 10) {
        trace(s"Fetching messages is disabled for ZStandard compressed partition $tp. Sending unsupported version response to $clientId.")
        errorResponse(Errors.UNSUPPORTED_COMPRESSION_TYPE)
      } else {
        // Down-conversion of the fetched records is needed when the stored magic version is
        // greater than that supported by the client (as indicated by the fetch request version). If the
        // configured magic version for the topic is less than or equal to that supported by the version of the
        // fetch request, we skip the iteration through the records in order to check the magic version since we
        // know it must be supported. However, if the magic version is changed from a higher version back to a
        // lower version, this check will no longer be valid and we will fail to down-convert the messages
        // which were written in the new format prior to the version downgrade.
        val unconvertedRecords = partitionData.records
        val downConvertMagic =
          logConfig.map(_.messageFormatVersion.recordVersion.value).flatMap { magic =>
            if (magic > RecordBatch.MAGIC_VALUE_V0 && versionId <= 1 && !unconvertedRecords.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V0))
              Some(RecordBatch.MAGIC_VALUE_V0)
            else if (magic > RecordBatch.MAGIC_VALUE_V1 && versionId <= 3 && !unconvertedRecords.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V1))
              Some(RecordBatch.MAGIC_VALUE_V1)
            else
              None
          }

        downConvertMagic match {
          case Some(magic) =>
            // For fetch requests from clients, check if down-conversion is disabled for the particular partition
            if (!fetchRequest.isFromFollower && !logConfig.forall(_.messageDownConversionEnable)) {
              trace(s"Conversion to message format ${downConvertMagic.get} is disabled for partition $tp. Sending unsupported version response to $clientId.")
              errorResponse(Errors.UNSUPPORTED_VERSION)
            } else {
              try {
                trace(s"Down converting records from partition $tp to message format version $magic for fetch request from $clientId")
                // Because down-conversion is extremely memory intensive, we want to try and delay the down-conversion as much
                // as possible. With KIP-283, we have the ability to lazily down-convert in a chunked manner. The lazy, chunked
                // down-conversion always guarantees that at least one batch of messages is down-converted and sent out to the
                // client.
                val error = maybeDownConvertStorageError(partitionData.error, versionId)
                new FetchResponse.PartitionData[BaseRecords](error, partitionData.highWatermark,
                  partitionData.lastStableOffset, partitionData.logStartOffset,
                  partitionData.preferredReadReplica, partitionData.abortedTransactions,
                  new LazyDownConversionRecords(tp, unconvertedRecords, magic, fetchContext.getFetchOffset(tp).get, time))
              } catch {
                case e: UnsupportedCompressionTypeException =>
                  trace("Received unsupported compression type error during down-conversion", e)
                  errorResponse(Errors.UNSUPPORTED_COMPRESSION_TYPE)
              }
            }
          case None =>
            val error = maybeDownConvertStorageError(partitionData.error, versionId)
            new FetchResponse.PartitionData[BaseRecords](error,
              partitionData.highWatermark,
              partitionData.lastStableOffset,
              partitionData.logStartOffset,
              partitionData.preferredReadReplica,
              partitionData.abortedTransactions,
              partitionData.divergingEpoch,
              unconvertedRecords)
        }
      }
    }

    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      val partitions = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
      val reassigningPartitions = mutable.Set[TopicPartition]()
      responsePartitionData.foreach { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        if (data.isReassignmentFetch)
          reassigningPartitions.add(tp)
        val error = maybeDownConvertStorageError(data.error, versionId)
        partitions.put(tp, new FetchResponse.PartitionData(
          error,
          data.highWatermark,
          lastStableOffset,
          data.logStartOffset,
          data.preferredReadReplica.map(int2Integer).asJava,
          abortedTransactions,
          data.divergingEpoch.asJava,
          data.records))
      }
      erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

      var unconvertedFetchResponse: FetchResponse[Records] = null

      def createResponse(throttleTimeMs: Int): FetchResponse[BaseRecords] = {
        // Down-convert messages for each partition if required
        val convertedData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[BaseRecords]]
        unconvertedFetchResponse.responseData.forEach { (tp, unconvertedPartitionData) =>
          if (unconvertedPartitionData.error != Errors.NONE)
            debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
              s"on partition $tp failed due to ${unconvertedPartitionData.error.exceptionName}")
          convertedData.put(tp, maybeConvertFetchedData(tp, unconvertedPartitionData))
        }

        // Prepare fetch response from converted data
        val response = new FetchResponse(unconvertedFetchResponse.error, convertedData, throttleTimeMs,
          unconvertedFetchResponse.sessionId)
        // record the bytes out metrics only when the response is being sent
        response.responseData.forEach { (tp, data) =>
          brokerTopicStats.updateBytesOut(tp.topic, fetchRequest.isFromFollower, reassigningPartitions.contains(tp), data.records.sizeInBytes)
        }
        response
      }

      def updateConversionStats(send: Send): Unit = {
        send match {
          case send: MultiRecordsSend if send.recordConversionStats != null =>
            send.recordConversionStats.asScala.toMap.foreach {
              case (tp, stats) => updateRecordConversionStats(request, tp, stats)
            }
          case _ =>
        }
      }

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
        val responseSize = KafkaApis.sizeOfThrottledPartitions(versionId, unconvertedFetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        trace(s"Sending Fetch response with partitions.size=${unconvertedFetchResponse.responseData.size}, " +
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
            quotas.fetch.throttle(request, bandwidthThrottleTimeMs, requestChannel.sendResponse)
          } else {
            quotas.request.throttle(request, requestThrottleTimeMs, requestChannel.sendResponse)
          }
          // If throttling is required, return an empty response.
          unconvertedFetchResponse = fetchContext.getThrottledResponse(maxThrottleTimeMs)
        } else {
          // Get the actual response. This will update the fetch context.
          unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
          trace(s"Sending Fetch response with partitions.size=$responseSize, metadata=${unconvertedFetchResponse.sessionId}")
        }

        // Send the response immediately.
        requestHelper.sendResponse(request, Some(createResponse(maxThrottleTimeMs)), Some(updateConversionStats))
      }
    }

    // for fetch from consumer, cap fetchMaxBytes to the maximum bytes that could be fetched without being throttled given
    // no bytes were recorded in the recent quota window
    // trying to fetch more bytes would result in a guaranteed throttling potentially blocking consumer progress
    val maxQuotaWindowBytes = if (fetchRequest.isFromFollower)
      Int.MaxValue
    else
      quotas.fetch.getMaxValueInQuotaWindow(request.session, clientId).toInt

    val fetchMaxBytes = Math.min(Math.min(fetchRequest.maxBytes, config.fetchMaxBytes), maxQuotaWindowBytes)
    val fetchMinBytes = Math.min(fetchRequest.minBytes, fetchMaxBytes)
    if (interesting.isEmpty)
      processResponseCallback(Seq.empty)
    else {
      // call the replica manager to fetch messages from the local replica
      replicaManager.fetchMessages(
        fetchRequest.maxWait.toLong,
        fetchRequest.replicaId,
        fetchMinBytes,
        fetchMaxBytes,
        versionId <= 2,
        interesting,
        replicationQuota(fetchRequest),
        processResponseCallback,
        fetchRequest.isolationLevel,
        clientMetadata)
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

  private def createTopic(topic: String,
                          numPartitions: Int,
                          replicationFactor: Int,
                          properties: util.Properties = new util.Properties()): MetadataResponseTopic = {
    try {
      adminZkClient.createTopic(topic, numPartitions, replicationFactor, properties, RackAwareMode.Safe, config.usesTopicId)
      info("Auto creation of topic %s with %d partitions and replication factor %d is successful"
        .format(topic, numPartitions, replicationFactor))
      metadataResponseTopic(Errors.LEADER_NOT_AVAILABLE, topic, isInternal(topic), util.Collections.emptyList())
    } catch {
      case _: TopicExistsException => // let it go, possibly another broker created this topic
        metadataResponseTopic(Errors.LEADER_NOT_AVAILABLE, topic, isInternal(topic), util.Collections.emptyList())
      case ex: Throwable  => // Catch all to prevent unhandled errors
        metadataResponseTopic(Errors.forException(ex), topic, isInternal(topic), util.Collections.emptyList())
    }
  }

  private def metadataResponseTopic(error: Errors, topic: String, isInternal: Boolean,
                                    partitionData: util.List[MetadataResponsePartition]): MetadataResponseTopic = {
    new MetadataResponseTopic()
      .setErrorCode(error.code)
      .setName(topic)
      .setIsInternal(isInternal)
      .setPartitions(partitionData)
  }

  private def createInternalTopic(topic: String): MetadataResponseTopic = {
    if (topic == null)
      throw new IllegalArgumentException("topic must not be null")

    val aliveBrokers = metadataCache.getAliveBrokers

    topic match {
      case GROUP_METADATA_TOPIC_NAME =>
        if (aliveBrokers.size < config.offsetsTopicReplicationFactor) {
          error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
            s"'${config.offsetsTopicReplicationFactor}' for the offsets topic (configured via " +
            s"'${KafkaConfig.OffsetsTopicReplicationFactorProp}'). This error can be ignored if the cluster is starting up " +
            s"and not all brokers are up yet.")
          metadataResponseTopic(Errors.COORDINATOR_NOT_AVAILABLE, topic, true, util.Collections.emptyList())
        } else {
          createTopic(topic, config.offsetsTopicPartitions, config.offsetsTopicReplicationFactor.toInt,
            groupCoordinator.offsetsTopicConfigs)
        }
      case TRANSACTION_STATE_TOPIC_NAME =>
        if (aliveBrokers.size < config.transactionTopicReplicationFactor) {
          error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
            s"'${config.transactionTopicReplicationFactor}' for the transactions state topic (configured via " +
            s"'${KafkaConfig.TransactionsTopicReplicationFactorProp}'). This error can be ignored if the cluster is starting up " +
            s"and not all brokers are up yet.")
          metadataResponseTopic(Errors.COORDINATOR_NOT_AVAILABLE, topic, true, util.Collections.emptyList())
        } else {
          createTopic(topic, config.transactionTopicPartitions, config.transactionTopicReplicationFactor.toInt,
            txnCoordinator.transactionTopicConfigs)
        }
      case _ => throw new IllegalArgumentException(s"Unexpected internal topic name: $topic")
    }
  }

  private def getOrCreateInternalTopic(topic: String, listenerName: ListenerName): MetadataResponseData.MetadataResponseTopic = {
    val topicMetadata = metadataCache.getTopicMetadata(Set(topic), listenerName)
    topicMetadata.headOption.getOrElse(createInternalTopic(topic))
  }

  private def getTopicMetadata(allowAutoTopicCreation: Boolean, isFetchAllMetadata: Boolean,
                               topics: Set[String], listenerName: ListenerName,
                               errorUnavailableEndpoints: Boolean,
                               errorUnavailableListeners: Boolean): Seq[MetadataResponseTopic] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, listenerName,
        errorUnavailableEndpoints, errorUnavailableListeners)

    if (topics.isEmpty || topicResponses.size == topics.size) {
      topicResponses
    } else {
      val nonExistentTopics = topics.diff(topicResponses.map(_.name).toSet)
      val responsesForNonExistentTopics = nonExistentTopics.flatMap { topic =>
        if (isInternal(topic)) {
          val topicMetadata = createInternalTopic(topic)
          Some(
            if (topicMetadata.errorCode == Errors.COORDINATOR_NOT_AVAILABLE.code)
              metadataResponseTopic(Errors.INVALID_REPLICATION_FACTOR, topic, true, util.Collections.emptyList())
            else
              topicMetadata
          )
        } else if (isFetchAllMetadata) {
          // A metadata request for all topics should never result in topic auto creation, but a topic may be deleted
          // in between the creation of the topics parameter and topicResponses, so make sure to return None for this case.
          None
        } else if (allowAutoTopicCreation && config.autoCreateTopicsEnable) {
          Some(createTopic(topic, config.numPartitions, config.defaultReplicationFactor))
        } else {
          Some(metadataResponseTopic(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, false, util.Collections.emptyList()))
        }
      }

      topicResponses ++ responsesForNonExistentTopics
    }
  }

  /**
   * Handle a topic metadata request
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request): Unit = {
    val metadataRequest = request.body[MetadataRequest]
    val requestVersion = request.header.apiVersion

    val topics = if (metadataRequest.isAllTopics)
      metadataCache.getAllTopics()
    else
      metadataRequest.topics.asScala.toSet

    val authorizedForDescribeTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
      topics, logIfDenied = !metadataRequest.isAllTopics)(identity)
    var (authorizedTopics, unauthorizedForDescribeTopics) = topics.partition(authorizedForDescribeTopics.contains)
    var unauthorizedForCreateTopics = Set[String]()

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = metadataCache.getNonExistingTopics(authorizedTopics)
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
      metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, isInternal(topic), util.Collections.emptyList()))


    // do not disclose the existence of topics unauthorized for Describe, so we've not even checked if they exist or not
    val unauthorizedForDescribeTopicMetadata =
      // In case of all topics, don't include topics unauthorized for Describe
      if ((requestVersion == 0 && (metadataRequest.topics == null || metadataRequest.topics.isEmpty)) || metadataRequest.isAllTopics)
        Set.empty[MetadataResponseTopic]
      else
        unauthorizedForDescribeTopics.map(topic =>
          metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, false, util.Collections.emptyList()))

    // In version 0, we returned an error when brokers with replicas were unavailable,
    // while in higher versions we simply don't include the broker in the returned broker list
    val errorUnavailableEndpoints = requestVersion == 0
    // In versions 5 and below, we returned LEADER_NOT_AVAILABLE if a matching listener was not found on the leader.
    // From version 6 onwards, we return LISTENER_NOT_FOUND to enable diagnosis of configuration errors.
    val errorUnavailableListeners = requestVersion >= 6
    val topicMetadata =
      if (authorizedTopics.isEmpty)
        Seq.empty[MetadataResponseTopic]
      else {
        getTopicMetadata(
          metadataRequest.allowAutoTopicCreation,
          metadataRequest.isAllTopics,
          authorizedTopics,
          request.context.listenerName,
          errorUnavailableEndpoints,
          errorUnavailableListeners
        )
      }

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
        topicMetadata.foreach { topicData =>
          topicData.setTopicAuthorizedOperations(authHelper.authorizedOperations(request, new Resource(ResourceType.TOPIC, topicData.name)))
        }
      }
    }

    val completeTopicMetadata = topicMetadata ++ unauthorizedForCreateTopicMetadata ++ unauthorizedForDescribeTopicMetadata

    val brokers = metadataCache.getAliveBrokers

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
       MetadataResponse.prepareResponse(
         requestVersion,
         requestThrottleMs,
         brokers.flatMap(_.getNode(request.context.listenerName)).asJava,
         clusterId,
         metadataCache.getControllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
         completeTopicMetadata.asJava,
         clusterAuthorizedOperations
      ))
  }

  /**
   * Handle an offset fetch request
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request): Unit = {
    val header = request.header
    val offsetFetchRequest = request.body[OffsetFetchRequest]

    def partitionByAuthorized(seq: Seq[TopicPartition]): (Seq[TopicPartition], Seq[TopicPartition]) =
      authHelper.partitionSeqByAuthorized(request.context, DESCRIBE, TOPIC, seq)(_.topic)

    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val offsetFetchResponse =
        // reject the request if not authorized to the group
        if (!authHelper.authorize(request.context, DESCRIBE, GROUP, offsetFetchRequest.groupId))
          offsetFetchRequest.getErrorResponse(requestThrottleMs, Errors.GROUP_AUTHORIZATION_FAILED)
        else {
          if (header.apiVersion == 0) {
            val (authorizedPartitions, unauthorizedPartitions) = partitionByAuthorized(
              offsetFetchRequest.partitions.asScala)

            // version 0 reads offsets from ZK
            val authorizedPartitionData = authorizedPartitions.map { topicPartition =>
              try {
                if (!metadataCache.contains(topicPartition))
                  (topicPartition, OffsetFetchResponse.UNKNOWN_PARTITION)
                else {
                  val payloadOpt = zkClient.getConsumerOffset(offsetFetchRequest.groupId, topicPartition)
                  payloadOpt match {
                    case Some(payload) =>
                      (topicPartition, new OffsetFetchResponse.PartitionData(payload.toLong,
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
          } else {
            // versions 1 and above read offsets from Kafka
            if (offsetFetchRequest.isAllPartitions) {
              val (error, allPartitionData) = groupCoordinator.handleFetchOffsets(offsetFetchRequest.groupId, offsetFetchRequest.requireStable)
              if (error != Errors.NONE)
                offsetFetchRequest.getErrorResponse(requestThrottleMs, error)
              else {
                // clients are not allowed to see offsets for topics that are not authorized for Describe
                val (authorizedPartitionData, _) = authHelper.partitionMapByAuthorized(request.context,
                  DESCRIBE, TOPIC, allPartitionData)(_.topic)
                new OffsetFetchResponse(requestThrottleMs, Errors.NONE, authorizedPartitionData.asJava)
              }
            } else {
              val (authorizedPartitions, unauthorizedPartitions) = partitionByAuthorized(
                offsetFetchRequest.partitions.asScala)
              val (error, authorizedPartitionData) = groupCoordinator.handleFetchOffsets(offsetFetchRequest.groupId,
                offsetFetchRequest.requireStable, Some(authorizedPartitions))
              if (error != Errors.NONE)
                offsetFetchRequest.getErrorResponse(requestThrottleMs, error)
              else {
                val unauthorizedPartitionData = unauthorizedPartitions.map(_ -> OffsetFetchResponse.UNAUTHORIZED_PARTITION).toMap
                new OffsetFetchResponse(requestThrottleMs, Errors.NONE, (authorizedPartitionData ++ unauthorizedPartitionData).asJava)
              }
            }
          }
        }

      trace(s"Sending offset fetch response $offsetFetchResponse for correlation id ${header.correlationId} to client ${header.clientId}.")
      offsetFetchResponse
    }

    requestHelper.sendResponseMaybeThrottle(request, createResponse)
  }

  def handleFindCoordinatorRequest(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    if (findCoordinatorRequest.data.keyType == CoordinatorType.GROUP.id &&
        !authHelper.authorize(request.context, DESCRIBE, GROUP, findCoordinatorRequest.data.key))
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.GROUP_AUTHORIZATION_FAILED.exception)
    else if (findCoordinatorRequest.data.keyType == CoordinatorType.TRANSACTION.id &&
        !authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, findCoordinatorRequest.data.key))
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
    else {
      // get metadata (and create the topic if necessary)
      val (partition, topicMetadata) = CoordinatorType.forId(findCoordinatorRequest.data.keyType) match {
        case CoordinatorType.GROUP =>
          val partition = groupCoordinator.partitionFor(findCoordinatorRequest.data.key)
          val metadata = getOrCreateInternalTopic(GROUP_METADATA_TOPIC_NAME, request.context.listenerName)
          (partition, metadata)

        case CoordinatorType.TRANSACTION =>
          val partition = txnCoordinator.partitionFor(findCoordinatorRequest.data.key)
          val metadata = getOrCreateInternalTopic(TRANSACTION_STATE_TOPIC_NAME, request.context.listenerName)
          (partition, metadata)

        case _ =>
          throw new InvalidRequestException("Unknown coordinator type in FindCoordinator request")
      }

      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        def createFindCoordinatorResponse(error: Errors, node: Node): FindCoordinatorResponse = {
          new FindCoordinatorResponse(
              new FindCoordinatorResponseData()
                .setErrorCode(error.code)
                .setErrorMessage(error.message)
                .setNodeId(node.id)
                .setHost(node.host)
                .setPort(node.port)
                .setThrottleTimeMs(requestThrottleMs))
        }
        val responseBody = if (topicMetadata.errorCode != Errors.NONE.code) {
          createFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
        } else {
          val coordinatorEndpoint = topicMetadata.partitions.asScala
            .find(_.partitionIndex == partition)
            .filter(_.leaderId != MetadataResponse.NO_LEADER_ID)
            .flatMap(metadata => metadataCache.getAliveBroker(metadata.leaderId))
            .flatMap(_.getNode(request.context.listenerName))
            .filterNot(_.isEmpty)

          coordinatorEndpoint match {
            case Some(endpoint) =>
              createFindCoordinatorResponse(Errors.NONE, endpoint)
            case _ =>
              createFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
          }
        }
        trace("Sending FindCoordinator response %s for correlation id %d to client %s."
          .format(responseBody, request.header.correlationId, request.header.clientId))
        responseBody
      }
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }
  }

  def handleDescribeGroupRequest(request: RequestChannel.Request): Unit = {

    def sendResponseCallback(describeGroupsResponseData: DescribeGroupsResponseData): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        describeGroupsResponseData.setThrottleTimeMs(requestThrottleMs)
        new DescribeGroupsResponse(describeGroupsResponseData)
      }
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    val describeRequest = request.body[DescribeGroupsRequest]
    val describeGroupsResponseData = new DescribeGroupsResponseData()

    describeRequest.data.groups.forEach { groupId =>
      if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
        describeGroupsResponseData.groups.add(DescribeGroupsResponse.forError(groupId, Errors.GROUP_AUTHORIZATION_FAILED))
      } else {
        val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
        val members = summary.members.map { member =>
          new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId(member.memberId)
            .setGroupInstanceId(member.groupInstanceId.orNull)
            .setClientId(member.clientId)
            .setClientHost(member.clientHost)
            .setMemberAssignment(member.assignment)
            .setMemberMetadata(member.metadata)
        }

        val describedGroup = new DescribeGroupsResponseData.DescribedGroup()
          .setErrorCode(error.code)
          .setGroupId(groupId)
          .setGroupState(summary.state)
          .setProtocolType(summary.protocolType)
          .setProtocolData(summary.protocol)
          .setMembers(members.asJava)

        if (request.header.apiVersion >= 3) {
          if (error == Errors.NONE && describeRequest.data.includeAuthorizedOperations) {
            describedGroup.setAuthorizedOperations(authHelper.authorizedOperations(request, new Resource(ResourceType.GROUP, groupId)))
          }
        }

        describeGroupsResponseData.groups.add(describedGroup)
      }
    }

    sendResponseCallback(describeGroupsResponseData)
  }

  def handleListGroupsRequest(request: RequestChannel.Request): Unit = {
    val listGroupsRequest = request.body[ListGroupsRequest]
    val states = if (listGroupsRequest.data.statesFilter == null)
      // Handle a null array the same as empty
      immutable.Set[String]()
    else
      listGroupsRequest.data.statesFilter.asScala.toSet

    def createResponse(throttleMs: Int, groups: List[GroupOverview], error: Errors): AbstractResponse = {
       new ListGroupsResponse(new ListGroupsResponseData()
            .setErrorCode(error.code)
            .setGroups(groups.map { group =>
                val listedGroup = new ListGroupsResponseData.ListedGroup()
                  .setGroupId(group.groupId)
                  .setProtocolType(group.protocolType)
                  .setGroupState(group.state.toString)
                listedGroup
            }.asJava)
            .setThrottleTimeMs(throttleMs)
        )
    }
    val (error, groups) = groupCoordinator.handleListGroups(states)
    if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME))
      // With describe cluster access all groups are returned. We keep this alternative for backward compatibility.
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        createResponse(requestThrottleMs, groups, error))
    else {
      val filteredGroups = groups.filter(group => authHelper.authorize(request.context, DESCRIBE, GROUP, group.groupId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        createResponse(requestThrottleMs, filteredGroups, error))
    }
  }

  def handleJoinGroupRequest(request: RequestChannel.Request): Unit = {
    val joinGroupRequest = request.body[JoinGroupRequest]

    // the callback for sending a join-group response
    def sendResponseCallback(joinResult: JoinGroupResult): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val protocolName = if (request.context.apiVersion() >= 7)
          joinResult.protocolName.orNull
        else
          joinResult.protocolName.getOrElse(GroupCoordinator.NoProtocol)

        val responseBody = new JoinGroupResponse(
          new JoinGroupResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(joinResult.error.code)
            .setGenerationId(joinResult.generationId)
            .setProtocolType(joinResult.protocolType.orNull)
            .setProtocolName(protocolName)
            .setLeader(joinResult.leaderId)
            .setMemberId(joinResult.memberId)
            .setMembers(joinResult.members.asJava)
        )

        trace("Sending join group response %s for correlation id %d to client %s."
          .format(responseBody, request.header.correlationId, request.header.clientId))
        responseBody
      }
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    if (joinGroupRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      sendResponseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNSUPPORTED_VERSION))
    } else if (!authHelper.authorize(request.context, READ, GROUP, joinGroupRequest.data.groupId)) {
      sendResponseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_AUTHORIZATION_FAILED))
    } else {
      val groupInstanceId = Option(joinGroupRequest.data.groupInstanceId)

      // Only return MEMBER_ID_REQUIRED error if joinGroupRequest version is >= 4
      // and groupInstanceId is configured to unknown.
      val requireKnownMemberId = joinGroupRequest.version >= 4 && groupInstanceId.isEmpty

      // let the coordinator handle join-group
      val protocols = joinGroupRequest.data.protocols.valuesList.asScala.map(protocol =>
        (protocol.name, protocol.metadata)).toList

      groupCoordinator.handleJoinGroup(
        joinGroupRequest.data.groupId,
        joinGroupRequest.data.memberId,
        groupInstanceId,
        requireKnownMemberId,
        request.header.clientId,
        request.context.clientAddress.toString,
        joinGroupRequest.data.rebalanceTimeoutMs,
        joinGroupRequest.data.sessionTimeoutMs,
        joinGroupRequest.data.protocolType,
        protocols,
        sendResponseCallback)
    }
  }

  def handleSyncGroupRequest(request: RequestChannel.Request): Unit = {
    val syncGroupRequest = request.body[SyncGroupRequest]

    def sendResponseCallback(syncGroupResult: SyncGroupResult): Unit = {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new SyncGroupResponse(
          new SyncGroupResponseData()
            .setErrorCode(syncGroupResult.error.code)
            .setProtocolType(syncGroupResult.protocolType.orNull)
            .setProtocolName(syncGroupResult.protocolName.orNull)
            .setAssignment(syncGroupResult.memberAssignment)
            .setThrottleTimeMs(requestThrottleMs)
        ))
    }

    if (syncGroupRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      sendResponseCallback(SyncGroupResult(Errors.UNSUPPORTED_VERSION))
    } else if (!syncGroupRequest.areMandatoryProtocolTypeAndNamePresent()) {
      // Starting from version 5, ProtocolType and ProtocolName fields are mandatory.
      sendResponseCallback(SyncGroupResult(Errors.INCONSISTENT_GROUP_PROTOCOL))
    } else if (!authHelper.authorize(request.context, READ, GROUP, syncGroupRequest.data.groupId)) {
      sendResponseCallback(SyncGroupResult(Errors.GROUP_AUTHORIZATION_FAILED))
    } else {
      val assignmentMap = immutable.Map.newBuilder[String, Array[Byte]]
      syncGroupRequest.data.assignments.forEach { assignment =>
        assignmentMap += (assignment.memberId -> assignment.assignment)
      }

      groupCoordinator.handleSyncGroup(
        syncGroupRequest.data.groupId,
        syncGroupRequest.data.generationId,
        syncGroupRequest.data.memberId,
        Option(syncGroupRequest.data.protocolType),
        Option(syncGroupRequest.data.protocolName),
        Option(syncGroupRequest.data.groupInstanceId),
        assignmentMap.result(),
        sendResponseCallback
      )
    }
  }

  def handleDeleteGroupsRequest(request: RequestChannel.Request): Unit = {
    val deleteGroupsRequest = request.body[DeleteGroupsRequest]
    val groups = deleteGroupsRequest.data.groupsNames.asScala.distinct

    val (authorizedGroups, unauthorizedGroups) = authHelper.partitionSeqByAuthorized(request.context, DELETE, GROUP,
      groups)(identity)

    val groupDeletionResult = groupCoordinator.handleDeleteGroups(authorizedGroups.toSet) ++
      unauthorizedGroups.map(_ -> Errors.GROUP_AUTHORIZATION_FAILED)

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
      val deletionCollections = new DeletableGroupResultCollection()
      groupDeletionResult.forKeyValue { (groupId, error) =>
        deletionCollections.add(new DeletableGroupResult()
          .setGroupId(groupId)
          .setErrorCode(error.code)
        )
      }

      new DeleteGroupsResponse(new DeleteGroupsResponseData()
        .setResults(deletionCollections)
        .setThrottleTimeMs(requestThrottleMs)
      )
    })
  }

  def handleHeartbeatRequest(request: RequestChannel.Request): Unit = {
    val heartbeatRequest = request.body[HeartbeatRequest]

    // the callback for sending a heartbeat response
    def sendResponseCallback(error: Errors): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val response = new HeartbeatResponse(
          new HeartbeatResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(error.code))
        trace("Sending heartbeat response %s for correlation id %d to client %s."
          .format(response, request.header.correlationId, request.header.clientId))
        response
      }
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    if (heartbeatRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      sendResponseCallback(Errors.UNSUPPORTED_VERSION)
    } else if (!authHelper.authorize(request.context, READ, GROUP, heartbeatRequest.data.groupId)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new HeartbeatResponse(
            new HeartbeatResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)))
    } else {
      // let the coordinator to handle heartbeat
      groupCoordinator.handleHeartbeat(
        heartbeatRequest.data.groupId,
        heartbeatRequest.data.memberId,
        Option(heartbeatRequest.data.groupInstanceId),
        heartbeatRequest.data.generationId,
        sendResponseCallback)
    }
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request): Unit = {
    val leaveGroupRequest = request.body[LeaveGroupRequest]

    val members = leaveGroupRequest.members.asScala.toList

    if (!authHelper.authorize(request.context, READ, GROUP, leaveGroupRequest.data.groupId)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        new LeaveGroupResponse(new LeaveGroupResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
        )
      })
    } else {
      def sendResponseCallback(leaveGroupResult : LeaveGroupResult): Unit = {
        val memberResponses = leaveGroupResult.memberResponses.map(
          leaveGroupResult =>
            new MemberResponse()
              .setErrorCode(leaveGroupResult.error.code)
              .setMemberId(leaveGroupResult.memberId)
              .setGroupInstanceId(leaveGroupResult.groupInstanceId.orNull)
        )
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          new LeaveGroupResponse(
            memberResponses.asJava,
            leaveGroupResult.topLevelError,
            requestThrottleMs,
            leaveGroupRequest.version)
        }
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      groupCoordinator.handleLeaveGroup(
        leaveGroupRequest.data.groupId,
        members,
        sendResponseCallback)
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
      if (apiVersionRequest.hasUnsupportedRequestVersion)
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      else if (!apiVersionRequest.isValid)
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
      else {
        val supportedFeatures = brokerFeatures.supportedFeatures
        val finalizedFeaturesOpt = finalizedFeatureCache.get
        val controllerApiVersions = forwardingManager.flatMap(_.controllerApiVersions)

        val apiVersionsResponse =
          finalizedFeaturesOpt match {
            case Some(finalizedFeatures) => ApiVersion.apiVersionsResponse(
              requestThrottleMs,
              config.interBrokerProtocolVersion.recordVersion.value,
              supportedFeatures,
              finalizedFeatures.features,
              finalizedFeatures.epoch,
              controllerApiVersions)
            case None => ApiVersion.apiVersionsResponse(
              requestThrottleMs,
              config.interBrokerProtocolVersion.recordVersion.value,
              supportedFeatures,
              controllerApiVersions)
          }
        if (request.context.fromPrivilegedListener) {
          apiVersionsResponse.data.apiKeys().add(
            new ApiVersionsResponseData.ApiVersion()
              .setApiKey(ApiKeys.ENVELOPE.id)
              .setMinVersion(ApiKeys.ENVELOPE.oldestVersion())
              .setMaxVersion(ApiKeys.ENVELOPE.latestVersion())
          )
        }
        apiVersionsResponse
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponseCallback)
  }

  def handleCreateTopicsRequest(request: RequestChannel.Request): Unit = {
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 6)

    def sendResponseCallback(results: CreatableTopicResultCollection): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseData = new CreateTopicsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(results)
        val responseBody = new CreateTopicsResponse(responseData)
        trace(s"Sending create topics response $responseData for correlation id " +
          s"${request.header.correlationId} to client ${request.header.clientId}.")
        responseBody
      }
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, createResponse)
    }

    val createTopicsRequest = request.body[CreateTopicsRequest]
    val results = new CreatableTopicResultCollection(createTopicsRequest.data.topics.size)
    if (!controller.isActive) {
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
      val topics = createTopicsRequest.data.topics.asScala.map(_.name)
      val authorizedTopics =
        if (hasClusterAuthorization) topics.toSet
        else authHelper.filterByAuthorized(request.context, CREATE, TOPIC, topics)(identity)
      val authorizedForDescribeConfigs = authHelper.filterByAuthorized(request.context, DESCRIBE_CONFIGS, TOPIC,
        topics, logIfDenied = false)(identity).map(name => name -> results.find(name)).toMap

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
      adminManager.createTopics(
        createTopicsRequest.data.timeoutMs,
        createTopicsRequest.data.validateOnly,
        toCreate,
        authorizedForDescribeConfigs,
        controllerMutationQuota,
        handleCreateTopicsResults)
    }
  }

  def handleCreatePartitionsRequest(request: RequestChannel.Request): Unit = {
    val createPartitionsRequest = request.body[CreatePartitionsRequest]
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 3)

    def sendResponseCallback(results: Map[String, ApiError]): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val createPartitionsResults = results.map {
          case (topic, error) => new CreatePartitionsTopicResult()
            .setName(topic)
            .setErrorCode(error.error.code)
            .setErrorMessage(error.message)
        }.toSeq
        val responseBody = new CreatePartitionsResponse(new CreatePartitionsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setResults(createPartitionsResults.asJava))
        trace(s"Sending create partitions response $responseBody for correlation id ${request.header.correlationId} to " +
          s"client ${request.header.clientId}.")
        responseBody
      }
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, createResponse)
    }

    if (!controller.isActive) {
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
        controller.topicDeletionManager.isTopicQueuedUpForDeletion(topic.name)
      }

      val errors = dupes.map(_ -> new ApiError(Errors.INVALID_REQUEST, "Duplicate topic in request.")) ++
        unauthorized.map(_.name -> new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, "The topic authorization is failed.")) ++
        queuedForDeletion.map(_.name -> new ApiError(Errors.INVALID_TOPIC_EXCEPTION, "The topic is queued for deletion."))

      adminManager.createPartitions(
        createPartitionsRequest.data.timeoutMs,
        valid,
        createPartitionsRequest.data.validateOnly,
        controllerMutationQuota,
        result => sendResponseCallback(result ++ errors))
    }
  }

  def handleDeleteTopicsRequest(request: RequestChannel.Request): Unit = {
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 5)

    def sendResponseCallback(results: DeletableTopicResultCollection): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseData = new DeleteTopicsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setResponses(results)
        val responseBody = new DeleteTopicsResponse(responseData)
        trace(s"Sending delete topics response $responseBody for correlation id ${request.header.correlationId} to client ${request.header.clientId}.")
        responseBody
      }
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, createResponse)
    }

    val deleteTopicRequest = request.body[DeleteTopicsRequest]
    val results = new DeletableTopicResultCollection(deleteTopicRequest.data.topicNames.size)
    val toDelete = mutable.Set[String]()
    if (!controller.isActive) {
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
        else controller.controllerContext.topicNames.getOrElse(topic.topicId(), null)
        results.add(new DeletableTopicResult()
          .setName(name)
          .setTopicId(topic.topicId()))
      }
      val authorizedDescribeTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
        results.asScala.filter(result => result.name() != null))(_.name)
      val authorizedDeleteTopics = authHelper.filterByAuthorized(request.context, DELETE, TOPIC,
        results.asScala.filter(result => result.name() != null))(_.name)
      results.forEach { topic =>
        val unresolvedTopicId = !(topic.topicId() == Uuid.ZERO_UUID) && topic.name() == null
         if (!config.usesTopicId && topicIdsFromRequest.contains(topic.topicId)) {
           topic.setErrorCode(Errors.UNSUPPORTED_VERSION.code)
           topic.setErrorMessage("Topic IDs are not supported on the server.")
         } else if (unresolvedTopicId)
             topic.setErrorCode(Errors.UNKNOWN_TOPIC_ID.code)
         else if (topicIdsFromRequest.contains(topic.topicId) && !authorizedDescribeTopics(topic.name))
           topic.setErrorCode(Errors.UNKNOWN_TOPIC_ID.code)
         else if (!authorizedDeleteTopics.contains(topic.name))
           topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
         else if (!metadataCache.contains(topic.name))
           topic.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
         else
           toDelete += topic.name
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

        adminManager.deleteTopics(
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
          .setTopics(new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection(mergedResponseStatus.groupBy(_._1.topic).map { case (topic, partitionMap) => {
            new DeleteRecordsTopicResult()
              .setName(topic)
              .setPartitions(new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(partitionMap.map { case (topicPartition, partitionResult) => {
                new DeleteRecordsPartitionResult().setPartitionIndex(topicPartition.partition)
                  .setLowWatermark(partitionResult.lowWatermark)
                  .setErrorCode(partitionResult.errorCode)
              }
              }.toList.asJava.iterator()))
          }
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

  def handleInitProducerIdRequest(request: RequestChannel.Request): Unit = {
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
        producerIdAndEpoch, sendResponseCallback)
      case Left(error) => requestHelper.sendErrorResponseMaybeThrottle(request, error.exception)
    }
  }

  def handleEndTxnRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
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
        sendResponseCallback)
    } else
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new EndTxnResponse(new EndTxnResponseData()
            .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
            .setThrottleTimeMs(requestThrottleMs))
      )
  }

  def handleWriteTxnMarkersRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
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
    def maybeSendResponseCallback(producerId: Long, result: TransactionResult)(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
      trace(s"End transaction marker append for producer id $producerId completed with status: $responseStatus")
      val currentErrors = new ConcurrentHashMap[TopicPartition, Errors](responseStatus.map { case (k, v) => k -> v.error }.asJava)
      updateErrors(producerId, currentErrors)
      val successfulOffsetsPartitions = responseStatus.filter { case (topicPartition, partitionResponse) =>
        topicPartition.topic == GROUP_METADATA_TOPIC_NAME && partitionResponse.error == Errors.NONE
      }.keys

      if (successfulOffsetsPartitions.nonEmpty) {
        // as soon as the end transaction marker has been written for a transactional offset commit,
        // call to the group coordinator to materialize the offsets into the cache
        try {
          groupCoordinator.scheduleHandleTxnCompletion(producerId, successfulOffsetsPartitions, result)
        } catch {
          case e: Exception =>
            error(s"Received an exception while trying to update the offsets cache on transaction marker append", e)
            val updatedErrors = new ConcurrentHashMap[TopicPartition, Errors]()
            successfulOffsetsPartitions.foreach(updatedErrors.put(_, Errors.UNKNOWN_SERVER_ERROR))
            updateErrors(producerId, updatedErrors)
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
        val controlRecords = partitionsWithCompatibleMessageFormat.map { partition =>
          val controlRecordType = marker.transactionResult match {
            case TransactionResult.COMMIT => ControlRecordType.COMMIT
            case TransactionResult.ABORT => ControlRecordType.ABORT
          }
          val endTxnMarker = new EndTransactionMarker(controlRecordType, marker.coordinatorEpoch)
          partition -> MemoryRecords.withEndTransactionMarker(producerId, marker.producerEpoch, endTxnMarker)
        }.toMap

        replicaManager.appendRecords(
          timeout = config.requestTimeoutMs.toLong,
          requiredAcks = -1,
          internalTopicsAllowed = true,
          origin = AppendOrigin.Coordinator,
          entriesPerPartition = controlRecords,
          responseCallback = maybeSendResponseCallback(producerId, marker.transactionResult))
      }
    }

    // No log appends were written as all partitions had incorrect log format
    // so we need to send the error response
    if (skippedMarkers == markers.size)
      requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
  }

  def ensureInterBrokerVersion(version: ApiVersion): Unit = {
    if (config.interBrokerProtocolVersion < version)
      throw new UnsupportedVersionException(s"inter.broker.protocol.version: ${config.interBrokerProtocolVersion.version} is less than the required version: ${version.version}")
  }

  def handleAddPartitionToTxnRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val addPartitionsToTxnRequest = request.body[AddPartitionsToTxnRequest]
    val transactionalId = addPartitionsToTxnRequest.data.transactionalId
    val partitionsToAdd = addPartitionsToTxnRequest.partitions.asScala
    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        addPartitionsToTxnRequest.getErrorResponse(requestThrottleMs, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception))
    else {
      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedPartitions = mutable.Set[TopicPartition]()

      val authorizedTopics = authHelper.filterByAuthorized(request.context, WRITE, TOPIC,
        partitionsToAdd.filterNot(tp => Topic.isInternal(tp.topic)))(_.topic)
      for (topicPartition <- partitionsToAdd) {
        if (!authorizedTopics.contains(topicPartition.topic))
          unauthorizedTopicErrors += topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED
        else if (!metadataCache.contains(topicPartition))
          nonExistingTopicErrors += topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION
        else
          authorizedPartitions.add(topicPartition)
      }

      if (unauthorizedTopicErrors.nonEmpty || nonExistingTopicErrors.nonEmpty) {
        // Any failed partition check causes the entire request to fail. We send the appropriate error codes for the
        // partitions which failed, and an 'OPERATION_NOT_ATTEMPTED' error code for the partitions which succeeded
        // the authorization check to indicate that they were not added to the transaction.
        val partitionErrors = unauthorizedTopicErrors ++ nonExistingTopicErrors ++
          authorizedPartitions.map(_ -> Errors.OPERATION_NOT_ATTEMPTED)
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new AddPartitionsToTxnResponse(requestThrottleMs, partitionErrors.asJava))
      } else {
        def sendResponseCallback(error: Errors): Unit = {
          def createResponse(requestThrottleMs: Int): AbstractResponse = {
            val finalError =
              if (addPartitionsToTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
                // For older clients, they could not understand the new PRODUCER_FENCED error code,
                // so we need to return the old INVALID_PRODUCER_EPOCH to have the same client handling logic.
                Errors.INVALID_PRODUCER_EPOCH
              } else {
                error
              }

            val responseBody: AddPartitionsToTxnResponse = new AddPartitionsToTxnResponse(requestThrottleMs,
              partitionsToAdd.map{tp => (tp, finalError)}.toMap.asJava)
            trace(s"Completed $transactionalId's AddPartitionsToTxnRequest with partitions $partitionsToAdd: errors: $error from client ${request.header.clientId}")
            responseBody
          }


          requestHelper.sendResponseMaybeThrottle(request, createResponse)
        }

        txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
          addPartitionsToTxnRequest.data.producerId,
          addPartitionsToTxnRequest.data.producerEpoch,
          authorizedPartitions,
          sendResponseCallback)
      }
    }
  }

  def handleAddOffsetsToTxnRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
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
        sendResponseCallback)
    }
  }

  def handleTxnOffsetCommitRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val header = request.header
    val txnOffsetCommitRequest = request.body[TxnOffsetCommitRequest]

    // authorize for the transactionalId and the consumer group. Note that we skip producerId authorization
    // since it is implied by transactionalId authorization
    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, txnOffsetCommitRequest.data.transactionalId))
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
    else if (!authHelper.authorize(request.context, READ, GROUP, txnOffsetCommitRequest.data.groupId))
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.GROUP_AUTHORIZATION_FAILED.exception)
    else {
      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedTopicCommittedOffsets = mutable.Map[TopicPartition, TxnOffsetCommitRequest.CommittedOffset]()
      val committedOffsets = txnOffsetCommitRequest.offsets.asScala
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, committedOffsets)(_._1.topic)

      for ((topicPartition, commitedOffset) <- committedOffsets) {
        if (!authorizedTopics.contains(topicPartition.topic))
          unauthorizedTopicErrors += topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED
        else if (!metadataCache.contains(topicPartition))
          nonExistingTopicErrors += topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION
        else
          authorizedTopicCommittedOffsets += (topicPartition -> commitedOffset)
      }

      // the callback for sending an offset commit response
      def sendResponseCallback(authorizedTopicErrors: Map[TopicPartition, Errors]): Unit = {
        val combinedCommitStatus = mutable.Map() ++= authorizedTopicErrors ++= unauthorizedTopicErrors ++= nonExistingTopicErrors
        if (isDebugEnabled)
          combinedCommitStatus.forKeyValue { (topicPartition, error) =>
            if (error != Errors.NONE) {
              debug(s"TxnOffsetCommit with correlation id ${header.correlationId} from client ${header.clientId} " +
                s"on partition $topicPartition failed due to ${error.exceptionName}")
            }
          }

        // We need to replace COORDINATOR_LOAD_IN_PROGRESS with COORDINATOR_NOT_AVAILABLE
        // for older producer client from 0.11 to prior 2.0, which could potentially crash due
        // to unexpected loading error. This bug is fixed later by KAFKA-7296. Clients using
        // txn commit protocol >= 2 (version 2.3 and onwards) are guaranteed to have
        // the fix to check for the loading error.
        if (txnOffsetCommitRequest.version < 2) {
          combinedCommitStatus ++= combinedCommitStatus.collect {
            case (tp, error) if error == Errors.COORDINATOR_LOAD_IN_PROGRESS => tp -> Errors.COORDINATOR_NOT_AVAILABLE
          }
        }

        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new TxnOffsetCommitResponse(requestThrottleMs, combinedCommitStatus.asJava))
      }

      if (authorizedTopicCommittedOffsets.isEmpty)
        sendResponseCallback(Map.empty)
      else {
        val offsetMetadata = convertTxnOffsets(authorizedTopicCommittedOffsets.toMap)
        groupCoordinator.handleTxnCommitOffsets(
          txnOffsetCommitRequest.data.groupId,
          txnOffsetCommitRequest.data.producerId,
          txnOffsetCommitRequest.data.producerEpoch,
          txnOffsetCommitRequest.data.memberId,
          Option(txnOffsetCommitRequest.data.groupInstanceId),
          txnOffsetCommitRequest.data.generationId,
          offsetMetadata,
          sendResponseCallback)
      }
    }
  }

  private def convertTxnOffsets(offsetsMap: immutable.Map[TopicPartition, TxnOffsetCommitRequest.CommittedOffset]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    val currentTimestamp = time.milliseconds
    offsetsMap.map { case (topicPartition, partitionData) =>
      val metadata = if (partitionData.metadata == null) OffsetAndMetadata.NoMetadata else partitionData.metadata
      topicPartition -> new OffsetAndMetadata(
        offset = partitionData.offset,
        leaderEpoch = partitionData.leaderEpoch,
        metadata = metadata,
        commitTimestamp = currentTimestamp,
        expireTimestamp = None)
    }
  }

  def handleDescribeAcls(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    val describeAclsRequest = request.body[DescribeAclsRequest]
    authorizer match {
      case None =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(new DescribeAclsResponseData()
            .setErrorCode(Errors.SECURITY_DISABLED.code)
            .setErrorMessage("No Authorizer is configured on the broker")
            .setThrottleTimeMs(requestThrottleMs),
          describeAclsRequest.version))
      case Some(auth) =>
        val filter = describeAclsRequest.filter
        val returnedAcls = new util.HashSet[AclBinding]()
        auth.acls(filter).forEach(returnedAcls.add)
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(new DescribeAclsResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setResources(DescribeAclsResponse.aclsResources(returnedAcls)),
          describeAclsRequest.version))
    }
  }

  def handleCreateAcls(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, ALTER)
    val createAclsRequest = request.body[CreateAclsRequest]

    authorizer match {
      case None => requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        createAclsRequest.getErrorResponse(requestThrottleMs,
          new SecurityDisabledException("No Authorizer is configured on the broker.")))
      case Some(auth) =>
        val allBindings = createAclsRequest.aclCreations.asScala.map(CreateAclsRequest.aclBinding)
        val errorResults = mutable.Map[AclBinding, AclCreateResult]()
        val validBindings = new ArrayBuffer[AclBinding]
        allBindings.foreach { acl =>
          val resource = acl.pattern
          val throwable = if (resource.resourceType == ResourceType.CLUSTER && !AuthorizerUtils.isClusterResource(resource.name))
              new InvalidRequestException("The only valid name for the CLUSTER resource is " + CLUSTER_NAME)
          else if (resource.name.isEmpty)
            new InvalidRequestException("Invalid empty resource name")
          else
            null
          if (throwable != null) {
            debug(s"Failed to add acl $acl to $resource", throwable)
            errorResults(acl) = new AclCreateResult(throwable)
          } else
            validBindings += acl
        }

        val createResults = auth.createAcls(request.context, validBindings.asJava).asScala.map(_.toCompletableFuture)

        def sendResponseCallback(): Unit = {
          val aclCreationResults = allBindings.map { acl =>
            val result = errorResults.getOrElse(acl, createResults(validBindings.indexOf(acl)).get)
            val creationResult = new AclCreationResult()
            result.exception.asScala.foreach { throwable =>
              val apiError = ApiError.fromThrowable(throwable)
              creationResult
                .setErrorCode(apiError.error.code)
                .setErrorMessage(apiError.message)
            }
            creationResult
          }
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new CreateAclsResponse(new CreateAclsResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setResults(aclCreationResults.asJava)))
        }

        alterAclsPurgatory.tryCompleteElseWatch(config.connectionsMaxIdleMs, createResults, sendResponseCallback)
    }
  }

  def handleDeleteAcls(request: RequestChannel.Request): Unit = {
    authHelper.authorizeClusterOperation(request, ALTER)
    val deleteAclsRequest = request.body[DeleteAclsRequest]
    authorizer match {
      case None =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          deleteAclsRequest.getErrorResponse(requestThrottleMs,
            new SecurityDisabledException("No Authorizer is configured on the broker.")))
      case Some(auth) =>

        val deleteResults = auth.deleteAcls(request.context, deleteAclsRequest.filters)
          .asScala.map(_.toCompletableFuture).toList

        def sendResponseCallback(): Unit = {
          val filterResults = deleteResults.map(_.get).map(DeleteAclsResponse.filterResult).asJava
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new DeleteAclsResponse(
              new DeleteAclsResponseData()
                .setThrottleTimeMs(requestThrottleMs)
                .setFilterResults(filterResults),
              deleteAclsRequest.version))
        }
        alterAclsPurgatory.tryCompleteElseWatch(config.connectionsMaxIdleMs, deleteResults, sendResponseCallback)
    }
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
    val alterConfigsRequest = request.body[AlterConfigsRequest]
    val (authorizedResources, unauthorizedResources) = alterConfigsRequest.configs.asScala.toMap.partition { case (resource, _) =>
      resource.`type` match {
        case ConfigResource.Type.BROKER_LOGGER =>
          throw new InvalidRequestException(s"AlterConfigs is deprecated and does not support the resource type ${ConfigResource.Type.BROKER_LOGGER}")
        case ConfigResource.Type.BROKER =>
          authHelper.authorize(request.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)
        case ConfigResource.Type.TOPIC =>
          authHelper.authorize(request.context, ALTER_CONFIGS, TOPIC, resource.name)
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt")
      }
    }
    val authorizedResult = adminManager.alterConfigs(authorizedResources, alterConfigsRequest.validateOnly)
    val unauthorizedResult = unauthorizedResources.keys.map { resource =>
      resource -> configsAuthorizationApiError(resource)
    }
    def responseCallback(requestThrottleMs: Int): AlterConfigsResponse = {
      val data = new AlterConfigsResponseData()
        .setThrottleTimeMs(requestThrottleMs)
      (authorizedResult ++ unauthorizedResult).foreach{ case (resource, error) =>
        data.responses().add(new AlterConfigsResourceResponse()
          .setErrorCode(error.error.code)
          .setErrorMessage(error.message)
          .setResourceName(resource.name)
          .setResourceType(resource.`type`.id))
      }
      new AlterConfigsResponse(data)
    }
    requestHelper.sendResponseMaybeThrottle(request, responseCallback)
  }

  def handleAlterPartitionReassignmentsRequest(request: RequestChannel.Request): Unit = {
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

    controller.alterPartitionReassignments(reassignments, sendResponseCallback)
  }

  def handleListPartitionReassignmentsRequest(request: RequestChannel.Request): Unit = {
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

    val partitionsOpt = listPartitionReassignmentsRequest.data.topics match {
      case topics: Any =>
        Some(topics.iterator().asScala.flatMap { topic =>
          topic.partitionIndexes.iterator().asScala
            .map { tp => new TopicPartition(topic.name(), tp) }
        }.toSet)
      case _ => None
    }

    controller.listPartitionReassignments(partitionsOpt, sendResponseCallback)
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
    val alterConfigsRequest = request.body[IncrementalAlterConfigsRequest]

    val configs = alterConfigsRequest.data.resources.iterator.asScala.map { alterConfigResource =>
      val configResource = new ConfigResource(ConfigResource.Type.forId(alterConfigResource.resourceType),
        alterConfigResource.resourceName)
      configResource -> alterConfigResource.configs.iterator.asScala.map {
        alterConfig => new AlterConfigOp(new ConfigEntry(alterConfig.name, alterConfig.value),
          OpType.forId(alterConfig.configOperation))
      }.toBuffer
    }.toMap

    val (authorizedResources, unauthorizedResources) = configs.partition { case (resource, _) =>
      resource.`type` match {
        case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER =>
          authHelper.authorize(request.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)
        case ConfigResource.Type.TOPIC =>
          authHelper.authorize(request.context, ALTER_CONFIGS, TOPIC, resource.name)
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt")
      }
    }

    val authorizedResult = adminManager.incrementalAlterConfigs(authorizedResources, alterConfigsRequest.data.validateOnly)
    val unauthorizedResult = unauthorizedResources.keys.map { resource =>
      resource -> configsAuthorizationApiError(resource)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => new IncrementalAlterConfigsResponse(
      requestThrottleMs, (authorizedResult ++ unauthorizedResult).asJava))
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): Unit = {
    val describeConfigsRequest = request.body[DescribeConfigsRequest]
    val (authorizedResources, unauthorizedResources) = describeConfigsRequest.data.resources.asScala.partition { resource =>
      ConfigResource.Type.forId(resource.resourceType) match {
        case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER =>
          authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)
        case ConfigResource.Type.TOPIC =>
          authHelper.authorize(request.context, DESCRIBE_CONFIGS, TOPIC, resource.resourceName)
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.resourceName}")
      }
    }
    val authorizedConfigs = adminManager.describeConfigs(authorizedResources.toList, describeConfigsRequest.data.includeSynonyms, describeConfigsRequest.data.includeDocumentation)
    val unauthorizedConfigs = unauthorizedResources.map { resource =>
      val error = ConfigResource.Type.forId(resource.resourceType) match {
        case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER => Errors.CLUSTER_AUTHORIZATION_FAILED
        case ConfigResource.Type.TOPIC => Errors.TOPIC_AUTHORIZATION_FAILED
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.resourceName}")
      }
      new DescribeConfigsResponseData.DescribeConfigsResult().setErrorCode(error.code)
        .setErrorMessage(error.message)
        .setConfigs(Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult])
        .setResourceName(resource.resourceName)
        .setResourceType(resource.resourceType)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeConfigsResponse(new DescribeConfigsResponseData().setThrottleTimeMs(requestThrottleMs)
        .setResults((authorizedConfigs ++ unauthorizedConfigs).asJava)))
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
    val logDirInfos = {
      if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
        val partitions =
          if (describeLogDirsDirRequest.isAllTopicPartitions)
            replicaManager.logManager.allLogs.map(_.topicPartition).toSet
          else
            describeLogDirsDirRequest.data.topics.asScala.flatMap(
              logDirTopic => logDirTopic.partitionIndex.asScala.map(partitionIndex =>
                new TopicPartition(logDirTopic.topic, partitionIndex))).toSet

        replicaManager.describeLogDirs(partitions)
      } else {
        List.empty[DescribeLogDirsResponseData.DescribeLogDirsResult]
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => new DescribeLogDirsResponse(new DescribeLogDirsResponseData()
      .setThrottleTimeMs(throttleTimeMs)
      .setResults(logDirInfos.asJava)))
  }

  def handleCreateTokenRequest(request: RequestChannel.Request): Unit = {
    val createTokenRequest = request.body[CreateDelegationTokenRequest]

    // the callback for sending a create token response
    def sendResponseCallback(createResult: CreateTokenResult): Unit = {
      trace(s"Sending create token response for correlation id ${request.header.correlationId} " +
        s"to client ${request.header.clientId}.")
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(requestThrottleMs, createResult.error, request.context.principal, createResult.issueTimestamp,
          createResult.expiryTimestamp, createResult.maxTimestamp, createResult.tokenId, ByteBuffer.wrap(createResult.hmac)))
    }

    if (!allowTokenRequests(request))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(requestThrottleMs, Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, request.context.principal))
    else {
      val renewerList = createTokenRequest.data.renewers.asScala.toList.map(entry =>
        new KafkaPrincipal(entry.principalType, entry.principalName))

      if (renewerList.exists(principal => principal.getPrincipalType != KafkaPrincipal.USER_TYPE)) {
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          CreateDelegationTokenResponse.prepareResponse(requestThrottleMs, Errors.INVALID_PRINCIPAL_TYPE, request.context.principal))
      }
      else {
        tokenManager.createToken(
          request.context.principal,
          renewerList,
          createTokenRequest.data.maxLifetimeMs,
          sendResponseCallback
        )
      }
    }
  }

  def handleRenewTokenRequest(request: RequestChannel.Request): Unit = {
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

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, DelegationTokenManager.ErrorTimestamp)
    else {
      tokenManager.renewToken(
        request.context.principal,
        ByteBuffer.wrap(renewTokenRequest.data.hmac),
        renewTokenRequest.data.renewPeriodMs,
        sendResponseCallback
      )
    }
  }

  def handleExpireTokenRequest(request: RequestChannel.Request): Unit = {
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

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, DelegationTokenManager.ErrorTimestamp)
    else {
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
        new DescribeDelegationTokenResponse(requestThrottleMs, error, tokenDetails.asJava))
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
        def eligible(token: TokenInformation) = DelegationTokenManager.filterToken(requestPrincipal, owners, token, authorizeToken)
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

  def handleElectReplicaLeader(request: RequestChannel.Request): Unit = {

    val electionRequest = request.body[ElectLeadersRequest]

    def sendResponseCallback(
      error: ApiError
    )(
      results: Map[TopicPartition, ApiError]
    ): Unit = {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        val adjustedResults = if (electionRequest.data.topicPartitions == null) {
          /* When performing elections across all of the partitions we should only return
           * partitions for which there was an eleciton or resulted in an error. In other
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
        metadataCache.getAllPartitions()
      } else {
        electionRequest.topicPartitions
      }

      replicaManager.electLeaders(
        controller,
        partitions,
        electionRequest.electionType,
        sendResponseCallback(ApiError.NONE),
        electionRequest.data.timeoutMs
      )
    }
  }

  def handleOffsetDeleteRequest(request: RequestChannel.Request): Unit = {
    val offsetDeleteRequest = request.body[OffsetDeleteRequest]
    val groupId = offsetDeleteRequest.data.groupId

    if (authHelper.authorize(request.context, DELETE, GROUP, groupId)) {
      val topics = offsetDeleteRequest.data.topics.asScala
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, topics)(_.name)

      val topicPartitionErrors = mutable.Map[TopicPartition, Errors]()
      val topicPartitions = mutable.ArrayBuffer[TopicPartition]()

      for (topic <- topics) {
        for (partition <- topic.partitions.asScala) {
          val tp = new TopicPartition(topic.name, partition.partitionIndex)
          if (!authorizedTopics.contains(topic.name))
            topicPartitionErrors(tp) = Errors.TOPIC_AUTHORIZATION_FAILED
          else if (!metadataCache.contains(tp))
            topicPartitionErrors(tp) = Errors.UNKNOWN_TOPIC_OR_PARTITION
          else
            topicPartitions += tp
        }
      }

      val (groupError, authorizedTopicPartitionsErrors) = groupCoordinator.handleDeleteOffsets(
        groupId, topicPartitions)

      topicPartitionErrors ++= authorizedTopicPartitionsErrors

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        if (groupError != Errors.NONE)
          offsetDeleteRequest.getErrorResponse(requestThrottleMs, groupError)
        else {
          val topics = new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection
          topicPartitionErrors.groupBy(_._1.topic).forKeyValue { (topic, topicPartitions) =>
            val partitions = new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection
            topicPartitions.forKeyValue { (topicPartition, error) =>
              partitions.add(
                new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                  .setPartitionIndex(topicPartition.partition)
                  .setErrorCode(error.code)
              )
            }
            topics.add(new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
              .setName(topic)
              .setPartitions(partitions))
          }

          new OffsetDeleteResponse(new OffsetDeleteResponseData()
            .setTopics(topics)
            .setThrottleTimeMs(requestThrottleMs))
        }
      })
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        offsetDeleteRequest.getErrorResponse(requestThrottleMs, Errors.GROUP_AUTHORIZATION_FAILED))
    }
  }

  def handleDescribeClientQuotasRequest(request: RequestChannel.Request): Unit = {
    val describeClientQuotasRequest = request.body[DescribeClientQuotasRequest]

    if (authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)) {
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
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeClientQuotasRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleAlterClientQuotasRequest(request: RequestChannel.Request): Unit = {
    val alterClientQuotasRequest = request.body[AlterClientQuotasRequest]

    if (authHelper.authorize(request.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      val result = adminManager.alterClientQuotas(alterClientQuotasRequest.entries.asScala,
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

    if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
      val result = adminManager.describeUserScramCredentials(
        Option(describeUserScramCredentialsRequest.data.users).map(_.asScala.map(_.name).toList))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeUserScramCredentialsResponse(result.setThrottleTimeMs(requestThrottleMs)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleAlterUserScramCredentialsRequest(request: RequestChannel.Request): Unit = {
    val alterUserScramCredentialsRequest = request.body[AlterUserScramCredentialsRequest]

    if (!controller.isActive) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.NOT_CONTROLLER.exception))
    } else if (authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      val result = adminManager.alterUserScramCredentials(
        alterUserScramCredentialsRequest.data.upsertions().asScala, alterUserScramCredentialsRequest.data.deletions().asScala)
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterUserScramCredentialsResponse(result.setThrottleTimeMs(requestThrottleMs)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleAlterIsrRequest(request: RequestChannel.Request): Unit = {
    val alterIsrRequest = request.body[AlterIsrRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    if (!controller.isActive)
      requestHelper.sendResponseExemptThrottle(request, alterIsrRequest.getErrorResponse(
        AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.NOT_CONTROLLER.exception))
    else
      controller.alterIsrs(alterIsrRequest.data, alterIsrResp =>
        requestHelper.sendResponseExemptThrottle(request, new AlterIsrResponse(alterIsrResp))
      )
  }

  def handleUpdateFeatures(request: RequestChannel.Request): Unit = {
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
    } else if (!controller.isActive) {
      sendResponseCallback(Left(new ApiError(Errors.NOT_CONTROLLER)))
    } else if (!config.isFeatureVersioningSupported) {
      sendResponseCallback(Left(new ApiError(Errors.INVALID_REQUEST, "Feature versioning system is disabled.")))
    } else {
      controller.updateFeatures(updateFeaturesRequest, sendResponseCallback)
    }
  }

  def handleDescribeCluster(request: RequestChannel.Request): Unit = {
    val describeClusterRequest = request.body[DescribeClusterRequest]

    var clusterAuthorizedOperations = Int.MinValue // Default value in the schema
    // get cluster authorized operations
    if (describeClusterRequest.data.includeClusterAuthorizedOperations) {
      if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME))
        clusterAuthorizedOperations = authHelper.authorizedOperations(request, Resource.CLUSTER)
      else
        clusterAuthorizedOperations = 0
    }

    val brokers = metadataCache.getAliveBrokers
    val controllerId = metadataCache.getControllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID)

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
      val data = new DescribeClusterResponseData()
        .setThrottleTimeMs(requestThrottleMs)
        .setClusterId(clusterId)
        .setControllerId(controllerId)
        .setClusterAuthorizedOperations(clusterAuthorizedOperations);

      brokers.flatMap(_.getNode(request.context.listenerName)).foreach { broker =>
        data.brokers.add(new DescribeClusterResponseData.DescribeClusterBroker()
          .setBrokerId(broker.id)
          .setHost(broker.host)
          .setPort(broker.port)
          .setRack(broker.rack))
      }

      new DescribeClusterResponse(data)
    })
  }

  def handleEnvelope(request: RequestChannel.Request): Unit = {
    val envelope = request.body[EnvelopeRequest]

    // If forwarding is not yet enabled or this request has been received on an invalid endpoint,
    // then we treat the request as unparsable and close the connection.
    if (!isForwardingEnabled(request)) {
      info(s"Closing connection ${request.context.connectionId} because it sent an `Envelope` " +
        "request even though forwarding has not been enabled")
      requestHelper.closeConnection(request, Collections.emptyMap())
      return
    } else if (!request.context.fromPrivilegedListener) {
      info(s"Closing connection ${request.context.connectionId} from listener ${request.context.listenerName} " +
        s"because it sent an `Envelope` request, which is only accepted on the inter-broker listener " +
        s"${config.interBrokerListenerName}.")
      requestHelper.closeConnection(request, Collections.emptyMap())
      return
    } else if (!authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new ClusterAuthorizationException(
        s"Principal ${request.context.principal} does not have required CLUSTER_ACTION for envelope"))
      return
    } else if (!controller.isActive) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new NotControllerException(
        s"Broker $brokerId is not the active controller"))
      return
    }

    val forwardedPrincipal = parseForwardedPrincipal(request.context, envelope.requestPrincipal)
    val forwardedClientAddress = parseForwardedClientAddress(envelope.clientAddress)

    val forwardedRequestBuffer = envelope.requestData.duplicate()
    val forwardedRequestHeader = parseForwardedRequestHeader(forwardedRequestBuffer)

    val forwardedApi = forwardedRequestHeader.apiKey
    if (!forwardedApi.forwardable) {
      throw new InvalidRequestException(s"API $forwardedApi is not enabled or is not eligible for forwarding")
    }

    val forwardedContext = new RequestContext(
      forwardedRequestHeader,
      request.context.connectionId,
      forwardedClientAddress,
      forwardedPrincipal,
      request.context.listenerName,
      request.context.securityProtocol,
      ClientInformation.EMPTY,
      request.context.fromPrivilegedListener
    )

    val forwardedRequest = parseForwardedRequest(request, forwardedContext, forwardedRequestBuffer)
    handle(forwardedRequest)
  }

  private def parseForwardedClientAddress(
    address: Array[Byte]
  ): InetAddress = {
    try {
      InetAddress.getByAddress(address)
    } catch {
      case e: UnknownHostException =>
        throw new InvalidRequestException("Failed to parse client address from envelope", e)
    }
  }

  private def parseForwardedRequest(
    envelope: RequestChannel.Request,
    forwardedContext: RequestContext,
    buffer: ByteBuffer
  ): RequestChannel.Request = {
    try {
      new RequestChannel.Request(
        processor = envelope.processor,
        context = forwardedContext,
        startTimeNanos = envelope.startTimeNanos,
        envelope.memoryPool,
        buffer,
        requestChannel.metrics,
        Some(envelope)
      )
    } catch {
      case e: InvalidRequestException =>
        // We use UNSUPPORTED_VERSION if the embedded request cannot be parsed.
        // The purpose is to disambiguate structural errors in the envelope request
        // itself, such as an invalid client address.
        throw new UnsupportedVersionException(s"Failed to parse forwarded request " +
          s"with header ${forwardedContext.header}", e)
    }
  }

  private def parseForwardedRequestHeader(
    buffer: ByteBuffer
  ): RequestHeader = {
    try {
      RequestHeader.parse(buffer)
    } catch {
      case e: InvalidRequestException =>
        // We use UNSUPPORTED_VERSION if the embedded request cannot be parsed.
        // The purpose is to disambiguate structural errors in the envelope request
        // itself, such as an invalid client address.
        throw new UnsupportedVersionException("Failed to parse request header from envelope", e)
    }
  }

  private def parseForwardedPrincipal(
    envelopeContext: RequestContext,
    principalBytes: Array[Byte]
  ): KafkaPrincipal = {
    envelopeContext.principalSerde.asScala match {
      case Some(serde) =>
        try {
          serde.deserialize(principalBytes)
        } catch {
          case e: Exception =>
            throw new PrincipalDeserializationException("Failed to deserialize client principal from envelope", e)
        }

      case None =>
        throw new PrincipalDeserializationException("Could not deserialize principal since " +
          "no `KafkaPrincipalSerde` has been defined")
    }
  }

  def handleDescribeProducersRequest(request: RequestChannel.Request): Unit = {
    val describeProducersRequest = request.body[DescribeProducersRequest]

    def partitionError(topicPartition: TopicPartition, error: Errors): DescribeProducersResponseData.PartitionResponse = {
      new DescribeProducersResponseData.PartitionResponse()
        .setPartitionIndex(topicPartition.partition)
        .setErrorCode(error.code)
    }

    val response = new DescribeProducersResponseData()
    describeProducersRequest.data.topics.forEach { topicRequest =>
      val topicResponse = new DescribeProducersResponseData.TopicResponse()
        .setName(topicRequest.name)
      val topicError = if (!authHelper.authorize(request.context, READ, TOPIC, topicRequest.name))
        Some(Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicRequest.name))
        Some(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        None

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

  private def updateRecordConversionStats(request: RequestChannel.Request,
                                          tp: TopicPartition,
                                          conversionStats: RecordConversionStats): Unit = {
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

  private def isBrokerEpochStale(brokerEpochInRequest: Long): Boolean = {
    // Broker epoch in LeaderAndIsr/UpdateMetadata/StopReplica request is unknown
    // if the controller hasn't been upgraded to use KIP-380
    if (brokerEpochInRequest == AbstractControlRequest.UNKNOWN_BROKER_EPOCH) false
    else {
      // brokerEpochInRequest > controller.brokerEpoch is possible in rare scenarios where the controller gets notified
      // about the new broker epoch and sends a control request with this epoch before the broker learns about it
      brokerEpochInRequest < controller.brokerEpoch
    }
  }

}

object KafkaApis {
  // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
  // traffic doesn't exceed quota.
  private[server] def sizeOfThrottledPartitions(versionId: Short,
                                                unconvertedResponse: FetchResponse[Records],
                                                quota: ReplicationQuotaManager): Int = {
    FetchResponse.sizeOf(versionId, unconvertedResponse.responseData.entrySet
      .iterator.asScala.filter(element => quota.isThrottled(element.getKey)).asJava)
  }
}
