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
import java.lang.{Byte => JByte}
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Optional, Properties}

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.api.{ApiVersion, KAFKA_0_11_0_IV0, KAFKA_2_3_IV0}
import kafka.cluster.Partition
import kafka.common.OffsetAndMetadata
import kafka.controller.KafkaController
import kafka.coordinator.group.{GroupCoordinator, JoinGroupResult, SyncGroupResult}
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.message.ZStdCompressionCodec
import kafka.network.RequestChannel
import kafka.security.SecurityUtils
import kafka.security.auth.{Resource, _}
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.utils.{CoreUtils, Logging}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, isInternal}
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData.{CreatableTopicResult, CreatableTopicResultCollection}
import org.apache.kafka.common.message._
import org.apache.kafka.common.message.DeleteTopicsResponseData.{DeletableTopicResult, DeletableTopicResultCollection}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ListenerName, Send}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.CreateAclsResponse.AclCreationResponse
import org.apache.kafka.common.requests.DeleteAclsResponse.{AclDeletionResult, AclFilterResponse}
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.{PatternType, ResourcePattern}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val adminManager: AdminManager,
                val groupCoordinator: GroupCoordinator,
                val txnCoordinator: TransactionCoordinator,
                val controller: KafkaController,
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
                val tokenManager: DelegationTokenManager) extends Logging {

  type FetchResponseStats = Map[TopicPartition, RecordConversionStats]
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  val adminZkClient = new AdminZkClient(zkClient)

  def close() {
    info("Shutdown complete.")
  }

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
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
        case ApiKeys.CREATE_TOPICS => handleCreateTopicsRequest(request)
        case ApiKeys.DELETE_TOPICS => handleDeleteTopicsRequest(request)
        case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
        case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request)
        case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
        case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionToTxnRequest(request)
        case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request)
        case ApiKeys.END_TXN => handleEndTxnRequest(request)
        case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request)
        case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request)
        case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
        case ApiKeys.CREATE_ACLS => handleCreateAcls(request)
        case ApiKeys.DELETE_ACLS => handleDeleteAcls(request)
        case ApiKeys.ALTER_CONFIGS => handleAlterConfigsRequest(request)
        case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
        case ApiKeys.ALTER_REPLICA_LOG_DIRS => handleAlterReplicaLogDirsRequest(request)
        case ApiKeys.DESCRIBE_LOG_DIRS => handleDescribeLogDirsRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.CREATE_PARTITIONS => handleCreatePartitionsRequest(request)
        case ApiKeys.CREATE_DELEGATION_TOKEN => handleCreateTokenRequest(request)
        case ApiKeys.RENEW_DELEGATION_TOKEN => handleRenewTokenRequest(request)
        case ApiKeys.EXPIRE_DELEGATION_TOKEN => handleExpireTokenRequest(request)
        case ApiKeys.DESCRIBE_DELEGATION_TOKEN => handleDescribeTokensRequest(request)
        case ApiKeys.DELETE_GROUPS => handleDeleteGroupsRequest(request)
        case ApiKeys.ELECT_PREFERRED_LEADERS => handleElectPreferredReplicaLeader(request)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => handleIncrementalAlterConfigsRequest(request)
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => handleError(request, e)
    } finally {
      request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val correlationId = request.header.correlationId
    val leaderAndIsrRequest = request.body[LeaderAndIsrRequest]

    def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]) {
      // for each new leader or follower, call coordinator to handle consumer group migration.
      // this callback is invoked under the replica state change lock to ensure proper order of
      // leadership changes
      updatedLeaders.foreach { partition =>
        if (partition.topic == GROUP_METADATA_TOPIC_NAME)
          groupCoordinator.handleGroupImmigration(partition.partitionId)
        else if (partition.topic == TRANSACTION_STATE_TOPIC_NAME)
          txnCoordinator.handleTxnImmigration(partition.partitionId, partition.getLeaderEpoch)
      }

      updatedFollowers.foreach { partition =>
        if (partition.topic == GROUP_METADATA_TOPIC_NAME)
          groupCoordinator.handleGroupEmigration(partition.partitionId)
        else if (partition.topic == TRANSACTION_STATE_TOPIC_NAME)
          txnCoordinator.handleTxnEmigration(partition.partitionId, partition.getLeaderEpoch)
      }
    }

    authorizeClusterAction(request)
    if (isBrokerEpochStale(leaderAndIsrRequest.brokerEpoch())) {
      // When the broker restarts very quickly, it is possible for this broker to receive request intended
      // for its previous generation so the broker should skip the stale request.
      info("Received LeaderAndIsr request with broker epoch " +
        s"${leaderAndIsrRequest.brokerEpoch()} smaller than the current broker epoch ${controller.brokerEpoch}")
      sendResponseExemptThrottle(request, leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_BROKER_EPOCH.exception))
    } else {
      val response = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest, onLeadershipChange)
      sendResponseExemptThrottle(request, response)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.body[StopReplicaRequest]
    authorizeClusterAction(request)
    if (isBrokerEpochStale(stopReplicaRequest.brokerEpoch())) {
      // When the broker restarts very quickly, it is possible for this broker to receive request intended
      // for its previous generation so the broker should skip the stale request.
      info("Received stop replica request with broker epoch " +
        s"${stopReplicaRequest.brokerEpoch()} smaller than the current broker epoch ${controller.brokerEpoch}")
      sendResponseExemptThrottle(request, new StopReplicaResponse(Errors.STALE_BROKER_EPOCH, Map.empty[TopicPartition, Errors].asJava))
    } else {
      val (result, error) = replicaManager.stopReplicas(stopReplicaRequest)
      // Clearing out the cache for groups that belong to an offsets topic partition for which this broker was the leader,
      // since this broker is no longer a replica for that offsets topic partition.
      // This is required to handle the following scenario :
      // Consider old replicas : {[1,2,3], Leader = 1} is reassigned to new replicas : {[2,3,4], Leader = 2}, broker 1 does not receive a LeaderAndIsr
      // request to become a follower due to which cache for groups that belong to an offsets topic partition for which broker 1 was the leader,
      // is not cleared.
      result.foreach { case (topicPartition, error) =>
        if (error == Errors.NONE && stopReplicaRequest.deletePartitions && topicPartition.topic == GROUP_METADATA_TOPIC_NAME) {
          groupCoordinator.handleGroupEmigration(topicPartition.partition)
        }
      }
      sendResponseExemptThrottle(request, new StopReplicaResponse(error, result.asJava))
    }

    CoreUtils.swallow(replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads(), this)
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val correlationId = request.header.correlationId
    val updateMetadataRequest = request.body[UpdateMetadataRequest]

    authorizeClusterAction(request)
    if (isBrokerEpochStale(updateMetadataRequest.brokerEpoch())) {
      // When the broker restarts very quickly, it is possible for this broker to receive request intended
      // for its previous generation so the broker should skip the stale request.
      info("Received update metadata request with broker epoch " +
        s"${updateMetadataRequest.brokerEpoch()} smaller than the current broker epoch ${controller.brokerEpoch}")
      sendResponseExemptThrottle(request, new UpdateMetadataResponse(Errors.STALE_BROKER_EPOCH))
    } else {
      val deletedPartitions = replicaManager.maybeUpdateMetadataCache(correlationId, updateMetadataRequest)
      if (deletedPartitions.nonEmpty)
        groupCoordinator.handleDeletedPartitions(deletedPartitions)

      if (adminManager.hasDelayedTopicOperations) {
        updateMetadataRequest.partitionStates.keySet.asScala.map(_.topic).foreach { topic =>
          adminManager.tryCompleteDelayedTopicOperations(topic)
        }
      }
      quotas.clientQuotaCallback.foreach { callback =>
        if (callback.updateClusterMetadata(metadataCache.getClusterMetadata(clusterId, request.context.listenerName))) {
          quotas.fetch.updateQuotaMetricConfigs()
          quotas.produce.updateQuotaMetricConfigs()
          quotas.request.updateQuotaMetricConfigs()
        }
      }
      if (replicaManager.hasDelayedElectionOperations) {
        updateMetadataRequest.partitionStates.asScala.foreach { case (tp, ps) =>
          replicaManager.tryCompleteElection(new TopicPartitionOperationKey(tp.topic(), tp.partition()))
        }
      }
      sendResponseExemptThrottle(request, new UpdateMetadataResponse(Errors.NONE))
    }
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.body[ControlledShutdownRequest]
    authorizeClusterAction(request)

    def controlledShutdownCallback(controlledShutdownResult: Try[Set[TopicPartition]]): Unit = {
      val response = controlledShutdownResult match {
        case Success(partitionsRemaining) =>
         ControlledShutdownResponse.prepareResponse(Errors.NONE, partitionsRemaining.asJava)

        case Failure(throwable) =>
          controlledShutdownRequest.getErrorResponse(throwable)
      }
      sendResponseExemptThrottle(request, response)
    }
    controller.controlledShutdown(controlledShutdownRequest.data.brokerId, controlledShutdownRequest.data.brokerEpoch, controlledShutdownCallback)
  }

  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val header = request.header
    val offsetCommitRequest = request.body[OffsetCommitRequest]

    val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
    val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
    // the callback for sending an offset commit response
    def sendResponseCallback(commitStatus: immutable.Map[TopicPartition, Errors]) {
      val combinedCommitStatus = commitStatus ++ unauthorizedTopicErrors ++ nonExistingTopicErrors
      if (isDebugEnabled)
        combinedCommitStatus.foreach { case (topicPartition, error) =>
          if (error != Errors.NONE) {
            debug(s"Offset commit request with correlation id ${header.correlationId} from client ${header.clientId} " +
              s"on partition $topicPartition failed due to ${error.exceptionName}")
          }
        }
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new OffsetCommitResponse(requestThrottleMs, combinedCommitStatus.asJava))
    }

    // reject the request if not authorized to the group
    if (!authorize(request.session, Read, Resource(Group, offsetCommitRequest.data().groupId, LITERAL))) {
      val error = Errors.GROUP_AUTHORIZATION_FAILED
      val responseTopicList = OffsetCommitRequest.getErrorResponseTopics(
        offsetCommitRequest.data().topics(),
        error)

      sendResponseMaybeThrottle(request, requestThrottleMs => new OffsetCommitResponse(
        new OffsetCommitResponseData()
            .setTopics(responseTopicList)
            .setThrottleTimeMs(requestThrottleMs)
      ))
    } else if (offsetCommitRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      var errorMap = new mutable.HashMap[TopicPartition, Errors]
      for (topicData <- offsetCommitRequest.data().topics().asScala) {
        for (partitionData <- topicData.partitions().asScala) {
          val topicPartition = new TopicPartition(topicData.name(), partitionData.partitionIndex())
          errorMap += topicPartition -> Errors.UNSUPPORTED_VERSION
        }
      }
      sendResponseCallback(errorMap.toMap)
    } else {
      val authorizedTopicRequestInfoBldr = immutable.Map.newBuilder[TopicPartition, OffsetCommitRequestData.OffsetCommitRequestPartition]

      for (topicData <- offsetCommitRequest.data().topics().asScala) {
        for (partitionData <- topicData.partitions().asScala) {
          val topicPartition = new TopicPartition(topicData.name(), partitionData.partitionIndex())
          if (!authorize(request.session, Read, Resource(Topic, topicData.name(), LITERAL)))
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
                  offsetCommitRequest.data().groupId(),
                  topicPartition,
                  partitionData.committedOffset())
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
        val partitionData = authorizedTopicRequestInfo.mapValues { partitionData =>
          val metadata = if (partitionData.committedMetadata() == null)
            OffsetAndMetadata.NoMetadata
          else
            partitionData.committedMetadata()

          new OffsetAndMetadata(
            offset = partitionData.committedOffset(),
            leaderEpoch = Optional.ofNullable[Integer](partitionData.committedLeaderEpoch),
            metadata = metadata,
            commitTimestamp = partitionData.commitTimestamp() match {
              case OffsetCommitRequest.DEFAULT_TIMESTAMP => currentTimestamp
              case customTimestamp => customTimestamp
            },
            expireTimestamp = offsetCommitRequest.data().retentionTimeMs() match {
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

  private def authorize(session: RequestChannel.Session, operation: Operation, resource: Resource): Boolean =
    authorizer.forall(_.authorize(session, operation, resource))

  /**
   * Handle a produce request
   */
  def handleProduceRequest(request: RequestChannel.Request) {
    val produceRequest = request.body[ProduceRequest]
    val numBytesAppended = request.header.toStruct.sizeOf + request.sizeOfBodyInBytes

    if (produceRequest.hasTransactionalRecords) {
      val isAuthorizedTransactional = produceRequest.transactionalId != null &&
        authorize(request.session, Write, Resource(TransactionalId, produceRequest.transactionalId, LITERAL))
      if (!isAuthorizedTransactional) {
        sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
      // Note that authorization to a transactionalId implies ProducerId authorization

    } else if (produceRequest.hasIdempotentRecords && !authorize(request.session, IdempotentWrite, Resource.ClusterResource)) {
      sendErrorResponseMaybeThrottle(request, Errors.CLUSTER_AUTHORIZATION_FAILED.exception)
      return
    }

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val invalidRequestResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val authorizedRequestInfo = mutable.Map[TopicPartition, MemoryRecords]()

    for ((topicPartition, memoryRecords) <- produceRequest.partitionRecordsOrFail.asScala) {
      if (!authorize(request.session, Write, Resource(Topic, topicPartition.topic, LITERAL)))
        unauthorizedTopicResponses += topicPartition -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        try {
          ProduceRequest.validateRecords(request.header.apiVersion(), memoryRecords)
          authorizedRequestInfo += (topicPartition -> memoryRecords)
        } catch {
          case e: ApiException =>
            invalidRequestResponses += topicPartition -> new PartitionResponse(Errors.forException(e))
        }
    }

    // the callback for sending a produce response
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ invalidRequestResponses
      var errorInResponse = false

      mergedResponseStatus.foreach { case (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            status.error.exceptionName))
        }
      }

      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeNanos = time.nanoseconds

      // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the quotas
      // have been violated. If both quotas have been violated, use the max throttle time between the two quotas. Note
      // that the request quota is not enforced if acks == 0.
      val bandwidthThrottleTimeMs = quotas.produce.maybeRecordAndGetThrottleTimeMs(request, numBytesAppended, time.milliseconds())
      val requestThrottleTimeMs = if (produceRequest.acks == 0) 0 else quotas.request.maybeRecordAndGetThrottleTimeMs(request)
      val maxThrottleTimeMs = Math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
      if (maxThrottleTimeMs > 0) {
        if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
          quotas.produce.throttle(request, bandwidthThrottleTimeMs, sendResponse)
        } else {
          quotas.request.throttle(request, requestThrottleTimeMs, sendResponse)
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
          closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
        } else {
          // Note that although request throttling is exempt for acks == 0, the channel may be throttled due to
          // bandwidth quota violation.
          sendNoOpResponseExemptThrottle(request)
        }
      } else {
        sendResponse(request, Some(new ProduceResponse(mergedResponseStatus.asJava, maxThrottleTimeMs)), None)
      }
    }

    def processingStatsCallback(processingStats: FetchResponseStats): Unit = {
      processingStats.foreach { case (tp, info) =>
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
        isFromClient = true,
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
  def handleFetchRequest(request: RequestChannel.Request) {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val fetchContext = fetchManager.newContext(
      fetchRequest.metadata,
      fetchRequest.fetchData,
      fetchRequest.toForget,
      fetchRequest.isFromFollower)

    def errorResponse[T >: MemoryRecords <: BaseRecords](error: Errors): FetchResponse.PartitionData[T] = {
      new FetchResponse.PartitionData[T](error, FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
        FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
    }

    val erroneous = mutable.ArrayBuffer[(TopicPartition, FetchResponse.PartitionData[Records])]()
    val interesting = mutable.ArrayBuffer[(TopicPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
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
      fetchContext.foreachPartition { (topicPartition, data) =>
        if (!authorize(request.session, Read, Resource(Topic, topicPartition.topic, LITERAL)))
          erroneous += topicPartition -> errorResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicPartition))
          erroneous += topicPartition -> errorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          interesting += (topicPartition -> data)
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
                new FetchResponse.PartitionData[BaseRecords](partitionData.error, partitionData.highWatermark,
                  partitionData.lastStableOffset, partitionData.logStartOffset, partitionData.abortedTransactions,
                  new LazyDownConversionRecords(tp, unconvertedRecords, magic, fetchContext.getFetchOffset(tp).get, time))
              } catch {
                case e: UnsupportedCompressionTypeException =>
                  trace("Received unsupported compression type error during down-conversion", e)
                  errorResponse(Errors.UNSUPPORTED_COMPRESSION_TYPE)
              }
            }
          case None => new FetchResponse.PartitionData[BaseRecords](partitionData.error, partitionData.highWatermark,
            partitionData.lastStableOffset, partitionData.logStartOffset, partitionData.abortedTransactions,
            unconvertedRecords)
        }
      }
    }

    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      val partitions = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
      responsePartitionData.foreach { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        partitions.put(tp, new FetchResponse.PartitionData(data.error, data.highWatermark, lastStableOffset,
          data.logStartOffset, abortedTransactions, data.records))
      }
      erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

      // When this callback is triggered, the remote API call has completed.
      // Record time before any byte-rate throttling.
      request.apiRemoteCompleteTimeNanos = time.nanoseconds

      var unconvertedFetchResponse: FetchResponse[Records] = null

      def createResponse(throttleTimeMs: Int): FetchResponse[BaseRecords] = {
        // Down-convert messages for each partition if required
        val convertedData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[BaseRecords]]
        unconvertedFetchResponse.responseData().asScala.foreach { case (tp, unconvertedPartitionData) =>
          if (unconvertedPartitionData.error != Errors.NONE)
            debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
              s"on partition $tp failed due to ${unconvertedPartitionData.error.exceptionName}")
          convertedData.put(tp, maybeConvertFetchedData(tp, unconvertedPartitionData))
        }

        // Prepare fetch response from converted data
        val response = new FetchResponse(unconvertedFetchResponse.error(), convertedData, throttleTimeMs,
          unconvertedFetchResponse.sessionId())
        response.responseData.asScala.foreach { case (topicPartition, data) =>
          // record the bytes out metrics only when the response is being sent
          brokerTopicStats.updateBytesOut(topicPartition.topic, fetchRequest.isFromFollower, data.records.sizeInBytes)
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
        val responseSize = sizeOfThrottledPartitions(versionId, unconvertedFetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        trace(s"Sending Fetch response with partitions.size=${unconvertedFetchResponse.responseData().size()}, " +
          s"metadata=${unconvertedFetchResponse.sessionId()}")
        sendResponseExemptThrottle(request, createResponse(0), Some(updateConversionStats))
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
        val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request)
        val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request, responseSize, timeMs)

        val maxThrottleTimeMs = math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
        if (maxThrottleTimeMs > 0) {
          // Even if we need to throttle for request quota violation, we should "unrecord" the already recorded value
          // from the fetch quota because we are going to return an empty response.
          quotas.fetch.unrecordQuotaSensor(request, responseSize, timeMs)
          if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
            quotas.fetch.throttle(request, bandwidthThrottleTimeMs, sendResponse)
          } else {
            quotas.request.throttle(request, requestThrottleTimeMs, sendResponse)
          }
          // If throttling is required, return an empty response.
          unconvertedFetchResponse = fetchContext.getThrottledResponse(maxThrottleTimeMs)
        } else {
          // Get the actual response. This will update the fetch context.
          unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
          trace(s"Sending Fetch response with partitions.size=$responseSize, metadata=${unconvertedFetchResponse.sessionId}")
        }

        // Send the response immediately.
        sendResponse(request, Some(createResponse(maxThrottleTimeMs)), Some(updateConversionStats))
      }
    }

    if (interesting.isEmpty)
      processResponseCallback(Seq.empty)
    else {
      // call the replica manager to fetch messages from the local replica
      replicaManager.fetchMessages(
        fetchRequest.maxWait.toLong,
        fetchRequest.replicaId,
        fetchRequest.minBytes,
        fetchRequest.maxBytes,
        versionId <= 2,
        interesting,
        replicationQuota(fetchRequest),
        processResponseCallback,
        fetchRequest.isolationLevel)
    }
  }

  class SelectingIterator(val partitions: util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]],
                          val quota: ReplicationQuotaManager)
                          extends util.Iterator[util.Map.Entry[TopicPartition, FetchResponse.PartitionData[Records]]] {
    val iter = partitions.entrySet().iterator()

    var nextElement: util.Map.Entry[TopicPartition, FetchResponse.PartitionData[Records]] = null

    override def hasNext: Boolean = {
      while ((nextElement == null) && iter.hasNext()) {
        val element = iter.next()
        if (quota.isThrottled(element.getKey)) {
          nextElement = element
        }
      }
      nextElement != null
    }

    override def next(): util.Map.Entry[TopicPartition, FetchResponse.PartitionData[Records]] = {
      if (!hasNext()) throw new NoSuchElementException()
      val element = nextElement
      nextElement = null
      element
    }

    override def remove() = throw new UnsupportedOperationException()
  }

  // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
  // traffic doesn't exceed quota.
  private def sizeOfThrottledPartitions(versionId: Short,
                                        unconvertedResponse: FetchResponse[Records],
                                        quota: ReplicationQuotaManager): Int = {
    val iter = new SelectingIterator(unconvertedResponse.responseData(), quota)
    FetchResponse.sizeOf(versionId, iter)
  }

  def replicationQuota(fetchRequest: FetchRequest): ReplicaQuota =
    if (fetchRequest.isFromFollower) quotas.leader else UnboundedQuota

  def handleListOffsetRequest(request: RequestChannel.Request) {
    val version = request.header.apiVersion()

    val mergedResponseMap = if (version == 0)
      handleListOffsetRequestV0(request)
    else
      handleListOffsetRequestV1AndAbove(request)

    sendResponseMaybeThrottle(request, requestThrottleMs => new ListOffsetResponse(requestThrottleMs, mergedResponseMap.asJava))
  }

  private def handleListOffsetRequestV0(request : RequestChannel.Request) : Map[TopicPartition, ListOffsetResponse.PartitionData] = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) = offsetRequest.partitionTimestamps.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Describe, Resource(Topic, topicPartition.topic, LITERAL))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ =>
      new ListOffsetResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED, List[JLong]().asJava)
    )

    val responseMap = authorizedRequestInfo.map {case (topicPartition, partitionData) =>
      try {
        val offsets = replicaManager.legacyFetchOffsetsForTimestamp(
          topicPartition = topicPartition,
          timestamp = partitionData.timestamp,
          maxNumOffsets = partitionData.maxNumOffsets,
          isFromConsumer = offsetRequest.replicaId == ListOffsetRequest.CONSUMER_REPLICA_ID,
          fetchOnlyFromLeader = offsetRequest.replicaId != ListOffsetRequest.DEBUGGING_REPLICA_ID)
        (topicPartition, new ListOffsetResponse.PartitionData(Errors.NONE, offsets.map(JLong.valueOf).asJava))
      } catch {
        // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
        // are typically transient and there is no value in logging the entire stack trace for the same
        case e @ (_ : UnknownTopicOrPartitionException |
                  _ : NotLeaderForPartitionException |
                  _ : KafkaStorageException) =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
            correlationId, clientId, topicPartition, e.getMessage))
          (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(e), List[JLong]().asJava))
        case e: Throwable =>
          error("Error while responding to offset request", e)
          (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(e), List[JLong]().asJava))
      }
    }
    responseMap ++ unauthorizedResponseStatus
  }

  private def handleListOffsetRequestV1AndAbove(request : RequestChannel.Request): Map[TopicPartition, ListOffsetResponse.PartitionData] = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) = offsetRequest.partitionTimestamps.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Describe, Resource(Topic, topicPartition.topic, LITERAL))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ => {
      new ListOffsetResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED,
        ListOffsetResponse.UNKNOWN_TIMESTAMP,
        ListOffsetResponse.UNKNOWN_OFFSET,
        Optional.empty())
    })

    val responseMap = authorizedRequestInfo.map { case (topicPartition, partitionData) =>
      if (offsetRequest.duplicatePartitions.contains(topicPartition)) {
        debug(s"OffsetRequest with correlation id $correlationId from client $clientId on partition $topicPartition " +
            s"failed because the partition is duplicated in the request.")
        (topicPartition, new ListOffsetResponse.PartitionData(Errors.INVALID_REQUEST,
          ListOffsetResponse.UNKNOWN_TIMESTAMP,
          ListOffsetResponse.UNKNOWN_OFFSET,
          Optional.empty()))
      } else {

        def buildErrorResponse(e: Errors): (TopicPartition, ListOffsetResponse.PartitionData) = {
          (topicPartition, new ListOffsetResponse.PartitionData(
            e,
            ListOffsetResponse.UNKNOWN_TIMESTAMP,
            ListOffsetResponse.UNKNOWN_OFFSET,
            Optional.empty()))
        }

        try {
          val fetchOnlyFromLeader = offsetRequest.replicaId != ListOffsetRequest.DEBUGGING_REPLICA_ID
          val isClientRequest = offsetRequest.replicaId == ListOffsetRequest.CONSUMER_REPLICA_ID
          val isolationLevelOpt = if (isClientRequest)
            Some(offsetRequest.isolationLevel)
          else
            None

          val foundOpt = replicaManager.fetchOffsetForTimestamp(topicPartition,
            partitionData.timestamp,
            isolationLevelOpt,
            partitionData.currentLeaderEpoch,
            fetchOnlyFromLeader)

          val response = foundOpt match {
            case Some(found) =>
              new ListOffsetResponse.PartitionData(Errors.NONE, found.timestamp, found.offset, found.leaderEpoch)
            case None =>
              new ListOffsetResponse.PartitionData(Errors.NONE, ListOffsetResponse.UNKNOWN_TIMESTAMP,
                ListOffsetResponse.UNKNOWN_OFFSET, Optional.empty())
          }
          (topicPartition, response)
        } catch {
          // NOTE: These exceptions are special cased since these error messages are typically transient or the client
          // would have received a clear exception and there is no value in logging the entire stack trace for the same
          case e @ (_ : UnknownTopicOrPartitionException |
                    _ : NotLeaderForPartitionException |
                    _ : UnknownLeaderEpochException |
                    _ : FencedLeaderEpochException |
                    _ : KafkaStorageException |
                    _ : UnsupportedForMessageFormatException) =>
            debug(s"Offset request with correlation id $correlationId from client $clientId on " +
                s"partition $topicPartition failed due to ${e.getMessage}")
            buildErrorResponse(Errors.forException(e))

          // Only V5 and newer ListOffset calls should get OFFSET_NOT_AVAILABLE
          case e: OffsetNotAvailableException =>
            if(request.header.apiVersion >= 5) {
              buildErrorResponse(Errors.forException(e))
            } else {
              buildErrorResponse(Errors.LEADER_NOT_AVAILABLE)
            }

          case e: Throwable =>
            error("Error while responding to offset request", e)
            buildErrorResponse(Errors.forException(e))
        }
      }
    }
    responseMap ++ unauthorizedResponseStatus
  }

  private def createTopic(topic: String,
                          numPartitions: Int,
                          replicationFactor: Int,
                          properties: Properties = new Properties()): MetadataResponse.TopicMetadata = {
    try {
      adminZkClient.createTopic(topic, numPartitions, replicationFactor, properties, RackAwareMode.Safe)
      info("Auto creation of topic %s with %d partitions and replication factor %d is successful"
        .format(topic, numPartitions, replicationFactor))
      new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, topic, isInternal(topic),
        java.util.Collections.emptyList())
    } catch {
      case _: TopicExistsException => // let it go, possibly another broker created this topic
        new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, topic, isInternal(topic),
          java.util.Collections.emptyList())
      case ex: Throwable  => // Catch all to prevent unhandled errors
        new MetadataResponse.TopicMetadata(Errors.forException(ex), topic, isInternal(topic),
          java.util.Collections.emptyList())
    }
  }

  private def createInternalTopic(topic: String): MetadataResponse.TopicMetadata = {
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
          new MetadataResponse.TopicMetadata(Errors.COORDINATOR_NOT_AVAILABLE, topic, true, java.util.Collections.emptyList())
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
          new MetadataResponse.TopicMetadata(Errors.COORDINATOR_NOT_AVAILABLE, topic, true, java.util.Collections.emptyList())
        } else {
          createTopic(topic, config.transactionTopicPartitions, config.transactionTopicReplicationFactor.toInt,
            txnCoordinator.transactionTopicConfigs)
        }
      case _ => throw new IllegalArgumentException(s"Unexpected internal topic name: $topic")
    }
  }

  private def getOrCreateInternalTopic(topic: String, listenerName: ListenerName): MetadataResponse.TopicMetadata = {
    val topicMetadata = metadataCache.getTopicMetadata(Set(topic), listenerName)
    topicMetadata.headOption.getOrElse(createInternalTopic(topic))
  }

  private def getTopicMetadata(allowAutoTopicCreation: Boolean, topics: Set[String], listenerName: ListenerName,
                               errorUnavailableEndpoints: Boolean,
                               errorUnavailableListeners: Boolean): Seq[MetadataResponse.TopicMetadata] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, listenerName,
        errorUnavailableEndpoints, errorUnavailableListeners)
    if (topics.isEmpty || topicResponses.size == topics.size) {
      topicResponses
    } else {
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet
      val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
        if (isInternal(topic)) {
          val topicMetadata = createInternalTopic(topic)
          if (topicMetadata.error == Errors.COORDINATOR_NOT_AVAILABLE)
            new MetadataResponse.TopicMetadata(Errors.INVALID_REPLICATION_FACTOR, topic, true, java.util.Collections.emptyList())
          else
            topicMetadata
        } else if (allowAutoTopicCreation && config.autoCreateTopicsEnable) {
          createTopic(topic, config.numPartitions, config.defaultReplicationFactor)
        } else {
          new MetadataResponse.TopicMetadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, false, java.util.Collections.emptyList())
        }
      }
      topicResponses ++ responsesForNonExistentTopics
    }
  }

  /**
   * Handle a topic metadata request
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.body[MetadataRequest]
    val requestVersion = request.header.apiVersion

    val topics = if (metadataRequest.isAllTopics)
      metadataCache.getAllTopics()
    else
      metadataRequest.topics.asScala.toSet

    var (authorizedTopics, unauthorizedForDescribeTopics) =
      topics.partition(topic => authorize(request.session, Describe, Resource(Topic, topic, LITERAL)))

    var unauthorizedForCreateTopics = Set[String]()

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = metadataCache.getNonExistingTopics(authorizedTopics)
      if (metadataRequest.allowAutoTopicCreation && config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        if (!authorize(request.session, Create, Resource.ClusterResource)) {
          unauthorizedForCreateTopics = nonExistingTopics.filter { topic =>
            !authorize(request.session, Create, new Resource(Topic, topic, PatternType.LITERAL))
          }
          authorizedTopics --= unauthorizedForCreateTopics
        }
      }
    }

    val unauthorizedForCreateTopicMetadata = unauthorizedForCreateTopics.map(topic =>
      new MetadataResponse.TopicMetadata(Errors.TOPIC_AUTHORIZATION_FAILED, topic, isInternal(topic),
        java.util.Collections.emptyList()))

    // do not disclose the existence of topics unauthorized for Describe, so we've not even checked if they exist or not
    val unauthorizedForDescribeTopicMetadata =
      // In case of all topics, don't include topics unauthorized for Describe
      if ((requestVersion == 0 && (metadataRequest.topics == null || metadataRequest.topics.isEmpty)) || metadataRequest.isAllTopics)
        Set.empty[MetadataResponse.TopicMetadata]
      else
        unauthorizedForDescribeTopics.map(topic =>
          new MetadataResponse.TopicMetadata(Errors.TOPIC_AUTHORIZATION_FAILED, topic, false, java.util.Collections.emptyList()))

    // In version 0, we returned an error when brokers with replicas were unavailable,
    // while in higher versions we simply don't include the broker in the returned broker list
    val errorUnavailableEndpoints = requestVersion == 0
    // In versions 5 and below, we returned LEADER_NOT_AVAILABLE if a matching listener was not found on the leader.
    // From version 6 onwards, we return LISTENER_NOT_FOUND to enable diagnosis of configuration errors.
    val errorUnavailableListeners = requestVersion >= 6
    val topicMetadata =
      if (authorizedTopics.isEmpty)
        Seq.empty[MetadataResponse.TopicMetadata]
      else
        getTopicMetadata(metadataRequest.allowAutoTopicCreation, authorizedTopics, request.context.listenerName,
          errorUnavailableEndpoints, errorUnavailableListeners)

    var clusterAuthorizedOperations = 0

    if (request.header.apiVersion >= 8) {
      // get cluster authorized operations
      if (metadataRequest.data().includeClusterAuthorizedOperations() &&
        authorize(request.session, Describe, Resource.ClusterResource))
        clusterAuthorizedOperations = authorizedOperations(request.session, Resource.ClusterResource)
      // get topic authorized operations
      if (metadataRequest.data().includeTopicAuthorizedOperations())
        topicMetadata.foreach(topicData => {
          topicData.authorizedOperations(authorizedOperations(request.session, Resource(Topic, topicData.topic(), LITERAL)))
        })
    }

    val completeTopicMetadata = topicMetadata ++ unauthorizedForCreateTopicMetadata ++ unauthorizedForDescribeTopicMetadata

    val brokers = metadataCache.getAliveBrokers

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))

    sendResponseMaybeThrottle(request, requestThrottleMs =>
       MetadataResponse.prepareResponse(
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
  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val header = request.header
    val offsetFetchRequest = request.body[OffsetFetchRequest]

    def authorizeTopicDescribe(partition: TopicPartition) =
      authorize(request.session, Describe, Resource(Topic, partition.topic, LITERAL))

    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val offsetFetchResponse =
        // reject the request if not authorized to the group
        if (!authorize(request.session, Describe, Resource(Group, offsetFetchRequest.groupId, LITERAL)))
          offsetFetchRequest.getErrorResponse(requestThrottleMs, Errors.GROUP_AUTHORIZATION_FAILED)
        else {
          if (header.apiVersion == 0) {
            val (authorizedPartitions, unauthorizedPartitions) = offsetFetchRequest.partitions.asScala
              .partition(authorizeTopicDescribe)

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
              val (error, allPartitionData) = groupCoordinator.handleFetchOffsets(offsetFetchRequest.groupId)
              if (error != Errors.NONE)
                offsetFetchRequest.getErrorResponse(requestThrottleMs, error)
              else {
                // clients are not allowed to see offsets for topics that are not authorized for Describe
                val authorizedPartitionData = allPartitionData.filter { case (topicPartition, _) => authorizeTopicDescribe(topicPartition) }
                new OffsetFetchResponse(requestThrottleMs, Errors.NONE, authorizedPartitionData.asJava)
              }
            } else {
              val (authorizedPartitions, unauthorizedPartitions) = offsetFetchRequest.partitions.asScala
                .partition(authorizeTopicDescribe)
              val (error, authorizedPartitionData) = groupCoordinator.handleFetchOffsets(offsetFetchRequest.groupId,
                Some(authorizedPartitions))
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

    sendResponseMaybeThrottle(request, createResponse)
  }

  def handleFindCoordinatorRequest(request: RequestChannel.Request) {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    if (findCoordinatorRequest.data.keyType == CoordinatorType.GROUP.id &&
        !authorize(request.session, Describe, Resource(Group, findCoordinatorRequest.data.key, LITERAL)))
      sendErrorResponseMaybeThrottle(request, Errors.GROUP_AUTHORIZATION_FAILED.exception)
    else if (findCoordinatorRequest.data.keyType == CoordinatorType.TRANSACTION.id &&
        !authorize(request.session, Describe, Resource(TransactionalId, findCoordinatorRequest.data.key, LITERAL)))
      sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
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
        val responseBody = if (topicMetadata.error != Errors.NONE) {
          createFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
        } else {
          val coordinatorEndpoint = topicMetadata.partitionMetadata.asScala
            .find(_.partition == partition)
            .map(_.leader)
            .flatMap(p => Option(p))

          coordinatorEndpoint match {
            case Some(endpoint) if !endpoint.isEmpty =>
              createFindCoordinatorResponse(Errors.NONE, endpoint)
            case _ =>
              createFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
          }
        }
        trace("Sending FindCoordinator response %s for correlation id %d to client %s."
          .format(responseBody, request.header.correlationId, request.header.clientId))
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }
  }

  def handleDescribeGroupRequest(request: RequestChannel.Request) {

    def sendResponseCallback(describeGroupsResponseData: DescribeGroupsResponseData): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        describeGroupsResponseData.setThrottleTimeMs(requestThrottleMs)
        new DescribeGroupsResponse(describeGroupsResponseData)
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    val describeRequest = request.body[DescribeGroupsRequest]
    val describeGroupsResponseData = new DescribeGroupsResponseData()

    describeRequest.data().groups().asScala.foreach { groupId =>
      val resource = Resource(Group, groupId, LITERAL)
      if (!authorize(request.session, Describe, resource)) {
        describeGroupsResponseData.groups().add(DescribeGroupsResponse.forError(groupId, Errors.GROUP_AUTHORIZATION_FAILED))
      } else {
        val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
        val members = summary.members.map { member =>
          new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId(member.memberId)
            .setClientId(member.clientId)
            .setClientHost(member.clientHost)
            .setMemberAssignment(member.assignment)
            .setMemberMetadata(member.assignment)
        }

        val describedGroup = new DescribeGroupsResponseData.DescribedGroup()
          .setErrorCode(error.code())
          .setGroupId(groupId)
          .setGroupState(summary.state)
          .setProtocolType(summary.protocolType)
          .setProtocolData(summary.protocol)
          .setMembers(members.asJava)

        if (request.header.apiVersion >= 3) {
          if (error == Errors.NONE && describeRequest.data().includeAuthorizedOperations()) {
            describedGroup.setAuthorizedOperations(authorizedOperations(request.session, resource))
          } else {
            describedGroup.setAuthorizedOperations(0)
          }
        }

        describeGroupsResponseData.groups().add(describedGroup)
      }
    }

    sendResponseCallback(describeGroupsResponseData)
  }


  private def authorizedOperations(session: RequestChannel.Session, resource: Resource): Int = {
    val authorizedOps = authorizer match {
      case None => resource.resourceType.supportedOperations
      case Some(auth) => resource.resourceType.supportedOperations
        .filter(operation => authorize(session, operation, resource))
    }

    Utils.to32BitField(authorizedOps.map(operation => operation.toJava.code().asInstanceOf[JByte]).asJava)
  }

  def handleListGroupsRequest(request: RequestChannel.Request) {
    val (error, groups) = groupCoordinator.handleListGroups()
    if (authorize(request.session, Describe, Resource.ClusterResource))
      // With describe cluster access all groups are returned. We keep this alternative for backward compatibility.
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ListGroupsResponse(requestThrottleMs, error, groups.map { group => new ListGroupsResponse.Group(group.groupId, group.protocolType) }.asJava))
    else {
      val filteredGroups = groups.filter(group => authorize(request.session, Describe, new Resource(Group, group.groupId, LITERAL)))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ListGroupsResponse(requestThrottleMs, error, filteredGroups.map { group => new ListGroupsResponse.Group(group.groupId, group.protocolType) }.asJava))
    }
  }

  def handleJoinGroupRequest(request: RequestChannel.Request) {
    val joinGroupRequest = request.body[JoinGroupRequest]

    // the callback for sending a join-group response
    def sendResponseCallback(joinResult: JoinGroupResult) {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseBody = new JoinGroupResponse(
          new JoinGroupResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(joinResult.error.code())
            .setGenerationId(joinResult.generationId)
            .setProtocolName(joinResult.subProtocol)
            .setLeader(joinResult.leaderId)
            .setMemberId(joinResult.memberId)
            .setMembers(joinResult.members.asJava)
        )

        trace("Sending join group response %s for correlation id %d to client %s."
          .format(responseBody, request.header.correlationId, request.header.clientId))
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (joinGroupRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      sendResponseCallback(JoinGroupResult(
        List.empty,
        JoinGroupResponse.UNKNOWN_MEMBER_ID,
        JoinGroupResponse.UNKNOWN_GENERATION_ID,
        JoinGroupResponse.UNKNOWN_PROTOCOL,
        JoinGroupResponse.UNKNOWN_MEMBER_ID,
        Errors.UNSUPPORTED_VERSION
      ))
    } else if (!authorize(request.session, Read, Resource(Group, joinGroupRequest.data().groupId(), LITERAL))) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new JoinGroupResponse(
          new JoinGroupResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
            .setGenerationId(JoinGroupResponse.UNKNOWN_GENERATION_ID)
            .setProtocolName(JoinGroupResponse.UNKNOWN_PROTOCOL)
            .setLeader(JoinGroupResponse.UNKNOWN_MEMBER_ID)
            .setMemberId(JoinGroupResponse.UNKNOWN_MEMBER_ID)
            .setMembers(Collections.emptyList())
        )
      )
    }  else {
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
        request.session.clientAddress.toString,
        joinGroupRequest.data.rebalanceTimeoutMs,
        joinGroupRequest.data.sessionTimeoutMs,
        joinGroupRequest.data.protocolType,
        protocols,
        sendResponseCallback)
    }
  }

  def handleSyncGroupRequest(request: RequestChannel.Request) {
    val syncGroupRequest = request.body[SyncGroupRequest]

    def sendResponseCallback(syncGroupResult: SyncGroupResult) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new SyncGroupResponse(
          new SyncGroupResponseData()
            .setErrorCode(syncGroupResult.error.code)
            .setAssignment(syncGroupResult.memberAssignment)
            .setThrottleTimeMs(requestThrottleMs)
        ))
    }

    if (syncGroupRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      sendResponseCallback(SyncGroupResult(Array[Byte](), Errors.UNSUPPORTED_VERSION))
    } else if (!authorize(request.session, Read, Resource(Group, syncGroupRequest.data.groupId, LITERAL))) {
      sendResponseCallback(SyncGroupResult(Array[Byte](), Errors.GROUP_AUTHORIZATION_FAILED))
    } else {
      val assignmentMap = immutable.Map.newBuilder[String, Array[Byte]]
      syncGroupRequest.data.assignments.asScala.foreach { assignment =>
        assignmentMap += (assignment.memberId -> assignment.assignment)
      }

      groupCoordinator.handleSyncGroup(
        syncGroupRequest.data.groupId,
        syncGroupRequest.data.generationId,
        syncGroupRequest.data.memberId,
        Option(syncGroupRequest.data.groupInstanceId),
        assignmentMap.result,
        sendResponseCallback
      )
    }
  }

  def handleDeleteGroupsRequest(request: RequestChannel.Request): Unit = {
    val deleteGroupsRequest = request.body[DeleteGroupsRequest]
    val groups = deleteGroupsRequest.groups.asScala.toSet

    val (authorizedGroups, unauthorizedGroups) = groups.partition { group =>
      authorize(request.session, Delete, Resource(Group, group, LITERAL))
    }

    val groupDeletionResult = groupCoordinator.handleDeleteGroups(authorizedGroups) ++
      unauthorizedGroups.map(_ -> Errors.GROUP_AUTHORIZATION_FAILED)

    sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DeleteGroupsResponse(requestThrottleMs, groupDeletionResult.asJava))
  }

  def handleHeartbeatRequest(request: RequestChannel.Request) {
    val heartbeatRequest = request.body[HeartbeatRequest]

    // the callback for sending a heartbeat response
    def sendResponseCallback(error: Errors) {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val response = new HeartbeatResponse(
            new HeartbeatResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(error.code))
        trace("Sending heartbeat response %s for correlation id %d to client %s."
          .format(response, request.header.correlationId, request.header.clientId))
        response
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (heartbeatRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // Only enable static membership when IBP >= 2.3, because it is not safe for the broker to use the static member logic
      // until we are sure that all brokers support it. If static group being loaded by an older coordinator, it will discard
      // the group.instance.id field, so static members could accidentally become "dynamic", which leads to wrong states.
      sendResponseCallback(Errors.UNSUPPORTED_VERSION)
    } else if (!authorize(request.session, Read, Resource(Group, heartbeatRequest.data.groupId, LITERAL))) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
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

  def handleLeaveGroupRequest(request: RequestChannel.Request) {
    val leaveGroupRequest = request.body[LeaveGroupRequest]

    // the callback for sending a leave-group response
    def sendResponseCallback(error: Errors) {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val response = new LeaveGroupResponse(new LeaveGroupResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setErrorCode(error.code()))
        trace("Sending leave group response %s for correlation id %d to client %s."
          .format(response, request.header.correlationId, request.header.clientId))
        response
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (!authorize(request.session, Read, Resource(Group, leaveGroupRequest.data().groupId(), LITERAL))) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new LeaveGroupResponse(new LeaveGroupResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code())))
    } else {
      // let the coordinator to handle leave-group
      groupCoordinator.handleLeaveGroup(
        leaveGroupRequest.data().groupId(),
        leaveGroupRequest.data().memberId(),
        sendResponseCallback)
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request) {
    val responseData = new SaslHandshakeResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
    sendResponseMaybeThrottle(request, _ => new SaslHandshakeResponse(responseData))
  }

  def handleSaslAuthenticateRequest(request: RequestChannel.Request) {
    val responseData = new SaslAuthenticateResponseData()
      .setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
      .setErrorMessage("SaslAuthenticate request received after successful authentication")
    sendResponseMaybeThrottle(request, _ => new SaslAuthenticateResponse(responseData))
  }

  def handleApiVersionsRequest(request: RequestChannel.Request) {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on a SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    def createResponseCallback(requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion)
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      else
        ApiVersionsResponse.apiVersionsResponse(requestThrottleMs,
          config.interBrokerProtocolVersion.recordVersion.value)
    }
    sendResponseMaybeThrottle(request, createResponseCallback)
  }

  def handleCreateTopicsRequest(request: RequestChannel.Request) {
    def sendResponseCallback(results: CreatableTopicResultCollection): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseData = new CreateTopicsResponseData().
          setThrottleTimeMs(requestThrottleMs).
          setTopics(results)
        val responseBody = new CreateTopicsResponse(responseData)
        trace(s"Sending create topics response $responseData for correlation id " +
          s"${request.header.correlationId} to client ${request.header.clientId}.")
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    val createTopicsRequest = request.body[CreateTopicsRequest]
    val results = new CreatableTopicResultCollection(createTopicsRequest.data().topics().size())
    if (!controller.isActive) {
      createTopicsRequest.data.topics.asScala.foreach { case topic =>
        results.add(new CreatableTopicResult().setName(topic.name()).
          setErrorCode(Errors.NOT_CONTROLLER.code()))
      }
      sendResponseCallback(results)
    } else {
      createTopicsRequest.data.topics.asScala.foreach { case topic =>
        results.add(new CreatableTopicResult().setName(topic.name()))
      }
      val hasClusterAuthorization = authorize(request.session, Create, Resource.ClusterResource)
      results.asScala.foreach(topic => {
        if (results.findAll(topic.name()).size() > 1) {
          topic.setErrorCode(Errors.INVALID_REQUEST.code())
          topic.setErrorMessage("Found multiple entries for this topic.")
        } else if ((!hasClusterAuthorization) && (!authorize(request.session, Create,
              new Resource(Topic, topic.name(), PatternType.LITERAL)))) {
          topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
          topic.setErrorMessage("Authorization failed.")
        }
      })
      val toCreate = mutable.Map[String, CreatableTopic]()
      createTopicsRequest.data.topics.asScala.foreach { case topic =>
        if (results.find(topic.name()).errorCode() == 0) {
          toCreate += topic.name() -> topic
        }
      }
      def handleCreateTopicsResults(errors: Map[String, ApiError]): Unit = {
        errors.foreach {
          case (topicName, error) =>
            results.find(topicName).
              setErrorCode(error.error().code()).
              setErrorMessage(error.message())
        }
        sendResponseCallback(results)
      }
      adminManager.createTopics(createTopicsRequest.data.timeoutMs(),
          createTopicsRequest.data.validateOnly(),
          toCreate,
          handleCreateTopicsResults)
    }
  }

  def handleCreatePartitionsRequest(request: RequestChannel.Request): Unit = {
    val createPartitionsRequest = request.body[CreatePartitionsRequest]

    def sendResponseCallback(results: Map[String, ApiError]): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseBody = new CreatePartitionsResponse(requestThrottleMs, results.asJava)
        trace(s"Sending create partitions response $responseBody for correlation id ${request.header.correlationId} to " +
          s"client ${request.header.clientId}.")
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (!controller.isActive) {
      val result = createPartitionsRequest.newPartitions.asScala.map { case (topic, _) =>
        (topic, new ApiError(Errors.NOT_CONTROLLER, null))
      }
      sendResponseCallback(result)
    } else {
      // Special handling to add duplicate topics to the response
      val dupes = createPartitionsRequest.duplicates.asScala
      val notDuped = createPartitionsRequest.newPartitions.asScala -- dupes
      val (authorized, unauthorized) = notDuped.partition { case (topic, _) =>
        authorize(request.session, Alter, Resource(Topic, topic, LITERAL))
      }

      val (queuedForDeletion, valid) = authorized.partition { case (topic, _) =>
        controller.topicDeletionManager.isTopicQueuedUpForDeletion(topic)
      }

      val errors = dupes.map(_ -> new ApiError(Errors.INVALID_REQUEST, "Duplicate topic in request.")) ++
        unauthorized.keySet.map(_ -> new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, "The topic authorization is failed.")) ++
        queuedForDeletion.keySet.map(_ -> new ApiError(Errors.INVALID_TOPIC_EXCEPTION, "The topic is queued for deletion."))

      adminManager.createPartitions(createPartitionsRequest.timeout, valid, createPartitionsRequest.validateOnly,
        request.context.listenerName, result => sendResponseCallback(result ++ errors))
    }
  }

  def handleDeleteTopicsRequest(request: RequestChannel.Request) {
    def sendResponseCallback(results: DeletableTopicResultCollection): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseData = new DeleteTopicsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setResponses(results)
        val responseBody = new DeleteTopicsResponse(responseData)
        trace(s"Sending delete topics response $responseBody for correlation id ${request.header.correlationId} to client ${request.header.clientId}.")
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    val deleteTopicRequest = request.body[DeleteTopicsRequest]
    val results = new DeletableTopicResultCollection(deleteTopicRequest.data.topicNames.size)
    val toDelete = mutable.Set[String]()
    if (!controller.isActive) {
      deleteTopicRequest.data.topicNames.asScala.foreach { case topic =>
        results.add(new DeletableTopicResult()
          .setName(topic)
          .setErrorCode(Errors.NOT_CONTROLLER.code))
      }
      sendResponseCallback(results)
    } else if (!config.deleteTopicEnable) {
      val error = if (request.context.apiVersion < 3) Errors.INVALID_REQUEST else Errors.TOPIC_DELETION_DISABLED
      deleteTopicRequest.data.topicNames.asScala.foreach { case topic =>
        results.add(new DeletableTopicResult()
          .setName(topic)
          .setErrorCode(error.code))
      }
      sendResponseCallback(results)
    } else {
      deleteTopicRequest.data.topicNames.asScala.foreach { case topic =>
        results.add(new DeletableTopicResult()
          .setName(topic))
      }
      results.asScala.foreach(topic => {
         if (!authorize(request.session, Delete, Resource(Topic, topic.name, LITERAL))) 
           topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
         else if (!metadataCache.contains(topic.name))
           topic.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
         else
           toDelete += topic.name
      })
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
          deleteTopicRequest.data.timeoutMs.toInt,
          toDelete,
          handleDeleteTopicsResults
        )
      }
    }
  }

  def handleDeleteRecordsRequest(request: RequestChannel.Request) {
    val deleteRecordsRequest = request.body[DeleteRecordsRequest]

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, DeleteRecordsResponse.PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, DeleteRecordsResponse.PartitionResponse]()
    val authorizedForDeleteTopicOffsets = mutable.Map[TopicPartition, Long]()

    for ((topicPartition, offset) <- deleteRecordsRequest.partitionOffsets.asScala) {
      if (!authorize(request.session, Delete, Resource(Topic, topicPartition.topic, LITERAL)))
        unauthorizedTopicResponses += topicPartition -> new DeleteRecordsResponse.PartitionResponse(
          DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new DeleteRecordsResponse.PartitionResponse(
          DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        authorizedForDeleteTopicOffsets += (topicPartition -> offset)
    }

    // the callback for sending a DeleteRecordsResponse
    def sendResponseCallback(authorizedTopicResponses: Map[TopicPartition, DeleteRecordsResponse.PartitionResponse]) {
      val mergedResponseStatus = authorizedTopicResponses ++ unauthorizedTopicResponses ++ nonExistingTopicResponses
      mergedResponseStatus.foreach { case (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          debug("DeleteRecordsRequest with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            status.error.exceptionName))
        }
      }

      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DeleteRecordsResponse(requestThrottleMs, mergedResponseStatus.asJava))
    }

    if (authorizedForDeleteTopicOffsets.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // call the replica manager to append messages to the replicas
      replicaManager.deleteRecords(
        deleteRecordsRequest.timeout.toLong,
        authorizedForDeleteTopicOffsets,
        sendResponseCallback)
    }
  }

  def handleInitProducerIdRequest(request: RequestChannel.Request): Unit = {
    val initProducerIdRequest = request.body[InitProducerIdRequest]
    val transactionalId = initProducerIdRequest.data.transactionalId

    if (transactionalId != null) {
      if (!authorize(request.session, Write, Resource(TransactionalId, transactionalId, LITERAL))) {
        sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    } else if (!authorize(request.session, IdempotentWrite, Resource.ClusterResource)) {
      sendErrorResponseMaybeThrottle(request, Errors.CLUSTER_AUTHORIZATION_FAILED.exception)
      return
    }

    def sendResponseCallback(result: InitProducerIdResult): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseData = new InitProducerIdResponseData()
          .setProducerId(result.producerId)
          .setProducerEpoch(result.producerEpoch)
          .setThrottleTimeMs(requestThrottleMs)
          .setErrorCode(result.error.code)
        val responseBody = new InitProducerIdResponse(responseData)
        trace(s"Completed $transactionalId's InitProducerIdRequest with result $result from client ${request.header.clientId}.")
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }
    txnCoordinator.handleInitProducerId(transactionalId, initProducerIdRequest.data.transactionTimeoutMs, sendResponseCallback)
  }

  def handleEndTxnRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val endTxnRequest = request.body[EndTxnRequest]
    val transactionalId = endTxnRequest.transactionalId

    if (authorize(request.session, Write, Resource(TransactionalId, transactionalId, LITERAL))) {
      def sendResponseCallback(error: Errors) {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val responseBody = new EndTxnResponse(requestThrottleMs, error)
          trace(s"Completed ${endTxnRequest.transactionalId}'s EndTxnRequest with command: ${endTxnRequest.command}, errors: $error from client ${request.header.clientId}.")
          responseBody
        }
        sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleEndTransaction(endTxnRequest.transactionalId,
        endTxnRequest.producerId,
        endTxnRequest.producerEpoch,
        endTxnRequest.command,
        sendResponseCallback)
    } else
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new EndTxnResponse(requestThrottleMs, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
  }

  def handleWriteTxnMarkersRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    authorizeClusterAction(request)
    val writeTxnMarkersRequest = request.body[WriteTxnMarkersRequest]
    val errors = new ConcurrentHashMap[java.lang.Long, util.Map[TopicPartition, Errors]]()
    val markers = writeTxnMarkersRequest.markers
    val numAppends = new AtomicInteger(markers.size)

    if (numAppends.get == 0) {
      sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
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
      val currentErrors = new ConcurrentHashMap[TopicPartition, Errors](responseStatus.mapValues(_.error).asJava)
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
        sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
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
      marker.partitions.asScala.foreach { partition =>
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
          isFromClient = false,
          entriesPerPartition = controlRecords,
          responseCallback = maybeSendResponseCallback(producerId, marker.transactionResult))
      }
    }

    // No log appends were written as all partitions had incorrect log format
    // so we need to send the error response
    if (skippedMarkers == markers.size())
      sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
  }

  def ensureInterBrokerVersion(version: ApiVersion): Unit = {
    if (config.interBrokerProtocolVersion < version)
      throw new UnsupportedVersionException(s"inter.broker.protocol.version: ${config.interBrokerProtocolVersion.version} is less than the required version: ${version.version}")
  }

  def handleAddPartitionToTxnRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val addPartitionsToTxnRequest = request.body[AddPartitionsToTxnRequest]
    val transactionalId = addPartitionsToTxnRequest.transactionalId
    val partitionsToAdd = addPartitionsToTxnRequest.partitions.asScala
    if (!authorize(request.session, Write, Resource(TransactionalId, transactionalId, LITERAL)))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        addPartitionsToTxnRequest.getErrorResponse(requestThrottleMs, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception))
    else {
      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedPartitions = mutable.Set[TopicPartition]()

      for (topicPartition <- partitionsToAdd) {
        if (org.apache.kafka.common.internals.Topic.isInternal(topicPartition.topic) ||
            !authorize(request.session, Write, Resource(Topic, topicPartition.topic, LITERAL)))
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
        sendResponseMaybeThrottle(request, requestThrottleMs =>
          new AddPartitionsToTxnResponse(requestThrottleMs, partitionErrors.asJava))
      } else {
        def sendResponseCallback(error: Errors): Unit = {
          def createResponse(requestThrottleMs: Int): AbstractResponse = {
            val responseBody: AddPartitionsToTxnResponse = new AddPartitionsToTxnResponse(requestThrottleMs,
              partitionsToAdd.map{tp => (tp, error)}.toMap.asJava)
            trace(s"Completed $transactionalId's AddPartitionsToTxnRequest with partitions $partitionsToAdd: errors: $error from client ${request.header.clientId}")
            responseBody
          }

          sendResponseMaybeThrottle(request, createResponse)
        }

        txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
          addPartitionsToTxnRequest.producerId,
          addPartitionsToTxnRequest.producerEpoch,
          authorizedPartitions,
          sendResponseCallback)
      }
    }
  }

  def handleAddOffsetsToTxnRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val addOffsetsToTxnRequest = request.body[AddOffsetsToTxnRequest]
    val transactionalId = addOffsetsToTxnRequest.transactionalId
    val groupId = addOffsetsToTxnRequest.consumerGroupId
    val offsetTopicPartition = new TopicPartition(GROUP_METADATA_TOPIC_NAME, groupCoordinator.partitionFor(groupId))

    if (!authorize(request.session, Write, Resource(TransactionalId, transactionalId, LITERAL)))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(requestThrottleMs, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
    else if (!authorize(request.session, Read, Resource(Group, groupId, LITERAL)))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(requestThrottleMs, Errors.GROUP_AUTHORIZATION_FAILED))
    else {
      def sendResponseCallback(error: Errors): Unit = {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val responseBody: AddOffsetsToTxnResponse = new AddOffsetsToTxnResponse(requestThrottleMs, error)
          trace(s"Completed $transactionalId's AddOffsetsToTxnRequest for group $groupId on partition " +
            s"$offsetTopicPartition: errors: $error from client ${request.header.clientId}")
          responseBody
        }
        sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
        addOffsetsToTxnRequest.producerId,
        addOffsetsToTxnRequest.producerEpoch,
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
    if (!authorize(request.session, Write, Resource(TransactionalId, txnOffsetCommitRequest.transactionalId, LITERAL)))
      sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
    else if (!authorize(request.session, Read, Resource(Group, txnOffsetCommitRequest.consumerGroupId, LITERAL)))
      sendErrorResponseMaybeThrottle(request, Errors.GROUP_AUTHORIZATION_FAILED.exception)
    else {
      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedTopicCommittedOffsets = mutable.Map[TopicPartition, TxnOffsetCommitRequest.CommittedOffset]()

      for ((topicPartition, commitedOffset) <- txnOffsetCommitRequest.offsets.asScala) {
        if (!authorize(request.session, Read, Resource(Topic, topicPartition.topic, LITERAL)))
          unauthorizedTopicErrors += topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED
        else if (!metadataCache.contains(topicPartition))
          nonExistingTopicErrors += topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION
        else
          authorizedTopicCommittedOffsets += (topicPartition -> commitedOffset)
      }

      // the callback for sending an offset commit response
      def sendResponseCallback(authorizedTopicErrors: Map[TopicPartition, Errors]) {
        val combinedCommitStatus = authorizedTopicErrors ++ unauthorizedTopicErrors ++ nonExistingTopicErrors
        if (isDebugEnabled)
          combinedCommitStatus.foreach { case (topicPartition, error) =>
            if (error != Errors.NONE) {
              debug(s"TxnOffsetCommit with correlation id ${header.correlationId} from client ${header.clientId} " +
                s"on partition $topicPartition failed due to ${error.exceptionName}")
            }
          }
        sendResponseMaybeThrottle(request, requestThrottleMs =>
          new TxnOffsetCommitResponse(requestThrottleMs, combinedCommitStatus.asJava))
      }

      if (authorizedTopicCommittedOffsets.isEmpty)
        sendResponseCallback(Map.empty)
      else {
        val offsetMetadata = convertTxnOffsets(authorizedTopicCommittedOffsets.toMap)
        groupCoordinator.handleTxnCommitOffsets(
          txnOffsetCommitRequest.consumerGroupId,
          txnOffsetCommitRequest.producerId,
          txnOffsetCommitRequest.producerEpoch,
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
    authorizeClusterDescribe(request)
    val describeAclsRequest = request.body[DescribeAclsRequest]
    authorizer match {
      case None =>
        sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(requestThrottleMs,
            new ApiError(Errors.SECURITY_DISABLED, "No Authorizer is configured on the broker"), Collections.emptySet()))
      case Some(auth) =>
        val filter = describeAclsRequest.filter()
        val returnedAcls = auth.getAcls.toSeq.flatMap { case (resource, acls) =>
          acls.flatMap { acl =>
            val fixture = new AclBinding(new ResourcePattern(resource.resourceType.toJava, resource.name, resource.patternType),
                new AccessControlEntry(acl.principal.toString, acl.host.toString, acl.operation.toJava, acl.permissionType.toJava))
            Some(fixture).filter(filter.matches)
          }
        }
        sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(requestThrottleMs, ApiError.NONE, returnedAcls.asJava))
    }
  }

  def handleCreateAcls(request: RequestChannel.Request): Unit = {
    authorizeClusterAlter(request)
    val createAclsRequest = request.body[CreateAclsRequest]
    authorizer match {
      case None =>
        sendResponseMaybeThrottle(request, requestThrottleMs =>
          createAclsRequest.getErrorResponse(requestThrottleMs,
            new SecurityDisabledException("No Authorizer is configured on the broker.")))
      case Some(auth) =>
        val aclCreationResults = createAclsRequest.aclCreations.asScala.map { aclCreation =>
          SecurityUtils.convertToResourceAndAcl(aclCreation.acl.toFilter) match {
            case Left(apiError) => new AclCreationResponse(apiError)
            case Right((resource, acl)) => try {
                if (resource.resourceType.equals(Cluster) && !SecurityUtils.isClusterResource(resource.name))
                  throw new InvalidRequestException("The only valid name for the CLUSTER resource is " +
                      Resource.ClusterResourceName)
                if (resource.name.isEmpty)
                  throw new InvalidRequestException("Invalid empty resource name")
                auth.addAcls(immutable.Set(acl), resource)

                debug(s"Added acl $acl to $resource")

                new AclCreationResponse(ApiError.NONE)
              } catch {
                case throwable: Throwable =>
                  debug(s"Failed to add acl $acl to $resource", throwable)
                  new AclCreationResponse(ApiError.fromThrowable(throwable))
              }
          }
        }
        sendResponseMaybeThrottle(request, requestThrottleMs =>
          new CreateAclsResponse(requestThrottleMs, aclCreationResults.asJava))
    }
  }

  def handleDeleteAcls(request: RequestChannel.Request): Unit = {
    authorizeClusterAlter(request)
    val deleteAclsRequest = request.body[DeleteAclsRequest]
    authorizer match {
      case None =>
        sendResponseMaybeThrottle(request, requestThrottleMs =>
          deleteAclsRequest.getErrorResponse(requestThrottleMs,
            new SecurityDisabledException("No Authorizer is configured on the broker.")))
      case Some(auth) =>
        val filters = deleteAclsRequest.filters.asScala
        val filterResponseMap = mutable.Map[Int, AclFilterResponse]()
        val toDelete = mutable.Map[Int, ArrayBuffer[(Resource, Acl)]]()

        if (filters.forall(_.matchesAtMostOne)) {
          // Delete based on a list of ACL fixtures.
          for ((filter, i) <- filters.zipWithIndex) {
            SecurityUtils.convertToResourceAndAcl(filter) match {
              case Left(apiError) => filterResponseMap.put(i, new AclFilterResponse(apiError, Seq.empty.asJava))
              case Right(binding) => toDelete.put(i, ArrayBuffer(binding))
            }
          }
        } else {
          // Delete based on filters that may match more than one ACL.
          val aclMap = auth.getAcls()
          val filtersWithIndex = filters.zipWithIndex
          for ((resource, acls) <- aclMap; acl <- acls) {
            val binding = new AclBinding(
              new ResourcePattern(resource.resourceType.toJava, resource.name, resource.patternType),
              new AccessControlEntry(acl.principal.toString, acl.host.toString, acl.operation.toJava,
                acl.permissionType.toJava))

            for ((filter, i) <- filtersWithIndex if filter.matches(binding))
              toDelete.getOrElseUpdate(i, ArrayBuffer.empty) += ((resource, acl))
          }
        }

        for ((i, acls) <- toDelete) {
          val deletionResults = acls.flatMap { case (resource, acl) =>
            val aclBinding = SecurityUtils.convertToAclBinding(resource, acl)
            try {
              if (auth.removeAcls(immutable.Set(acl), resource))
                Some(new AclDeletionResult(aclBinding))
              else None
            } catch {
              case throwable: Throwable =>
                Some(new AclDeletionResult(ApiError.fromThrowable(throwable), aclBinding))
            }
          }.asJava

          filterResponseMap.put(i, new AclFilterResponse(deletionResults))
        }

        val filterResponses = filters.indices.map { i =>
          filterResponseMap.getOrElse(i, new AclFilterResponse(Seq.empty.asJava))
        }.asJava
        sendResponseMaybeThrottle(request, requestThrottleMs => new DeleteAclsResponse(requestThrottleMs, filterResponses))
    }
  }

  def handleOffsetForLeaderEpochRequest(request: RequestChannel.Request): Unit = {
    val offsetForLeaderEpoch = request.body[OffsetsForLeaderEpochRequest]
    val requestInfo = offsetForLeaderEpoch.epochsByTopicPartition.asScala

    // The OffsetsForLeaderEpoch API was initially only used for inter-broker communication and required
    // cluster permission. With KIP-320, the consumer now also uses this API to check for log truncation
    // following a leader change, so we also allow topic describe permission.
    val (authorizedPartitions, unauthorizedPartitions) = if (isAuthorizedClusterAction(request)) {
      (requestInfo, Map.empty[TopicPartition, OffsetsForLeaderEpochRequest.PartitionData])
    } else {
      requestInfo.partition {
        case (tp, _) => authorize(request.session, Describe, Resource(Topic, tp.topic, LITERAL))
      }
    }

    val endOffsetsForAuthorizedPartitions = replicaManager.lastOffsetForLeaderEpoch(authorizedPartitions)
    val endOffsetsForUnauthorizedPartitions = unauthorizedPartitions.mapValues(_ =>
      new EpochEndOffset(Errors.TOPIC_AUTHORIZATION_FAILED, EpochEndOffset.UNDEFINED_EPOCH,
        EpochEndOffset.UNDEFINED_EPOCH_OFFSET))

    val endOffsetsForAllPartitions = endOffsetsForAuthorizedPartitions ++ endOffsetsForUnauthorizedPartitions
    sendResponseMaybeThrottle(request, requestThrottleMs =>
      new OffsetsForLeaderEpochResponse(requestThrottleMs, endOffsetsForAllPartitions.asJava))
  }

  def handleAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val alterConfigsRequest = request.body[AlterConfigsRequest]
    val (authorizedResources, unauthorizedResources) = alterConfigsRequest.configs.asScala.partition { case (resource, _) =>
      resource.`type` match {
        case ConfigResource.Type.BROKER =>
          authorize(request.session, AlterConfigs, Resource.ClusterResource)
        case ConfigResource.Type.TOPIC =>
          authorize(request.session, AlterConfigs, Resource(Topic, resource.name, LITERAL))
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt")
      }
    }
    val authorizedResult = adminManager.alterConfigs(authorizedResources, alterConfigsRequest.validateOnly)
    val unauthorizedResult = unauthorizedResources.keys.map { resource =>
      resource -> configsAuthorizationApiError(request.session, resource)
    }
    sendResponseMaybeThrottle(request, requestThrottleMs =>
      new AlterConfigsResponse(requestThrottleMs, (authorizedResult ++ unauthorizedResult).asJava))
  }

  private def configsAuthorizationApiError(session: RequestChannel.Session, resource: ConfigResource): ApiError = {
    val error = resource.`type` match {
      case ConfigResource.Type.BROKER => Errors.CLUSTER_AUTHORIZATION_FAILED
      case ConfigResource.Type.TOPIC => Errors.TOPIC_AUTHORIZATION_FAILED
      case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.name}")
    }
    new ApiError(error, null)
  }

  def handleIncrementalAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val alterConfigsRequest = request.body[IncrementalAlterConfigsRequest]

    val configs = alterConfigsRequest.data().resources().iterator().asScala.map { alterConfigResource =>
      val configResource = new ConfigResource(ConfigResource.Type.forId(alterConfigResource.resourceType()), alterConfigResource.resourceName())
      configResource -> alterConfigResource.configs().iterator().asScala.map {
        alterConfig => new AlterConfigOp(new ConfigEntry(alterConfig.name(), alterConfig.value()), OpType.forId(alterConfig.configOperation())) }.toList
    }.toMap

    val (authorizedResources, unauthorizedResources) = configs.partition { case (resource, _) =>
      resource.`type` match {
        case ConfigResource.Type.BROKER =>
          authorize(request.session, AlterConfigs, Resource.ClusterResource)
        case ConfigResource.Type.TOPIC =>
          authorize(request.session, AlterConfigs, Resource(Topic, resource.name, LITERAL))
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt")
      }
    }

    val authorizedResult = adminManager.incrementalAlterConfigs(authorizedResources, alterConfigsRequest.data().validateOnly())
    val unauthorizedResult = unauthorizedResources.keys.map { resource =>
      resource -> configsAuthorizationApiError(request.session, resource)
    }
    sendResponseMaybeThrottle(request, requestThrottleMs =>
      new IncrementalAlterConfigsResponse(IncrementalAlterConfigsResponse.toResponseData(requestThrottleMs,
        (authorizedResult ++ unauthorizedResult).asJava)))
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): Unit = {
    val describeConfigsRequest = request.body[DescribeConfigsRequest]
    val (authorizedResources, unauthorizedResources) = describeConfigsRequest.resources.asScala.partition { resource =>
      resource.`type` match {
        case ConfigResource.Type.BROKER => authorize(request.session, DescribeConfigs, Resource.ClusterResource)
        case ConfigResource.Type.TOPIC =>
          authorize(request.session, DescribeConfigs, Resource(Topic, resource.name, LITERAL))
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.name}")
      }
    }
    val authorizedConfigs = adminManager.describeConfigs(authorizedResources.map { resource =>
      resource -> Option(describeConfigsRequest.configNames(resource)).map(_.asScala.toSet)
    }.toMap, describeConfigsRequest.includeSynonyms)
    val unauthorizedConfigs = unauthorizedResources.map { resource =>
      val error = configsAuthorizationApiError(request.session, resource)
      resource -> new DescribeConfigsResponse.Config(error, Collections.emptyList[DescribeConfigsResponse.ConfigEntry])
    }

    sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeConfigsResponse(requestThrottleMs, (authorizedConfigs ++ unauthorizedConfigs).asJava))
  }

  def handleAlterReplicaLogDirsRequest(request: RequestChannel.Request): Unit = {
    val alterReplicaDirsRequest = request.body[AlterReplicaLogDirsRequest]
    val responseMap = {
      if (authorize(request.session, Alter, Resource.ClusterResource))
        replicaManager.alterReplicaLogDirs(alterReplicaDirsRequest.partitionDirs.asScala)
      else
        alterReplicaDirsRequest.partitionDirs.asScala.keys.map((_, Errors.CLUSTER_AUTHORIZATION_FAILED)).toMap
    }
    sendResponseMaybeThrottle(request, requestThrottleMs => new AlterReplicaLogDirsResponse(requestThrottleMs, responseMap.asJava))
  }

  def handleDescribeLogDirsRequest(request: RequestChannel.Request): Unit = {
    val describeLogDirsDirRequest = request.body[DescribeLogDirsRequest]
    val logDirInfos = {
      if (authorize(request.session, Describe, Resource.ClusterResource)) {
        val partitions =
          if (describeLogDirsDirRequest.isAllTopicPartitions)
            replicaManager.logManager.allLogs.map(_.topicPartition).toSet
          else
            describeLogDirsDirRequest.topicPartitions().asScala

        replicaManager.describeLogDirs(partitions)
      } else {
        Map.empty[String, LogDirInfo]
      }
    }
    sendResponseMaybeThrottle(request, throttleTimeMs => new DescribeLogDirsResponse(throttleTimeMs, logDirInfos.asJava))
  }

  def handleCreateTokenRequest(request: RequestChannel.Request) {
    val createTokenRequest = request.body[CreateDelegationTokenRequest]

    // the callback for sending a create token response
    def sendResponseCallback(createResult: CreateTokenResult) {
      trace("Sending create token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new CreateDelegationTokenResponse(requestThrottleMs, createResult.error, request.session.principal, createResult.issueTimestamp,
          createResult.expiryTimestamp, createResult.maxTimestamp, createResult.tokenId, ByteBuffer.wrap(createResult.hmac)))
    }

    if (!allowTokenRequests(request))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new CreateDelegationTokenResponse(requestThrottleMs, Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, request.session.principal))
    else {
      val renewerList = createTokenRequest.renewers().asScala.toList

      if (renewerList.exists(principal =>  principal.getPrincipalType != KafkaPrincipal.USER_TYPE)) {
        sendResponseMaybeThrottle(request, requestThrottleMs =>
          new CreateDelegationTokenResponse(requestThrottleMs, Errors.INVALID_PRINCIPAL_TYPE, request.session.principal))
      }
      else {
        tokenManager.createToken(
          request.session.principal,
          createTokenRequest.renewers().asScala.toList,
          createTokenRequest.maxLifeTime(),
          sendResponseCallback
        )
      }
    }
  }

  def handleRenewTokenRequest(request: RequestChannel.Request) {
    val renewTokenRequest = request.body[RenewDelegationTokenRequest]

    // the callback for sending a renew token response
    def sendResponseCallback(error: Errors, expiryTimestamp: Long) {
      trace("Sending renew token response %s for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new RenewDelegationTokenResponse(requestThrottleMs, error, expiryTimestamp))
    }

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, DelegationTokenManager.ErrorTimestamp)
    else {
      tokenManager.renewToken(
        request.session.principal,
        renewTokenRequest.hmac,
        renewTokenRequest.renewTimePeriod(),
        sendResponseCallback
      )
    }
  }

  def handleExpireTokenRequest(request: RequestChannel.Request) {
    val expireTokenRequest = request.body[ExpireDelegationTokenRequest]

    // the callback for sending a expire token response
    def sendResponseCallback(error: Errors, expiryTimestamp: Long) {
      trace("Sending expire token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ExpireDelegationTokenResponse(requestThrottleMs, error, expiryTimestamp))
    }

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, DelegationTokenManager.ErrorTimestamp)
    else {
      tokenManager.expireToken(
        request.session.principal,
        expireTokenRequest.hmac(),
        expireTokenRequest.expiryTimePeriod(),
        sendResponseCallback
      )
    }
  }

  def handleDescribeTokensRequest(request: RequestChannel.Request) {
    val describeTokenRequest = request.body[DescribeDelegationTokenRequest]

    // the callback for sending a describe token response
    def sendResponseCallback(error: Errors, tokenDetails: List[DelegationToken]) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeDelegationTokenResponse(requestThrottleMs, error, tokenDetails.asJava))
      trace("Sending describe token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
    }

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, List.empty)
    else if (!config.tokenAuthEnabled)
      sendResponseCallback(Errors.DELEGATION_TOKEN_AUTH_DISABLED, List.empty)
    else {
      val requestPrincipal = request.session.principal

      if (describeTokenRequest.ownersListEmpty()) {
        sendResponseCallback(Errors.NONE, List())
      }
      else {
        val owners = if (describeTokenRequest.owners == null) None else Some(describeTokenRequest.owners.asScala.toList)
        def authorizeToken(tokenId: String) = authorize(request.session, Describe, Resource(kafka.security.auth.DelegationToken, tokenId, LITERAL))
        def eligible(token: TokenInformation) = DelegationTokenManager.filterToken(requestPrincipal, owners, token, authorizeToken)
        val tokens =  tokenManager.getTokens(eligible)
        sendResponseCallback(Errors.NONE, tokens)
      }
    }
  }

  def allowTokenRequests(request: RequestChannel.Request): Boolean = {
    val protocol = request.context.securityProtocol
    if (request.session.principal.tokenAuthenticated ||
      protocol == SecurityProtocol.PLAINTEXT ||
      // disallow requests from 1-way SSL
      (protocol == SecurityProtocol.SSL && request.session.principal == KafkaPrincipal.ANONYMOUS))
      false
    else
      true
  }

  def handleElectPreferredReplicaLeader(request: RequestChannel.Request): Unit = {

    val electionRequest = request.body[ElectPreferredLeadersRequest]
    val partitions =
      if (electionRequest.data().topicPartitions() == null) {
        metadataCache.getAllPartitions()
      } else {
        electionRequest.data().topicPartitions().asScala.flatMap{tp =>
          tp.partitionId().asScala.map(partitionId => new TopicPartition(tp.topic, partitionId))}.toSet
      }
    def sendResponseCallback(result: Map[TopicPartition, ApiError]): Unit = {
      sendResponseMaybeThrottle(request, requestThrottleMs => {
        val results = result.
          groupBy{case (tp, error) => tp.topic}.
          map{case (topic, ps) => new ElectPreferredLeadersResponseData.ReplicaElectionResult()
            .setTopic(topic)
            .setPartitionResult(ps.map{
            case (tp, error) =>
              new ElectPreferredLeadersResponseData.PartitionResult()
                .setErrorCode(error.error.code)
                .setErrorMessage(error.message())
                .setPartitionId(tp.partition)}.toList.asJava)}
        val data = new ElectPreferredLeadersResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setReplicaElectionResults(results.toList.asJava)
        new ElectPreferredLeadersResponse(data)})
    }
    if (!authorize(request.session, Alter, Resource.ClusterResource)) {
      val error = new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, null);
      val partitionErrors =
      if (electionRequest.data().topicPartitions() == null) {
        // Don't leak the set of partitions if the client lack authz
        Map.empty[TopicPartition, ApiError]
      } else {
        partitions.map(partition => partition -> error).toMap
      }
      sendResponseCallback(partitionErrors)
    } else {
      replicaManager.electPreferredLeaders(controller, partitions, sendResponseCallback, electionRequest.data().timeoutMs())
    }
  }

  def authorizeClusterAction(request: RequestChannel.Request): Unit = {
    if (!isAuthorizedClusterAction(request))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }

  private def isAuthorizedClusterAction(request: RequestChannel.Request): Boolean = {
    authorize(request.session, ClusterAction, Resource.ClusterResource)
  }

  def authorizeClusterAlter(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, Alter, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }

  def authorizeClusterDescribe(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, Describe, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
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

  private def handleError(request: RequestChannel.Request, e: Throwable) {
    val mayThrottle = e.isInstanceOf[ClusterAuthorizationException] || !request.header.apiKey.clusterAction
    error("Error when handling request: " +
      s"clientId=${request.header.clientId}, " +
      s"correlationId=${request.header.correlationId}, " +
      s"api=${request.header.apiKey}, " +
      s"body=${request.body[AbstractRequest]}", e)
    if (mayThrottle)
      sendErrorResponseMaybeThrottle(request, e)
    else
      sendErrorResponseExemptThrottle(request, e)
  }

  // Throttle the channel if the request quota is enabled but has been violated. Regardless of throttling, send the
  // response immediately.
  private def sendResponseMaybeThrottle(request: RequestChannel.Request,
                                        createResponse: Int => AbstractResponse,
                                        onComplete: Option[Send => Unit] = None): Unit = {
    val throttleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request)
    quotas.request.throttle(request, throttleTimeMs, sendResponse)
    sendResponse(request, Some(createResponse(throttleTimeMs)), onComplete)
  }

  private def sendErrorResponseMaybeThrottle(request: RequestChannel.Request, error: Throwable) {
    val throttleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request)
    quotas.request.throttle(request, throttleTimeMs, sendResponse)
    sendErrorOrCloseConnection(request, error, throttleTimeMs)
  }

  private def sendResponseExemptThrottle(request: RequestChannel.Request,
                                         response: AbstractResponse,
                                         onComplete: Option[Send => Unit] = None): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendResponse(request, Some(response), onComplete)
  }

  private def sendErrorResponseExemptThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendErrorOrCloseConnection(request, error, 0)
  }

  private def sendErrorOrCloseConnection(request: RequestChannel.Request, error: Throwable, throttleMs: Int): Unit = {
    val requestBody = request.body[AbstractRequest]
    val response = requestBody.getErrorResponse(throttleMs, error)
    if (response == null)
      closeConnection(request, requestBody.errorCounts(error))
    else
      sendResponse(request, Some(response), None)
  }

  private def sendNoOpResponseExemptThrottle(request: RequestChannel.Request): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendResponse(request, None, None)
  }

  private def closeConnection(request: RequestChannel.Request, errorCounts: java.util.Map[Errors, Integer]): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    requestChannel.updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    requestChannel.sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  private def sendResponse(request: RequestChannel.Request,
                           responseOpt: Option[AbstractResponse],
                           onComplete: Option[Send => Unit]): Unit = {
    // Update error metrics for each error code in the response including Errors.NONE
    responseOpt.foreach(response => requestChannel.updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala))

    val response = responseOpt match {
      case Some(response) =>
        val responseSend = request.context.buildResponse(response)
        val responseString =
          if (RequestChannel.isRequestLoggingEnabled) Some(response.toString(request.context.apiVersion))
          else None
        new RequestChannel.SendResponse(request, responseSend, responseString, onComplete)
      case None =>
        new RequestChannel.NoOpResponse(request)
    }
    sendResponse(response)
  }

  private def sendResponse(response: RequestChannel.Response): Unit = {
    requestChannel.sendResponse(response)
  }

  private def isBrokerEpochStale(brokerEpochInRequest: Long): Boolean = {
    // Broker epoch in LeaderAndIsr/UpdateMetadata/StopReplica request is unknown
    // if the controller hasn't been upgraded to use KIP-380
    if (brokerEpochInRequest == AbstractControlRequest.UNKNOWN_BROKER_EPOCH) false
    else {
      val curBrokerEpoch = controller.brokerEpoch
      if (brokerEpochInRequest < curBrokerEpoch) true
      else if (brokerEpochInRequest == curBrokerEpoch) false
      else throw new IllegalStateException(s"Epoch $brokerEpochInRequest larger than current broker epoch $curBrokerEpoch")
    }
  }

}
