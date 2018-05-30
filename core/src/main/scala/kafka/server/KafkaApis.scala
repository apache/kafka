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

import java.nio.ByteBuffer
import java.lang.{Long => JLong}
import java.util.{Collections, Properties}
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.api.{ApiVersion, KAFKA_0_11_0_IV0}
import kafka.cluster.Partition
import kafka.common.{OffsetAndMetadata, OffsetMetadata}
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.controller.KafkaController
import kafka.coordinator.group.{GroupCoordinator, JoinGroupResult}
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.log.{Log, LogManager, TimestampOffset}
import kafka.network.RequestChannel
import kafka.network.RequestChannel.{CloseConnectionAction, NoOpAction, SendAction}
import kafka.security.SecurityUtils
import kafka.security.auth.{Resource, _}
import kafka.utils.{CoreUtils, Logging}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, isInternal}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{ControlRecordType, EndTransactionMarker, MemoryRecords, RecordBatch, RecordsProcessingStats}
import org.apache.kafka.common.requests.CreateAclsResponse.AclCreationResponse
import org.apache.kafka.common.requests.DeleteAclsResponse.{AclDeletionResult, AclFilterResponse}
import org.apache.kafka.common.requests.{Resource => RResource, ResourceType => RResourceType, _}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.requests.{SaslAuthenticateResponse, SaslHandshakeResponse}
import org.apache.kafka.common.resource.{Resource => AdminResource}
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding}
import DescribeLogDirsResponse.LogDirInfo
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}

import scala.collection._
import scala.collection.JavaConverters._
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

    if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
      val response = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest, onLeadershipChange)
      sendResponseExemptThrottle(request, response)
    } else {
      sendResponseMaybeThrottle(request, throttleTimeMs => leaderAndIsrRequest.getErrorResponse(throttleTimeMs,
        Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.body[StopReplicaRequest]

    if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
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
    } else {
      val result = stopReplicaRequest.partitions.asScala.map((_, Errors.CLUSTER_AUTHORIZATION_FAILED)).toMap
      sendResponseMaybeThrottle(request, _ =>
        new StopReplicaResponse(Errors.CLUSTER_AUTHORIZATION_FAILED, result.asJava))
    }

    CoreUtils.swallow(replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads(), this)
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val correlationId = request.header.correlationId
    val updateMetadataRequest = request.body[UpdateMetadataRequest]

    if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
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
      sendResponseExemptThrottle(request, new UpdateMetadataResponse(Errors.NONE))
    } else {
      sendResponseMaybeThrottle(request, _ => new UpdateMetadataResponse(Errors.CLUSTER_AUTHORIZATION_FAILED))
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
          new ControlledShutdownResponse(Errors.NONE, partitionsRemaining.asJava)

        case Failure(throwable) =>
          controlledShutdownRequest.getErrorResponse(throwable)
      }
      sendResponseExemptThrottle(request, response)
    }
    controller.controlledShutdown(controlledShutdownRequest.brokerId, controlledShutdownCallback)
  }

  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val header = request.header
    val offsetCommitRequest = request.body[OffsetCommitRequest]

    // reject the request if not authorized to the group
    if (!authorize(request.session, Read, new Resource(Group, offsetCommitRequest.groupId))) {
      val error = Errors.GROUP_AUTHORIZATION_FAILED
      val results = offsetCommitRequest.offsetData.keySet.asScala.map { topicPartition =>
        (topicPartition, error)
      }.toMap
      sendResponseMaybeThrottle(request, requestThrottleMs => new OffsetCommitResponse(requestThrottleMs, results.asJava))
    } else {

      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedTopicRequestInfoBldr = immutable.Map.newBuilder[TopicPartition, OffsetCommitRequest.PartitionData]

      for ((topicPartition, partitionData) <- offsetCommitRequest.offsetData.asScala) {
        if (!authorize(request.session, Read, new Resource(Topic, topicPartition.topic)))
          unauthorizedTopicErrors += (topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicPartition))
          nonExistingTopicErrors += (topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          authorizedTopicRequestInfoBldr += (topicPartition -> partitionData)
      }

      val authorizedTopicRequestInfo = authorizedTopicRequestInfoBldr.result()

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

      if (authorizedTopicRequestInfo.isEmpty)
        sendResponseCallback(Map.empty)
      else if (header.apiVersion == 0) {
        // for version 0 always store offsets to ZK
        val responseInfo = authorizedTopicRequestInfo.map {
          case (topicPartition, partitionData) =>
            try {
              if (partitionData.metadata != null && partitionData.metadata.length > config.offsetMetadataMaxSize)
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE)
              else {
                zkClient.setOrCreateConsumerOffset(offsetCommitRequest.groupId, topicPartition, partitionData.offset)
                (topicPartition, Errors.NONE)
              }
            } catch {
              case e: Throwable => (topicPartition, Errors.forException(e))
            }
        }
        sendResponseCallback(responseInfo)
      } else {
        // for version 1 and beyond store offsets in offset manager

        // compute the retention time based on the request version:
        // if it is v1 or not specified by user, we can use the default retention
        val offsetRetention =
          if (header.apiVersion <= 1 ||
            offsetCommitRequest.retentionTime == OffsetCommitRequest.DEFAULT_RETENTION_TIME)
            groupCoordinator.offsetConfig.offsetsRetentionMs
          else
            offsetCommitRequest.retentionTime

        // commit timestamp is always set to now.
        // "default" expiration timestamp is now + retention (and retention may be overridden if v2)
        // expire timestamp is computed differently for v1 and v2.
        //   - If v1 and no explicit commit timestamp is provided we use default expiration timestamp.
        //   - If v1 and explicit commit timestamp is provided we calculate retention from that explicit commit timestamp
        //   - If v2 we use the default expiration timestamp
        val currentTimestamp = time.milliseconds
        val defaultExpireTimestamp = offsetRetention + currentTimestamp
        val partitionData = authorizedTopicRequestInfo.mapValues { partitionData =>
          val metadata = if (partitionData.metadata == null) OffsetMetadata.NoMetadata else partitionData.metadata
          new OffsetAndMetadata(
            offsetMetadata = OffsetMetadata(partitionData.offset, metadata),
            commitTimestamp = currentTimestamp,
            expireTimestamp = {
              if (partitionData.timestamp == OffsetCommitRequest.DEFAULT_TIMESTAMP)
                defaultExpireTimestamp
              else
                offsetRetention + partitionData.timestamp
            }
          )
        }

        // call coordinator to handle commit offset
        groupCoordinator.handleCommitOffsets(
          offsetCommitRequest.groupId,
          offsetCommitRequest.memberId,
          offsetCommitRequest.generationId,
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

    if (produceRequest.isTransactional) {
      if (!authorize(request.session, Write, new Resource(TransactionalId, produceRequest.transactionalId))) {
        sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
      // Note that authorization to a transactionalId implies ProducerId authorization

    } else if (produceRequest.isIdempotent && !authorize(request.session, IdempotentWrite, Resource.ClusterResource)) {
      sendErrorResponseMaybeThrottle(request, Errors.CLUSTER_AUTHORIZATION_FAILED.exception)
      return
    }

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val authorizedRequestInfo = mutable.Map[TopicPartition, MemoryRecords]()

    for ((topicPartition, memoryRecords) <- produceRequest.partitionRecordsOrFail.asScala) {
      if (!authorize(request.session, Write, new Resource(Topic, topicPartition.topic)))
        unauthorizedTopicResponses += topicPartition -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        authorizedRequestInfo += (topicPartition -> memoryRecords)
    }

    // the callback for sending a produce response
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {

      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses
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

      def produceResponseCallback(bandwidthThrottleTimeMs: Int) {
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
            sendNoOpResponseExemptThrottle(request)
          }
        } else {
          sendResponseMaybeThrottle(request, requestThrottleMs =>
            new ProduceResponse(mergedResponseStatus.asJava, bandwidthThrottleTimeMs + requestThrottleMs))
        }
      }

      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeNanos = time.nanoseconds

      quotas.produce.maybeRecordAndThrottle(
        request.session,
        request.header.clientId,
        numBytesAppended,
        produceResponseCallback)
    }

    def processingStatsCallback(processingStats: Map[TopicPartition, RecordsProcessingStats]): Unit = {
      processingStats.foreach { case (tp, info) =>
        updateRecordsProcessingStats(request, tp, info)
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
        processingStatsCallback = processingStatsCallback)

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
    val fetchContext = fetchManager.newContext(fetchRequest.metadata(),
          fetchRequest.fetchData(),
          fetchRequest.toForget(),
          fetchRequest.isFromFollower())

    val erroneous = mutable.ArrayBuffer[(TopicPartition, FetchResponse.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower()) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
        fetchContext.foreachPartition((topicPartition, data) => {
          if (!metadataCache.contains(topicPartition)) {
            erroneous += topicPartition -> new FetchResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION,
              FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
              FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
          } else {
            interesting += (topicPartition -> data)
          }
        })
      } else {
        fetchContext.foreachPartition((part, _) => {
          erroneous += part -> new FetchResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED,
            FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
            FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
        })
      }
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      fetchContext.foreachPartition((topicPartition, data) => {
        if (!authorize(request.session, Read, new Resource(Topic, topicPartition.topic)))
          erroneous += topicPartition -> new FetchResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED,
            FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
            FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
        else if (!metadataCache.contains(topicPartition))
          erroneous += topicPartition -> new FetchResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION,
            FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
            FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
        else
          interesting += (topicPartition -> data)
      })
    }

    def convertedPartitionData(tp: TopicPartition, data: FetchResponse.PartitionData) = {
      // Down-conversion of the fetched records is needed when the stored magic version is
      // greater than that supported by the client (as indicated by the fetch request version). If the
      // configured magic version for the topic is less than or equal to that supported by the version of the
      // fetch request, we skip the iteration through the records in order to check the magic version since we
      // know it must be supported. However, if the magic version is changed from a higher version back to a
      // lower version, this check will no longer be valid and we will fail to down-convert the messages
      // which were written in the new format prior to the version downgrade.
      replicaManager.getMagic(tp).flatMap { magic =>
        val downConvertMagic = {
          if (magic > RecordBatch.MAGIC_VALUE_V0 && versionId <= 1 && !data.records.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V0))
            Some(RecordBatch.MAGIC_VALUE_V0)
          else if (magic > RecordBatch.MAGIC_VALUE_V1 && versionId <= 3 && !data.records.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V1))
            Some(RecordBatch.MAGIC_VALUE_V1)
          else
            None
        }

        downConvertMagic.map { magic =>
          trace(s"Down converting records from partition $tp to message format version $magic for fetch request from $clientId")
          val converted = data.records.downConvert(magic, fetchContext.getFetchOffset(tp).get, time)
          updateRecordsProcessingStats(request, tp, converted.recordsProcessingStats)
          new FetchResponse.PartitionData(data.error, data.highWatermark, FetchResponse.INVALID_LAST_STABLE_OFFSET,
            data.logStartOffset, data.abortedTransactions, converted.records)
        }

      }.getOrElse(data)
    }

    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]) {
      val partitions = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData]
      responsePartitionData.foreach{ case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        partitions.put(tp, new FetchResponse.PartitionData(data.error, data.highWatermark, lastStableOffset,
          data.logStartOffset, abortedTransactions, data.records))
      }
      erroneous.foreach{case (tp, data) => partitions.put(tp, data)}
      val unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)

      // fetch response callback invoked after any throttling
      def fetchResponseCallback(bandwidthThrottleTimeMs: Int) {
        def createResponse(requestThrottleTimeMs: Int): FetchResponse = {
          val convertedData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData]
          unconvertedFetchResponse.responseData().asScala.foreach { case (tp, partitionData) =>
            if (partitionData.error != Errors.NONE)
              debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
                s"on partition $tp failed due to ${partitionData.error.exceptionName}")
            convertedData.put(tp, convertedPartitionData(tp, partitionData))
          }
          val response = new FetchResponse(unconvertedFetchResponse.error(), convertedData,
            bandwidthThrottleTimeMs + requestThrottleTimeMs, unconvertedFetchResponse.sessionId())
          response.responseData.asScala.foreach { case (topicPartition, data) =>
            // record the bytes out metrics only when the response is being sent
            brokerTopicStats.updateBytesOut(topicPartition.topic, fetchRequest.isFromFollower, data.records.sizeInBytes)
          }
          response
        }

        trace(s"Sending Fetch response with partitions.size=${unconvertedFetchResponse.responseData().size()}, " +
          s"metadata=${unconvertedFetchResponse.sessionId()}")

        if (fetchRequest.isFromFollower)
          sendResponseExemptThrottle(request, createResponse(0))
        else
          sendResponseMaybeThrottle(request, requestThrottleMs => createResponse(requestThrottleMs))
      }

      // When this callback is triggered, the remote API call has completed.
      // Record time before any byte-rate throttling.
      request.apiRemoteCompleteTimeNanos = time.nanoseconds

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        val responseSize = sizeOfThrottledPartitions(versionId, unconvertedFetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        fetchResponseCallback(bandwidthThrottleTimeMs = 0)
      } else {
        // Fetch size used to determine throttle time is calculated before any down conversions.
        // This may be slightly different from the actual response size. But since down conversions
        // result in data being loaded into memory, it is better to do this after throttling to avoid OOM.
        val responseStruct = unconvertedFetchResponse.toStruct(versionId)
        quotas.fetch.maybeRecordAndThrottle(request.session, clientId, responseStruct.sizeOf,
          fetchResponseCallback)
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

  class SelectingIterator(val partitions: util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData],
                          val quota: ReplicationQuotaManager)
                          extends util.Iterator[util.Map.Entry[TopicPartition, FetchResponse.PartitionData]] {
    val iter = partitions.entrySet().iterator()

    var nextElement: util.Map.Entry[TopicPartition, FetchResponse.PartitionData] = null

    override def hasNext: Boolean = {
      while ((nextElement == null) && iter.hasNext()) {
        val element = iter.next()
        if (quota.isThrottled(element.getKey)) {
          nextElement = element
        }
      }
      nextElement != null
    }

    override def next(): util.Map.Entry[TopicPartition, FetchResponse.PartitionData] = {
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
                                        unconvertedResponse: FetchResponse,
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

    val (authorizedRequestInfo, unauthorizedRequestInfo) = offsetRequest.offsetData.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Describe, new Resource(Topic, topicPartition.topic))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ =>
      new ListOffsetResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED, List[JLong]().asJava)
    )

    val responseMap = authorizedRequestInfo.map {case (topicPartition, partitionData) =>
      try {
        // ensure leader exists
        val localReplica = if (offsetRequest.replicaId != ListOffsetRequest.DEBUGGING_REPLICA_ID)
          replicaManager.getLeaderReplicaIfLocal(topicPartition)
        else
          replicaManager.getReplicaOrException(topicPartition)
        val offsets = {
          val allOffsets = fetchOffsets(replicaManager.logManager,
            topicPartition,
            partitionData.timestamp,
            partitionData.maxNumOffsets)
          if (offsetRequest.replicaId != ListOffsetRequest.CONSUMER_REPLICA_ID) {
            allOffsets
          } else {
            val hw = localReplica.highWatermark.messageOffset
            if (allOffsets.exists(_ > hw))
              hw +: allOffsets.dropWhile(_ > hw)
            else
              allOffsets
          }
        }
        (topicPartition, new ListOffsetResponse.PartitionData(Errors.NONE, offsets.map(new JLong(_)).asJava))
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
      case (topicPartition, _) => authorize(request.session, Describe, new Resource(Topic, topicPartition.topic))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ => {
      new ListOffsetResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED,
                                           ListOffsetResponse.UNKNOWN_TIMESTAMP,
                                           ListOffsetResponse.UNKNOWN_OFFSET)
    })

    val responseMap = authorizedRequestInfo.map { case (topicPartition, timestamp) =>
      if (offsetRequest.duplicatePartitions().contains(topicPartition)) {
        debug(s"OffsetRequest with correlation id $correlationId from client $clientId on partition $topicPartition " +
            s"failed because the partition is duplicated in the request.")
        (topicPartition, new ListOffsetResponse.PartitionData(Errors.INVALID_REQUEST,
                                                              ListOffsetResponse.UNKNOWN_TIMESTAMP,
                                                              ListOffsetResponse.UNKNOWN_OFFSET))
      } else {
        try {
          // ensure leader exists
          val localReplica = if (offsetRequest.replicaId != ListOffsetRequest.DEBUGGING_REPLICA_ID)
            replicaManager.getLeaderReplicaIfLocal(topicPartition)
          else
            replicaManager.getReplicaOrException(topicPartition)

          val fromConsumer = offsetRequest.replicaId == ListOffsetRequest.CONSUMER_REPLICA_ID
          val found = if (fromConsumer) {
            val lastFetchableOffset = offsetRequest.isolationLevel match {
              case IsolationLevel.READ_COMMITTED => localReplica.lastStableOffset.messageOffset
              case IsolationLevel.READ_UNCOMMITTED => localReplica.highWatermark.messageOffset
            }

            if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP)
              TimestampOffset(RecordBatch.NO_TIMESTAMP, lastFetchableOffset)
            else {
              def allowed(timestampOffset: TimestampOffset): Boolean =
                timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP || timestampOffset.offset < lastFetchableOffset

              fetchOffsetForTimestamp(topicPartition, timestamp)
                .filter(allowed).getOrElse(TimestampOffset.Unknown)
            }
          } else {
            fetchOffsetForTimestamp(topicPartition, timestamp)
              .getOrElse(TimestampOffset.Unknown)
          }

          (topicPartition, new ListOffsetResponse.PartitionData(Errors.NONE, found.timestamp, found.offset))
        } catch {
          // NOTE: These exceptions are special cased since these error messages are typically transient or the client
          // would have received a clear exception and there is no value in logging the entire stack trace for the same
          case e @ (_ : UnknownTopicOrPartitionException |
                    _ : NotLeaderForPartitionException |
                    _ : KafkaStorageException |
                    _ : UnsupportedForMessageFormatException) =>
            debug(s"Offset request with correlation id $correlationId from client $clientId on " +
                s"partition $topicPartition failed due to ${e.getMessage}")
            (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(e),
                                                                  ListOffsetResponse.UNKNOWN_TIMESTAMP,
                                                                  ListOffsetResponse.UNKNOWN_OFFSET))
          case e: Throwable =>
            error("Error while responding to offset request", e)
            (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(e),
                                                                  ListOffsetResponse.UNKNOWN_TIMESTAMP,
                                                                  ListOffsetResponse.UNKNOWN_OFFSET))
        }
      }
    }
    responseMap ++ unauthorizedResponseStatus
  }

  def fetchOffsets(logManager: LogManager, topicPartition: TopicPartition, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    logManager.getLog(topicPartition) match {
      case Some(log) =>
        fetchOffsetsBefore(log, timestamp, maxNumOffsets)
      case None =>
        if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP || timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
          Seq(0L)
        else
          Nil
    }
  }

  private def fetchOffsetForTimestamp(topicPartition: TopicPartition, timestamp: Long): Option[TimestampOffset] = {
    replicaManager.getLog(topicPartition) match {
      case Some(log) =>
        log.fetchOffsetsByTimestamp(timestamp)
      case None =>
        throw new UnknownTopicOrPartitionException(s"$topicPartition does not exist on the broker.")
    }
  }

  private[server] def fetchOffsetsBefore(log: Log, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
    // constant time access while being safe to use with concurrent collections unlike `toArray`.
    val segments = log.logSegments.toBuffer
    val lastSegmentHasSize = segments.last.size > 0

    val offsetTimeArray =
      if (lastSegmentHasSize)
        new Array[(Long, Long)](segments.length + 1)
      else
        new Array[(Long, Long)](segments.length)

    for (i <- segments.indices)
      offsetTimeArray(i) = (math.max(segments(i).baseOffset, log.logStartOffset), segments(i).lastModified)
    if (lastSegmentHasSize)
      offsetTimeArray(segments.length) = (log.logEndOffset, time.milliseconds)

    var startIndex = -1
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        startIndex = offsetTimeArray.length - 1
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -= 1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for (j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(-_)
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
                               errorUnavailableEndpoints: Boolean): Seq[MetadataResponse.TopicMetadata] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, listenerName, errorUnavailableEndpoints)
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

    val topics =
      // Handle old metadata request logic. Version 0 has no way to specify "no topics".
      if (requestVersion == 0) {
        if (metadataRequest.topics() == null || metadataRequest.topics.isEmpty)
          metadataCache.getAllTopics()
        else
          metadataRequest.topics.asScala.toSet
      } else {
        if (metadataRequest.isAllTopics)
          metadataCache.getAllTopics()
        else
          metadataRequest.topics.asScala.toSet
      }

    var (authorizedTopics, unauthorizedForDescribeTopics) =
      topics.partition(topic => authorize(request.session, Describe, new Resource(Topic, topic)))

    var unauthorizedForCreateTopics = Set[String]()

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = metadataCache.getNonExistingTopics(authorizedTopics)
      if (metadataRequest.allowAutoTopicCreation && config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        if (!authorize(request.session, Create, Resource.ClusterResource)) {
          authorizedTopics --= nonExistingTopics
          unauthorizedForCreateTopics ++= nonExistingTopics
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
    val topicMetadata =
      if (authorizedTopics.isEmpty)
        Seq.empty[MetadataResponse.TopicMetadata]
      else
        getTopicMetadata(metadataRequest.allowAutoTopicCreation, authorizedTopics, request.context.listenerName,
          errorUnavailableEndpoints)

    val completeTopicMetadata = topicMetadata ++ unauthorizedForCreateTopicMetadata ++ unauthorizedForDescribeTopicMetadata

    val brokers = metadataCache.getAliveBrokers

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))

    sendResponseMaybeThrottle(request, requestThrottleMs =>
      new MetadataResponse(
        requestThrottleMs,
        brokers.flatMap(_.getNode(request.context.listenerName)).asJava,
        clusterId,
        metadataCache.getControllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
        completeTopicMetadata.asJava
      ))
  }

  /**
   * Handle an offset fetch request
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val header = request.header
    val offsetFetchRequest = request.body[OffsetFetchRequest]

    def authorizeTopicDescribe(partition: TopicPartition) =
      authorize(request.session, Describe, new Resource(Topic, partition.topic))

    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val offsetFetchResponse =
        // reject the request if not authorized to the group
        if (!authorize(request.session, Describe, new Resource(Group, offsetFetchRequest.groupId)))
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
                      (topicPartition, new OffsetFetchResponse.PartitionData(
                          payload.toLong, OffsetFetchResponse.NO_METADATA, Errors.NONE))
                    case None =>
                      (topicPartition, OffsetFetchResponse.UNKNOWN_PARTITION)
                  }
                }
              } catch {
                case e: Throwable =>
                  (topicPartition, new OffsetFetchResponse.PartitionData(
                      OffsetFetchResponse.INVALID_OFFSET, OffsetFetchResponse.NO_METADATA, Errors.forException(e)))
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

    if (findCoordinatorRequest.coordinatorType == FindCoordinatorRequest.CoordinatorType.GROUP &&
        !authorize(request.session, Describe, new Resource(Group, findCoordinatorRequest.coordinatorKey)))
      sendErrorResponseMaybeThrottle(request, Errors.GROUP_AUTHORIZATION_FAILED.exception)
    else if (findCoordinatorRequest.coordinatorType == FindCoordinatorRequest.CoordinatorType.TRANSACTION &&
        !authorize(request.session, Describe, new Resource(TransactionalId, findCoordinatorRequest.coordinatorKey)))
      sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
    else {
      // get metadata (and create the topic if necessary)
      val (partition, topicMetadata) = findCoordinatorRequest.coordinatorType match {
        case FindCoordinatorRequest.CoordinatorType.GROUP =>
          val partition = groupCoordinator.partitionFor(findCoordinatorRequest.coordinatorKey)
          val metadata = getOrCreateInternalTopic(GROUP_METADATA_TOPIC_NAME, request.context.listenerName)
          (partition, metadata)

        case FindCoordinatorRequest.CoordinatorType.TRANSACTION =>
          val partition = txnCoordinator.partitionFor(findCoordinatorRequest.coordinatorKey)
          val metadata = getOrCreateInternalTopic(TRANSACTION_STATE_TOPIC_NAME, request.context.listenerName)
          (partition, metadata)

        case _ =>
          throw new InvalidRequestException("Unknown coordinator type in FindCoordinator request")
      }

      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseBody = if (topicMetadata.error != Errors.NONE) {
          new FindCoordinatorResponse(requestThrottleMs, Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
        } else {
          val coordinatorEndpoint = topicMetadata.partitionMetadata.asScala
            .find(_.partition == partition)
            .map(_.leader)
            .flatMap(p => Option(p))

          coordinatorEndpoint match {
            case Some(endpoint) if !endpoint.isEmpty =>
              new FindCoordinatorResponse(requestThrottleMs, Errors.NONE, endpoint)
            case _ =>
              new FindCoordinatorResponse(requestThrottleMs, Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
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
    val describeRequest = request.body[DescribeGroupsRequest]

    val groups = describeRequest.groupIds.asScala.map { groupId =>
      if (!authorize(request.session, Describe, new Resource(Group, groupId))) {
        groupId -> DescribeGroupsResponse.GroupMetadata.forError(Errors.GROUP_AUTHORIZATION_FAILED)
      } else {
        val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
        val members = summary.members.map { member =>
          val metadata = ByteBuffer.wrap(member.metadata)
          val assignment = ByteBuffer.wrap(member.assignment)
          new DescribeGroupsResponse.GroupMember(member.memberId, member.clientId, member.clientHost, metadata, assignment)
        }
        groupId -> new DescribeGroupsResponse.GroupMetadata(error, summary.state, summary.protocolType,
          summary.protocol, members.asJava)
      }
    }.toMap

    sendResponseMaybeThrottle(request, requestThrottleMs => new DescribeGroupsResponse(requestThrottleMs, groups.asJava))
  }

  def handleListGroupsRequest(request: RequestChannel.Request) {
    if (!authorize(request.session, Describe, Resource.ClusterResource)) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        request.body[ListGroupsRequest].getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      val (error, groups) = groupCoordinator.handleListGroups()
      val allGroups = groups.map { group => new ListGroupsResponse.Group(group.groupId, group.protocolType) }
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ListGroupsResponse(requestThrottleMs, error, allGroups.asJava))
    }
  }

  def handleJoinGroupRequest(request: RequestChannel.Request) {
    val joinGroupRequest = request.body[JoinGroupRequest]

    // the callback for sending a join-group response
    def sendResponseCallback(joinResult: JoinGroupResult) {
      val members = joinResult.members map { case (memberId, metadataArray) => (memberId, ByteBuffer.wrap(metadataArray)) }
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseBody = new JoinGroupResponse(requestThrottleMs, joinResult.error, joinResult.generationId,
          joinResult.subProtocol, joinResult.memberId, joinResult.leaderId, members.asJava)

        trace("Sending join group response %s for correlation id %d to client %s."
          .format(responseBody, request.header.correlationId, request.header.clientId))
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (!authorize(request.session, Read, new Resource(Group, joinGroupRequest.groupId()))) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new JoinGroupResponse(
          requestThrottleMs,
          Errors.GROUP_AUTHORIZATION_FAILED,
          JoinGroupResponse.UNKNOWN_GENERATION_ID,
          JoinGroupResponse.UNKNOWN_PROTOCOL,
          JoinGroupResponse.UNKNOWN_MEMBER_ID, // memberId
          JoinGroupResponse.UNKNOWN_MEMBER_ID, // leaderId
          Collections.emptyMap())
      )
    } else {
      // let the coordinator to handle join-group
      val protocols = joinGroupRequest.groupProtocols().asScala.map(protocol =>
        (protocol.name, Utils.toArray(protocol.metadata))).toList
      groupCoordinator.handleJoinGroup(
        joinGroupRequest.groupId,
        joinGroupRequest.memberId,
        request.header.clientId,
        request.session.clientAddress.toString,
        joinGroupRequest.rebalanceTimeout,
        joinGroupRequest.sessionTimeout,
        joinGroupRequest.protocolType,
        protocols,
        sendResponseCallback)
    }
  }

  def handleSyncGroupRequest(request: RequestChannel.Request) {
    val syncGroupRequest = request.body[SyncGroupRequest]

    def sendResponseCallback(memberState: Array[Byte], error: Errors) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new SyncGroupResponse(requestThrottleMs, error, ByteBuffer.wrap(memberState)))
    }

    if (!authorize(request.session, Read, new Resource(Group, syncGroupRequest.groupId()))) {
      sendResponseCallback(Array[Byte](), Errors.GROUP_AUTHORIZATION_FAILED)
    } else {
      groupCoordinator.handleSyncGroup(
        syncGroupRequest.groupId,
        syncGroupRequest.generationId,
        syncGroupRequest.memberId,
        syncGroupRequest.groupAssignment().asScala.mapValues(Utils.toArray),
        sendResponseCallback
      )
    }
  }

  def handleDeleteGroupsRequest(request: RequestChannel.Request): Unit = {
    val deleteGroupsRequest = request.body[DeleteGroupsRequest]
    var groups = deleteGroupsRequest.groups.asScala.toSet

    val (authorizedGroups, unauthorizedGroups) = groups.partition { group =>
      authorize(request.session, Delete, new Resource(Group, group))
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
        val response = new HeartbeatResponse(requestThrottleMs, error)
        trace("Sending heartbeat response %s for correlation id %d to client %s."
          .format(response, request.header.correlationId, request.header.clientId))
        response
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (!authorize(request.session, Read, new Resource(Group, heartbeatRequest.groupId))) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new HeartbeatResponse(requestThrottleMs, Errors.GROUP_AUTHORIZATION_FAILED))
    } else {
      // let the coordinator to handle heartbeat
      groupCoordinator.handleHeartbeat(
        heartbeatRequest.groupId,
        heartbeatRequest.memberId,
        heartbeatRequest.groupGenerationId,
        sendResponseCallback)
    }
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request) {
    val leaveGroupRequest = request.body[LeaveGroupRequest]

    // the callback for sending a leave-group response
    def sendResponseCallback(error: Errors) {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val response = new LeaveGroupResponse(requestThrottleMs, error)
        trace("Sending leave group response %s for correlation id %d to client %s."
                    .format(response, request.header.correlationId, request.header.clientId))
        response
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (!authorize(request.session, Read, new Resource(Group, leaveGroupRequest.groupId))) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new LeaveGroupResponse(requestThrottleMs, Errors.GROUP_AUTHORIZATION_FAILED))
    } else {
      // let the coordinator to handle leave-group
      groupCoordinator.handleLeaveGroup(
        leaveGroupRequest.groupId,
        leaveGroupRequest.memberId,
        sendResponseCallback)
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request) {
    sendResponseMaybeThrottle(request, _ => new SaslHandshakeResponse(Errors.ILLEGAL_SASL_STATE, Collections.emptySet()))
  }

  def handleSaslAuthenticateRequest(request: RequestChannel.Request) {
    sendResponseMaybeThrottle(request, _ => new SaslAuthenticateResponse(Errors.ILLEGAL_SASL_STATE,
        "SaslAuthenticate request received after successful authentication"))
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
    val createTopicsRequest = request.body[CreateTopicsRequest]

    def sendResponseCallback(results: Map[String, ApiError]): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseBody = new CreateTopicsResponse(requestThrottleMs, results.asJava)
        trace(s"Sending create topics response $responseBody for correlation id ${request.header.correlationId} to client ${request.header.clientId}.")
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (!controller.isActive) {
      val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
        (topic, new ApiError(Errors.NOT_CONTROLLER, null))
      }
      sendResponseCallback(results)
    } else if (!authorize(request.session, Create, Resource.ClusterResource)) {
      val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
        (topic, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, null))
      }
      sendResponseCallback(results)
    } else {
      val (validTopics, duplicateTopics) = createTopicsRequest.topics.asScala.partition { case (topic, _) =>
        !createTopicsRequest.duplicateTopics.contains(topic)
      }

      // Special handling to add duplicate topics to the response
      def sendResponseWithDuplicatesCallback(results: Map[String, ApiError]): Unit = {

        val duplicatedTopicsResults =
          if (duplicateTopics.nonEmpty) {
            val errorMessage = s"Create topics request from client `${request.header.clientId}` contains multiple entries " +
              s"for the following topics: ${duplicateTopics.keySet.mkString(",")}"
            // We can send the error message in the response for version 1, so we don't have to log it any more
            if (request.header.apiVersion == 0)
              warn(errorMessage)
            duplicateTopics.keySet.map((_, new ApiError(Errors.INVALID_REQUEST, errorMessage))).toMap
          } else Map.empty

        val completeResults = results ++ duplicatedTopicsResults
        sendResponseCallback(completeResults)
      }

      adminManager.createTopics(
        createTopicsRequest.timeout,
        createTopicsRequest.validateOnly,
        validTopics,
        sendResponseWithDuplicatesCallback
      )
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
        authorize(request.session, Alter, new Resource(Topic, topic))
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
    val deleteTopicRequest = request.body[DeleteTopicsRequest]

    val unauthorizedTopicErrors = mutable.Map[String, Errors]()
    val nonExistingTopicErrors = mutable.Map[String, Errors]()
    val authorizedForDeleteTopics =  mutable.Set[String]()

    for (topic <- deleteTopicRequest.topics.asScala) {
      if (!authorize(request.session, Delete, new Resource(Topic, topic)))
        unauthorizedTopicErrors += topic -> Errors.TOPIC_AUTHORIZATION_FAILED
      else if (!metadataCache.contains(topic))
        nonExistingTopicErrors += topic -> Errors.UNKNOWN_TOPIC_OR_PARTITION
      else
        authorizedForDeleteTopics.add(topic)
    }

    def sendResponseCallback(authorizedTopicErrors: Map[String, Errors]): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val completeResults = unauthorizedTopicErrors ++ nonExistingTopicErrors ++ authorizedTopicErrors
        val responseBody = new DeleteTopicsResponse(requestThrottleMs, completeResults.asJava)
        trace(s"Sending delete topics response $responseBody for correlation id ${request.header.correlationId} to client ${request.header.clientId}.")
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (!controller.isActive) {
      val results = deleteTopicRequest.topics.asScala.map { topic =>
        (topic, Errors.NOT_CONTROLLER)
      }.toMap
      sendResponseCallback(results)
    } else {
      // If no authorized topics return immediately
      if (authorizedForDeleteTopics.isEmpty)
        sendResponseCallback(Map())
      else {
        adminManager.deleteTopics(
          deleteTopicRequest.timeout.toInt,
          authorizedForDeleteTopics,
          sendResponseCallback
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
      if (!authorize(request.session, Delete, new Resource(Topic, topicPartition.topic)))
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
    val transactionalId = initProducerIdRequest.transactionalId

    if (transactionalId != null) {
      if (!authorize(request.session, Write, new Resource(TransactionalId, transactionalId))) {
        sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    } else if (!authorize(request.session, IdempotentWrite, Resource.ClusterResource)) {
      sendErrorResponseMaybeThrottle(request, Errors.CLUSTER_AUTHORIZATION_FAILED.exception)
      return
    }

    def sendResponseCallback(result: InitProducerIdResult): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseBody = new InitProducerIdResponse(requestThrottleMs, result.error, result.producerId, result.producerEpoch)
        trace(s"Completed $transactionalId's InitProducerIdRequest with result $result from client ${request.header.clientId}.")
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }
    txnCoordinator.handleInitProducerId(transactionalId, initProducerIdRequest.transactionTimeoutMs, sendResponseCallback)
  }

  def handleEndTxnRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val endTxnRequest = request.body[EndTxnRequest]
    val transactionalId = endTxnRequest.transactionalId

    if (authorize(request.session, Write, new Resource(TransactionalId, transactionalId))) {
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
    if (!authorize(request.session, Write, new Resource(TransactionalId, transactionalId)))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        addPartitionsToTxnRequest.getErrorResponse(requestThrottleMs, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception))
    else {
      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedPartitions = mutable.Set[TopicPartition]()

      for (topicPartition <- partitionsToAdd) {
        if (org.apache.kafka.common.internals.Topic.isInternal(topicPartition.topic) ||
            !authorize(request.session, Write, new Resource(Topic, topicPartition.topic)))
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

    if (!authorize(request.session, Write, new Resource(TransactionalId, transactionalId)))
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(requestThrottleMs, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
    else if (!authorize(request.session, Read, new Resource(Group, groupId)))
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
    if (!authorize(request.session, Write, new Resource(TransactionalId, txnOffsetCommitRequest.transactionalId)))
      sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
    else if (!authorize(request.session, Read, new Resource(Group, txnOffsetCommitRequest.consumerGroupId)))
      sendErrorResponseMaybeThrottle(request, Errors.GROUP_AUTHORIZATION_FAILED.exception)
    else {
      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedTopicCommittedOffsets = mutable.Map[TopicPartition, TxnOffsetCommitRequest.CommittedOffset]()

      for ((topicPartition, commitedOffset) <- txnOffsetCommitRequest.offsets.asScala) {
        if (!authorize(request.session, Read, new Resource(Topic, topicPartition.topic)))
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
    val offsetRetention = groupCoordinator.offsetConfig.offsetsRetentionMs
    val currentTimestamp = time.milliseconds
    val defaultExpireTimestamp = offsetRetention + currentTimestamp
    offsetsMap.map { case (topicPartition, partitionData) =>
      val metadata = if (partitionData.metadata == null) OffsetMetadata.NoMetadata else partitionData.metadata
      topicPartition -> new OffsetAndMetadata(
        offsetMetadata = OffsetMetadata(partitionData.offset, metadata),
        commitTimestamp = currentTimestamp,
        expireTimestamp = defaultExpireTimestamp)
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
            val fixture = new AclBinding(new AdminResource(resource.resourceType.toJava, resource.name),
                new AccessControlEntry(acl.principal.toString, acl.host.toString, acl.operation.toJava, acl.permissionType.toJava))
            if (filter.matches(fixture)) Some(fixture)
            else None
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
                if (resource.resourceType.equals(Cluster) &&
                    !resource.name.equals(Resource.ClusterResourceName))
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
              new AdminResource(resource.resourceType.toJava, resource.name),
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
    val requestInfo = offsetForLeaderEpoch.epochsByTopicPartition()
    authorizeClusterAction(request)

    val lastOffsetForLeaderEpoch = replicaManager.lastOffsetForLeaderEpoch(requestInfo.asScala).asJava
    sendResponseExemptThrottle(request, new OffsetsForLeaderEpochResponse(lastOffsetForLeaderEpoch))
  }

  def handleAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val alterConfigsRequest = request.body[AlterConfigsRequest]
    val (authorizedResources, unauthorizedResources) = alterConfigsRequest.configs.asScala.partition { case (resource, _) =>
      resource.`type` match {
        case RResourceType.BROKER =>
          authorize(request.session, AlterConfigs, Resource.ClusterResource)
        case RResourceType.TOPIC =>
          authorize(request.session, AlterConfigs, new Resource(Topic, resource.name))
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

  private def configsAuthorizationApiError(session: RequestChannel.Session, resource: RResource): ApiError = {
    val error = resource.`type` match {
      case RResourceType.BROKER => Errors.CLUSTER_AUTHORIZATION_FAILED
      case RResourceType.TOPIC => Errors.TOPIC_AUTHORIZATION_FAILED
      case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.name}")
    }
    new ApiError(error, null)
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): Unit = {
    val describeConfigsRequest = request.body[DescribeConfigsRequest]
    val (authorizedResources, unauthorizedResources) = describeConfigsRequest.resources.asScala.partition { resource =>
      resource.`type` match {
        case RResourceType.BROKER => authorize(request.session, DescribeConfigs, Resource.ClusterResource)
        case RResourceType.TOPIC =>
          authorize(request.session, DescribeConfigs, new Resource(Topic, resource.name))
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
        def authorizeToken(tokenId: String) = authorize(request.session, Describe, new Resource(kafka.security.auth.DelegationToken, tokenId))
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

  def authorizeClusterAction(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, ClusterAction, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }

  def authorizeClusterAlter(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, Alter, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }

  def authorizeClusterDescribe(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, Describe, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }

  private def updateRecordsProcessingStats(request: RequestChannel.Request, tp: TopicPartition,
                                           processingStats: RecordsProcessingStats): Unit = {
    val conversionCount = processingStats.numRecordsConverted
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
      request.messageConversionsTimeNanos = processingStats.conversionTimeNanos
    }
    request.temporaryMemoryBytes = processingStats.temporaryMemoryBytes
  }

  private def handleError(request: RequestChannel.Request, e: Throwable) {
    val mayThrottle = e.isInstanceOf[ClusterAuthorizationException] || !request.header.apiKey.clusterAction
    error("Error when handling request %s".format(request.body[AbstractRequest]), e)
    if (mayThrottle)
      sendErrorResponseMaybeThrottle(request, e)
    else
      sendErrorResponseExemptThrottle(request, e)
  }

  private def sendResponseMaybeThrottle(request: RequestChannel.Request, createResponse: Int => AbstractResponse): Unit = {
    quotas.request.maybeRecordAndThrottle(request,
      throttleTimeMs => sendResponse(request, Some(createResponse(throttleTimeMs))))
  }

  private def sendErrorResponseMaybeThrottle(request: RequestChannel.Request, error: Throwable) {
    quotas.request.maybeRecordAndThrottle(request, sendErrorOrCloseConnection(request, error))
  }

  private def sendResponseExemptThrottle(request: RequestChannel.Request, response: AbstractResponse): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendResponse(request, Some(response))
  }

  private def sendErrorResponseExemptThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendErrorOrCloseConnection(request, error)(throttleMs = 0)
  }

  private def sendErrorOrCloseConnection(request: RequestChannel.Request, error: Throwable)(throttleMs: Int): Unit = {
    val requestBody = request.body[AbstractRequest]
    val response = requestBody.getErrorResponse(throttleMs, error)
    if (response == null)
      closeConnection(request, requestBody.errorCounts(error))
    else
      sendResponse(request, Some(response))
  }

  private def sendNoOpResponseExemptThrottle(request: RequestChannel.Request): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendResponse(request, None)
  }

  private def closeConnection(request: RequestChannel.Request, errorCounts: java.util.Map[Errors, Integer]): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    requestChannel.updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    requestChannel.sendResponse(new RequestChannel.Response(request, None, CloseConnectionAction, None))
  }

  private def sendResponse(request: RequestChannel.Request, responseOpt: Option[AbstractResponse]): Unit = {
    // Update error metrics for each error code in the response including Errors.NONE
    responseOpt.foreach(response => requestChannel.updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala))

    responseOpt match {
      case Some(response) =>
        val responseSend = request.context.buildResponse(response)
        val responseString =
          if (RequestChannel.isRequestLoggingEnabled) Some(response.toString(request.context.apiVersion))
          else None
        requestChannel.sendResponse(new RequestChannel.Response(request, Some(responseSend), SendAction, responseString))
      case None =>
        requestChannel.sendResponse(new RequestChannel.Response(request, None, NoOpAction, None))
    }
  }

}
