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
import java.lang.{Long => JLong, Short => JShort}
import java.util.{Collections, Properties}
import java.util

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.api.{ControlledShutdownRequest, ControlledShutdownResponse}
import kafka.cluster.Partition
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.common._
import kafka.controller.KafkaController
import kafka.coordinator.{GroupCoordinator, JoinGroupResult}
import kafka.log._
import kafka.network._
import kafka.network.RequestChannel.{Response, Session}
import kafka.security.auth
import kafka.security.auth.{Authorizer, ClusterAction, Create, Delete, Describe, Group, Operation, Read, Resource, Write}
import kafka.utils.{Exit, Logging, ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, NotLeaderForPartitionException, TopicExistsException, UnknownTopicOrPartitionException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors, Protocol}
import org.apache.kafka.common.record.{RecordBatch, MemoryRecords, TimestampType}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.requests.SaslHandshakeResponse

import scala.collection._
import scala.collection.JavaConverters._

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val adminManager: AdminManager,
                val coordinator: GroupCoordinator,
                val controller: KafkaController,
                val zkUtils: ZkUtils,
                val brokerId: Int,
                val config: KafkaConfig,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizer: Option[Authorizer],
                val quotas: QuotaManagers,
                val clusterId: String,
                time: Time) extends Logging {

  this.logIdent = "[KafkaApi-%d] ".format(brokerId)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try {
      trace("Handling request:%s from connection %s;securityProtocol:%s,principal:%s".
        format(request.requestDesc(true), request.connectionId, request.securityProtocol, request.session.principal))
      ApiKeys.forId(request.requestId) match {
        case ApiKeys.PRODUCE => handleProducerRequest(request)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
        case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
        case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
        case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
        case ApiKeys.GROUP_COORDINATOR => handleGroupCoordinatorRequest(request)
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
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable =>
        if (request.requestObj != null) {
          request.requestObj.handleError(e, requestChannel, request)
          error("Error when handling request %s".format(request.requestObj), e)
        } else {
          val response = request.body[AbstractRequest].getErrorResponse(e)

          /* If request doesn't have a default error response, we just close the connection.
             For example, when produce request has acks set to 0 */
          if (response == null)
            requestChannel.closeConnection(request.processor, request)
          else
            requestChannel.sendResponse(new Response(request, response))

          error("Error when handling request %s".format(request.body[AbstractRequest]), e)
        }
    } finally
      request.apiLocalCompleteTimeMs = time.milliseconds
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val correlationId = request.header.correlationId
    val leaderAndIsrRequest = request.body[LeaderAndIsrRequest]

    try {
      def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]) {
        // for each new leader or follower, call coordinator to handle consumer group migration.
        // this callback is invoked under the replica state change lock to ensure proper order of
        // leadership changes
        updatedLeaders.foreach { partition =>
          if (partition.topic == Topic.GroupMetadataTopicName)
            coordinator.handleGroupImmigration(partition.partitionId)
        }
        updatedFollowers.foreach { partition =>
          if (partition.topic == Topic.GroupMetadataTopicName)
            coordinator.handleGroupEmigration(partition.partitionId)
        }
      }

      val leaderAndIsrResponse =
        if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
          val result = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest, metadataCache, onLeadershipChange)
          new LeaderAndIsrResponse(result.error, result.responseMap.asJava)
        } else {
          val result = leaderAndIsrRequest.partitionStates.asScala.keys.map((_, Errors.CLUSTER_AUTHORIZATION_FAILED)).toMap
          new LeaderAndIsrResponse(Errors.CLUSTER_AUTHORIZATION_FAILED, result.asJava)
        }

      requestChannel.sendResponse(new Response(request, leaderAndIsrResponse))
    } catch {
      case e: FatalExitError => throw e
      case e: KafkaStorageException =>
        fatal("Disk error during leadership change.", e)
        Exit.halt(1)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.body[StopReplicaRequest]

    val response =
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
        val (result, error) = replicaManager.stopReplicas(stopReplicaRequest)
        // Clearing out the cache for groups that belong to an offsets topic partition for which this broker was the leader,
        // since this broker is no longer a replica for that offsets topic partition.
        // This is required to handle the following scenario :
        // Consider old replicas : {[1,2,3], Leader = 1} is reassigned to new replicas : {[2,3,4], Leader = 2}, broker 1 does not receive a LeaderAndIsr
        // request to become a follower due to which cache for groups that belong to an offsets topic partition for which broker 1 was the leader,
        // is not cleared.
        result.foreach { case (topicPartition, error) =>
          if (error == Errors.NONE && stopReplicaRequest.deletePartitions() && topicPartition.topic == Topic.GroupMetadataTopicName) {
            coordinator.handleGroupEmigration(topicPartition.partition)
          }
        }
        new StopReplicaResponse(error, result.asJava)
      } else {
        val result = stopReplicaRequest.partitions.asScala.map((_, Errors.CLUSTER_AUTHORIZATION_FAILED)).toMap
        new StopReplicaResponse(Errors.CLUSTER_AUTHORIZATION_FAILED, result.asJava)
      }

    requestChannel.sendResponse(new RequestChannel.Response(request, response))
    replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads()
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val correlationId = request.header.correlationId
    val updateMetadataRequest = request.body[UpdateMetadataRequest]

    val updateMetadataResponse =
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
        val deletedPartitions = replicaManager.maybeUpdateMetadataCache(correlationId, updateMetadataRequest, metadataCache)
        if (deletedPartitions.nonEmpty)
          coordinator.handleDeletedPartitions(deletedPartitions)

        if (adminManager.hasDelayedTopicOperations) {
          updateMetadataRequest.partitionStates.keySet.asScala.map(_.topic).foreach { topic =>
            adminManager.tryCompleteDelayedTopicOperations(topic)
          }
        }
        new UpdateMetadataResponse(Errors.NONE)
      } else {
        new UpdateMetadataResponse(Errors.CLUSTER_AUTHORIZATION_FAILED)
      }

    requestChannel.sendResponse(new Response(request, updateMetadataResponse))
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.requestObj.asInstanceOf[ControlledShutdownRequest]

    authorizeClusterAction(request)

    val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId)
    val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
      Errors.NONE, partitionsRemaining)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, controlledShutdownResponse)))
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
      val response = new OffsetCommitResponse(results.asJava)
      requestChannel.sendResponse(new RequestChannel.Response(request, response))
    } else {
      val (existingAndAuthorizedForDescribeTopics, nonExistingOrUnauthorizedForDescribeTopics) = offsetCommitRequest.offsetData.asScala.toMap.partition {
        case (topicPartition, _) =>
          val authorizedForDescribe = authorize(request.session, Describe, new Resource(auth.Topic, topicPartition.topic))
          val exists = metadataCache.contains(topicPartition.topic)
          if (!authorizedForDescribe && exists)
              debug(s"Offset commit request with correlation id ${header.correlationId} from client ${header.clientId} " +
                s"on partition $topicPartition failing due to user not having DESCRIBE authorization, but returning UNKNOWN_TOPIC_OR_PARTITION")
          authorizedForDescribe && exists
      }

      val (authorizedTopics, unauthorizedForReadTopics) = existingAndAuthorizedForDescribeTopics.partition {
        case (topicPartition, _) => authorize(request.session, Read, new Resource(auth.Topic, topicPartition.topic))
      }

      // the callback for sending an offset commit response
      def sendResponseCallback(commitStatus: immutable.Map[TopicPartition, Errors]) {
        val combinedCommitStatus = commitStatus ++
          unauthorizedForReadTopics.mapValues(_ => Errors.TOPIC_AUTHORIZATION_FAILED) ++
          nonExistingOrUnauthorizedForDescribeTopics.mapValues(_ => Errors.UNKNOWN_TOPIC_OR_PARTITION)

        if (isDebugEnabled)
          combinedCommitStatus.foreach { case (topicPartition, error) =>
            if (error != Errors.NONE) {
              debug(s"Offset commit request with correlation id ${header.correlationId} from client ${header.clientId} " +
                s"on partition $topicPartition failed due to ${error.exceptionName}")
            }
          }
        val response = new OffsetCommitResponse(combinedCommitStatus.asJava)
        requestChannel.sendResponse(new RequestChannel.Response(request, response))
      }

      if (authorizedTopics.isEmpty)
        sendResponseCallback(Map.empty)
      else if (header.apiVersion == 0) {
        // for version 0 always store offsets to ZK
        val responseInfo = authorizedTopics.map {
          case (topicPartition, partitionData) =>
            val topicDirs = new ZKGroupTopicDirs(offsetCommitRequest.groupId, topicPartition.topic)
            try {
              if (partitionData.metadata != null && partitionData.metadata.length > config.offsetMetadataMaxSize)
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE)
              else {
                zkUtils.updatePersistentPath(s"${topicDirs.consumerOffsetDir}/${topicPartition.partition}", partitionData.offset.toString)
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
            coordinator.offsetConfig.offsetsRetentionMs
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
        val partitionData = authorizedTopics.mapValues { partitionData =>
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
        coordinator.handleCommitOffsets(
          offsetCommitRequest.groupId,
          offsetCommitRequest.memberId,
          offsetCommitRequest.generationId,
          partitionData,
          sendResponseCallback)
      }
    }
  }

  private def authorize(session: Session, operation: Operation, resource: Resource): Boolean =
    authorizer.forall(_.authorize(session, operation, resource))

  /**
   * Handle a produce request
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.body[ProduceRequest]
    val numBytesAppended = request.header.toStruct.sizeOf + request.bodyAndSize.size

    val (existingAndAuthorizedForDescribeTopics, nonExistingOrUnauthorizedForDescribeTopics) =
      produceRequest.partitionRecordsOrFail.asScala.partition { case (tp, _) =>
        authorize(request.session, Describe, new Resource(auth.Topic, tp.topic)) && metadataCache.contains(tp.topic)
      }

    val (authorizedRequestInfo, unauthorizedForWriteRequestInfo) = existingAndAuthorizedForDescribeTopics.partition {
      case (tp, _) => authorize(request.session, Write, new Resource(auth.Topic, tp.topic))
    }

    // the callback for sending a produce response
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {

      val mergedResponseStatus = responseStatus ++
        unauthorizedForWriteRequestInfo.mapValues(_ => new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)) ++
        nonExistingOrUnauthorizedForDescribeTopics.mapValues(_ => new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION))

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

      def produceResponseCallback(delayTimeMs: Int) {
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
            requestChannel.closeConnection(request.processor, request)
          } else {
            requestChannel.noOperation(request.processor, request)
          }
        } else {
          val respBody = new ProduceResponse(mergedResponseStatus.asJava, delayTimeMs)
          requestChannel.sendResponse(new RequestChannel.Response(request, respBody))
        }
      }

      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeMs = time.milliseconds

      quotas.produce.recordAndMaybeThrottle(
        request.session.sanitizedUser,
        request.header.clientId,
        numBytesAppended,
        produceResponseCallback)
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId

      // call the replica manager to append messages to the replicas
      replicaManager.appendRecords(
        produceRequest.timeout.toLong,
        produceRequest.acks,
        internalTopicsAllowed,
        authorizedRequestInfo,
        sendResponseCallback)

      // if the request is put into the purgatory, it will have a held reference and hence cannot be garbage collected;
      // hence we clear its data here inorder to let GC re-claim its memory since it is already appended to log
      produceRequest.clearPartitionRecords()
    }
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = request.body[FetchRequest]
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId

    val (existingAndAuthorizedForDescribeTopics, nonExistingOrUnauthorizedForDescribeTopics) = fetchRequest.fetchData.asScala.toSeq.partition {
      case (tp, _) => authorize(request.session, Describe, new Resource(auth.Topic, tp.topic)) && metadataCache.contains(tp.topic)
    }

    val (authorizedRequestInfo, unauthorizedForReadRequestInfo) = existingAndAuthorizedForDescribeTopics.partition {
      case (tp, _) => authorize(request.session, Read, new Resource(auth.Topic, tp.topic))
    }

    val nonExistingOrUnauthorizedForDescribePartitionData = nonExistingOrUnauthorizedForDescribeTopics.map {
      case (tp, _) => (tp, new FetchResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION,
        FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LSO, null, MemoryRecords.EMPTY))
    }

    val unauthorizedForReadPartitionData = unauthorizedForReadRequestInfo.map {
      case (tp, _) => (tp, new FetchResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED,
        FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LSO, null, MemoryRecords.EMPTY))
    }

    // the callback for sending a fetch response
    def sendResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]) {
      val convertedPartitionData = {
        responsePartitionData.map { case (tp, data) =>

          // Down-conversion of the fetched records is needed when the stored magic version is
          // greater than that supported by the client (as indicated by the fetch request version). If the
          // configured magic version for the topic is less than or equal to that supported by the version of the
          // fetch request, we skip the iteration through the records in order to check the magic version since we
          // know it must be supported. However, if the magic version is changed from a higher version back to a
          // lower version, this check will no longer be valid and we will fail to down-convert the messages
          // which were written in the new format prior to the version downgrade.
          val convertedData = replicaManager.getMagic(tp) match {
            case Some(magic) if magic > 0 && versionId <= 1 && !data.records.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V0) =>
              trace(s"Down converting message to V0 for fetch request from $clientId")
              FetchPartitionData(data.error, data.hw, data.records.downConvert(RecordBatch.MAGIC_VALUE_V0))

            case Some(magic) if magic > 1 && versionId <= 3 && !data.records.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V1) =>
              trace(s"Down converting message to V1 for fetch request from $clientId")
              FetchPartitionData(data.error, data.hw, data.records.downConvert(RecordBatch.MAGIC_VALUE_V1))

            case _ => data
          }

          tp -> new FetchResponse.PartitionData(convertedData.error, convertedData.hw, FetchResponse.INVALID_LSO,
            null, convertedData.records)
        }
      }

      val mergedPartitionData = convertedPartitionData ++ unauthorizedForReadPartitionData ++ nonExistingOrUnauthorizedForDescribePartitionData

      val fetchedPartitionData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData]()

      mergedPartitionData.foreach { case (topicPartition, data) =>
        if (data.error != Errors.NONE)
          debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
            s"on partition $topicPartition failed due to ${data.error.exceptionName}")

        fetchedPartitionData.put(topicPartition, data)

        // record the bytes out metrics only when the response is being sent
        BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesOutRate.mark(data.records.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats().bytesOutRate.mark(data.records.sizeInBytes)
      }

      val response = new FetchResponse(fetchedPartitionData, 0)
      val responseStruct = response.toStruct(versionId)

      def fetchResponseCallback(throttleTimeMs: Int) {
        trace(s"Sending fetch response to client $clientId of ${responseStruct.sizeOf} bytes.")
        val responseSend = response.toSend(responseStruct, throttleTimeMs, request.connectionId, request.header)
        requestChannel.sendResponse(new RequestChannel.Response(request, responseSend))
      }

      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeMs = time.milliseconds

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        val responseSize = sizeOfThrottledPartitions(versionId, fetchRequest, mergedPartitionData, quotas.leader)
        quotas.leader.record(responseSize)
        fetchResponseCallback(throttleTimeMs = 0)
      } else {
        quotas.fetch.recordAndMaybeThrottle(request.session.sanitizedUser, clientId, responseStruct.sizeOf,
          fetchResponseCallback)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Seq.empty)
    else {
      // call the replica manager to fetch messages from the local replica
      replicaManager.fetchMessages(
        fetchRequest.maxWait.toLong,
        fetchRequest.replicaId,
        fetchRequest.minBytes,
        fetchRequest.maxBytes,
        versionId <= 2,
        authorizedRequestInfo,
        replicationQuota(fetchRequest),
        sendResponseCallback)
    }
  }

  private def sizeOfThrottledPartitions(versionId: Short,
                                        fetchRequest: FetchRequest,
                                        mergedPartitionData: Seq[(TopicPartition, FetchResponse.PartitionData)],
                                        quota: ReplicationQuotaManager): Int = {
    val partitionData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData]
    mergedPartitionData.foreach { case (tp, data) =>
      if (quota.isThrottled(tp))
        partitionData.put(tp, data)
    }
    FetchResponse.sizeOf(versionId, partitionData)
  }

  def replicationQuota(fetchRequest: FetchRequest): ReplicaQuota =
    if (fetchRequest.isFromFollower) quotas.leader else UnboundedQuota

  /**
   * Handle an offset request
   */
  def handleListOffsetRequest(request: RequestChannel.Request) {
    val version = request.header.apiVersion()

    val mergedResponseMap =
      if (version == 0)
        handleListOffsetRequestV0(request)
      else
        handleListOffsetRequestV1(request)

    val response = new ListOffsetResponse(mergedResponseMap.asJava)
    requestChannel.sendResponse(new RequestChannel.Response(request, response))
  }

  private def handleListOffsetRequestV0(request : RequestChannel.Request) : Map[TopicPartition, ListOffsetResponse.PartitionData] = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) = offsetRequest.offsetData.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Describe, new Resource(auth.Topic, topicPartition.topic))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ =>
      new ListOffsetResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION, List[JLong]().asJava)
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
        case e @ ( _ : UnknownTopicOrPartitionException | _ : NotLeaderForPartitionException) =>
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

  private def handleListOffsetRequestV1(request : RequestChannel.Request): Map[TopicPartition, ListOffsetResponse.PartitionData] = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) = offsetRequest.partitionTimestamps.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Describe, new Resource(auth.Topic, topicPartition.topic))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ => {
      new ListOffsetResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION,
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
          val fromConsumer = offsetRequest.replicaId == ListOffsetRequest.CONSUMER_REPLICA_ID

          // ensure leader exists
          val localReplica = if (offsetRequest.replicaId != ListOffsetRequest.DEBUGGING_REPLICA_ID)
            replicaManager.getLeaderReplicaIfLocal(topicPartition)
          else
            replicaManager.getReplicaOrException(topicPartition)

          val found = {
            if (fromConsumer && timestamp == ListOffsetRequest.LATEST_TIMESTAMP)
              TimestampOffset(RecordBatch.NO_TIMESTAMP, localReplica.highWatermark.messageOffset)
            else {
              def allowed(timestampOffset: TimestampOffset): Boolean =
                !fromConsumer || timestampOffset.offset <= localReplica.highWatermark.messageOffset

              fetchOffsetForTimestamp(replicaManager.logManager, topicPartition, timestamp) match {
                case Some(timestampOffset) if allowed(timestampOffset) => timestampOffset
                case _ => TimestampOffset(ListOffsetResponse.UNKNOWN_TIMESTAMP, ListOffsetResponse.UNKNOWN_OFFSET)
              }
            }
          }

          (topicPartition, new ListOffsetResponse.PartitionData(Errors.NONE, found.timestamp, found.offset))
        } catch {
          // NOTE: These exceptions are special cased since these error messages are typically transient or the client
          // would have received a clear exception and there is no value in logging the entire stack trace for the same
          case e @ (_ : UnknownTopicOrPartitionException |
                    _ : NotLeaderForPartitionException |
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

  private def fetchOffsetForTimestamp(logManager: LogManager, topicPartition: TopicPartition, timestamp: Long) : Option[TimestampOffset] = {
    logManager.getLog(topicPartition) match {
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
      offsetTimeArray(i) = (segments(i).baseOffset, segments(i).lastModified)
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
      AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, properties, RackAwareMode.Safe)
      info("Auto creation of topic %s with %d partitions and replication factor %d is successful"
        .format(topic, numPartitions, replicationFactor))
      new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, topic, Topic.isInternal(topic),
        java.util.Collections.emptyList())
    } catch {
      case _: TopicExistsException => // let it go, possibly another broker created this topic
        new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, topic, Topic.isInternal(topic),
          java.util.Collections.emptyList())
      case ex: Throwable  => // Catch all to prevent unhandled errors
        new MetadataResponse.TopicMetadata(Errors.forException(ex), topic, Topic.isInternal(topic),
          java.util.Collections.emptyList())
    }
  }

  private def createGroupMetadataTopic(): MetadataResponse.TopicMetadata = {
    val aliveBrokers = metadataCache.getAliveBrokers
    if (aliveBrokers.size < config.offsetsTopicReplicationFactor) {
      error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
        s"'${config.offsetsTopicReplicationFactor}' for the offsets topic (configured via " +
        s"'${KafkaConfig.OffsetsTopicReplicationFactorProp}'). This error can be ignored if the cluster is starting up " +
        s"and not all brokers are up yet.")
      new MetadataResponse.TopicMetadata(Errors.GROUP_COORDINATOR_NOT_AVAILABLE, Topic.GroupMetadataTopicName, true,
        java.util.Collections.emptyList())
    } else {
      createTopic(Topic.GroupMetadataTopicName, config.offsetsTopicPartitions,
        config.offsetsTopicReplicationFactor.toInt, coordinator.offsetsTopicConfigs)
    }
  }

  private def getOrCreateGroupMetadataTopic(listenerName: ListenerName): MetadataResponse.TopicMetadata = {
    val topicMetadata = metadataCache.getTopicMetadata(Set(Topic.GroupMetadataTopicName), listenerName)
    topicMetadata.headOption.getOrElse(createGroupMetadataTopic())
  }

  private def getTopicMetadata(topics: Set[String], listenerName: ListenerName, errorUnavailableEndpoints: Boolean): Seq[MetadataResponse.TopicMetadata] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, listenerName, errorUnavailableEndpoints)
    if (topics.isEmpty || topicResponses.size == topics.size) {
      topicResponses
    } else {
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet
      val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
        if (topic == Topic.GroupMetadataTopicName) {
          val topicMetadata = createGroupMetadataTopic()
          if (topicMetadata.error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE) {
            new MetadataResponse.TopicMetadata(Errors.INVALID_REPLICATION_FACTOR, topic, Topic.isInternal(topic),
              java.util.Collections.emptyList())
          } else topicMetadata
        } else if (config.autoCreateTopicsEnable) {
          createTopic(topic, config.numPartitions, config.defaultReplicationFactor)
        } else {
          new MetadataResponse.TopicMetadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, false,
            java.util.Collections.emptyList())
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
    val requestVersion = request.header.apiVersion()

    val topics =
      // Handle old metadata request logic. Version 0 has no way to specify "no topics".
      if (requestVersion == 0) {
        if (metadataRequest.topics() == null || metadataRequest.topics().isEmpty)
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
      topics.partition(topic => authorize(request.session, Describe, new Resource(auth.Topic, topic)))

    var unauthorizedForCreateTopics = Set[String]()

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = metadataCache.getNonExistingTopics(authorizedTopics)
      if (config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        if (!authorize(request.session, Create, Resource.ClusterResource)) {
          authorizedTopics --= nonExistingTopics
          unauthorizedForCreateTopics ++= nonExistingTopics
        }
      }
    }

    val unauthorizedForCreateTopicMetadata = unauthorizedForCreateTopics.map(topic =>
      new MetadataResponse.TopicMetadata(Errors.TOPIC_AUTHORIZATION_FAILED, topic, Topic.isInternal(topic),
        java.util.Collections.emptyList()))

    // do not disclose the existence of topics unauthorized for Describe, so we've not even checked if they exist or not
    val unauthorizedForDescribeTopicMetadata =
      // In case of all topics, don't include topics unauthorized for Describe
      if ((requestVersion == 0 && (metadataRequest.topics == null || metadataRequest.topics.isEmpty)) || metadataRequest.isAllTopics)
        Set.empty[MetadataResponse.TopicMetadata]
      else
        unauthorizedForDescribeTopics.map(topic =>
          new MetadataResponse.TopicMetadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, false, java.util.Collections.emptyList()))

    // In version 0, we returned an error when brokers with replicas were unavailable,
    // while in higher versions we simply don't include the broker in the returned broker list
    val errorUnavailableEndpoints = requestVersion == 0
    val topicMetadata =
      if (authorizedTopics.isEmpty)
        Seq.empty[MetadataResponse.TopicMetadata]
      else
        getTopicMetadata(authorizedTopics, request.listenerName, errorUnavailableEndpoints)

    val completeTopicMetadata = topicMetadata ++ unauthorizedForCreateTopicMetadata ++ unauthorizedForDescribeTopicMetadata

    val brokers = metadataCache.getAliveBrokers

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))

    val responseBody = new MetadataResponse(
      brokers.map(_.getNode(request.listenerName)).asJava,
      clusterId,
      metadataCache.getControllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
      completeTopicMetadata.asJava
    )
    requestChannel.sendResponse(new RequestChannel.Response(request, responseBody))
  }

  /**
   * Handle an offset fetch request
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val header = request.header
    val offsetFetchRequest = request.body[OffsetFetchRequest]

    def authorizeTopicDescribe(partition: TopicPartition) =
      authorize(request.session, Describe, new Resource(auth.Topic, partition.topic))

    val offsetFetchResponse =
      // reject the request if not authorized to the group
      if (!authorize(request.session, Read, new Resource(Group, offsetFetchRequest.groupId)))
        offsetFetchRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED)
      else {
        if (header.apiVersion == 0) {
          val (authorizedPartitions, unauthorizedPartitions) = offsetFetchRequest.partitions.asScala
            .partition(authorizeTopicDescribe)

          // version 0 reads offsets from ZK
          val authorizedPartitionData = authorizedPartitions.map { topicPartition =>
            val topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, topicPartition.topic)
            try {
              if (!metadataCache.contains(topicPartition.topic))
                (topicPartition, OffsetFetchResponse.UNKNOWN_PARTITION)
              else {
                val payloadOpt = zkUtils.readDataMaybeNull(s"${topicDirs.consumerOffsetDir}/${topicPartition.partition}")._1
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

          val unauthorizedPartitionData = unauthorizedPartitions.map(_ -> OffsetFetchResponse.UNKNOWN_PARTITION).toMap
          new OffsetFetchResponse(Errors.NONE, (authorizedPartitionData ++ unauthorizedPartitionData).asJava)
        } else {
          // versions 1 and above read offsets from Kafka
          if (offsetFetchRequest.isAllPartitions) {
            val (error, allPartitionData) = coordinator.handleFetchOffsets(offsetFetchRequest.groupId)
            if (error != Errors.NONE)
              offsetFetchRequest.getErrorResponse(error)
            else {
              // clients are not allowed to see offsets for topics that are not authorized for Describe
              val authorizedPartitionData = allPartitionData.filter { case (topicPartition, _) => authorizeTopicDescribe(topicPartition) }
              new OffsetFetchResponse(Errors.NONE, authorizedPartitionData.asJava)
            }
          } else {
            val (authorizedPartitions, unauthorizedPartitions) = offsetFetchRequest.partitions.asScala
              .partition(authorizeTopicDescribe)
            val (error, authorizedPartitionData) = coordinator.handleFetchOffsets(offsetFetchRequest.groupId,
              Some(authorizedPartitions))
            if (error != Errors.NONE)
              offsetFetchRequest.getErrorResponse(error)
            else {
              val unauthorizedPartitionData = unauthorizedPartitions.map(_ -> OffsetFetchResponse.UNKNOWN_PARTITION).toMap
              new OffsetFetchResponse(Errors.NONE, (authorizedPartitionData ++ unauthorizedPartitionData).asJava)
            }
          }
        }
      }

    trace(s"Sending offset fetch response $offsetFetchResponse for correlation id ${header.correlationId} to client ${header.clientId}.")
    requestChannel.sendResponse(new Response(request, offsetFetchResponse))
  }

  def handleGroupCoordinatorRequest(request: RequestChannel.Request) {
    val groupCoordinatorRequest = request.body[GroupCoordinatorRequest]

    if (!authorize(request.session, Describe, new Resource(Group, groupCoordinatorRequest.groupId))) {
      val responseBody = new GroupCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED, Node.noNode)
      requestChannel.sendResponse(new RequestChannel.Response(request, responseBody))
    } else {
      val partition = coordinator.partitionFor(groupCoordinatorRequest.groupId)

      // get metadata (and create the topic if necessary)
      val offsetsTopicMetadata = getOrCreateGroupMetadataTopic(request.listenerName)

      val responseBody = if (offsetsTopicMetadata.error != Errors.NONE) {
        new GroupCoordinatorResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE, Node.noNode)
      } else {
        val coordinatorEndpoint = offsetsTopicMetadata.partitionMetadata().asScala
          .find(_.partition == partition)
          .map(_.leader())

        coordinatorEndpoint match {
          case Some(endpoint) if !endpoint.isEmpty =>
            new GroupCoordinatorResponse(Errors.NONE, endpoint)
          case _ =>
            new GroupCoordinatorResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE, Node.noNode)
        }
      }

      trace("Sending consumer metadata %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, responseBody))
    }
  }

  def handleDescribeGroupRequest(request: RequestChannel.Request) {
    val describeRequest = request.body[DescribeGroupsRequest]

    val groups = describeRequest.groupIds().asScala.map { groupId =>
        if (!authorize(request.session, Describe, new Resource(Group, groupId))) {
          groupId -> DescribeGroupsResponse.GroupMetadata.forError(Errors.GROUP_AUTHORIZATION_FAILED)
        } else {
          val (error, summary) = coordinator.handleDescribeGroup(groupId)
          val members = summary.members.map { member =>
            val metadata = ByteBuffer.wrap(member.metadata)
            val assignment = ByteBuffer.wrap(member.assignment)
            new DescribeGroupsResponse.GroupMember(member.memberId, member.clientId, member.clientHost, metadata, assignment)
          }
          groupId -> new DescribeGroupsResponse.GroupMetadata(error, summary.state, summary.protocolType,
            summary.protocol, members.asJava)
        }
    }.toMap

    val responseBody = new DescribeGroupsResponse(groups.asJava)
    requestChannel.sendResponse(new RequestChannel.Response(request, responseBody))
  }

  def handleListGroupsRequest(request: RequestChannel.Request) {
    val responseBody = if (!authorize(request.session, Describe, Resource.ClusterResource)) {
      ListGroupsResponse.fromError(Errors.CLUSTER_AUTHORIZATION_FAILED)
    } else {
      val (error, groups) = coordinator.handleListGroups()
      val allGroups = groups.map { group => new ListGroupsResponse.Group(group.groupId, group.protocolType) }
      new ListGroupsResponse(error, allGroups.asJava)
    }
    requestChannel.sendResponse(new RequestChannel.Response(request, responseBody))
  }

  def handleJoinGroupRequest(request: RequestChannel.Request) {
    val joinGroupRequest = request.body[JoinGroupRequest]

    // the callback for sending a join-group response
    def sendResponseCallback(joinResult: JoinGroupResult) {
      val members = joinResult.members map { case (memberId, metadataArray) => (memberId, ByteBuffer.wrap(metadataArray)) }
      val responseBody = new JoinGroupResponse(joinResult.error, joinResult.generationId,
        joinResult.subProtocol, joinResult.memberId, joinResult.leaderId, members.asJava)

      trace("Sending join group response %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, responseBody))
    }

    if (!authorize(request.session, Read, new Resource(Group, joinGroupRequest.groupId()))) {
      val responseBody = new JoinGroupResponse(
        Errors.GROUP_AUTHORIZATION_FAILED,
        JoinGroupResponse.UNKNOWN_GENERATION_ID,
        JoinGroupResponse.UNKNOWN_PROTOCOL,
        JoinGroupResponse.UNKNOWN_MEMBER_ID, // memberId
        JoinGroupResponse.UNKNOWN_MEMBER_ID, // leaderId
        Collections.emptyMap())
      requestChannel.sendResponse(new RequestChannel.Response(request, responseBody))
    } else {
      // let the coordinator to handle join-group
      val protocols = joinGroupRequest.groupProtocols().asScala.map(protocol =>
        (protocol.name, Utils.toArray(protocol.metadata))).toList
      coordinator.handleJoinGroup(
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
      val responseBody = new SyncGroupResponse(error, ByteBuffer.wrap(memberState))
      requestChannel.sendResponse(new Response(request, responseBody))
    }

    if (!authorize(request.session, Read, new Resource(Group, syncGroupRequest.groupId()))) {
      sendResponseCallback(Array[Byte](), Errors.GROUP_AUTHORIZATION_FAILED)
    } else {
      coordinator.handleSyncGroup(
        syncGroupRequest.groupId(),
        syncGroupRequest.generationId(),
        syncGroupRequest.memberId(),
        syncGroupRequest.groupAssignment().asScala.mapValues(Utils.toArray),
        sendResponseCallback
      )
    }
  }

  def handleHeartbeatRequest(request: RequestChannel.Request) {
    val heartbeatRequest = request.body[HeartbeatRequest]

    // the callback for sending a heartbeat response
    def sendResponseCallback(error: Errors) {
      val response = new HeartbeatResponse(error)
      trace("Sending heartbeat response %s for correlation id %d to client %s."
        .format(response, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, response))
    }

    if (!authorize(request.session, Read, new Resource(Group, heartbeatRequest.groupId))) {
      val heartbeatResponse = new HeartbeatResponse(Errors.GROUP_AUTHORIZATION_FAILED)
      requestChannel.sendResponse(new Response(request, heartbeatResponse))
    }
    else {
      // let the coordinator to handle heartbeat
      coordinator.handleHeartbeat(
        heartbeatRequest.groupId(),
        heartbeatRequest.memberId(),
        heartbeatRequest.groupGenerationId(),
        sendResponseCallback)
    }
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request) {
    val leaveGroupRequest = request.body[LeaveGroupRequest]

    // the callback for sending a leave-group response
    def sendResponseCallback(error: Errors) {
      val response = new LeaveGroupResponse(error)
      trace("Sending leave group response %s for correlation id %d to client %s."
                    .format(response, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, response))
    }

    if (!authorize(request.session, Read, new Resource(Group, leaveGroupRequest.groupId))) {
      val leaveGroupResponse = new LeaveGroupResponse(Errors.GROUP_AUTHORIZATION_FAILED)
      requestChannel.sendResponse(new Response(request, leaveGroupResponse))
    } else {
      // let the coordinator to handle leave-group
      coordinator.handleLeaveGroup(
        leaveGroupRequest.groupId(),
        leaveGroupRequest.memberId(),
        sendResponseCallback)
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request) {
    val response = new SaslHandshakeResponse(Errors.ILLEGAL_SASL_STATE, config.saslEnabledMechanisms)
    requestChannel.sendResponse(new RequestChannel.Response(request, response))
  }

  def handleApiVersionsRequest(request: RequestChannel.Request) {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on a SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    val responseSend =
      if (Protocol.apiVersionSupported(ApiKeys.API_VERSIONS.id, request.header.apiVersion))
        ApiVersionsResponse.API_VERSIONS_RESPONSE.toSend(request.connectionId, request.header)
      else ApiVersionsResponse.unsupportedVersionSend(request.connectionId, request.header)
    requestChannel.sendResponse(new RequestChannel.Response(request, responseSend))
  }

  def close() {
    quotas.shutdown()
    info("Shutdown complete.")
  }

  def handleCreateTopicsRequest(request: RequestChannel.Request) {
    val createTopicsRequest = request.body[CreateTopicsRequest]

    def sendResponseCallback(results: Map[String, CreateTopicsResponse.Error]): Unit = {
      val responseBody = new CreateTopicsResponse(results.asJava)
      trace(s"Sending create topics response $responseBody for correlation id ${request.header.correlationId} to client ${request.header.clientId}.")
      requestChannel.sendResponse(new RequestChannel.Response(request, responseBody))
    }

    if (!controller.isActive) {
      val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
        (topic, new CreateTopicsResponse.Error(Errors.NOT_CONTROLLER, null))
      }
      sendResponseCallback(results)
    } else if (!authorize(request.session, Create, Resource.ClusterResource)) {
      val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
        (topic, new CreateTopicsResponse.Error(Errors.CLUSTER_AUTHORIZATION_FAILED, null))
      }
      sendResponseCallback(results)
    } else {
      val (validTopics, duplicateTopics) = createTopicsRequest.topics.asScala.partition { case (topic, _) =>
        !createTopicsRequest.duplicateTopics.contains(topic)
      }

      // Special handling to add duplicate topics to the response
      def sendResponseWithDuplicatesCallback(results: Map[String, CreateTopicsResponse.Error]): Unit = {

        val duplicatedTopicsResults =
          if (duplicateTopics.nonEmpty) {
            val errorMessage = s"Create topics request from client `${request.header.clientId}` contains multiple entries " +
              s"for the following topics: ${duplicateTopics.keySet.mkString(",")}"
            // We can send the error message in the response for version 1, so we don't have to log it any more
            if (request.header.apiVersion == 0)
              warn(errorMessage)
            duplicateTopics.keySet.map((_, new CreateTopicsResponse.Error(Errors.INVALID_REQUEST, errorMessage))).toMap
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

  def handleDeleteTopicsRequest(request: RequestChannel.Request) {
    val deleteTopicRequest = request.body[DeleteTopicsRequest]

    val (existingAndAuthorizedForDescribeTopics, nonExistingOrUnauthorizedForDescribeTopics) = deleteTopicRequest.topics.asScala.partition { topic =>
      authorize(request.session, Describe, new Resource(auth.Topic, topic)) && metadataCache.contains(topic)
    }

    val (authorizedTopics, unauthorizedForDeleteTopics) = existingAndAuthorizedForDescribeTopics.partition { topic =>
      authorize(request.session, Delete, new Resource(auth.Topic, topic))
    }

    def sendResponseCallback(results: Map[String, Errors]): Unit = {
      val completeResults = nonExistingOrUnauthorizedForDescribeTopics.map(topic => (topic, Errors.UNKNOWN_TOPIC_OR_PARTITION)).toMap ++
          unauthorizedForDeleteTopics.map(topic => (topic, Errors.TOPIC_AUTHORIZATION_FAILED)).toMap ++ results
      val responseBody = new DeleteTopicsResponse(completeResults.asJava)
      trace(s"Sending delete topics response $responseBody for correlation id ${request.header.correlationId} to client ${request.header.clientId}.")
      requestChannel.sendResponse(new RequestChannel.Response(request, responseBody))
    }

    if (!controller.isActive) {
      val results = deleteTopicRequest.topics.asScala.map { topic =>
        (topic, Errors.NOT_CONTROLLER)
      }.toMap
      sendResponseCallback(results)
    } else {
      // If no authorized topics return immediately
      if (authorizedTopics.isEmpty)
        sendResponseCallback(Map())
      else {
        adminManager.deleteTopics(
          deleteTopicRequest.timeout.toInt,
          authorizedTopics,
          sendResponseCallback
        )
      }
    }
  }

  def authorizeClusterAction(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, ClusterAction, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }
}
