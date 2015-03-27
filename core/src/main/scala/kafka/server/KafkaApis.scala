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

import org.apache.kafka.common.requests.JoinGroupResponse
import org.apache.kafka.common.requests.HeartbeatResponse
import org.apache.kafka.common.TopicPartition

import kafka.api._
import kafka.admin.AdminUtils
import kafka.common._
import kafka.controller.KafkaController
import kafka.coordinator.ConsumerCoordinator
import kafka.log._
import kafka.network._
import kafka.network.RequestChannel.Response
import kafka.utils.{SystemTime, Logging}

import scala.collection._

import org.I0Itec.zkclient.ZkClient

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val offsetManager: OffsetManager,
                val coordinator: ConsumerCoordinator,
                val controller: KafkaController,
                val zkClient: ZkClient,
                val brokerId: Int,
                val config: KafkaConfig,
                val metadataCache: MetadataCache) extends Logging {

  this.logIdent = "[KafkaApi-%d] ".format(brokerId)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try{
      trace("Handling request: " + request.requestObj + " from client: " + request.remoteAddress)
      request.requestId match {
        case RequestKeys.ProduceKey => handleProducerRequest(request)
        case RequestKeys.FetchKey => handleFetchRequest(request)
        case RequestKeys.OffsetsKey => handleOffsetRequest(request)
        case RequestKeys.MetadataKey => handleTopicMetadataRequest(request)
        case RequestKeys.LeaderAndIsrKey => handleLeaderAndIsrRequest(request)
        case RequestKeys.StopReplicaKey => handleStopReplicaRequest(request)
        case RequestKeys.UpdateMetadataKey => handleUpdateMetadataRequest(request)
        case RequestKeys.ControlledShutdownKey => handleControlledShutdownRequest(request)
        case RequestKeys.OffsetCommitKey => handleOffsetCommitRequest(request)
        case RequestKeys.OffsetFetchKey => handleOffsetFetchRequest(request)
        case RequestKeys.ConsumerMetadataKey => handleConsumerMetadataRequest(request)
        case RequestKeys.JoinGroupKey => handleJoinGroupRequest(request)
        case RequestKeys.HeartbeatKey => handleHeartbeatRequest(request)
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable =>
        request.requestObj.handleError(e, requestChannel, request)
        error("error when handling request %s".format(request.requestObj), e)
    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val leaderAndIsrRequest = request.requestObj.asInstanceOf[LeaderAndIsrRequest]
    try {
      val (response, error) = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest, offsetManager)
      val leaderAndIsrResponse = new LeaderAndIsrResponse(leaderAndIsrRequest.correlationId, response, error)
      requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(leaderAndIsrResponse)))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error during leadership change.", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.requestObj.asInstanceOf[StopReplicaRequest]
    val (response, error) = replicaManager.stopReplicas(stopReplicaRequest)
    val stopReplicaResponse = new StopReplicaResponse(stopReplicaRequest.correlationId, response.toMap, error)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(stopReplicaResponse)))
    replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads()
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val updateMetadataRequest = request.requestObj.asInstanceOf[UpdateMetadataRequest]
    replicaManager.maybeUpdateMetadataCache(updateMetadataRequest, metadataCache)

    val updateMetadataResponse = new UpdateMetadataResponse(updateMetadataRequest.correlationId)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(updateMetadataResponse)))
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.requestObj.asInstanceOf[ControlledShutdownRequest]
    val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId)
    val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
      ErrorMapping.NoError, partitionsRemaining)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(controlledShutdownResponse)))
  }


  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val offsetCommitRequest = request.requestObj.asInstanceOf[OffsetCommitRequest]

    // the callback for sending an offset commit response
    def sendResponseCallback(commitStatus: immutable.Map[TopicAndPartition, Short]) {
      commitStatus.foreach { case (topicAndPartition, errorCode) =>
        // we only print warnings for known errors here; only replica manager could see an unknown
        // exception while trying to write the offset message to the local log, and it will log
        // an error message and write the error code in this case; hence it can be ignored here
        if (errorCode != ErrorMapping.NoError && errorCode != ErrorMapping.UnknownCode) {
          debug("Offset commit request with correlation id %d from client %s on partition %s failed due to %s"
            .format(offsetCommitRequest.correlationId, offsetCommitRequest.clientId,
            topicAndPartition, ErrorMapping.exceptionNameFor(errorCode)))
        }
      }

      val response = OffsetCommitResponse(commitStatus, offsetCommitRequest.correlationId)
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    }

    // compute the retention time based on the request version:
    // if it is before v2 or not specified by user, we can use the default retention
    val offsetRetention =
      if (offsetCommitRequest.versionId <= 1 ||
        offsetCommitRequest.retentionMs == org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_RETENTION_TIME) {
        offsetManager.config.offsetsRetentionMs
      } else {
        offsetCommitRequest.retentionMs
      }

    // commit timestamp is always set to now.
    // "default" expiration timestamp is now + retention (and retention may be overridden if v2)
    // expire timestamp is computed differently for v1 and v2.
    //   - If v1 and no explicit commit timestamp is provided we use default expiration timestamp.
    //   - If v1 and explicit commit timestamp is provided we calculate retention from that explicit commit timestamp
    //   - If v2 we use the default expiration timestamp
    val currentTimestamp = SystemTime.milliseconds
    val defaultExpireTimestamp = offsetRetention + currentTimestamp

    val offsetData = offsetCommitRequest.requestInfo.mapValues(offsetAndMetadata =>
      offsetAndMetadata.copy(
        commitTimestamp = currentTimestamp,
        expireTimestamp = {
          if (offsetAndMetadata.commitTimestamp == org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP)
            defaultExpireTimestamp
          else
            offsetRetention + offsetAndMetadata.commitTimestamp
        }
      )
    )

    // call offset manager to store offsets
    offsetManager.storeOffsets(
      offsetCommitRequest.groupId,
      offsetCommitRequest.consumerId,
      offsetCommitRequest.groupGenerationId,
      offsetData,
      sendResponseCallback)
  }

  /**
   * Handle a produce request
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.requestObj.asInstanceOf[ProducerRequest]

    // the callback for sending a produce response
    def sendResponseCallback(responseStatus: Map[TopicAndPartition, ProducerResponseStatus]) {
      var errorInResponse = false
      responseStatus.foreach { case (topicAndPartition, status) =>
        // we only print warnings for known errors here; if it is unknown, it will cause
        // an error message in the replica manager
        if (status.error != ErrorMapping.NoError && status.error != ErrorMapping.UnknownCode) {
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s"
            .format(produceRequest.correlationId, produceRequest.clientId,
            topicAndPartition, ErrorMapping.exceptionNameFor(status.error)))
          errorInResponse = true
        }
      }

      if (produceRequest.requiredAcks == 0) {
        // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
        // the request, since no response is expected by the producer, the server will close socket server so that
        // the producer client will know that some error has happened and will refresh its metadata
        if (errorInResponse) {
          info("Close connection due to error handling produce request with correlation id %d from client id %s with ack=0"
                  .format(produceRequest.correlationId, produceRequest.clientId))
          requestChannel.closeConnection(request.processor, request)
        } else {
          requestChannel.noOperation(request.processor, request)
        }
      } else {
        val response = ProducerResponse(produceRequest.correlationId, responseStatus)
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
      }
    }

    // only allow appending to internal topic partitions
    // if the client is not from admin
    val internalTopicsAllowed = produceRequest.clientId == AdminUtils.AdminClientId

    // call the replica manager to append messages to the replicas
    replicaManager.appendMessages(
      produceRequest.ackTimeoutMs.toLong,
      produceRequest.requiredAcks,
      internalTopicsAllowed,
      produceRequest.data,
      sendResponseCallback)

    // if the request is put into the purgatory, it will have a held reference
    // and hence cannot be garbage collected; hence we clear its data here in
    // order to let GC re-claim its memory since it is already appended to log
    produceRequest.emptyData()
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]

    // the callback for sending a fetch response
    def sendResponseCallback(responsePartitionData: Map[TopicAndPartition, FetchResponsePartitionData]) {
      responsePartitionData.foreach { case (topicAndPartition, data) =>
        // we only print warnings for known errors here; if it is unknown, it will cause
        // an error message in the replica manager already and hence can be ignored here
        if (data.error != ErrorMapping.NoError && data.error != ErrorMapping.UnknownCode) {
          debug("Fetch request with correlation id %d from client %s on partition %s failed due to %s"
            .format(fetchRequest.correlationId, fetchRequest.clientId,
            topicAndPartition, ErrorMapping.exceptionNameFor(data.error)))
        }

        // record the bytes out metrics only when the response is being sent
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesOutRate.mark(data.messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats().bytesOutRate.mark(data.messages.sizeInBytes)
      }

      val response = FetchResponse(fetchRequest.correlationId, responsePartitionData)
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)))
    }

    // call the replica manager to fetch messages from the local replica
    replicaManager.fetchMessages(
      fetchRequest.maxWait.toLong,
      fetchRequest.replicaId,
      fetchRequest.minBytes,
      fetchRequest.requestInfo,
      sendResponseCallback)
  }

  /**
   * Handle an offset request
   */
  def handleOffsetRequest(request: RequestChannel.Request) {
    val offsetRequest = request.requestObj.asInstanceOf[OffsetRequest]
    val responseMap = offsetRequest.requestInfo.map(elem => {
      val (topicAndPartition, partitionOffsetRequestInfo) = elem
      try {
        // ensure leader exists
        val localReplica = if(!offsetRequest.isFromDebuggingClient)
          replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition)
        else
          replicaManager.getReplicaOrException(topicAndPartition.topic, topicAndPartition.partition)
        val offsets = {
          val allOffsets = fetchOffsets(replicaManager.logManager,
                                        topicAndPartition,
                                        partitionOffsetRequestInfo.time,
                                        partitionOffsetRequestInfo.maxNumOffsets)
          if (!offsetRequest.isFromOrdinaryClient) {
            allOffsets
          } else {
            val hw = localReplica.highWatermark.messageOffset
            if (allOffsets.exists(_ > hw))
              hw +: allOffsets.dropWhile(_ > hw)
            else
              allOffsets
          }
        }
        (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.NoError, offsets))
      } catch {
        // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
        // are typically transient and there is no value in logging the entire stack trace for the same
        case utpe: UnknownTopicOrPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition, utpe.getMessage))
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case nle: NotLeaderForPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition,nle.getMessage))
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case e: Throwable =>
          error("Error while responding to offset request", e)
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), Nil) )
      }
    })
    val response = OffsetResponse(offsetRequest.correlationId, responseMap)
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  def fetchOffsets(logManager: LogManager, topicAndPartition: TopicAndPartition, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    logManager.getLog(topicAndPartition) match {
      case Some(log) =>
        fetchOffsetsBefore(log, timestamp, maxNumOffsets)
      case None =>
        if (timestamp == OffsetRequest.LatestTime || timestamp == OffsetRequest.EarliestTime)
          Seq(0L)
        else
          Nil
    }
  }

  private def fetchOffsetsBefore(log: Log, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    val segsArray = log.logSegments.toArray
    var offsetTimeArray: Array[(Long, Long)] = null
    if(segsArray.last.size > 0)
      offsetTimeArray = new Array[(Long, Long)](segsArray.length + 1)
    else
      offsetTimeArray = new Array[(Long, Long)](segsArray.length)

    for(i <- 0 until segsArray.length)
      offsetTimeArray(i) = (segsArray(i).baseOffset, segsArray(i).lastModified)
    if(segsArray.last.size > 0)
      offsetTimeArray(segsArray.length) = (log.logEndOffset, SystemTime.milliseconds)

    var startIndex = -1
    timestamp match {
      case OffsetRequest.LatestTime =>
        startIndex = offsetTimeArray.length - 1
      case OffsetRequest.EarliestTime =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -=1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for(j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(- _)
  }

  private def getTopicMetadata(topics: Set[String]): Seq[TopicMetadata] = {
    val topicResponses = metadataCache.getTopicMetadata(topics)
    if (topics.size > 0 && topicResponses.size != topics.size) {
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet
      val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
        if (topic == OffsetManager.OffsetsTopicName || config.autoCreateTopicsEnable) {
          try {
            if (topic == OffsetManager.OffsetsTopicName) {
              val aliveBrokers = metadataCache.getAliveBrokers
              val offsetsTopicReplicationFactor =
                if (aliveBrokers.length > 0)
                  Math.min(config.offsetsTopicReplicationFactor, aliveBrokers.length)
                else
                  config.offsetsTopicReplicationFactor
              AdminUtils.createTopic(zkClient, topic, config.offsetsTopicPartitions,
                                     offsetsTopicReplicationFactor,
                                     offsetManager.offsetsTopicConfig)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                .format(topic, config.offsetsTopicPartitions, offsetsTopicReplicationFactor))
            }
            else {
              AdminUtils.createTopic(zkClient, topic, config.numPartitions, config.defaultReplicationFactor)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                   .format(topic, config.numPartitions, config.defaultReplicationFactor))
            }
          } catch {
            case e: TopicExistsException => // let it go, possibly another broker created this topic
          }
          new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.LeaderNotAvailableCode)
        } else {
          new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)
        }
      }
      topicResponses.appendAll(responsesForNonExistentTopics)
    }
    topicResponses
  }

  /**
   * Handle a topic metadata request
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.requestObj.asInstanceOf[TopicMetadataRequest]
    val topicMetadata = getTopicMetadata(metadataRequest.topics.toSet)
    val brokers = metadataCache.getAliveBrokers
    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(topicMetadata.mkString(","), brokers.mkString(","), metadataRequest.correlationId, metadataRequest.clientId))
    val response = new TopicMetadataResponse(brokers, topicMetadata, metadataRequest.correlationId)
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  /*
   * Handle an offset fetch request
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val offsetFetchRequest = request.requestObj.asInstanceOf[OffsetFetchRequest]

    val (unknownTopicPartitions, knownTopicPartitions) = offsetFetchRequest.requestInfo.partition(topicAndPartition =>
      metadataCache.getPartitionInfo(topicAndPartition.topic, topicAndPartition.partition).isEmpty
    )
    val unknownStatus = unknownTopicPartitions.map(topicAndPartition => (topicAndPartition, OffsetMetadataAndError.UnknownTopicOrPartition)).toMap
    val knownStatus =
      if (knownTopicPartitions.size > 0)
        offsetManager.getOffsets(offsetFetchRequest.groupId, knownTopicPartitions).toMap
      else
        Map.empty[TopicAndPartition, OffsetMetadataAndError]
    val status = unknownStatus ++ knownStatus

    val response = OffsetFetchResponse(status, offsetFetchRequest.correlationId)

    trace("Sending offset fetch response %s for correlation id %d to client %s."
          .format(response, offsetFetchRequest.correlationId, offsetFetchRequest.clientId))
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  /*
   * Handle a consumer metadata request
   */
  def handleConsumerMetadataRequest(request: RequestChannel.Request) {
    val consumerMetadataRequest = request.requestObj.asInstanceOf[ConsumerMetadataRequest]

    val partition = offsetManager.partitionFor(consumerMetadataRequest.group)

    // get metadata (and create the topic if necessary)
    val offsetsTopicMetadata = getTopicMetadata(Set(OffsetManager.OffsetsTopicName)).head

    val errorResponse = ConsumerMetadataResponse(None, ErrorMapping.ConsumerCoordinatorNotAvailableCode, consumerMetadataRequest.correlationId)

    val response =
      offsetsTopicMetadata.partitionsMetadata.find(_.partitionId == partition).map { partitionMetadata =>
        partitionMetadata.leader.map { leader =>
          ConsumerMetadataResponse(Some(leader), ErrorMapping.NoError, consumerMetadataRequest.correlationId)
        }.getOrElse(errorResponse)
      }.getOrElse(errorResponse)

    trace("Sending consumer metadata %s for correlation id %d to client %s."
          .format(response, consumerMetadataRequest.correlationId, consumerMetadataRequest.clientId))
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  def handleJoinGroupRequest(request: RequestChannel.Request) {
    import JavaConversions._

    val joinGroupRequest = request.requestObj.asInstanceOf[JoinGroupRequestAndHeader]

    // the callback for sending a join-group response
    def sendResponseCallback(partitions: List[TopicAndPartition], generationId: Int, errorCode: Short) {
      val partitionList = partitions.map(tp => new TopicPartition(tp.topic, tp.partition)).toBuffer
      val responseBody = new JoinGroupResponse(errorCode, generationId, joinGroupRequest.body.consumerId, partitionList)
      val response = new JoinGroupResponseAndHeader(joinGroupRequest.correlationId, responseBody)
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    }

    // let the coordinator to handle join-group
    coordinator.consumerJoinGroup(
      joinGroupRequest.body.groupId(),
      joinGroupRequest.body.consumerId(),
      joinGroupRequest.body.topics().toList,
      joinGroupRequest.body.sessionTimeout(),
      joinGroupRequest.body.strategy(),
      sendResponseCallback)
  }

  def handleHeartbeatRequest(request: RequestChannel.Request) {
    val heartbeatRequest = request.requestObj.asInstanceOf[HeartbeatRequestAndHeader]

    // the callback for sending a heartbeat response
    def sendResponseCallback(errorCode: Short) {
      val response = new HeartbeatResponseAndHeader(heartbeatRequest.correlationId, new HeartbeatResponse(errorCode))
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    }

    // let the coordinator to handle heartbeat
    coordinator.consumerHeartbeat(
      heartbeatRequest.body.groupId(),
      heartbeatRequest.body.consumerId(),
      heartbeatRequest.body.groupGenerationId(),
      sendResponseCallback)
  }

  def close() {
    // TODO currently closing the API is an no-op since the API no longer maintain any modules
    // maybe removing the closing call in the end when KafkaAPI becomes a pure stateless layer
    debug("Shut down complete.")
  }
}
