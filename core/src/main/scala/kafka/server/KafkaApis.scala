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

import java.io.IOException
import kafka.admin.{CreateTopicCommand, AdminUtils}
import kafka.api._
import kafka.common._
import kafka.message._
import kafka.network._
import kafka.utils.{TopicNameValidator, Pool, SystemTime, Logging}
import org.apache.log4j.Logger
import scala.collection._
import mutable.HashMap
import scala.math._
import kafka.network.RequestChannel.Response
import java.util.concurrent.TimeUnit
import kafka.metrics.KafkaMetricsGroup
import org.I0Itec.zkclient.ZkClient

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val zkClient: ZkClient,
                brokerId: Int) extends Logging {

  private val metricsGroup = brokerId.toString
  private val producerRequestPurgatory = new ProducerRequestPurgatory(brokerId)
  private val fetchRequestPurgatory = new FetchRequestPurgatory(brokerId, requestChannel)
  private val delayedRequestMetrics = new DelayedRequestMetrics
  private val topicNameValidator = new TopicNameValidator(replicaManager.config.maxTopicNameLength)

  private val requestLogger = Logger.getLogger("kafka.request.logger")
  this.logIdent = "[KafkaApi on Broker " + brokerId + "], "

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    val apiId = request.request.buffer.getShort()
    apiId match {
      case RequestKeys.Produce => handleProducerRequest(request)
      case RequestKeys.Fetch => handleFetchRequest(request)
      case RequestKeys.Offsets => handleOffsetRequest(request)
      case RequestKeys.TopicMetadata => handleTopicMetadataRequest(request)
      case RequestKeys.LeaderAndISRRequest => handleLeaderAndISRRequest(request)
      case RequestKeys.StopReplicaRequest => handleStopReplicaRequest(request)
      case _ => throw new KafkaException("No mapping found for handler id " + apiId)
    }
  }

  def handleLeaderAndISRRequest(request: RequestChannel.Request){
    val leaderAndISRRequest = LeaderAndIsrRequest.readFrom(request.request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling leader and isr request " + leaderAndISRRequest)
    trace("Handling leader and isr request " + leaderAndISRRequest)

    val responseMap = replicaManager.becomeLeaderOrFollower(leaderAndISRRequest)
    val leaderAndISRResponse = new LeaderAndISRResponse(leaderAndISRRequest.versionId, responseMap)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(leaderAndISRResponse)))
  }


  def handleStopReplicaRequest(request: RequestChannel.Request){
    val stopReplicaRequest = StopReplicaRequest.readFrom(request.request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling stop replica request " + stopReplicaRequest)
    trace("Handling stop replica request " + stopReplicaRequest)

    val responseMap = new HashMap[(String, Int), Short]

    for((topic, partitionId) <- stopReplicaRequest.stopReplicaSet){
      val errorCode = replicaManager.stopReplica(topic, partitionId)
      responseMap.put((topic, partitionId), errorCode)
    }
    val stopReplicaResponse = new StopReplicaResponse(stopReplicaRequest.versionId, responseMap)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(stopReplicaResponse)))
  }

  /**
   * Check if the partitionDataArray from a produce request can unblock any
   * DelayedFetch requests.
   */
  def maybeUnblockDelayedFetchRequests(topic: String, partitionDatas: Array[PartitionData]) {
    var satisfied = new mutable.ArrayBuffer[DelayedFetch]
    for(partitionData <- partitionDatas)
      satisfied ++= fetchRequestPurgatory.update(RequestKey(topic, partitionData.partition), null)
    trace("Producer request to %s unblocked %d fetch requests.".format(topic, satisfied.size))
    // send any newly unblocked responses
    for(fetchReq <- satisfied) {
      val topicData = readMessageSets(fetchReq.fetch)
      val response = new FetchResponse(FetchRequest.CurrentVersion, fetchReq.fetch.correlationId, topicData)

      val fromFollower = fetchReq.fetch.replicaId != FetchRequest.NonFollowerId
      delayedRequestMetrics.recordDelayedFetchSatisfied(
        fromFollower, SystemTime.nanoseconds - fetchReq.creationTimeNs, response)

      requestChannel.sendResponse(new RequestChannel.Response(fetchReq.request, new FetchResponseSend(response)))
    }
  }

  /**
   * Handle a produce request
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = ProducerRequest.readFrom(request.request.buffer)
    val sTime = SystemTime.milliseconds
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling producer request " + request.toString)
    trace("Handling producer request " + request.toString)

    val response = produceToLocalLog(produceRequest)
    debug("Produce to local log in %d ms".format(SystemTime.milliseconds - sTime))
    val partitionsInError = response.errors.count(_ != ErrorMapping.NoError)
    
    for (topicData <- produceRequest.data)
      maybeUnblockDelayedFetchRequests(topicData.topic, topicData.partitionDataArray)
    
    if (produceRequest.requiredAcks == 0 || produceRequest.requiredAcks == 1 ||
        produceRequest.data.size <= 0 || partitionsInError == response.errors.size)
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    else {
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val producerRequestKeys = produceRequest.data.flatMap(topicData => {
        val topic = topicData.topic
        topicData.partitionDataArray.map(partitionData => {
          RequestKey(topic, partitionData.partition)
        })
      })

      val delayedProduce = new DelayedProduce(
        producerRequestKeys, request,
        response.errors, response.offsets,
        produceRequest, produceRequest.ackTimeoutMs.toLong)
      producerRequestPurgatory.watch(delayedProduce)

      /*
       * Replica fetch requests may have arrived (and potentially satisfied)
       * delayedProduce requests while they were being added to the purgatory.
       * Here, we explicitly check if any of them can be satisfied.
       */
      var satisfiedProduceRequests = new mutable.ArrayBuffer[DelayedProduce]
      producerRequestKeys.foreach(key =>
        satisfiedProduceRequests ++=
          producerRequestPurgatory.update(key, key))
      debug(satisfiedProduceRequests.size +
        " producer requests unblocked during produce to local log.")
      satisfiedProduceRequests.foreach(_.respond())
    }
  }

  /**
   * Helper method for handling a parsed producer request
   */
  private def produceToLocalLog(request: ProducerRequest): ProducerResponse = {
    trace("Produce [%s] to local log ".format(request.toString))
    val requestSize = request.topicPartitionCount
    val errors = new Array[Short](requestSize)
    val offsets = new Array[Long](requestSize)

    var msgIndex = -1
    for(topicData <- request.data) {
      for(partitionData <- topicData.partitionDataArray) {
        msgIndex += 1
        BrokerTopicStat.getBrokerTopicStat(topicData.topic).recordBytesIn(partitionData.messages.sizeInBytes)
        BrokerTopicStat.getBrokerAllTopicStat.recordBytesIn(partitionData.messages.sizeInBytes)
        try {
          val localReplica = replicaManager.getLeaderReplicaIfLocal(topicData.topic, partitionData.partition)
          val log = localReplica.log.get
          log.append(partitionData.messages.asInstanceOf[ByteBufferMessageSet])
          // we may need to increment high watermark since ISR could be down to 1
          localReplica.partition.maybeIncrementLeaderHW(localReplica)
          offsets(msgIndex) = log.logEndOffset
          errors(msgIndex) = ErrorMapping.NoError.toShort
          trace("%d bytes written to logs, nextAppendOffset = %d"
            .format(partitionData.messages.sizeInBytes, offsets(msgIndex)))
        } catch {
          case e =>
            BrokerTopicStat.getBrokerTopicStat(topicData.topic).recordFailedProduceRequest
            BrokerTopicStat.getBrokerAllTopicStat.recordFailedProduceRequest
            error("Error processing ProducerRequest on %s:%d".format(topicData.topic, partitionData.partition), e)
            e match {
              case _: IOException =>
                fatal("Halting due to unrecoverable I/O error while handling producer request: " + e.getMessage, e)
                System.exit(1)
              case _ =>
                errors(msgIndex) = ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]).toShort
                offsets(msgIndex) = -1
            }
        }
      }
    }
    new ProducerResponse(request.versionId, request.correlationId, errors, offsets)
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = FetchRequest.readFrom(request.request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling fetch request " + fetchRequest.toString)
    trace("Handling fetch request " + fetchRequest.toString)

    // validate the request
    try {
      fetchRequest.validate()
    } catch {
      case e:FetchRequestFormatException =>
        val response = new FetchResponse(fetchRequest.versionId, fetchRequest.correlationId, Array.empty)
        val channelResponse = new RequestChannel.Response(request, new FetchResponseSend(response))
        requestChannel.sendResponse(channelResponse)
    }

    if(fetchRequest.replicaId != FetchRequest.NonFollowerId) {
      maybeUpdatePartitionHW(fetchRequest)
      // after updating HW, some delayed produce requests may be unblocked
      var satisfiedProduceRequests = new mutable.ArrayBuffer[DelayedProduce]
      fetchRequest.offsetInfo.foreach(topicOffsetInfo => {
        topicOffsetInfo.partitions.foreach(partition => {
          val key = RequestKey(topicOffsetInfo.topic, partition)
          satisfiedProduceRequests ++= producerRequestPurgatory.update(key, key)
        })
      })
      debug("Replica %d fetch unblocked %d producer requests.".format(fetchRequest.replicaId, satisfiedProduceRequests.size))
      satisfiedProduceRequests.foreach(_.respond())
    }

    // if there are enough bytes available right now we can answer the request, otherwise we have to punt
    val availableBytes = availableFetchBytes(fetchRequest)
    if(fetchRequest.maxWait <= 0 ||
       availableBytes >= fetchRequest.minBytes ||
       fetchRequest.numPartitions <= 0) {
      val topicData = readMessageSets(fetchRequest)
      debug("Returning fetch response %s for fetch request with correlation id %d".format(
        topicData.map(_.partitionDataArray.map(_.error).mkString(",")).mkString(","), fetchRequest.correlationId))
      val response = new FetchResponse(FetchRequest.CurrentVersion, fetchRequest.correlationId, topicData)
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)))
    } else {
      debug("Putting fetch request into purgatory")
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val delayedFetchKeys = fetchRequest.offsetInfo.flatMap(o => o.partitions.map(RequestKey(o.topic, _)))
      val delayedFetch = new DelayedFetch(delayedFetchKeys, request, fetchRequest, fetchRequest.maxWait)
      fetchRequestPurgatory.watch(delayedFetch)
    }
  }

  /**
   * Calculate the number of available bytes for the given fetch request
   */
  private def availableFetchBytes(fetchRequest: FetchRequest): Long = {
    var totalBytes = 0L
    for(offsetDetail <- fetchRequest.offsetInfo) {
      for(i <- 0 until offsetDetail.partitions.size) {
        debug("Fetching log for topic %s partition %d".format(offsetDetail.topic, offsetDetail.partitions(i)))
        try {
          val leader = replicaManager.getLeaderReplicaIfLocal(offsetDetail.topic, offsetDetail.partitions(i))
          val end = if(fetchRequest.replicaId == FetchRequest.NonFollowerId) {
            leader.highWatermark
          } else {
            leader.logEndOffset
          }
          val available = max(0, end - offsetDetail.offsets(i))
          totalBytes += math.min(offsetDetail.fetchSizes(i), available)
        } catch {
          case e: UnknownTopicOrPartitionException =>
            info("Invalid partition %d in fetch request from client %d."
              .format(offsetDetail.partitions(i), fetchRequest.clientId))
          case e =>
            error("Error determining available fetch bytes for topic %s partition %s on broker %s for client %s"
              .format(offsetDetail.topic, offsetDetail.partitions(i), brokerId, fetchRequest.clientId), e)
        }
      }
    }
    trace(totalBytes + " available bytes for fetch request.")
    totalBytes
  }

  private def maybeUpdatePartitionHW(fetchRequest: FetchRequest) {
    val offsets = fetchRequest.offsetInfo
    debug("Act on update partition HW, check offset detail: %s ".format(offsets))
    for(offsetDetail <- offsets) {
      val topic = offsetDetail.topic
      val (partitions, offsets) = (offsetDetail.partitions, offsetDetail.offsets)
      for( (partition, offset) <- (partitions, offsets).zipped.map((_,_))) {
        replicaManager.recordFollowerPosition(topic, partition, fetchRequest.replicaId, offset)
      }
    }
  }

  /**
   * Read from all the offset details given and produce an array of topic datas
   */
  private def readMessageSets(fetchRequest: FetchRequest): Array[TopicData] = {
    val offsets = fetchRequest.offsetInfo
    val fetchedData = new mutable.ArrayBuffer[TopicData]()

    for(offsetDetail <- offsets) {
      val info = new mutable.ArrayBuffer[PartitionData]()
      val topic = offsetDetail.topic
      val (partitions, offsets, fetchSizes) = (offsetDetail.partitions, offsetDetail.offsets, offsetDetail.fetchSizes)
      for( (partition, offset, fetchSize) <- (partitions, offsets, fetchSizes).zipped.map((_,_,_)) ) {
        val isFetchFromFollower = fetchRequest.replicaId != FetchRequest.NonFollowerId
        val partitionInfo =
          try {
            val (messages, highWatermark) = readMessageSet(topic, partition, offset, fetchSize, isFetchFromFollower)
            BrokerTopicStat.getBrokerTopicStat(topic).recordBytesOut(messages.sizeInBytes)
            BrokerTopicStat.getBrokerAllTopicStat.recordBytesOut(messages.sizeInBytes)
            if (!isFetchFromFollower) {
              new PartitionData(partition, ErrorMapping.NoError, offset, highWatermark, messages)
            } else {
              debug("Leader %d for topic %s partition %d received fetch request from follower %d"
                .format(brokerId, topic, partition, fetchRequest.replicaId))
              debug("Leader %d returning %d messages for topic %s partition %d to follower %d"
                .format(brokerId, messages.sizeInBytes, topic, partition, fetchRequest.replicaId))
              new PartitionData(partition, ErrorMapping.NoError, offset, highWatermark, messages)
            }
          }
          catch {
            case e =>
              BrokerTopicStat.getBrokerTopicStat(topic).recordFailedFetchRequest
              BrokerTopicStat.getBrokerAllTopicStat.recordFailedFetchRequest
              error("error when processing request " + (topic, partition, offset, fetchSize), e)
              new PartitionData(partition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]),
                                offset, -1L, MessageSet.Empty)
          }
        info.append(partitionInfo)
      }
      fetchedData.append(new TopicData(topic, info.toArray))
    }
    fetchedData.toArray
  }

  /**
   * Read from a single topic/partition at the given offset upto maxSize bytes
   */
  private def readMessageSet(topic: String, partition: Int, offset: Long,
                             maxSize: Int, fromFollower: Boolean): (MessageSet, Long) = {
    // check if the current broker is the leader for the partitions
    val leader = replicaManager.getLeaderReplicaIfLocal(topic, partition)
    trace("Fetching log segment for topic, partition, offset, size = " + (topic, partition, offset, maxSize))
    val actualSize = if (!fromFollower) {
      min(leader.highWatermark - offset, maxSize).toInt
    } else {
      maxSize
    }
    val messages = leader.log match {
      case Some(log) =>
        log.read(offset, actualSize)
      case None =>
        error("Leader for topic %s partition %d on broker %d does not have a local log".format(topic, partition, brokerId))
        MessageSet.Empty
    }
    (messages, leader.highWatermark)
  }

  /**
   * Service the offset request API 
   */
  def handleOffsetRequest(request: RequestChannel.Request) {
    val offsetRequest = OffsetRequest.readFrom(request.request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling offset request " + offsetRequest.toString)
    trace("Handling offset request " + offsetRequest.toString)

    var response: OffsetResponse = null
    try {
      // ensure leader exists
      replicaManager.getLeaderReplicaIfLocal(offsetRequest.topic, offsetRequest.partition)
      val offsets = replicaManager.logManager.getOffsets(offsetRequest)
      response = new OffsetResponse(offsetRequest.versionId, offsets)
    } catch {
      case ioe: IOException =>
        fatal("Halting due to unrecoverable I/O error while handling producer request: " + ioe.getMessage, ioe)
        System.exit(1)
      case e =>
        warn("Error while responding to offset request", e)
        response = new OffsetResponse(offsetRequest.versionId, Array.empty[Long],
          ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]).toShort)
    }
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  /**
   * Service the topic metadata request API
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = TopicMetadataRequest.readFrom(request.request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling topic metadata request " + metadataRequest.toString())
    trace("Handling topic metadata request " + metadataRequest.toString())

    val topicsMetadata = new mutable.ArrayBuffer[TopicMetadata]()
    var errorCode = ErrorMapping.NoError
    val config = replicaManager.config
    try {
      val topicMetadataList = AdminUtils.getTopicMetaDataFromZK(metadataRequest.topics, zkClient)
      metadataRequest.topics.zip(topicMetadataList).foreach(
        topicAndMetadata =>{
          val topic = topicAndMetadata._1
          topicAndMetadata._2.errorCode match {
            case ErrorMapping.NoError => topicsMetadata += topicAndMetadata._2
            case ErrorMapping.UnknownTopicOrPartitionCode =>
              /* check if auto creation of topics is turned on */
              if(config.autoCreateTopics) {
                CreateTopicCommand.createTopic(zkClient, topic, config.numPartitions, config.defaultReplicationFactor,
                                               topicNameValidator = topicNameValidator)
                info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                             .format(topic, config.numPartitions, config.defaultReplicationFactor))
                val newTopicMetadata = AdminUtils.getTopicMetaDataFromZK(List(topic), zkClient).head
                newTopicMetadata.errorCode match {
                  case ErrorMapping.NoError => topicsMetadata += newTopicMetadata
                  case _ =>
                    throw new KafkaException("Topic metadata for automatically created topic %s does not exist".format(topic))
                }
              }
            case _ => error("Error while fetching topic metadata for topic " + topic,
                            ErrorMapping.exceptionFor(topicAndMetadata._2.errorCode).getCause)
          }
        })
    } catch {
      case e => error("Error while retrieving topic metadata", e)
      // convert exception type to error code
      errorCode = ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]])
    }
    topicsMetadata.foreach(metadata => trace("Sending topic metadata " + metadata.toString))
    val response = new TopicMetaDataResponse(metadataRequest.versionId, topicsMetadata.toSeq, errorCode)
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  def close() {
    debug("Shutting down.")
    fetchRequestPurgatory.shutdown()
    producerRequestPurgatory.shutdown()
    debug("Shut down complete.")
  }

  private [kafka] trait MetricKey {
    def keyLabel: String
  }
  private [kafka] object MetricKey {
    val globalLabel = "all"
  }

  private [kafka] case class RequestKey(topic: String, partition: Int)
          extends MetricKey {
    override def keyLabel = "%s-%d".format(topic, partition)
  }
  /**
   * A delayed fetch request
   */
  class DelayedFetch(keys: Seq[RequestKey], request: RequestChannel.Request, val fetch: FetchRequest, delayMs: Long)
    extends DelayedRequest(keys, request, delayMs)

  /**
   * A holding pen for fetch requests waiting to be satisfied
   */
  class FetchRequestPurgatory(brokerId: Int, requestChannel: RequestChannel) extends RequestPurgatory[DelayedFetch, Null](brokerId) {

    this.logIdent = "[FetchRequestPurgatory-%d], ".format(brokerId)


    /**
     * A fetch request is satisfied when it has accumulated enough data to meet the min_bytes field
     */
    def checkSatisfied(n: Null, delayedFetch: DelayedFetch): Boolean =
      availableFetchBytes(delayedFetch.fetch) >= delayedFetch.fetch.minBytes

    /**
     * When a request expires just answer it with whatever data is present
     */
    def expire(delayed: DelayedFetch) {
      val topicData = readMessageSets(delayed.fetch)
      val response = new FetchResponse(FetchRequest.CurrentVersion, delayed.fetch.correlationId, topicData)
      val fromFollower = delayed.fetch.replicaId != FetchRequest.NonFollowerId
      delayedRequestMetrics.recordDelayedFetchExpired(fromFollower, response)
      requestChannel.sendResponse(new RequestChannel.Response(delayed.request, new FetchResponseSend(response)))
    }
  }

  class DelayedProduce(keys: Seq[RequestKey],
                       request: RequestChannel.Request,
                       localErrors: Array[Short],
                       requiredOffsets: Array[Long],
                       val produce: ProducerRequest,
                       delayMs: Long)
          extends DelayedRequest(keys, request, delayMs) with Logging {

    /**
     * Map of (topic, partition) -> partition status
     * The values in this map don't need to be synchronized since updates to the
     * values are effectively synchronized by the ProducerRequestPurgatory's
     * update method
     */
    private [kafka] val partitionStatus = keys.map(key => {
      val keyIndex = keys.indexOf(key)
      // if there was an error in writing to the local replica's log, then don't
      // wait for acks on this partition
      val acksPending =
        if (localErrors(keyIndex) == ErrorMapping.NoError) {
          // Timeout error state will be cleared when requiredAcks are received
          localErrors(keyIndex) = ErrorMapping.RequestTimedOutCode
          true
        }
        else
          false

      val initialStatus = new PartitionStatus(acksPending, localErrors(keyIndex), requiredOffsets(keyIndex))
      trace("Initial partition status for %s = %s".format(key, initialStatus))
      (key, initialStatus)
    }).toMap


    def respond() {
      val errorsAndOffsets: (List[Short], List[Long]) = (
        keys.foldRight
          ((List[Short](), List[Long]()))
          ((key: RequestKey, result: (List[Short], List[Long])) => {
            val status = partitionStatus(key)
            (status.error :: result._1, status.requiredOffset :: result._2)
          })
        )
      val response = new ProducerResponse(produce.versionId, produce.correlationId,
                                          errorsAndOffsets._1.toArray, errorsAndOffsets._2.toArray)

      requestChannel.sendResponse(new RequestChannel.Response(
        request, new BoundedByteBufferSend(response)))
    }

    /**
     * Returns true if this delayed produce request is satisfied (or more
     * accurately, unblocked) -- this is the case if for every partition:
     * Case A: This broker is not the leader: unblock - should return error.
     * Case B: This broker is the leader:
     *   B.1 - If there was a localError (when writing to the local log): unblock - should return error
     *   B.2 - else, at least requiredAcks replicas should be caught up to this request.
     *
     * As partitions become acknowledged, we may be able to unblock
     * DelayedFetchRequests that are pending on those partitions.
     */
    def isSatisfied(followerFetchRequestKey: RequestKey) = {
      val topic = followerFetchRequestKey.topic
      val partitionId = followerFetchRequestKey.partition
      val key = RequestKey(topic, partitionId)
      val fetchPartitionStatus = partitionStatus(key)
      val durationNs = SystemTime.nanoseconds - creationTimeNs
      trace("Checking producer request satisfaction for %s-%d, acksPending = %b"
        .format(topic, partitionId, fetchPartitionStatus.acksPending))
      if (fetchPartitionStatus.acksPending) {
        val partition = replicaManager.getOrCreatePartition(topic, partitionId)
        val (hasEnough, errorCode) = partition.checkEnoughReplicasReachOffset(fetchPartitionStatus.requiredOffset, produce.requiredAcks)
        if (errorCode != ErrorMapping.NoError) {
          fetchPartitionStatus.acksPending = false
          fetchPartitionStatus.error = errorCode
        } else if (hasEnough) {
          fetchPartitionStatus.acksPending = false
          fetchPartitionStatus.error = ErrorMapping.NoError
        }
        if (!fetchPartitionStatus.acksPending) {
          val topicData = produce.data.find(_.topic == topic).get
          val partitionData = topicData.partitionDataArray.find(_.partition == partitionId).get
          delayedRequestMetrics.recordDelayedProducerKeyCaughtUp(key,
                                                                 durationNs,
                                                                 partitionData.sizeInBytes)
          maybeUnblockDelayedFetchRequests(topic, Array(partitionData))
        }
      }

      // unblocked if there are no partitions with pending acks
      val satisfied = ! partitionStatus.exists(p => p._2.acksPending)
      if (satisfied)
        delayedRequestMetrics.recordDelayedProduceSatisfied(durationNs)
      satisfied
    }

    class PartitionStatus(var acksPending: Boolean,
                          var error: Short,
                          val requiredOffset: Long) {
      def setThisBrokerNotLeader() {
        error = ErrorMapping.NotLeaderForPartitionCode
        acksPending = false
      }

      override def toString =
        "acksPending:%b, error: %d, requiredOffset: %d".format(
          acksPending, error, requiredOffset
        )
    }
  }

  /**
   * A holding pen for produce requests waiting to be satisfied.
   */
  private [kafka] class ProducerRequestPurgatory(brokerId: Int) extends RequestPurgatory[DelayedProduce, RequestKey](brokerId) {

    this.logIdent = "[ProducerRequestPurgatory-%d], ".format(brokerId)

    protected def checkSatisfied(followerFetchRequestKey: RequestKey,
                                 delayedProduce: DelayedProduce) =
      delayedProduce.isSatisfied(followerFetchRequestKey)

    /**
     * Handle an expired delayed request
     */
    protected def expire(delayedProduce: DelayedProduce) {
      for (partitionStatus <- delayedProduce.partitionStatus if partitionStatus._2.acksPending)
        delayedRequestMetrics.recordDelayedProducerKeyExpired(partitionStatus._1)

      delayedProduce.respond()
    }
  }

  private class DelayedRequestMetrics {
    private class DelayedProducerRequestMetrics(keyLabel: String = MetricKey.globalLabel) extends KafkaMetricsGroup {
      val caughtUpFollowerFetchRequestMeter =
        newMeter("CaughtUpFollowerFetchRequestsPerSecond-" + keyLabel, "requests", TimeUnit.SECONDS)
      val followerCatchUpTimeHistogram = if (keyLabel == MetricKey.globalLabel)
        Some(newHistogram("FollowerCatchUpTimeInNs", biased = true))
      else None

      /*
       * Note that throughput is updated on individual key satisfaction.
       * Therefore, it is an upper bound on throughput since the
       * DelayedProducerRequest may get expired.
       */
      val throughputMeter = newMeter("Throughput-" + keyLabel, "bytes", TimeUnit.SECONDS)
      val expiredRequestMeter = newMeter("ExpiredRequestsPerSecond-" + keyLabel, "requests", TimeUnit.SECONDS)

      val satisfiedRequestMeter = if (keyLabel == MetricKey.globalLabel)
        Some(newMeter("SatisfiedRequestsPerSecond", "requests", TimeUnit.SECONDS))
      else None
      val satisfactionTimeHistogram = if (keyLabel == MetricKey.globalLabel)
        Some(newHistogram("SatisfactionTimeInNs", biased = true))
      else None
    }


    private class DelayedFetchRequestMetrics(forFollower: Boolean,
                                             keyLabel: String = MetricKey.globalLabel) extends KafkaMetricsGroup {
      private val metricPrefix = if (forFollower) "Follower" else "NonFollower"

      val satisfiedRequestMeter = if (keyLabel == MetricKey.globalLabel)
        Some(newMeter(metricPrefix + "-SatisfiedRequestsPerSecond",
          "requests", TimeUnit.SECONDS))
      else None

      val satisfactionTimeHistogram = if (keyLabel == MetricKey.globalLabel)
        Some(newHistogram(metricPrefix + "-SatisfactionTimeInNs", biased = true))
      else None

      val expiredRequestMeter = if (keyLabel == MetricKey.globalLabel)
        Some(newMeter(metricPrefix + "-ExpiredRequestsPerSecond",
          "requests", TimeUnit.SECONDS))
      else None

      val throughputMeter = newMeter("%s-Throughput-%s".format(metricPrefix, keyLabel),
        "bytes", TimeUnit.SECONDS)
    }

    private val producerRequestMetricsForKey = {
      val valueFactory = (k: MetricKey) => new DelayedProducerRequestMetrics(k.keyLabel)
      new Pool[MetricKey, DelayedProducerRequestMetrics](Some(valueFactory))
    }

    private val aggregateProduceRequestMetrics = new DelayedProducerRequestMetrics

    private val aggregateFollowerFetchRequestMetrics = new DelayedFetchRequestMetrics(forFollower = true)
    private val aggregateNonFollowerFetchRequestMetrics = new DelayedFetchRequestMetrics(forFollower = false)

    private val followerFetchRequestMetricsForKey = {
      val valueFactory = (k: MetricKey) => new DelayedFetchRequestMetrics(forFollower = true, k.keyLabel)
      new Pool[MetricKey, DelayedFetchRequestMetrics](Some(valueFactory))
    }

    private val nonFollowerFetchRequestMetricsForKey = {
      val valueFactory = (k: MetricKey) => new DelayedFetchRequestMetrics(forFollower = false, k.keyLabel)
      new Pool[MetricKey, DelayedFetchRequestMetrics](Some(valueFactory))
    }

    def recordDelayedProducerKeyExpired(key: MetricKey) {
      val keyMetrics = producerRequestMetricsForKey.getAndMaybePut(key)
      List(keyMetrics, aggregateProduceRequestMetrics).foreach(_.expiredRequestMeter.mark())
    }


    def recordDelayedProducerKeyCaughtUp(key: MetricKey, timeToCatchUpNs: Long, bytes: Int) {
      val keyMetrics = producerRequestMetricsForKey.getAndMaybePut(key)
      List(keyMetrics, aggregateProduceRequestMetrics).foreach(m => {
        m.caughtUpFollowerFetchRequestMeter.mark()
        m.followerCatchUpTimeHistogram.foreach(_.update(timeToCatchUpNs))
        m.throughputMeter.mark(bytes)
      })
    }


    def recordDelayedProduceSatisfied(timeToSatisfyNs: Long) {
      aggregateProduceRequestMetrics.satisfiedRequestMeter.foreach(_.mark())
      aggregateProduceRequestMetrics.satisfactionTimeHistogram.foreach(_.update(timeToSatisfyNs))
    }

    private def recordDelayedFetchThroughput(forFollower: Boolean, response: FetchResponse) {
      val metrics = if (forFollower) aggregateFollowerFetchRequestMetrics
        else aggregateNonFollowerFetchRequestMetrics
      metrics.throughputMeter.mark(response.sizeInBytes)

      response.topicMap.foreach(topicAndData => {
        val topic = topicAndData._1
        topicAndData._2.partitionDataArray.foreach(partitionData => {
          val key = RequestKey(topic, partitionData.partition)
          val keyMetrics = if (forFollower)
            followerFetchRequestMetricsForKey.getAndMaybePut(key)
          else
            nonFollowerFetchRequestMetricsForKey.getAndMaybePut(key)
          keyMetrics.throughputMeter.mark(partitionData.sizeInBytes)
        })
      })
    }


    def recordDelayedFetchExpired(forFollower: Boolean, response: FetchResponse) {
      val metrics = if (forFollower) aggregateFollowerFetchRequestMetrics
        else aggregateNonFollowerFetchRequestMetrics
      
      metrics.expiredRequestMeter.foreach(_.mark())

      recordDelayedFetchThroughput(forFollower, response)
    }


    def recordDelayedFetchSatisfied(forFollower: Boolean, timeToSatisfyNs: Long, response: FetchResponse) {
      val aggregateMetrics = if (forFollower) aggregateFollowerFetchRequestMetrics
        else aggregateNonFollowerFetchRequestMetrics

      aggregateMetrics.satisfactionTimeHistogram.foreach(_.update(timeToSatisfyNs))
      aggregateMetrics.satisfiedRequestMeter.foreach(_.mark())

      recordDelayedFetchThroughput(forFollower, response)
    }
  }
}

