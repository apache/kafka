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
import kafka.message._
import kafka.network._
import kafka.utils.{Pool, SystemTime, Logging}
import org.apache.log4j.Logger
import scala.collection._
import mutable.HashMap
import scala.math._
import kafka.network.RequestChannel.Response
import java.util.concurrent.TimeUnit
import kafka.metrics.KafkaMetricsGroup
import org.I0Itec.zkclient.ZkClient
import kafka.common._

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val zkClient: ZkClient,
                brokerId: Int) extends Logging {

  private val producerRequestPurgatory = new ProducerRequestPurgatory
  private val fetchRequestPurgatory = new FetchRequestPurgatory(requestChannel)
  private val delayedRequestMetrics = new DelayedRequestMetrics

  private val requestLogger = Logger.getLogger("kafka.request.logger")
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    request.requestId match {
      case RequestKeys.ProduceKey => handleProducerRequest(request)
      case RequestKeys.FetchKey => handleFetchRequest(request)
      case RequestKeys.OffsetsKey => handleOffsetRequest(request)
      case RequestKeys.MetadataKey => handleTopicMetadataRequest(request)
      case RequestKeys.LeaderAndIsrKey => handleLeaderAndISRRequest(request)
      case RequestKeys.StopReplicaKey => handleStopReplicaRequest(request)
      case requestId => throw new KafkaException("No mapping found for handler id " + requestId)
    }
    request.apiLocalCompleteTimeNs = SystemTime.nanoseconds
  }

  def handleLeaderAndISRRequest(request: RequestChannel.Request){
    val leaderAndISRRequest = request.requestObj.asInstanceOf[LeaderAndIsrRequest]
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling leader and isr request " + leaderAndISRRequest)
    trace("Handling leader and isr request " + leaderAndISRRequest)

    val responseMap = replicaManager.becomeLeaderOrFollower(leaderAndISRRequest)
    val leaderAndISRResponse = new LeaderAndISRResponse(leaderAndISRRequest.versionId, responseMap)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(leaderAndISRResponse)))
  }


  def handleStopReplicaRequest(request: RequestChannel.Request){
    val stopReplicaRequest = request.requestObj.asInstanceOf[StopReplicaRequest]
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
   * Check if a partitionData from a produce request can unblock any
   * DelayedFetch requests.
   */
  def maybeUnblockDelayedFetchRequests(topic: String, partitionData: PartitionData) {
    val partition = partitionData.partition
    val satisfied =  fetchRequestPurgatory.update(RequestKey(topic, partition), null)
    trace("Producer request to (%s-%d) unblocked %d fetch requests.".format(topic, partition, satisfied.size))

    // send any newly unblocked responses
    for(fetchReq <- satisfied) {
      val topicData = readMessageSets(fetchReq.fetch)
      val response = FetchResponse(FetchRequest.CurrentVersion, fetchReq.fetch.correlationId, topicData)
      requestChannel.sendResponse(new RequestChannel.Response(fetchReq.request, new FetchResponseSend(response)))
    }
  }

  /**
   * Handle a produce request
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.requestObj.asInstanceOf[ProducerRequest]
    val sTime = SystemTime.milliseconds
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling producer request " + request.toString)
    trace("Handling producer request " + request.toString)

    val localProduceResponse = produceToLocalLog(produceRequest)
    debug("Produce to local log in %d ms".format(SystemTime.milliseconds - sTime))

    val numPartitionsInError = localProduceResponse.status.count(_._2.error != ErrorMapping.NoError)
    produceRequest.data.foreach(partitionAndData =>
      maybeUnblockDelayedFetchRequests(partitionAndData._1.topic, partitionAndData._2))

    if (produceRequest.requiredAcks == 0 ||
        produceRequest.requiredAcks == 1 ||
        produceRequest.numPartitions <= 0 ||
        numPartitionsInError == produceRequest.numPartitions)
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(localProduceResponse)))
    else {
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val producerRequestKeys = produceRequest.data.keys.map(
        topicAndPartition => new RequestKey(topicAndPartition)).toSeq

      val delayedProduce = new DelayedProduce(
        producerRequestKeys, request, localProduceResponse,
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

    val localErrorsAndOffsets = request.data.map (topicAndPartitionData => {
      val (topic, partitionData) = (topicAndPartitionData._1.topic, topicAndPartitionData._2)
      BrokerTopicStat.getBrokerTopicStat(topic).bytesInRate.mark(partitionData.messages.sizeInBytes)
      BrokerTopicStat.getBrokerAllTopicStat.bytesInRate.mark(partitionData.messages.sizeInBytes)

      try {
        val localReplica = replicaManager.getLeaderReplicaIfLocal(topic, partitionData.partition)
        val log = localReplica.log.get
        log.append(partitionData.messages.asInstanceOf[ByteBufferMessageSet])
        // we may need to increment high watermark since ISR could be down to 1
        localReplica.partition.maybeIncrementLeaderHW(localReplica)
        val responseStatus = ProducerResponseStatus(ErrorMapping.NoError, log.logEndOffset)
        trace("%d bytes written to logs, nextAppendOffset = %d"
                      .format(partitionData.messages.sizeInBytes, responseStatus.nextOffset))
        (TopicAndPartition(topic, partitionData.partition), responseStatus)
      } catch {
        case e: Throwable =>
          BrokerTopicStat.getBrokerTopicStat(topic).failedProduceRequestRate.mark()
          BrokerTopicStat.getBrokerAllTopicStat.failedProduceRequestRate.mark()
          error("Error processing ProducerRequest on %s:%d".format(topic, partitionData.partition), e)
          e match {
            case _: KafkaStorageException =>
              fatal("Halting due to unrecoverable I/O error while handling producer request", e)
              Runtime.getRuntime.halt(1)
            case _ =>
          }
          val (errorCode, offset) = (ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), -1L)
          (TopicAndPartition(topic, partitionData.partition), ProducerResponseStatus(errorCode, offset))
      }
    }
    )

    ProducerResponse(request.versionId, request.correlationId, localErrorsAndOffsets)
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling fetch request " + fetchRequest.toString)
    trace("Handling fetch request " + fetchRequest.toString)

    if(fetchRequest.isFromFollower) {
      maybeUpdatePartitionHW(fetchRequest)
      // after updating HW, some delayed produce requests may be unblocked
      var satisfiedProduceRequests = new mutable.ArrayBuffer[DelayedProduce]
      fetchRequest.requestInfo.foreach {
        case (topicAndPartition, _) =>
          val key = new RequestKey(topicAndPartition)
          satisfiedProduceRequests ++= producerRequestPurgatory.update(key, key)
      }
      debug("Replica %d fetch unblocked %d producer requests."
        .format(fetchRequest.replicaId, satisfiedProduceRequests.size))
      satisfiedProduceRequests.foreach(_.respond())
    }

    // if there are enough bytes available right now we can answer the request, otherwise we have to punt
    val availableBytes = availableFetchBytes(fetchRequest)
    if(fetchRequest.maxWait <= 0 ||
       availableBytes >= fetchRequest.minBytes ||
       fetchRequest.numPartitions <= 0) {
      val topicData = readMessageSets(fetchRequest)
      debug("Returning fetch response %s for fetch request with correlation id %d".format(
        topicData.values.map(_.error).mkString(","), fetchRequest.correlationId))
      val response = FetchResponse(FetchRequest.CurrentVersion, fetchRequest.correlationId, topicData)
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)))
    } else {
      debug("Putting fetch request into purgatory")
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val delayedFetchKeys = fetchRequest.requestInfo.keys.toSeq.map(new RequestKey(_))
      val delayedFetch = new DelayedFetch(delayedFetchKeys, request, fetchRequest, fetchRequest.maxWait)
      fetchRequestPurgatory.watch(delayedFetch)
    }
  }

  /**
   * Calculate the number of available bytes for the given fetch request
   */
  private def availableFetchBytes(fetchRequest: FetchRequest): Long = {
    val totalBytes = fetchRequest.requestInfo.foldLeft(0L)((folded, curr) => {
      folded +
      {
        val (topic, partition) = (curr._1.topic, curr._1.partition)
        val (offset, fetchSize) = (curr._2.offset, curr._2.fetchSize)
        debug("Fetching log for topic %s partition %d".format(topic, partition))
        try {
          val leader = replicaManager.getLeaderReplicaIfLocal(topic, partition)
          val end = if (!fetchRequest.isFromFollower) {
            leader.highWatermark
          } else {
            leader.logEndOffset
          }
          val available = max(0, end - offset)
          math.min(fetchSize, available)
        } catch {
          case e: UnknownTopicOrPartitionException =>
            info("Invalid partition %d in fetch request from client %s."
                         .format(partition, fetchRequest.clientId))
            0
          case e =>
            warn("Error determining available fetch bytes for topic %s partition %s on broker %s for client %s"
                          .format(topic, partition, brokerId, fetchRequest.clientId), e)
            0
        }
      }
    })
    trace(totalBytes + " available bytes for fetch request.")
    totalBytes
  }

  private def maybeUpdatePartitionHW(fetchRequest: FetchRequest) {
    debug("Maybe update partition HW due to fetch request: %s ".format(fetchRequest))
    fetchRequest.requestInfo.foreach(info => {
      val (topic, partition, offset) = (info._1.topic, info._1.partition, info._2.offset)
      replicaManager.recordFollowerPosition(topic, partition, fetchRequest.replicaId, offset)
    })
  }

  /**
   * Read from all the offset details given and return a map of
   * (topic, partition) -> PartitionData
   */
  private def readMessageSets(fetchRequest: FetchRequest) = {
    val isFetchFromFollower = fetchRequest.isFromFollower
    fetchRequest.requestInfo.map {
      case (TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize)) =>
        val partitionData = try {
          val (messages, highWatermark) = readMessageSet(topic, partition, offset, fetchSize, isFetchFromFollower)
          BrokerTopicStat.getBrokerTopicStat(topic).bytesOutRate.mark(messages.sizeInBytes)
          BrokerTopicStat.getBrokerAllTopicStat.bytesOutRate.mark(messages.sizeInBytes)
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
          case t: Throwable =>
            BrokerTopicStat.getBrokerTopicStat(topic).failedFetchRequestRate.mark()
            BrokerTopicStat.getBrokerAllTopicStat.failedFetchRequestRate.mark()
            error("error when processing request " + (topic, partition, offset, fetchSize), t)
            new PartitionData(partition, ErrorMapping.codeFor(t.getClass.asInstanceOf[Class[Throwable]]),
                              offset, -1L, MessageSet.Empty)
        }
        (TopicAndPartition(topic, partition), partitionData)
    }
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
    val offsetRequest = request.requestObj.asInstanceOf[OffsetRequest]
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling offset request " + offsetRequest.toString)
    trace("Handling offset request " + offsetRequest.toString)

    val responseMap = offsetRequest.requestInfo.map(elem => {
      val (topicAndPartition, partitionOffsetRequestInfo) = elem
      try {
        // ensure leader exists
        val leader = replicaManager.getLeaderReplicaIfLocal(
          topicAndPartition.topic, topicAndPartition.partition)
        val offsets = {
          val allOffsets = replicaManager.logManager.getOffsets(topicAndPartition,
                                                                partitionOffsetRequestInfo.time,
                                                                partitionOffsetRequestInfo.maxNumOffsets)
          if (offsetRequest.isFromFollower) allOffsets
          else {
            val hw = leader.highWatermark
            if (allOffsets.exists(_ > hw))
              hw +: allOffsets.dropWhile(_ > hw)
            else allOffsets
          }
        }
        (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.NoError, offsets))
      } catch {
        case e =>
          warn("Error while responding to offset request", e)
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), Nil) )
      }
    })
    val response = OffsetResponse(OffsetRequest.CurrentVersion, responseMap)
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  /**
   * Service the topic metadata request API
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.requestObj.asInstanceOf[TopicMetadataRequest]
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling topic metadata request " + metadataRequest.toString())
    trace("Handling topic metadata request " + metadataRequest.toString())

    val topicsMetadata = new mutable.ArrayBuffer[TopicMetadata]()
    val config = replicaManager.config
    val topicMetadataList = AdminUtils.getTopicMetaDataFromZK(metadataRequest.topics, zkClient)
    metadataRequest.topics.zip(topicMetadataList).foreach(
      topicAndMetadata => {
        val topic = topicAndMetadata._1
        topicAndMetadata._2.errorCode match {
          case ErrorMapping.NoError => topicsMetadata += topicAndMetadata._2
          case ErrorMapping.UnknownTopicOrPartitionCode =>
            try {
              /* check if auto creation of topics is turned on */
              if (config.autoCreateTopics) {
                CreateTopicCommand.createTopic(zkClient, topic, config.numPartitions, config.defaultReplicationFactor)
                info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                             .format(topic, config.numPartitions, config.defaultReplicationFactor))
                val newTopicMetadata = AdminUtils.getTopicMetaDataFromZK(List(topic), zkClient).head
                topicsMetadata += newTopicMetadata
                newTopicMetadata.errorCode match {
                  case ErrorMapping.NoError =>
                  case _ => throw new KafkaException("Topic metadata for automatically created topic %s does not exist".format(topic))
                }
              }
            } catch {
              case e => error("Error while retrieving topic metadata", e)
            }
          case _ => 
            error("Error while fetching topic metadata for topic " + topic,
                  ErrorMapping.exceptionFor(topicAndMetadata._2.errorCode).getCause)
            topicsMetadata += topicAndMetadata._2
        }
      })
    topicsMetadata.foreach(metadata => trace("Sending topic metadata " + metadata.toString))
    val response = new TopicMetadataResponse(metadataRequest.versionId, topicsMetadata.toSeq)
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
    val globalLabel = "All"
  }

  private [kafka] case class RequestKey(topic: String, partition: Int)
          extends MetricKey {

    def this(topicAndPartition: TopicAndPartition) = this(topicAndPartition.topic, topicAndPartition.partition)

    def topicAndPartition = TopicAndPartition(topic, partition)

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
  class FetchRequestPurgatory(requestChannel: RequestChannel) extends RequestPurgatory[DelayedFetch, Null](brokerId) {

    this.logIdent = "[FetchRequestPurgatory-%d] ".format(brokerId)

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
      val response = FetchResponse(FetchRequest.CurrentVersion, delayed.fetch.correlationId, topicData)
      val fromFollower = delayed.fetch.isFromFollower
      delayedRequestMetrics.recordDelayedFetchExpired(fromFollower)
      requestChannel.sendResponse(new RequestChannel.Response(delayed.request, new FetchResponseSend(response)))
    }
  }

  class DelayedProduce(keys: Seq[RequestKey],
                       request: RequestChannel.Request,
                       localProduceResponse: ProducerResponse,
                       val produce: ProducerRequest,
                       delayMs: Long)
          extends DelayedRequest(keys, request, delayMs) with Logging {

    private val initialErrorsAndOffsets = localProduceResponse.status
    /**
     * Map of (topic, partition) -> partition status
     * The values in this map don't need to be synchronized since updates to the
     * values are effectively synchronized by the ProducerRequestPurgatory's
     * update method
     */
    private [kafka] val partitionStatus = keys.map(requestKey => {
      val producerResponseStatus = initialErrorsAndOffsets(TopicAndPartition(requestKey.topic, requestKey.partition))
      // if there was an error in writing to the local replica's log, then don't
      // wait for acks on this partition
      val (acksPending, error, nextOffset) =
        if (producerResponseStatus.error == ErrorMapping.NoError) {
          // Timeout error state will be cleared when requiredAcks are received
          (true, ErrorMapping.RequestTimedOutCode, producerResponseStatus.nextOffset)
        }
        else (false, producerResponseStatus.error, producerResponseStatus.nextOffset)

      val initialStatus = PartitionStatus(acksPending, error, nextOffset)
      trace("Initial partition status for %s = %s".format(requestKey.keyLabel, initialStatus))
      (requestKey, initialStatus)
    }).toMap

    def respond() {
      
      val finalErrorsAndOffsets = initialErrorsAndOffsets.map(
        status => {
          val pstat = partitionStatus(new RequestKey(status._1))
          (status._1, ProducerResponseStatus(pstat.error, pstat.requiredOffset))
        })
      
      val response = ProducerResponse(produce.versionId, produce.correlationId, finalErrorsAndOffsets)

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
          val partitionData = produce.data(followerFetchRequestKey.topicAndPartition)
          maybeUnblockDelayedFetchRequests(topic, partitionData)
        }
      }

      // unblocked if there are no partitions with pending acks
      val satisfied = ! partitionStatus.exists(p => p._2.acksPending)
      trace("Producer request satisfaction for %s-%d = %b".format(topic, partitionId, satisfied))
      satisfied
    }

    case class PartitionStatus(var acksPending: Boolean,
                          var error: Short,
                          requiredOffset: Long) {
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
  private [kafka] class ProducerRequestPurgatory extends RequestPurgatory[DelayedProduce, RequestKey](brokerId) {

    this.logIdent = "[ProducerRequestPurgatory-%d] ".format(brokerId)

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
      val expiredRequestMeter = newMeter(keyLabel + "ExpiresPerSecond", "requests", TimeUnit.SECONDS)
    }


    private class DelayedFetchRequestMetrics(forFollower: Boolean) extends KafkaMetricsGroup {
      private val metricPrefix = if (forFollower) "Follower" else "Consumer"

      val expiredRequestMeter = newMeter(metricPrefix + "ExpiresPerSecond", "requests", TimeUnit.SECONDS)
    }

    private val producerRequestMetricsForKey = {
      val valueFactory = (k: MetricKey) => new DelayedProducerRequestMetrics(k.keyLabel + "-")
      new Pool[MetricKey, DelayedProducerRequestMetrics](Some(valueFactory))
    }

    private val aggregateProduceRequestMetrics = new DelayedProducerRequestMetrics

    private val aggregateFollowerFetchRequestMetrics = new DelayedFetchRequestMetrics(forFollower = true)
    private val aggregateNonFollowerFetchRequestMetrics = new DelayedFetchRequestMetrics(forFollower = false)

    def recordDelayedProducerKeyExpired(key: MetricKey) {
      val keyMetrics = producerRequestMetricsForKey.getAndMaybePut(key)
      List(keyMetrics, aggregateProduceRequestMetrics).foreach(_.expiredRequestMeter.mark())
    }

    def recordDelayedFetchExpired(forFollower: Boolean) {
      val metrics = if (forFollower) aggregateFollowerFetchRequestMetrics
        else aggregateNonFollowerFetchRequestMetrics
      
      metrics.expiredRequestMeter.mark()
    }
  }
}

