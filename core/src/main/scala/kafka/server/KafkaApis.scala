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

import kafka.admin.{CreateTopicCommand, AdminUtils}
import kafka.api._
import kafka.message._
import kafka.network._
import org.apache.log4j.Logger
import scala.collection._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic._
import kafka.metrics.KafkaMetricsGroup
import org.I0Itec.zkclient.ZkClient
import kafka.common._
import kafka.utils.{ZkUtils, Pool, SystemTime, Logging}
import kafka.network.RequestChannel.Response


/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val zkClient: ZkClient,
                brokerId: Int) extends Logging {

  private val producerRequestPurgatory =
    new ProducerRequestPurgatory(replicaManager.config.producerPurgatoryPurgeIntervalRequests)
  private val fetchRequestPurgatory =
    new FetchRequestPurgatory(requestChannel, replicaManager.config.fetchPurgatoryPurgeIntervalRequests)
  private val delayedRequestMetrics = new DelayedRequestMetrics

  private val requestLogger = Logger.getLogger("kafka.request.logger")
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try{
      if(requestLogger.isTraceEnabled)
        requestLogger.trace("Handling request: " + request.requestObj + " from client: " + request.remoteAddress)
      request.requestId match {
        case RequestKeys.ProduceKey => handleProducerRequest(request)
        case RequestKeys.FetchKey => handleFetchRequest(request)
        case RequestKeys.OffsetsKey => handleOffsetRequest(request)
        case RequestKeys.MetadataKey => handleTopicMetadataRequest(request)
        case RequestKeys.LeaderAndIsrKey => handleLeaderAndIsrRequest(request)
        case RequestKeys.StopReplicaKey => handleStopReplicaRequest(request)
        case requestId => throw new KafkaException("No mapping found for handler id " + requestId)
      }
    } catch {
      case e: Throwable =>
        request.requestObj.handleError(e, requestChannel, request)
        error("error when handling request %s".format(request.requestObj), e)
    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
    val leaderAndIsrRequest = request.requestObj.asInstanceOf[LeaderAndIsrRequest]
    try {
      val (response, error) = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest)
      val leaderAndIsrResponse = new LeaderAndIsrResponse(leaderAndIsrRequest.correlationId, response, error)
      requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(leaderAndIsrResponse)))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error during leadership change.", e)
        Runtime.getRuntime.halt(1)
    }
  }


  def handleStopReplicaRequest(request: RequestChannel.Request) {
    val stopReplicaRequest = request.requestObj.asInstanceOf[StopReplicaRequest]
    val (response, error) = replicaManager.stopReplicas(stopReplicaRequest)
    val stopReplicaResponse = new StopReplicaResponse(stopReplicaRequest.correlationId, response.toMap, error)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(stopReplicaResponse)))
    replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads()
  }

  /**
   * Check if a partitionData from a produce request can unblock any
   * DelayedFetch requests.
   */
  def maybeUnblockDelayedFetchRequests(topic: String, partition: Int, messageSizeInBytes: Int) {
    val satisfied =  fetchRequestPurgatory.update(RequestKey(topic, partition), messageSizeInBytes)
    trace("Producer request to (%s-%d) unblocked %d fetch requests.".format(topic, partition, satisfied.size))

    // send any newly unblocked responses
    for(fetchReq <- satisfied) {
      val topicData = readMessageSets(fetchReq.fetch)
      val response = FetchResponse(fetchReq.fetch.correlationId, topicData)
      requestChannel.sendResponse(new RequestChannel.Response(fetchReq.request, new FetchResponseSend(response)))
    }
  }

  /**
   * Handle a produce request
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.requestObj.asInstanceOf[ProducerRequest]
    val sTime = SystemTime.milliseconds
    val localProduceResults = appendToLocalLog(produceRequest)
    debug("Produce to local log in %d ms".format(SystemTime.milliseconds - sTime))

    val numPartitionsInError = localProduceResults.count(_.error.isDefined)
    produceRequest.data.foreach(partitionAndData =>
      maybeUnblockDelayedFetchRequests(partitionAndData._1.topic, partitionAndData._1.partition, partitionAndData._2.sizeInBytes))

    val allPartitionHaveReplicationFactorOne =
      !produceRequest.data.keySet.exists(
        m => replicaManager.getReplicationFactorForPartition(m.topic, m.partition) != 1)
    if(produceRequest.requiredAcks == 0) {
      // send a fake producer response if producer request.required.acks = 0. This mimics the behavior of a 0.7 producer
      // and is tuned for very high throughput
      requestChannel.sendResponse(new RequestChannel.Response(request.processor, request, null))
    } else if (produceRequest.requiredAcks == 1 ||
        produceRequest.numPartitions <= 0 ||
        allPartitionHaveReplicationFactorOne ||
        numPartitionsInError == produceRequest.numPartitions) {
      val statuses = localProduceResults.map(r => r.key -> ProducerResponseStatus(r.errorCode, r.start)).toMap
      val response = ProducerResponse(produceRequest.correlationId, statuses)
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    } else {
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val producerRequestKeys = produceRequest.data.keys.map(
        topicAndPartition => new RequestKey(topicAndPartition)).toSeq
      val statuses = localProduceResults.map(r => r.key -> ProducerResponseStatus(r.errorCode, r.end + 1)).toMap
      val delayedProduce = new DelayedProduce(producerRequestKeys, 
                                              request,
                                              statuses,
                                              produceRequest, 
                                              produceRequest.ackTimeoutMs.toLong)
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
      // we do not need the data anymore
      produceRequest.emptyData()
    }
  }
  
  case class ProduceResult(key: TopicAndPartition, start: Long, end: Long, error: Option[Throwable] = None) {
    def this(key: TopicAndPartition, throwable: Throwable) = 
      this(key, -1L, -1L, Some(throwable))
    
    def errorCode = error match {
      case None => ErrorMapping.NoError
      case Some(error) => ErrorMapping.codeFor(error.getClass.asInstanceOf[Class[Throwable]])
    }
  }

  /**
   * Helper method for handling a parsed producer request
   */
  private def appendToLocalLog(producerRequest: ProducerRequest): Iterable[ProduceResult] = {
    val partitionAndData: Map[TopicAndPartition, MessageSet] = producerRequest.data
    trace("Append [%s] to local log ".format(partitionAndData.toString))
    partitionAndData.map {case (topicAndPartition, messages) =>
      BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesInRate.mark(messages.sizeInBytes)
      BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(messages.sizeInBytes)

      try {
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        val (start, end) =
          partitionOpt match {
            case Some(partition) => partition.appendMessagesToLeader(messages.asInstanceOf[ByteBufferMessageSet])
            case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
              .format(topicAndPartition, brokerId))

          }
        trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
              .format(messages.size, topicAndPartition.topic, topicAndPartition.partition, start, end))
        ProduceResult(topicAndPartition, start, end)
      } catch {
        // NOTE: Failed produce requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
        // since failed produce requests metric is supposed to indicate failure of a broker in handling a produce request
        // for a partition it is the leader for
        case e: KafkaStorageException =>
          fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
          Runtime.getRuntime.halt(1)
          null
        case utpe: UnknownTopicOrPartitionException =>
          warn("Produce request: " + utpe.getMessage)
          new ProduceResult(topicAndPartition, utpe)
        case nle: NotLeaderForPartitionException =>
          warn("Produce request: " + nle.getMessage)
          new ProduceResult(topicAndPartition, nle)
        case e =>
          BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).failedProduceRequestRate.mark()
          BrokerTopicStats.getBrokerAllTopicsStats.failedProduceRequestRate.mark()
          error("Error processing ProducerRequest with correlation id %d from client %s on %s:%d"
            .format(producerRequest.correlationId, producerRequest.clientId, topicAndPartition.topic, topicAndPartition.partition), e)
          new ProduceResult(topicAndPartition, e)
       }
    }
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]
    if(fetchRequest.isFromFollower) {
      maybeUpdatePartitionHw(fetchRequest)
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

    val dataRead = readMessageSets(fetchRequest)
    val bytesReadable = dataRead.values.map(_.messages.sizeInBytes).sum
    if(fetchRequest.maxWait <= 0 ||
       bytesReadable >= fetchRequest.minBytes ||
       fetchRequest.numPartitions <= 0) {
      debug("Returning fetch response %s for fetch request with correlation id %d to client %s"
        .format(dataRead.values.map(_.error).mkString(","), fetchRequest.correlationId, fetchRequest.clientId))
      val response = new FetchResponse(fetchRequest.correlationId, dataRead)
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)))
    } else {
      debug("Putting fetch request into purgatory")
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val delayedFetchKeys = fetchRequest.requestInfo.keys.toSeq.map(new RequestKey(_))
      val delayedFetch = new DelayedFetch(delayedFetchKeys, request, fetchRequest, fetchRequest.maxWait, bytesReadable)
      fetchRequestPurgatory.watch(delayedFetch)
    }
  }

  private def maybeUpdatePartitionHw(fetchRequest: FetchRequest) {
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
    fetchRequest.requestInfo.map
    {
      case (TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize)) =>
        val partitionData =
          try {
            val (messages, highWatermark) = readMessageSet(topic, partition, offset, fetchSize, fetchRequest.replicaId)
            BrokerTopicStats.getBrokerTopicStats(topic).bytesOutRate.mark(messages.sizeInBytes)
            BrokerTopicStats.getBrokerAllTopicsStats.bytesOutRate.mark(messages.sizeInBytes)
            if (!isFetchFromFollower) {
              new FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, messages)
            } else {
              debug("Leader %d for topic %s partition %d received fetch request from follower %d"
                            .format(brokerId, topic, partition, fetchRequest.replicaId))
              new FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, messages)
            }
          } catch {
            // NOTE: Failed fetch requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
            // since failed fetch requests metric is supposed to indicate failure of a broker in handling a fetch request
            // for a partition it is the leader for
            case utpe: UnknownTopicOrPartitionException =>
              warn("Fetch request: " + utpe.getMessage)
              new FetchResponsePartitionData(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty)
            case nle: NotLeaderForPartitionException =>
              warn("Fetch request: " + nle.getMessage)
              new FetchResponsePartitionData(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty)
            case t =>
              BrokerTopicStats.getBrokerTopicStats(topic).failedFetchRequestRate.mark()
              BrokerTopicStats.getBrokerAllTopicsStats.failedFetchRequestRate.mark()
              error("Error when processing fetch request for topic %s partition %d offset %d from %s with correlation id %d"
                    .format(topic, partition, offset, if (isFetchFromFollower) "follower" else "consumer", fetchRequest.correlationId),
                    t)
              new FetchResponsePartitionData(ErrorMapping.codeFor(t.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty)
          }
        (TopicAndPartition(topic, partition), partitionData)
    }
  }

  /**
   * Read from a single topic/partition at the given offset upto maxSize bytes
   */
  private def readMessageSet(topic: String, 
                             partition: Int, 
                             offset: Long,
                             maxSize: Int, 
                             fromReplicaId: Int): (MessageSet, Long) = {
    // check if the current broker is the leader for the partitions
    val localReplica = if(fromReplicaId == Request.DebuggingConsumerId)
      replicaManager.getReplicaOrException(topic, partition)
    else
      replicaManager.getLeaderReplicaIfLocal(topic, partition)
    trace("Fetching log segment for topic, partition, offset, size = " + (topic, partition, offset, maxSize))
    val maxOffsetOpt = if (fromReplicaId == Request.OrdinaryConsumerId) {
      Some(localReplica.highWatermark)
    } else {
      None
    }
    val messages = localReplica.log match {
      case Some(log) =>
        log.read(offset, maxSize, maxOffsetOpt)
      case None =>
        error("Leader for topic %s partition %d on broker %d does not have a local log".format(topic, partition, brokerId))
        MessageSet.Empty
    }
    (messages, localReplica.highWatermark)
  }

  /**
   * Service the offset request API 
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
          val allOffsets = replicaManager.logManager.getOffsets(topicAndPartition,
                                                                partitionOffsetRequestInfo.time,
                                                                partitionOffsetRequestInfo.maxNumOffsets)
          if (!offsetRequest.isFromOrdinaryClient) allOffsets
          else {
            val hw = localReplica.highWatermark
            if (allOffsets.exists(_ > hw))
              hw +: allOffsets.dropWhile(_ > hw)
            else allOffsets
          }
        }
        (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.NoError, offsets))
      } catch {
        // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
        // are typically transient and there is no value in logging the entire stack trace for the same
        case utpe: UnknownTopicOrPartitionException =>
          warn(utpe.getMessage)
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case nle: NotLeaderForPartitionException =>
          warn(nle.getMessage)
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case e =>
          warn("Error while responding to offset request", e)
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), Nil) )
      }
    })
    val response = OffsetResponse(offsetRequest.correlationId, responseMap)
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  /**
   * Service the topic metadata request API
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.requestObj.asInstanceOf[TopicMetadataRequest]
    val topicsMetadata = new mutable.ArrayBuffer[TopicMetadata]()
    val config = replicaManager.config
    val uniqueTopics = {
      if(metadataRequest.topics.size > 0)
        metadataRequest.topics.toSet
      else
        ZkUtils.getAllTopics(zkClient).toSet
    }
    val topicMetadataList = AdminUtils.fetchTopicMetadataFromZk(uniqueTopics, zkClient)
    topicMetadataList.foreach(
      topicAndMetadata => {
        topicAndMetadata.errorCode match {
          case ErrorMapping.NoError => topicsMetadata += topicAndMetadata
          case ErrorMapping.UnknownTopicOrPartitionCode =>
            try {
              /* check if auto creation of topics is turned on */
              if (config.autoCreateTopicsEnable) {
                try {
                  CreateTopicCommand.createTopic(zkClient, topicAndMetadata.topic, config.numPartitions, config.defaultReplicationFactor)
                  info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                               .format(topicAndMetadata.topic, config.numPartitions, config.defaultReplicationFactor))
                } catch {
                  case e: TopicExistsException => // let it go, possibly another broker created this topic
                }
                val newTopicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicAndMetadata.topic, zkClient)
                topicsMetadata += newTopicMetadata
                newTopicMetadata.errorCode match {
                  case ErrorMapping.NoError =>
                  case _ => throw new KafkaException("Topic metadata for automatically created topic %s does not exist".format(topicAndMetadata.topic))
                }
              }
            } catch {
              case e => error("Error while retrieving topic metadata", e)
            }
          case _ => 
            error("Error while fetching topic metadata for topic " + topicAndMetadata.topic,
                  ErrorMapping.exceptionFor(topicAndMetadata.errorCode).getCause)
            topicsMetadata += topicAndMetadata
        }
      })
    trace("Sending topic metadata for correlation id %d to client %s".format(metadataRequest.correlationId, metadataRequest.clientId))
    topicsMetadata.foreach(metadata => trace("Sending topic metadata " + metadata.toString))
    val response = new TopicMetadataResponse(topicsMetadata.toSeq, metadataRequest.correlationId)
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
  class DelayedFetch(keys: Seq[RequestKey], request: RequestChannel.Request, val fetch: FetchRequest, delayMs: Long, initialSize: Long)
    extends DelayedRequest(keys, request, delayMs) {
    val bytesAccumulated = new AtomicLong(initialSize)
  }

  /**
   * A holding pen for fetch requests waiting to be satisfied
   */
  class FetchRequestPurgatory(requestChannel: RequestChannel, purgeInterval: Int)
          extends RequestPurgatory[DelayedFetch, Int](brokerId, purgeInterval) {
    this.logIdent = "[FetchRequestPurgatory-%d] ".format(brokerId)

    /**
     * A fetch request is satisfied when it has accumulated enough data to meet the min_bytes field
     */
    def checkSatisfied(messageSizeInBytes: Int, delayedFetch: DelayedFetch): Boolean = {
      val accumulatedSize = delayedFetch.bytesAccumulated.addAndGet(messageSizeInBytes)
      accumulatedSize >= delayedFetch.fetch.minBytes
    }

    /**
     * When a request expires just answer it with whatever data is present
     */
    def expire(delayed: DelayedFetch) {
      debug("Expiring fetch request %s.".format(delayed.fetch))
      try {
        val topicData = readMessageSets(delayed.fetch)
        val response = FetchResponse(delayed.fetch.correlationId, topicData)
        val fromFollower = delayed.fetch.isFromFollower
        delayedRequestMetrics.recordDelayedFetchExpired(fromFollower)
        requestChannel.sendResponse(new RequestChannel.Response(delayed.request, new FetchResponseSend(response)))
      }
      catch {
        case e1: LeaderNotAvailableException =>
          debug("Leader changed before fetch request %s expired.".format(delayed.fetch))
        case e2: UnknownTopicOrPartitionException =>
          debug("Replica went offline before fetch request %s expired.".format(delayed.fetch))
      }
    }
  }

  class DelayedProduce(keys: Seq[RequestKey],
                       request: RequestChannel.Request,
                       initialErrorsAndOffsets: Map[TopicAndPartition, ProducerResponseStatus],
                       val produce: ProducerRequest,
                       delayMs: Long)
          extends DelayedRequest(keys, request, delayMs) with Logging {

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
          (true, ErrorMapping.RequestTimedOutCode, producerResponseStatus.offset)
        }
        else (false, producerResponseStatus.error, producerResponseStatus.offset)

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
      
      val response = ProducerResponse(produce.correlationId, finalErrorsAndOffsets)

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
        val partitionOpt = replicaManager.getPartition(topic, partitionId)
        val (hasEnough, errorCode) = partitionOpt match {
          case Some(partition) =>
            partition.checkEnoughReplicasReachOffset(fetchPartitionStatus.requiredOffset, produce.requiredAcks)
          case None =>
            (false, ErrorMapping.UnknownTopicOrPartitionCode)
        }
        if (errorCode != ErrorMapping.NoError) {
          fetchPartitionStatus.acksPending = false
          fetchPartitionStatus.error = errorCode
        } else if (hasEnough) {
          fetchPartitionStatus.acksPending = false
          fetchPartitionStatus.error = ErrorMapping.NoError
        }
        if (!fetchPartitionStatus.acksPending) {
          val messageSizeInBytes = produce.topicPartitionMessageSizeMap(followerFetchRequestKey.topicAndPartition)
          maybeUnblockDelayedFetchRequests(topic, partitionId, messageSizeInBytes)
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
  private [kafka] class ProducerRequestPurgatory(purgeInterval: Int)
          extends RequestPurgatory[DelayedProduce, RequestKey](brokerId, purgeInterval) {
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

