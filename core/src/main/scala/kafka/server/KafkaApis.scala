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
import kafka.log._
import kafka.message._
import kafka.network._
import kafka.utils.{ZkUtils, Pool, SystemTime, Logging}
import org.apache.log4j.Logger
import scala.collection._
import mutable.HashMap
import scala.math._
import kafka.network.RequestChannel.Response
import java.util.concurrent.TimeUnit
import kafka.metrics.KafkaMetricsGroup
import kafka.cluster.Replica


/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel, val logManager: LogManager,
                val replicaManager: ReplicaManager, val kafkaZookeeper: KafkaZooKeeper,
                addReplicaCbk: (String, Int, Set[Int]) => Replica,
                stopReplicaCbk: (String, Int) => Short,
                becomeLeader: (Replica, LeaderAndISR) => Short,
                becomeFollower: (Replica, LeaderAndISR) => Short,
                brokerId: Int) extends Logging {

  private val metricsGroup = brokerId.toString
  private val producerRequestPurgatory = new ProducerRequestPurgatory
  private val fetchRequestPurgatory = new FetchRequestPurgatory(requestChannel)
  private val delayedRequestMetrics = new DelayedRequestMetrics

  private val requestLogger = Logger.getLogger("kafka.request.logger")
  this.logIdent = "KafkaApis-%d ".format(brokerId)

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
    val responseMap = new HashMap[(String, Int), Short]
    val leaderAndISRRequest = LeaderAndISRRequest.readFrom(request.request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling leader and isr request " + leaderAndISRRequest)
    trace("Handling leader and isr request " + leaderAndISRRequest)

    for((partitionInfo, leaderAndISR) <- leaderAndISRRequest.leaderAndISRInfos){
      var errorCode = ErrorMapping.NoError
      val topic = partitionInfo._1
      val partition = partitionInfo._2

      // If the partition does not exist locally, create it
      if(replicaManager.getPartition(topic, partition) == None){
        trace("The partition (%s, %d) does not exist locally, check if current broker is in assigned replicas, if so, start the local replica".format(topic, partition))
        val assignedReplicas = ZkUtils.getReplicasForPartition(kafkaZookeeper.getZookeeperClient, topic, partition)
        trace("Assigned replicas list for topic [%s] partition [%d] is [%s]".format(topic, partition, assignedReplicas.toString))
        if(assignedReplicas.contains(brokerId)) {
          val replica = addReplicaCbk(topic, partition, assignedReplicas.toSet)
          info("Starting replica for topic [%s] partition [%d]".format(replica.topic, replica.partition.partitionId))
        }
      }
      val replica = replicaManager.getReplica(topic, partition).get
      // The command ask this broker to be new leader for P and it isn't the leader yet
      val requestedLeaderId = leaderAndISR.leader
      // If the broker is requested to be the leader and it's not current the leader (the leader id is set and not equal to broker id)
      if(requestedLeaderId == brokerId && (!replica.partition.leaderId().isDefined || replica.partition.leaderId().get != brokerId)){
        info("Becoming the leader for partition [%s, %d] at the leader and isr request %s".format(topic, partition, leaderAndISRRequest))
        errorCode = becomeLeader(replica, leaderAndISR)
      }
      else if (requestedLeaderId != brokerId) {
        info("Becoming the follower for partition [%s, %d] at the leader and isr request %s".format(topic, partition, leaderAndISRRequest))
        errorCode = becomeFollower(replica, leaderAndISR)
      }

      responseMap.put(partitionInfo, errorCode)
    }

    if(leaderAndISRRequest.isInit == LeaderAndISRRequest.IsInit){
      replicaManager.startHighWaterMarksCheckPointThread
      val partitionsToRemove = replicaManager.allPartitions.filter(p => !leaderAndISRRequest.leaderAndISRInfos.contains(p._1)).keySet
      info("Init flag is set in leaderAndISR request, partitions to remove: %s".format(partitionsToRemove))
      partitionsToRemove.foreach(p => stopReplicaCbk(p._1, p._2))
    }

    val leaderAndISRResponse = new LeaderAndISRResponse(leaderAndISRRequest.versionId, responseMap)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(leaderAndISRResponse)))
  }


  def handleStopReplicaRequest(request: RequestChannel.Request){
    val stopReplicaRequest = StopReplicaRequest.readFrom(request.request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Handling stop replica request " + stopReplicaRequest)
    trace("Handling stop replica request " + stopReplicaRequest)

    val responseMap = new HashMap[(String, Int), Short]

    for((topic, partition) <- stopReplicaRequest.stopReplicaSet){
      val errorCode = stopReplicaCbk(topic, partition)
      responseMap.put((topic, partition), errorCode)
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
    
    for (topicData <- produceRequest.data)
      maybeUnblockDelayedFetchRequests(topicData.topic, topicData.partitionDataArray)
    
    if (produceRequest.requiredAcks == 0 ||
        produceRequest.requiredAcks == 1 ||
        produceRequest.data.size <= 0)
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
          kafkaZookeeper.ensurePartitionLeaderOnThisBroker(topicData.topic, partitionData.partition)
          val log = logManager.getOrCreateLog(topicData.topic, partitionData.partition)
          log.append(partitionData.messages.asInstanceOf[ByteBufferMessageSet])
          replicaManager.recordLeaderLogEndOffset(topicData.topic, partitionData.partition, log.logEndOffset)
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
        try {
          val maybeLog = logManager.getLog(offsetDetail.topic, offsetDetail.partitions(i))
          val available = maybeLog match {
            case Some(log) => max(0, log.logEndOffset - offsetDetail.offsets(i))
            case None => 0
          }
          totalBytes += math.min(offsetDetail.fetchSizes(i), available)
        } catch {
          case e: InvalidPartitionException =>
            info("Invalid partition %d in fetch request from client %d."
              .format(offsetDetail.partitions(i), fetchRequest.clientId))
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
        replicaManager.recordFollowerPosition(topic, partition, fetchRequest.replicaId, offset,
                                              kafkaZookeeper.getZookeeperClient)
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
      for( (partition, offset, fetchSize) <- (partitions, offsets, fetchSizes).zipped.map((_,_,_)) ){
        val partitionInfo = readMessageSet(topic, partition, offset, fetchSize) match {
          case Left(err) =>
            BrokerTopicStat.getBrokerTopicStat(topic).recordFailedFetchRequest
            BrokerTopicStat.getBrokerAllTopicStat.recordFailedFetchRequest
            fetchRequest.replicaId match {
              case -1 => new PartitionData(partition, err, offset, -1L, MessageSet.Empty)
              case _ =>
                new PartitionData(partition, err, offset, -1L, MessageSet.Empty)
            }
          case Right(messages) =>
            BrokerTopicStat.getBrokerTopicStat(topic).recordBytesOut(messages.sizeInBytes)
            BrokerTopicStat.getBrokerAllTopicStat.recordBytesOut(messages.sizeInBytes)
            val leaderReplicaOpt = replicaManager.getReplica(topic, partition, brokerId)
            assert(leaderReplicaOpt.isDefined, "Leader replica for topic %s partition %d must exist on leader broker %d".format(topic, partition, brokerId))
            val leaderReplica = leaderReplicaOpt.get
            fetchRequest.replicaId match {
              case FetchRequest.NonFollowerId =>
               // replica id value of -1 signifies a fetch request from an external client, not from one of the replicas
                new PartitionData(partition, ErrorMapping.NoError, offset, leaderReplica.highWatermark(), messages)
              case _ => // fetch request from a follower
                val replicaOpt = replicaManager.getReplica(topic, partition, fetchRequest.replicaId)
                assert(replicaOpt.isDefined, "No replica %d in replica manager on %d".format(fetchRequest.replicaId, brokerId))
                val replica = replicaOpt.get
                debug("Leader for topic [%s] partition [%d] received fetch request from follower [%d]"
                  .format(replica.topic, replica.partition.partitionId, fetchRequest.replicaId))
                debug("Leader returning %d messages for topic %s partition %d to follower %d"
                  .format(messages.sizeInBytes, replica.topic, replica.partition.partitionId, fetchRequest.replicaId))
                new PartitionData(partition, ErrorMapping.NoError, offset, leaderReplica.highWatermark(), messages)
            }
        }
        info.append(partitionInfo)
      }
      fetchedData.append(new TopicData(topic, info.toArray))
    }
    fetchedData.toArray
  }

  /**
   * Read from a single topic/partition at the given offset
   */
  private def readMessageSet(topic: String, partition: Int, offset: Long, maxSize: Int): Either[Short, MessageSet] = {
    var response: Either[Short, MessageSet] = null
    try {
      // check if the current broker is the leader for the partitions
      kafkaZookeeper.ensurePartitionLeaderOnThisBroker(topic, partition)
      trace("Fetching log segment for topic, partition, offset, size = " + (topic, partition, offset, maxSize))
      val log = logManager.getLog(topic, partition)
      response = Right(log match { case Some(l) => l.read(offset, maxSize) case None => MessageSet.Empty })
    } catch {
      case e =>
        error("error when processing request " + (topic, partition, offset, maxSize), e)
        response = Left(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }
    response
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
      kafkaZookeeper.ensurePartitionLeaderOnThisBroker(offsetRequest.topic, offsetRequest.partition)
      val offsets = logManager.getOffsets(offsetRequest)
      response = new OffsetResponse(offsetRequest.versionId, offsets)
    }catch {
      case ioe: IOException =>
        fatal("Halting due to unrecoverable I/O error while handling producer request: " + ioe.getMessage, ioe)
        System.exit(1)
      case e =>
        warn("Error while responding to offset request", e)
        response = new OffsetResponse(offsetRequest.versionId, Array.empty[Long],ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]).toShort)
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
    val zkClient = kafkaZookeeper.getZookeeperClient
    var errorCode = ErrorMapping.NoError
    val config = logManager.config

    try {
      val topicMetadataList = AdminUtils.getTopicMetaDataFromZK(metadataRequest.topics, zkClient)
      metadataRequest.topics.zip(topicMetadataList).foreach(
        topicAndMetadata =>{
          val topic = topicAndMetadata._1
          topicAndMetadata._2.errorCode match {
            case ErrorMapping.NoError => topicsMetadata += topicAndMetadata._2
            case ErrorMapping.UnknownTopicCode =>
              /* check if auto creation of topics is turned on */
              if(config.autoCreateTopics) {
                CreateTopicCommand.createTopic(zkClient, topic, config.numPartitions, config.defaultReplicationFactor)
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
    }catch {
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
  class FetchRequestPurgatory(requestChannel: RequestChannel) extends RequestPurgatory[DelayedFetch, Null](brokerId) {

    this.logIdent = "FetchRequestPurgatory-%d ".format(brokerId)

    override def metricsGroupIdent = metricsGroup

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
        val leaderReplica = replicaManager.getLeaderReplica(topic, partitionId)
        leaderReplica match {
          case Some(leader) => {
            if (leader.isLocal) {
              val isr = leader.partition.inSyncReplicas
              val numAcks = isr.count(r => {
                if (!r.isLocal) {
                  r.logEndOffset() >= partitionStatus(key).requiredOffset
                }
                else
                  true /* also count the local (leader) replica */
              })

              trace("Received %d/%d acks for producer request to %s-%d; isr size = %d".format(
                numAcks, produce.requiredAcks,
                topic, partitionId, isr.size))
              if ((produce.requiredAcks < 0 && numAcks >= isr.size) ||
                      (produce.requiredAcks > 0 && numAcks >= produce.requiredAcks)) {
                /*
                 * requiredAcks < 0 means acknowledge after all replicas in ISR
                 * are fully caught up to the (local) leader's offset
                 * corresponding to this produce request.
                 */

                fetchPartitionStatus.acksPending = false
                fetchPartitionStatus.error = ErrorMapping.NoError
                val topicData =
                  produce.data.find(_.topic == topic).get
                val partitionData =
                  topicData.partitionDataArray.find(_.partition == partitionId).get
                delayedRequestMetrics.recordDelayedProducerKeyCaughtUp(key,
                                                                       durationNs,
                                                                       partitionData.sizeInBytes)
                maybeUnblockDelayedFetchRequests(
                  topic, Array(partitionData))
              }
            }
            else {
              debug("Broker not leader for %s-%d".format(topic, partitionId))
              fetchPartitionStatus.setThisBrokerNotLeader()
            }
          }
          case None =>
            debug("Broker not leader for %s-%d".format(topic, partitionId))
            fetchPartitionStatus.setThisBrokerNotLeader()
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
  private [kafka] class ProducerRequestPurgatory extends RequestPurgatory[DelayedProduce, RequestKey](brokerId) {


    this.logIdent = "ProducerRequestPurgatory-%d ".format(brokerId)

    override def metricsGroupIdent = metricsGroup

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
      override def metricsGroupIdent = metricsGroup
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

      override def metricsGroupIdent = metricsGroup
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

