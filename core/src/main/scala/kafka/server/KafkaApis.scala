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
import java.util.concurrent.atomic._
import kafka.admin.{CreateTopicCommand, AdminUtils}
import kafka.api._
import kafka.common._
import kafka.log._
import kafka.message._
import kafka.network._
import org.apache.log4j.Logger
import scala.collection._
import mutable.HashMap
import scala.math._
import kafka.network.RequestChannel.Response
import kafka.utils.{ZkUtils, SystemTime, Logging}
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

  private val produceRequestPurgatory = new ProducerRequestPurgatory(brokerId)
  private val fetchRequestPurgatory = new FetchRequestPurgatory(brokerId, requestChannel)
  private val requestLogger = Logger.getLogger("kafka.request.logger")
  this.logIdent = "KafkaApi on Broker " + brokerId + ", "

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
    info("handling leader and isr request " + leaderAndISRRequest)

    for((partitionInfo, leaderAndISR) <- leaderAndISRRequest.leaderAndISRInfos){
      var errorCode = ErrorMapping.NoError
      val topic = partitionInfo._1
      val partition = partitionInfo._2

      // If the partition does not exist locally, create it
      if(replicaManager.getPartition(topic, partition) == None){
        trace("the partition (%s, %d) does not exist locally, check if current broker is in assigned replicas, if so, start the local replica".format(topic, partition))
        val assignedReplicas = ZkUtils.getReplicasForPartition(kafkaZookeeper.getZookeeperClient, topic, partition)
        trace("assigned replicas list for topic [%s] partition [%d] is [%s]".format(topic, partition, assignedReplicas.toString))
        if(assignedReplicas.contains(brokerId)) {
          val replica = addReplicaCbk(topic, partition, assignedReplicas.toSet)
          info("starting replica for topic [%s] partition [%d]".format(replica.topic, replica.partition.partitionId))
        }
      }
      val replica = replicaManager.getReplica(topic, partition).get
      // The command ask this broker to be new leader for P and it isn't the leader yet
      val requestedLeaderId = leaderAndISR.leader
      // If the broker is requested to be the leader and it's not current the leader (the leader id is set and not equal to broker id)
      if(requestedLeaderId == brokerId && (!replica.partition.leaderId().isDefined || replica.partition.leaderId().get != brokerId)){
        info("becoming the leader for partition [%s, %d] at the leader and isr request %s".format(topic, partition, leaderAndISRRequest))
        errorCode = becomeLeader(replica, leaderAndISR)
      }
      else if (requestedLeaderId != brokerId) {
        info("becoming the follower for partition [%s, %d] at the leader and isr request %s".format(topic, partition, leaderAndISRRequest))
        errorCode = becomeFollower(replica, leaderAndISR)
      }

      responseMap.put(partitionInfo, errorCode)
    }

    if(leaderAndISRRequest.isInit == LeaderAndISRRequest.IsInit){
      replicaManager.startHighWaterMarksCheckPointThread
      val partitionsToRemove = replicaManager.allPartitions.filter(p => !leaderAndISRRequest.leaderAndISRInfos.contains(p._1)).keySet
      info("init flag is set in leaderAndISR request, partitions to remove: %s".format(partitionsToRemove))
      partitionsToRemove.foreach(p => stopReplicaCbk(p._1, p._2))
    }

    val leaderAndISRResponse = new LeaderAndISRResponse(leaderAndISRRequest.versionId, responseMap)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(leaderAndISRResponse)))
  }


  def handleStopReplicaRequest(request: RequestChannel.Request){
    val stopReplicaRequest = StopReplicaRequest.readFrom(request.request.buffer)
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
      satisfied ++= fetchRequestPurgatory.update((topic, partitionData.partition), partitionData)
    trace("produce request to %s unblocked %d DelayedFetchRequests.".format(topic, satisfied.size))
    // send any newly unblocked responses
    for(fetchReq <- satisfied) {
      val topicData = readMessageSets(fetchReq.fetch)
      val response = new FetchResponse(FetchRequest.CurrentVersion, fetchReq.fetch.correlationId, topicData)
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
      requestLogger.trace("producer request %s".format(produceRequest.toString))
    trace("Broker %s received produce request %s".format(logManager.config.brokerId, produceRequest.toString))

    val response = produceToLocalLog(produceRequest)
    debug("produce to local log in %d ms".format(SystemTime.milliseconds - sTime))

    if (produceRequest.requiredAcks == 0 ||
        produceRequest.requiredAcks == 1 ||
        produceRequest.data.size <= 0) {
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))

      for (topicData <- produceRequest.data)
        maybeUnblockDelayedFetchRequests(topicData.topic, topicData.partitionDataArray)
    }
    else {
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val topicPartitionPairs = produceRequest.data.flatMap(topicData => {
        val topic = topicData.topic
        topicData.partitionDataArray.map(partitionData => {
          (topic, partitionData.partition)
        })
      })
      val delayedProduce = new DelayedProduce(
        topicPartitionPairs, request,
        response.errors, response.offsets,
        produceRequest, produceRequest.ackTimeoutMs.toLong)
      produceRequestPurgatory.watch(delayedProduce)
      /*
       * Replica fetch requests may have arrived (and potentially satisfied)
       * delayedProduce requests before they even made it to the purgatory.
       * Here, we explicitly check if any of them can be satisfied.
       */
      var satisfiedProduceRequests = new mutable.ArrayBuffer[DelayedProduce]
      topicPartitionPairs.foreach(topicPartition =>
                                    satisfiedProduceRequests ++=
                                            produceRequestPurgatory.update(topicPartition, topicPartition))
      debug("%d DelayedProduce requests unblocked after produce to local log.".format(satisfiedProduceRequests.size))
      satisfiedProduceRequests.foreach(_.respond())
    }
  }

  /**
   * Helper method for handling a parsed producer request
   */
  private def produceToLocalLog(request: ProducerRequest): ProducerResponse = {
    trace("produce [%s] to local log ".format(request.toString))
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
          trace(partitionData.messages.sizeInBytes + " bytes written to logs.")
        } catch {
          case e =>
            BrokerTopicStat.getBrokerTopicStat(topicData.topic).recordFailedProduceRequest
            BrokerTopicStat.getBrokerAllTopicStat.recordFailedProduceRequest
            error("error processing ProducerRequest on " + topicData.topic + ":" + partitionData.partition, e)
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
    val ret = new ProducerResponse(request.versionId, request.correlationId, errors, offsets)
    ret
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = FetchRequest.readFrom(request.request.buffer)
    trace("handling fetch request: " + fetchRequest.toString)
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
          satisfiedProduceRequests ++= produceRequestPurgatory.update(
            (topicOffsetInfo.topic, partition), (topicOffsetInfo.topic, partition)
          )
        })
      })
      debug("replica %d fetch unblocked %d DelayedProduce requests.".format(fetchRequest.replicaId, satisfiedProduceRequests.size))
      satisfiedProduceRequests.foreach(_.respond())
    }

    // if there are enough bytes available right now we can answer the request, otherwise we have to punt
    val availableBytes = availableFetchBytes(fetchRequest)
    if(fetchRequest.maxWait <= 0 ||
       availableBytes >= fetchRequest.minBytes ||
       fetchRequest.numPartitions <= 0) {
      val topicData = readMessageSets(fetchRequest)
      debug("returning fetch response %s for fetch request with correlation id %d".format(topicData.map(_.partitionDataArray.map(_.error).mkString(",")).mkString(","), fetchRequest.correlationId))
      val response = new FetchResponse(FetchRequest.CurrentVersion, fetchRequest.correlationId, topicData)
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)))
    } else {
      debug("putting fetch request into purgatory")
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val topicPartitionPairs: Seq[Any] = fetchRequest.offsetInfo.flatMap(o => o.partitions.map((o.topic, _)))
      val delayedFetch = new DelayedFetch(topicPartitionPairs, request, fetchRequest, fetchRequest.maxWait, availableBytes)
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
            info("invalid partition " + offsetDetail.partitions(i) + "in fetch request from client '" + fetchRequest.clientId + "'")
        }
      }
    }
    totalBytes
  }

  private def maybeUpdatePartitionHW(fetchRequest: FetchRequest) {
    val offsets = fetchRequest.offsetInfo
    debug("act on update partition HW, check offset detail: %s ".format(offsets))
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
      for( (partition, offset, fetchSize) <- (partitions, offsets, fetchSizes).zipped.map((_,_,_)) ) {
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
            val leaderReplicaOpt = replicaManager.getReplica(topic, partition, logManager.config.brokerId)
            assert(leaderReplicaOpt.isDefined, "Leader replica for topic %s partition %d".format(topic, partition) + " must exist on leader broker %d".format(logManager.config.brokerId))
            val leaderReplica = leaderReplicaOpt.get
            fetchRequest.replicaId match {
              case FetchRequest.NonFollowerId => // replica id value of -1 signifies a fetch request from an external client, not from one of the replicas
                new PartitionData(partition, ErrorMapping.NoError, offset, leaderReplica.highWatermark(), messages)
              case _ => // fetch request from a follower
                val replicaOpt = replicaManager.getReplica(topic, partition, fetchRequest.replicaId)
                assert(replicaOpt.isDefined, "No replica %d in replica manager on %d".format(fetchRequest.replicaId, replicaManager.config.brokerId))
                val replica = replicaOpt.get
                debug("leader [%d] for topic [%s] partition [%d] received fetch request from follower [%d]".format(logManager.config.brokerId, replica.topic, replica.partition.partitionId, fetchRequest.replicaId))
                debug("Leader %d returning %d messages for topic %s partition %d to follower %d".format(logManager.config.brokerId, messages.sizeInBytes, replica.topic, replica.partition.partitionId, fetchRequest.replicaId))
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
      trace("fetching log segment for topic, partition, offset, size = " + (topic, partition, offset, maxSize))
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
      requestLogger.trace("offset request " + offsetRequest.toString)
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
      requestLogger.trace("topic metadata request " + metadataRequest.toString())
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
    debug("shut down")
    fetchRequestPurgatory.shutdown()
    produceRequestPurgatory.shutdown()
    debug("shutted down completely")
  }

  /**
   * A delayed fetch request
   */
  class DelayedFetch(keys: Seq[Any], request: RequestChannel.Request, val fetch: FetchRequest, delayMs: Long, initialSize: Long) extends DelayedRequest(keys, request, delayMs) {
    val bytesAccumulated = new AtomicLong(initialSize)
  }

  /**
   * A holding pen for fetch requests waiting to be satisfied
   */
  class FetchRequestPurgatory(brokerId: Int, requestChannel: RequestChannel) extends RequestPurgatory[DelayedFetch, PartitionData]("Fetch Request Purgatory on Broker " + brokerId + ", ") {

    /**
     * A fetch request is satisfied when it has accumulated enough data to meet the min_bytes field
     */
    def checkSatisfied(partitionData: PartitionData, delayedFetch: DelayedFetch): Boolean = {
      val messageDataSize = partitionData.messages.sizeInBytes
      val accumulatedSize = delayedFetch.bytesAccumulated.addAndGet(messageDataSize)
      debug("fetch request check, accm size: " + accumulatedSize + " delay fetch min bytes: " + delayedFetch.fetch.minBytes)
      accumulatedSize >= delayedFetch.fetch.minBytes
    }

    /**
     * When a request expires just answer it with whatever data is present
     */
    def expire(delayed: DelayedFetch) {
      val topicData = readMessageSets(delayed.fetch)
      val response = new FetchResponse(FetchRequest.CurrentVersion, delayed.fetch.correlationId, topicData)
      requestChannel.sendResponse(new RequestChannel.Response(delayed.request, new FetchResponseSend(response)))
    }
  }

  class DelayedProduce(keys: Seq[Any],
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
    private val partitionStatus = keys.map(key => {
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
                      ((key: Any, result: (List[Short], List[Long])) => {
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
    def isSatisfied(followerFetchPartition: (String, Int)) = {
      val (topic, partitionId) = followerFetchPartition
      val fetchPartitionStatus = partitionStatus(followerFetchPartition)
      if (fetchPartitionStatus.acksPending) {
        val leaderReplica = replicaManager.getLeaderReplica(topic, partitionId)
        leaderReplica match {
          case Some(leader) => {
            if (leader.isLocal) {
              val isr = leader.partition.inSyncReplicas
              val numAcks = isr.count(r => {
                if (!r.isLocal)
                  r.logEndOffset() >= partitionStatus(followerFetchPartition).requiredOffset
                else
                  true /* also count the local (leader) replica */
              })
              trace("Received %d/%d acks for produce request to %s-%d".format(
                numAcks, produce.requiredAcks,
                topic, partitionId))
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
      ! partitionStatus.exists(p => p._2.acksPending)
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
  private [kafka] class ProducerRequestPurgatory(brokerId: Int) extends RequestPurgatory[DelayedProduce, (String, Int)]("Producer Request Purgatory on Broker " + brokerId + ", ") {

    protected def checkSatisfied(fetchRequestPartition: (String, Int),
                                 delayedProduce: DelayedProduce) =
      delayedProduce.isSatisfied(fetchRequestPartition)

    /**
     * Handle an expired delayed request
     */
    protected def expire(delayedProduce: DelayedProduce) {
      delayedProduce.respond()
    }
  }
}

