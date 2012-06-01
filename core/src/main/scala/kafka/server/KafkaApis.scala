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
import kafka.utils.{SystemTime, Logging}
import org.apache.log4j.Logger
import scala.collection._
import scala.math._
import java.lang.IllegalStateException

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel, val logManager: LogManager,
                val replicaManager: ReplicaManager, val kafkaZookeeper: KafkaZooKeeper) extends Logging {

  private val fetchRequestPurgatory = new FetchRequestPurgatory(requestChannel)
  private val requestLogger = Logger.getLogger("kafka.request.logger")

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
      case _ => throw new IllegalStateException("No mapping found for handler id " + apiId)
    }
  }

  /**
   * Handle a produce request
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = ProducerRequest.readFrom(request.request.buffer)
    val sTime = SystemTime.milliseconds
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Producer request " + request.toString)

    val response = produce(produceRequest)
    debug("kafka produce time " + (SystemTime.milliseconds - sTime) + " ms")
    requestChannel.sendResponse(new RequestChannel.Response(request, new ProducerResponseSend(response), -1))
    
    // Now check any outstanding fetches this produce just unblocked
    var satisfied = new mutable.ArrayBuffer[DelayedFetch]
    for(topicData <- produceRequest.data) {
      for(partition <- topicData.partitionData)
        satisfied ++= fetchRequestPurgatory.update((topicData.topic, partition.partition), topicData)
    }
    // send any newly unblocked responses
    for(fetchReq <- satisfied) {
       val topicData = readMessageSets(fetchReq.fetch)
       val response = new FetchResponse(FetchRequest.CurrentVersion, fetchReq.fetch.correlationId, topicData)
       requestChannel.sendResponse(new RequestChannel.Response(fetchReq.request, new FetchResponseSend(response, ErrorMapping.NoError), -1))
    }
  }

  /**
   * Helper method for handling a parsed producer request
   */
  private def produce(request: ProducerRequest): ProducerResponse = {
    val requestSize = request.topicPartitionCount
    val errors = new Array[Short](requestSize)
    val offsets = new Array[Long](requestSize)

    var msgIndex = -1
    for(topicData <- request.data) {
      for(partitionData <- topicData.partitionData) {
        msgIndex += 1
        try {
          kafkaZookeeper.ensurePartitionLeaderOnThisBroker(topicData.topic, partitionData.partition)
          val log = logManager.getOrCreateLog(topicData.topic, partitionData.partition)
          log.append(partitionData.messages.asInstanceOf[ByteBufferMessageSet])
          replicaManager.recordLeaderLogUpdate(topicData.topic, partitionData.partition)
          offsets(msgIndex) = log.nextAppendOffset
          errors(msgIndex) = ErrorMapping.NoError.toShort
          trace(partitionData.messages.sizeInBytes + " bytes written to logs.")
        } catch {
          case e =>
            error("Error processing ProducerRequest on " + topicData.topic + ":" + partitionData.partition, e)
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
    new ProducerResponse(ProducerResponse.CurrentVersion, request.correlationId, errors, offsets)
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = FetchRequest.readFrom(request.request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Fetch request " + fetchRequest.toString)

    // validate the request
    try {
      fetchRequest.validate()
    } catch {
      case e:FetchRequestFormatException =>
        val response = new FetchResponse(FetchResponse.CurrentVersion, fetchRequest.correlationId, Array.empty)
        val channelResponse = new RequestChannel.Response(request, new FetchResponseSend(response,
          ErrorMapping.InvalidFetchRequestFormatCode), -1)
        requestChannel.sendResponse(channelResponse)
    }

    if(fetchRequest.replicaId != -1)
      maybeUpdatePartitionHW(fetchRequest)

    // if there are enough bytes available right now we can answer the request, otherwise we have to punt
    val availableBytes = availableFetchBytes(fetchRequest)
    if(fetchRequest.maxWait <= 0 || availableBytes >= fetchRequest.minBytes) {
      val topicData = readMessageSets(fetchRequest)
      debug("Returning fetch response %s for fetch request with correlation id %d"
        .format(topicData.map(_.partitionData.map(_.error).mkString(",")).mkString(","), fetchRequest.correlationId))
      val response = new FetchResponse(FetchRequest.CurrentVersion, fetchRequest.correlationId, topicData)
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response, ErrorMapping.NoError), -1))
    } else {
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val keys: Seq[Any] = fetchRequest.offsetInfo.flatMap(o => o.partitions.map((o.topic, _)))
      val delayedFetch = new DelayedFetch(keys, request, fetchRequest, fetchRequest.maxWait, availableBytes)
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
          debug("Fetching log for topic %s partition %d".format(offsetDetail.topic, offsetDetail.partitions(i)))
          val maybeLog = logManager.getLog(offsetDetail.topic, offsetDetail.partitions(i))
          val available = maybeLog match {
            case Some(log) => max(0, log.highwaterMark - offsetDetail.offsets(i))
            case None => 0
          }
          totalBytes += math.min(offsetDetail.fetchSizes(i), available)
        } catch {
          case e: InvalidPartitionException =>
            info("Invalid partition " + offsetDetail.partitions(i) + "in fetch request from client '" + fetchRequest.clientId + "'")
        }
      }
    }
    totalBytes
  }

  private def maybeUpdatePartitionHW(fetchRequest: FetchRequest) {
    val offsets = fetchRequest.offsetInfo

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
            fetchRequest.replicaId match {
              case -1 => new PartitionData(partition, err, offset, -1L, MessageSet.Empty)
              case _ =>
                new PartitionData(partition, err, offset, -1L, MessageSet.Empty)
            }
          case Right(messages) =>
            val leaderReplicaOpt = replicaManager.getReplica(topic, partition, logManager.config.brokerId)
            assert(leaderReplicaOpt.isDefined, "Leader replica for topic %s partition %d".format(topic, partition) +
              " must exist on leader broker %d".format(logManager.config.brokerId))
            val leaderReplica = leaderReplicaOpt.get
            fetchRequest.replicaId match {
              case -1 => // replica id value of -1 signifies a fetch request from an external client, not from one of the replicas
                new PartitionData(partition, ErrorMapping.NoError, offset, leaderReplica.highWatermark(), messages)
              case _ => // fetch request from a follower
                val replicaOpt = replicaManager.getReplica(topic, partition, fetchRequest.replicaId)
                assert(replicaOpt.isDefined, "No replica %d in replica manager on %d"
                  .format(fetchRequest.replicaId, replicaManager.config.brokerId))
                val replica = replicaOpt.get
                debug("Leader %d for topic %s partition %d received fetch request from follower %d"
                  .format(logManager.config.brokerId, replica.topic, replica.partition.partitionId, fetchRequest.replicaId))
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
  private def readMessageSet(topic: String, partition: Int, offset: Long, maxSize: Int): Either[Int, MessageSet] = {
    var response: Either[Int, MessageSet] = null
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
      requestLogger.trace("Offset request " + offsetRequest.toString)
    val offsets = logManager.getOffsets(offsetRequest)
    val response = new OffsetArraySend(offsets)
    requestChannel.sendResponse(new RequestChannel.Response(request, response, -1))
  }

  /**
   * Service the topic metadata request API
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = TopicMetadataRequest.readFrom(request.request.buffer)

    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Topic metadata request " + metadataRequest.toString())

    val topicsMetadata = new mutable.ArrayBuffer[TopicMetadata]()
    val config = logManager.getServerConfig
    val zkClient = kafkaZookeeper.getZookeeperClient
    val topicMetadataList = AdminUtils.getTopicMetaDataFromZK(metadataRequest.topics, zkClient)

    metadataRequest.topics.zip(topicMetadataList).foreach { topicAndMetadata =>
      val topic = topicAndMetadata._1
      topicAndMetadata._2 match {
        case Some(metadata) => topicsMetadata += metadata
        case None =>
          /* check if auto creation of topics is turned on */
          if(config.autoCreateTopics) {
            CreateTopicCommand.createTopic(zkClient, topic, config.numPartitions,
              config.defaultReplicationFactor)
            info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
              .format(topic, config.numPartitions, config.defaultReplicationFactor))
            val newTopicMetadata = AdminUtils.getTopicMetaDataFromZK(List(topic), zkClient).head
            newTopicMetadata match {
              case Some(topicMetadata) => topicsMetadata += topicMetadata
              case None =>
                throw new IllegalStateException("Topic metadata for automatically created topic %s does not exist".format(topic))
            }
          }
      }
    }
    info("Sending response for topic metadata request")
    requestChannel.sendResponse(new RequestChannel.Response(request, new TopicMetadataSend(topicsMetadata), -1))
  }

  def close() {
    fetchRequestPurgatory.shutdown()
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
  class FetchRequestPurgatory(requestChannel: RequestChannel) extends RequestPurgatory[DelayedFetch, TopicData] {
    
    /**
     * A fetch request is satisfied when it has accumulated enough data to meet the min_bytes field
     */
    def checkSatisfied(topicData: TopicData, delayedFetch: DelayedFetch): Boolean = {
      val messageDataSize = topicData.partitionData.map(_.messages.sizeInBytes).sum
      val accumulatedSize = delayedFetch.bytesAccumulated.addAndGet(messageDataSize)
      accumulatedSize >= delayedFetch.fetch.minBytes
    }
    
    /**
     * When a request expires just answer it with whatever data is present
     */
    def expire(delayed: DelayedFetch) {
      val topicData = readMessageSets(delayed.fetch)
      val response = new FetchResponse(FetchRequest.CurrentVersion, delayed.fetch.correlationId, topicData)
      requestChannel.sendResponse(new RequestChannel.Response(delayed.request, new FetchResponseSend(response, ErrorMapping.NoError), -1))
    }
  }
}
