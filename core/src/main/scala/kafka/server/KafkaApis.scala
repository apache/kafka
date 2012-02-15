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
import java.lang.IllegalStateException
import kafka.admin.{CreateTopicCommand, AdminUtils}
import kafka.api._
import kafka.log._
import kafka.message._
import kafka.network._
import kafka.utils.{SystemTime, Logging}
import org.apache.log4j.Logger
import scala.collection.mutable.ListBuffer
import kafka.common.{FetchRequestFormatException, ErrorMapping}

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val logManager: LogManager) extends Logging {
  
  private val requestLogger = Logger.getLogger("kafka.request.logger")

  def handle(receive: Receive): Option[Send] = { 
    val apiId = receive.buffer.getShort() 
    apiId match {
      case RequestKeys.Produce => handleProducerRequest(receive)
      case RequestKeys.Fetch => handleFetchRequest(receive)
      case RequestKeys.MultiProduce => handleMultiProducerRequest(receive)
      case RequestKeys.Offsets => handleOffsetRequest(receive)
      case RequestKeys.TopicMetadata => handleTopicMetadataRequest(receive)
      case _ => throw new IllegalStateException("No mapping found for handler id " + apiId)
    }
  }

  def handleProducerRequest(receive: Receive): Option[Send] = {
    val sTime = SystemTime.milliseconds
    val request = ProducerRequest.readFrom(receive.buffer)

    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Producer request " + request.toString)
    handleProducerRequest(request, "ProduceRequest")
    debug("kafka produce time " + (SystemTime.milliseconds - sTime) + " ms")
    None
  }

  def handleMultiProducerRequest(receive: Receive): Option[Send] = {
    val request = MultiProducerRequest.readFrom(receive.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Multiproducer request " + request.toString)
    request.produces.map(handleProducerRequest(_, "MultiProducerRequest"))
    None
  }

  private def handleProducerRequest(request: ProducerRequest, requestHandlerName: String) = {
    val partition = request.getTranslatedPartition(logManager.chooseRandomPartition)
    try {
      logManager.getOrCreateLog(request.topic, partition).append(request.messages)
      trace(request.messages.sizeInBytes + " bytes written to logs.")
    } catch {
      case e =>
        error("Error processing " + requestHandlerName + " on " + request.topic + ":" + partition, e)
        e match {
          case _: IOException => 
            fatal("Halting due to unrecoverable I/O error while handling producer request: " + e.getMessage, e)
            System.exit(1)
          case _ =>
        }
        throw e
    }
    None
  }

  def handleFetchRequest(request: Receive): Option[Send] = {
    val fetchRequest = FetchRequest.readFrom(request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Fetch request " + fetchRequest.toString)

    // validate the request
    try {
      fetchRequest.validate()  
    } catch {
      case e:FetchRequestFormatException =>
        val response = new FetchResponse(FetchResponse.CurrentVersion, fetchRequest.correlationId, Array.empty)
        return Some(new FetchResponseSend(response, ErrorMapping.InvalidFetchRequestFormatCode))
    }

    val fetchedData = new ListBuffer[TopicData]()

    for(offsetDetail <- fetchRequest.offsetInfo) {
      val info = new ListBuffer[PartitionData]()
      val topic = offsetDetail.topic
      val (partitions, offsets, fetchSizes) = (offsetDetail.partitions, offsetDetail.offsets, offsetDetail.fetchSizes)
      for( (partition, offset, fetchSize) <- (partitions, offsets, fetchSizes).zipped.map((_,_,_)) ) {
        val partitionInfo = readMessageSet(topic, partition, offset, fetchSize) match {
          case Left(err) => new PartitionData(partition, err, offset, MessageSet.Empty)
          case Right(messages) => new PartitionData(partition, ErrorMapping.NoError, offset, messages)
        }
        info.append(partitionInfo)
      }
      fetchedData.append(new TopicData(topic, info.toArray))
    }
    val response = new FetchResponse(FetchRequest.CurrentVersion, fetchRequest.correlationId, fetchedData.toArray )
    Some(new FetchResponseSend(response, ErrorMapping.NoError))
  }

  private def readMessageSet(topic: String, partition: Int, offset: Long, maxSize: Int): Either[Int, MessageSet] = {
    var response: Either[Int, MessageSet] = null
    try {
      trace("Fetching log segment for topic, partition, offset, size = " + (topic, partition, offset, maxSize))
      val log = logManager.getLog(topic, partition)
      response = Right(if(log != null) log.read(offset, maxSize) else MessageSet.Empty)
    } catch {
      case e =>
        error("error when processing request " + (topic, partition, offset, maxSize), e)
        response = Left(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }
    response
  }

  def handleOffsetRequest(request: Receive): Option[Send] = {
    val offsetRequest = OffsetRequest.readFrom(request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Offset request " + offsetRequest.toString)
    val offsets = logManager.getOffsets(offsetRequest)
    val response = new OffsetArraySend(offsets)
    Some(response)
  }

  def handleTopicMetadataRequest(request: Receive): Option[Send] = {
    val metadataRequest = TopicMetadataRequest.readFrom(request.buffer)

    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Topic metadata request " + metadataRequest.toString())

    val topicsMetadata = new ListBuffer[TopicMetadata]()
    val config = logManager.getServerConfig
    val zkClient = logManager.getZookeeperClient
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
            info("Auto creation of topic %s with partitions %d and replication factor %d is successful!"
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
    Some(new TopicMetadataSend(topicsMetadata))
  }
}
