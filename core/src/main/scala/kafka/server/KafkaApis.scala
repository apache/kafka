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
import org.apache.log4j.Logger
import scala.collection.mutable.ListBuffer
import kafka.utils.{SystemTime, Logging}
import kafka.common.{FetchRequestFormatException, ErrorMapping}

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val logManager: LogManager, val kafkaZookeeper: KafkaZooKeeper) extends Logging {
  
  private val requestLogger = Logger.getLogger("kafka.request.logger")

  def handle(receive: Receive): Option[Send] = { 
    val apiId = receive.buffer.getShort() 
    apiId match {
      case RequestKeys.Produce => handleProducerRequest(receive)
      case RequestKeys.Fetch => handleFetchRequest(receive)
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

    val response = handleProducerRequest(request)
    debug("kafka produce time " + (SystemTime.milliseconds - sTime) + " ms")
    Some(new ProducerResponseSend(response))
  }

  private def handleProducerRequest(request: ProducerRequest): ProducerResponse = {
    val requestSize = request.getNumTopicPartitions
    val errors = new Array[Short](requestSize)
    val offsets = new Array[Long](requestSize)

    var msgIndex = -1
    for( topicData <- request.data ) {
      for( partitionData <- topicData.partitionData ) {
        msgIndex += 1
        val partition = partitionData.getTranslatedPartition(topicData.topic, logManager.chooseRandomPartition)
        try {
          // TODO: need to handle ack's here!  Will probably move to another method.
          kafkaZookeeper.ensurePartitionOnThisBroker(topicData.topic, partition)
          val log = logManager.getOrCreateLog(topicData.topic, partition)
          log.append(partitionData.messages)
          offsets(msgIndex) = log.nextAppendOffset
          errors(msgIndex) = ErrorMapping.NoError.toShort
          trace(partitionData.messages.sizeInBytes + " bytes written to logs.")
        } catch {
          case e =>
            error("Error processing ProducerRequest on " + topicData.topic + ":" + partition, e)
            e match {
              case _: IOException =>
                fatal("Halting due to unrecoverable I/O error while handling producer request: " + e.getMessage, e)
                Runtime.getRuntime.halt(1)
              case _ =>
                errors(msgIndex) = ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]).toShort
                offsets(msgIndex) = -1
            }
        }
      }
    }
    new ProducerResponse(ProducerResponse.CurrentVersion, request.correlationId, errors, offsets)
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
      response = Right(log match { case Some(l) => l.read(offset, maxSize) case None => MessageSet.Empty })
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
    Some(new TopicMetadataSend(topicsMetadata))
  }
}
