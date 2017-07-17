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

package kafka.producer.async

import kafka.common._
import kafka.message.{ByteBufferMessageSet, Message, NoCompressionCodec}
import kafka.producer._
import kafka.serializer.Encoder
import kafka.utils._
import org.apache.kafka.common.errors.{LeaderNotAvailableException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.protocol.Errors

import scala.util.Random
import scala.collection.{Map, Seq}
import scala.collection.mutable.{ArrayBuffer, HashMap, Set}
import java.util.concurrent.atomic._

import kafka.api.{ProducerRequest, TopicMetadata}
import org.apache.kafka.common.utils.{Time, Utils}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.10.0.0")
class DefaultEventHandler[K,V](config: ProducerConfig,
                               private val partitioner: Partitioner,
                               private val encoder: Encoder[V],
                               private val keyEncoder: Encoder[K],
                               private val producerPool: ProducerPool,
                               private val topicPartitionInfos: HashMap[String, TopicMetadata] = new HashMap[String, TopicMetadata],
                               private val time: Time = Time.SYSTEM)
  extends EventHandler[K,V] with Logging {

  val isSync = ("sync" == config.producerType)

  val correlationId = new AtomicInteger(0)
  val brokerPartitionInfo = new BrokerPartitionInfo(config, producerPool, topicPartitionInfos)

  private val topicMetadataRefreshInterval = config.topicMetadataRefreshIntervalMs
  private var lastTopicMetadataRefreshTime = 0L
  private val topicMetadataToRefresh = Set.empty[String]
  private val sendPartitionPerTopicCache = HashMap.empty[String, Int]

  private val producerStats = ProducerStatsRegistry.getProducerStats(config.clientId)
  private val producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId)

  def handle(events: Seq[KeyedMessage[K,V]]) {
    val serializedData = serialize(events)
    serializedData.foreach {
      keyed =>
        val dataSize = keyed.message.payloadSize
        producerTopicStats.getProducerTopicStats(keyed.topic).byteRate.mark(dataSize)
        producerTopicStats.getProducerAllTopicsStats.byteRate.mark(dataSize)
    }
    var outstandingProduceRequests = serializedData
    var remainingRetries = config.messageSendMaxRetries + 1
    val correlationIdStart = correlationId.get()
    debug("Handling %d events".format(events.size))
    while (remainingRetries > 0 && outstandingProduceRequests.nonEmpty) {
      topicMetadataToRefresh ++= outstandingProduceRequests.map(_.topic)
      if (topicMetadataRefreshInterval >= 0 &&
          Time.SYSTEM.milliseconds - lastTopicMetadataRefreshTime > topicMetadataRefreshInterval) {
        CoreUtils.swallowError(brokerPartitionInfo.updateInfo(topicMetadataToRefresh.toSet, correlationId.getAndIncrement))
        sendPartitionPerTopicCache.clear()
        topicMetadataToRefresh.clear
        lastTopicMetadataRefreshTime = Time.SYSTEM.milliseconds
      }
      outstandingProduceRequests = dispatchSerializedData(outstandingProduceRequests)
      if (outstandingProduceRequests.nonEmpty) {
        info("Back off for %d ms before retrying send. Remaining retries = %d".format(config.retryBackoffMs, remainingRetries-1))
        // back off and update the topic metadata cache before attempting another send operation
        Thread.sleep(config.retryBackoffMs)
        // get topics of the outstanding produce requests and refresh metadata for those
        CoreUtils.swallowError(brokerPartitionInfo.updateInfo(outstandingProduceRequests.map(_.topic).toSet, correlationId.getAndIncrement))
        sendPartitionPerTopicCache.clear()
        remainingRetries -= 1
        producerStats.resendRate.mark()
      }
    }
    if(outstandingProduceRequests.nonEmpty) {
      producerStats.failedSendRate.mark()
      val correlationIdEnd = correlationId.get()
      error("Failed to send requests for topics %s with correlation ids in [%d,%d]"
        .format(outstandingProduceRequests.map(_.topic).toSet.mkString(","),
        correlationIdStart, correlationIdEnd-1))
      throw new FailedToSendMessageException("Failed to send messages after " + config.messageSendMaxRetries + " tries.", null)
    }
  }

  private def dispatchSerializedData(messages: Seq[KeyedMessage[K,Message]]): Seq[KeyedMessage[K, Message]] = {
    val partitionedDataOpt = partitionAndCollate(messages)
    partitionedDataOpt match {
      case Some(partitionedData) =>
        val failedProduceRequests = new ArrayBuffer[KeyedMessage[K, Message]]
        for ((brokerid, messagesPerBrokerMap) <- partitionedData) {
          if (logger.isTraceEnabled) {
            messagesPerBrokerMap.foreach(partitionAndEvent =>
              trace("Handling event for Topic: %s, Broker: %d, Partitions: %s".format(partitionAndEvent._1, brokerid, partitionAndEvent._2)))
          }
          val messageSetPerBrokerOpt = groupMessagesToSet(messagesPerBrokerMap)
          messageSetPerBrokerOpt match {
            case Some(messageSetPerBroker) =>
              val failedTopicPartitions = send(brokerid, messageSetPerBroker)
              failedTopicPartitions.foreach(topicPartition => {
                messagesPerBrokerMap.get(topicPartition).foreach(failedProduceRequests.appendAll)
              })
            case None => // failed to group messages
              messagesPerBrokerMap.values.foreach(m => failedProduceRequests.appendAll(m))
          }
        }
        failedProduceRequests
      case None => // failed to collate messages
        messages
    }
  }

  def serialize(events: Seq[KeyedMessage[K,V]]): Seq[KeyedMessage[K,Message]] = {
    val serializedMessages = new ArrayBuffer[KeyedMessage[K,Message]](events.size)
    events.foreach{e =>
      try {
        if(e.hasKey)
          serializedMessages += new KeyedMessage[K,Message](
            topic = e.topic,
            key = e.key,
            partKey = e.partKey,
            message = new Message(key = keyEncoder.toBytes(e.key),
                                  bytes = encoder.toBytes(e.message),
                                  timestamp = time.milliseconds,
                                  magicValue = Message.MagicValue_V1))
        else
          serializedMessages += new KeyedMessage[K,Message](
            topic = e.topic,
            key = e.key,
            partKey = e.partKey,
            message = new Message(bytes = encoder.toBytes(e.message),
                                  timestamp = time.milliseconds,
                                  magicValue = Message.MagicValue_V1))
      } catch {
        case t: Throwable =>
          producerStats.serializationErrorRate.mark()
          if (isSync) {
            throw t
          } else {
            // currently, if in async mode, we just log the serialization error. We need to revisit
            // this when doing kafka-496
            error("Error serializing message for topic %s".format(e.topic), t)
          }
      }
    }
    serializedMessages
  }

  def partitionAndCollate(messages: Seq[KeyedMessage[K,Message]]): Option[Map[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]] = {
    val ret = new HashMap[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]
    try {
      for (message <- messages) {
        val topicPartitionsList = getPartitionListForTopic(message)
        val partitionIndex = getPartition(message.topic, message.partitionKey, topicPartitionsList)
        val brokerPartition = topicPartitionsList(partitionIndex)

        // postpone the failure until the send operation, so that requests for other brokers are handled correctly
        val leaderBrokerId = brokerPartition.leaderBrokerIdOpt.getOrElse(-1)

        var dataPerBroker: HashMap[TopicAndPartition, Seq[KeyedMessage[K,Message]]] = null
        ret.get(leaderBrokerId) match {
          case Some(element) =>
            dataPerBroker = element.asInstanceOf[HashMap[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]
          case None =>
            dataPerBroker = new HashMap[TopicAndPartition, Seq[KeyedMessage[K,Message]]]
            ret.put(leaderBrokerId, dataPerBroker)
        }

        val topicAndPartition = TopicAndPartition(message.topic, brokerPartition.partitionId)
        var dataPerTopicPartition: ArrayBuffer[KeyedMessage[K,Message]] = null
        dataPerBroker.get(topicAndPartition) match {
          case Some(element) =>
            dataPerTopicPartition = element.asInstanceOf[ArrayBuffer[KeyedMessage[K,Message]]]
          case None =>
            dataPerTopicPartition = new ArrayBuffer[KeyedMessage[K,Message]]
            dataPerBroker.put(topicAndPartition, dataPerTopicPartition)
        }
        dataPerTopicPartition.append(message)
      }
      Some(ret)
    }catch {    // Swallow recoverable exceptions and return None so that they can be retried.
      case ute: UnknownTopicOrPartitionException => warn("Failed to collate messages by topic,partition due to: " + ute.getMessage); None
      case lnae: LeaderNotAvailableException => warn("Failed to collate messages by topic,partition due to: " + lnae.getMessage); None
      case oe: Throwable => error("Failed to collate messages by topic, partition due to: " + oe.getMessage); None
    }
  }

  private def getPartitionListForTopic(m: KeyedMessage[K,Message]): Seq[PartitionAndLeader] = {
    val topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(m.topic, correlationId.getAndIncrement)
    debug("Broker partitions registered for topic: %s are %s"
      .format(m.topic, topicPartitionsList.map(p => p.partitionId).mkString(",")))
    val totalNumPartitions = topicPartitionsList.length
    if(totalNumPartitions == 0)
      throw new NoBrokersForPartitionException("Partition key = " + m.key)
    topicPartitionsList
  }

  /**
   * Retrieves the partition id and throws an UnknownTopicOrPartitionException if
   * the value of partition is not between 0 and numPartitions-1
   * @param topic The topic
   * @param key the partition key
   * @param topicPartitionList the list of available partitions
   * @return the partition id
   */
  private def getPartition(topic: String, key: Any, topicPartitionList: Seq[PartitionAndLeader]): Int = {
    val numPartitions = topicPartitionList.size
    if(numPartitions <= 0)
      throw new UnknownTopicOrPartitionException("Topic " + topic + " doesn't exist")
    val partition =
      if(key == null) {
        // If the key is null, we don't really need a partitioner
        // So we look up in the send partition cache for the topic to decide the target partition
        val id = sendPartitionPerTopicCache.get(topic)
        id match {
          case Some(partitionId) =>
            // directly return the partitionId without checking availability of the leader,
            // since we want to postpone the failure until the send operation anyways
            partitionId
          case None =>
            val availablePartitions = topicPartitionList.filter(_.leaderBrokerIdOpt.isDefined)
            if (availablePartitions.isEmpty)
              throw new LeaderNotAvailableException("No leader for any partition in topic " + topic)
            val index = Utils.abs(Random.nextInt) % availablePartitions.size
            val partitionId = availablePartitions(index).partitionId
            sendPartitionPerTopicCache.put(topic, partitionId)
            partitionId
        }
      } else
        partitioner.partition(key, numPartitions)
    if(partition < 0 || partition >= numPartitions)
      throw new UnknownTopicOrPartitionException("Invalid partition id: " + partition + " for topic " + topic +
        "; Valid values are in the inclusive range of [0, " + (numPartitions-1) + "]")
    trace("Assigning message of topic %s and key %s to a selected partition %d".format(topic, if (key == null) "[none]" else key.toString, partition))
    partition
  }

  /**
   * Constructs and sends the produce request based on a map from (topic, partition) -> messages
   *
   * @param brokerId the broker that will receive the request
   * @param messagesPerTopic the messages as a map from (topic, partition) -> messages
   * @return the set (topic, partitions) messages which incurred an error sending or processing
   */
  private def send(brokerId: Int, messagesPerTopic: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]) = {
    if(brokerId < 0) {
      warn("Failed to send data since partitions %s don't have a leader".format(messagesPerTopic.keys.mkString(",")))
      messagesPerTopic.keys.toSeq
    } else if(messagesPerTopic.nonEmpty) {
      val currentCorrelationId = correlationId.getAndIncrement
      val producerRequest = new ProducerRequest(currentCorrelationId, config.clientId, config.requestRequiredAcks,
        config.requestTimeoutMs, messagesPerTopic)
      var failedTopicPartitions = Seq.empty[TopicAndPartition]
      try {
        val syncProducer = producerPool.getProducer(brokerId)
        debug("Producer sending messages with correlation id %d for topics %s to broker %d on %s:%d"
          .format(currentCorrelationId, messagesPerTopic.keySet.mkString(","), brokerId, syncProducer.config.host, syncProducer.config.port))
        val response = syncProducer.send(producerRequest)
        debug("Producer sent messages with correlation id %d for topics %s to broker %d on %s:%d"
          .format(currentCorrelationId, messagesPerTopic.keySet.mkString(","), brokerId, syncProducer.config.host, syncProducer.config.port))
        if(response != null) {
          if (response.status.size != producerRequest.data.size)
            throw new KafkaException("Incomplete response (%s) for producer request (%s)".format(response, producerRequest))
          if (logger.isTraceEnabled) {
            val successfullySentData = response.status.filter(_._2.error == Errors.NONE)
            successfullySentData.foreach(m => messagesPerTopic(m._1).foreach(message =>
              trace("Successfully sent message: %s".format(if(message.message.isNull) null else message.message.toString()))))
          }
          val failedPartitionsAndStatus = response.status.filter(_._2.error != Errors.NONE).toSeq
          failedTopicPartitions = failedPartitionsAndStatus.map(partitionStatus => partitionStatus._1)
          if(failedTopicPartitions.nonEmpty) {
            val errorString = failedPartitionsAndStatus
              .sortWith((p1, p2) => p1._1.topic.compareTo(p2._1.topic) < 0 ||
                                    (p1._1.topic.compareTo(p2._1.topic) == 0 && p1._1.partition < p2._1.partition))
              .map{
                case(topicAndPartition, status) =>
                  topicAndPartition.toString + ": " + status.error.exceptionName
              }.mkString(",")
            warn("Produce request with correlation id %d failed due to %s".format(currentCorrelationId, errorString))
          }
          failedTopicPartitions
        } else {
          Seq.empty[TopicAndPartition]
        }
      } catch {
        case t: Throwable =>
          warn("Failed to send producer request with correlation id %d to broker %d with data for partitions %s"
            .format(currentCorrelationId, brokerId, messagesPerTopic.keys.mkString(",")), t)
          messagesPerTopic.keys.toSeq
      }
    } else {
      List.empty
    }
  }

  private def groupMessagesToSet(messagesPerTopicAndPartition: collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K, Message]]]) = {
    /** enforce the compressed.topics config here.
      * If the compression codec is anything other than NoCompressionCodec,
      * Enable compression only for specified topics if any
      * If the list of compressed topics is empty, then enable the specified compression codec for all topics
      * If the compression codec is NoCompressionCodec, compression is disabled for all topics
      */
    try {
      val messagesPerTopicPartition = messagesPerTopicAndPartition.map { case (topicAndPartition, messages) =>
        val rawMessages = messages.map(_.message)
        (topicAndPartition,
          config.compressionCodec match {
            case NoCompressionCodec =>
              debug("Sending %d messages with no compression to %s".format(messages.size, topicAndPartition))
              new ByteBufferMessageSet(NoCompressionCodec, rawMessages: _*)
            case _ =>
              config.compressedTopics.size match {
                case 0 =>
                  debug("Sending %d messages with compression codec %d to %s"
                    .format(messages.size, config.compressionCodec.codec, topicAndPartition))
                  new ByteBufferMessageSet(config.compressionCodec, rawMessages: _*)
                case _ =>
                  if (config.compressedTopics.contains(topicAndPartition.topic)) {
                    debug("Sending %d messages with compression codec %d to %s"
                      .format(messages.size, config.compressionCodec.codec, topicAndPartition))
                    new ByteBufferMessageSet(config.compressionCodec, rawMessages: _*)
                  }
                  else {
                    debug("Sending %d messages to %s with no compression as it is not in compressed.topics - %s"
                      .format(messages.size, topicAndPartition, config.compressedTopics.toString))
                    new ByteBufferMessageSet(NoCompressionCodec, rawMessages: _*)
                  }
              }
          }
          )
      }
      Some(messagesPerTopicPartition)
    } catch {
      case t: Throwable => error("Failed to group messages", t); None
    }
  }

  def close() {
    if (producerPool != null)
      producerPool.close
  }
}
