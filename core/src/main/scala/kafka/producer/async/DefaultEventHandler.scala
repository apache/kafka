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
import kafka.message.{NoCompressionCodec, Message, ByteBufferMessageSet}
import kafka.producer._
import kafka.serializer.Encoder
import kafka.utils.{Utils, Logging}
import scala.collection.{Seq, Map}
import scala.collection.mutable.{ArrayBuffer, HashMap}
import java.util.concurrent.atomic._
import kafka.api.{TopicMetadata, ProducerRequest}


class DefaultEventHandler[K,V](config: ProducerConfig,
                               private val partitioner: Partitioner[K],
                               private val encoder: Encoder[V],
                               private val keyEncoder: Encoder[K],
                               private val producerPool: ProducerPool,
                               private val topicPartitionInfos: HashMap[String, TopicMetadata] = new HashMap[String, TopicMetadata],
                               private val producerStats: ProducerStats,
                               private val producerTopicStats: ProducerTopicStats)
  extends EventHandler[K,V] with Logging {
  val isSync = ("sync" == config.producerType)

  val partitionCounter = new AtomicInteger(0)
  val correlationCounter = new AtomicInteger(0)
  val brokerPartitionInfo = new BrokerPartitionInfo(config, producerPool, topicPartitionInfos)

  private val lock = new Object()

  def handle(events: Seq[KeyedMessage[K,V]]) {
    lock synchronized {
      val serializedData = serialize(events)
      serializedData.foreach{
        keyed =>
          val dataSize = keyed.message.payloadSize
          producerTopicStats.getProducerTopicStats(keyed.topic).byteRate.mark(dataSize)
          producerTopicStats.getProducerAllTopicStats.byteRate.mark(dataSize)
      }
      var outstandingProduceRequests = serializedData
      var remainingRetries = config.producerRetries + 1
      while (remainingRetries > 0 && outstandingProduceRequests.size > 0) {
        outstandingProduceRequests = dispatchSerializedData(outstandingProduceRequests)
        if (outstandingProduceRequests.size > 0)  {
          // back off and update the topic metadata cache before attempting another send operation
          Thread.sleep(config.producerRetryBackoffMs)
          // get topics of the outstanding produce requests and refresh metadata for those
          Utils.swallowError(brokerPartitionInfo.updateInfo(outstandingProduceRequests.map(_.topic).toSet))
          remainingRetries -= 1
          producerStats.resendRate.mark()
        }
      }
      if(outstandingProduceRequests.size > 0) {
        producerStats.failedSendRate.mark()
        error("Failed to send the following requests: " + outstandingProduceRequests)
        throw new FailedToSendMessageException("Failed to send messages after " + config.producerRetries + " tries.", null)
      }
    }
  }

  private def dispatchSerializedData(messages: Seq[KeyedMessage[K,Message]]): Seq[KeyedMessage[K, Message]] = {
    val partitionedDataOpt = partitionAndCollate(messages)
    partitionedDataOpt match {
      case Some(partitionedData) =>
        val failedProduceRequests = new ArrayBuffer[KeyedMessage[K,Message]]
        try {
          for ((brokerid, messagesPerBrokerMap) <- partitionedData) {
            if (logger.isTraceEnabled)
              messagesPerBrokerMap.foreach(partitionAndEvent =>
                trace("Handling event for Topic: %s, Broker: %d, Partitions: %s".format(partitionAndEvent._1, brokerid, partitionAndEvent._2)))
            val messageSetPerBroker = groupMessagesToSet(messagesPerBrokerMap)

            val failedTopicPartitions = send(brokerid, messageSetPerBroker)
            failedTopicPartitions.foreach(topicPartition => {
              messagesPerBrokerMap.get(topicPartition) match {
                case Some(data) => failedProduceRequests.appendAll(data)
                case None => // nothing
              }
            })
          }
        } catch {
          case t: Throwable => error("Failed to send messages", t)
        }
        failedProduceRequests
      case None => // all produce requests failed
        messages
    }
  }

  def serialize(events: Seq[KeyedMessage[K,V]]): Seq[KeyedMessage[K,Message]] = {
    val serializedMessages = new ArrayBuffer[KeyedMessage[K,Message]](events.size)
    events.map{e =>
      try {
        if(e.hasKey)
          serializedMessages += KeyedMessage[K,Message](topic = e.topic, key = e.key, message = new Message(key = keyEncoder.toBytes(e.key), bytes = encoder.toBytes(e.message)))
        else
          serializedMessages += KeyedMessage[K,Message](topic = e.topic, key = null.asInstanceOf[K], message = new Message(bytes = encoder.toBytes(e.message)))
      } catch {
        case t =>
          producerStats.serializationErrorRate.mark()
          if (isSync) {
            throw t
          } else {
            // currently, if in async mode, we just log the serialization error. We need to revisit
            // this when doing kafka-496
            error("Error serializing message ", t)
          }
      }
    }
    serializedMessages
  }

  def partitionAndCollate(messages: Seq[KeyedMessage[K,Message]]): Option[Map[Int, Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]] = {
    val ret = new HashMap[Int, Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]
    try {
      for (message <- messages) {
        val topicPartitionsList = getPartitionListForTopic(message)
        val totalNumPartitions = topicPartitionsList.length

        val partitionIndex = getPartition(message.key, totalNumPartitions)
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
      case ute: UnknownTopicException => warn("Failed to collate messages by topic,partition due to", ute); None
      case lnae: LeaderNotAvailableException => warn("Failed to collate messages by topic,partition due to", lnae); None
      case oe => error("Failed to collate messages by topic, partition due to", oe); throw oe
    }
  }

  private def getPartitionListForTopic(m: KeyedMessage[K,Message]): Seq[PartitionAndLeader] = {
    debug("Getting the number of broker partitions registered for topic: " + m.topic)
    val topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(m.topic)
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
   * @param key the partition key
   * @param numPartitions the total number of available partitions
   * @return the partition id
   */
  private def getPartition(key: K, numPartitions: Int): Int = {
    if(numPartitions <= 0)
      throw new UnknownTopicOrPartitionException("Invalid number of partitions: " + numPartitions +
        "\n Valid values are > 0")
    val partition =
      if(key == null)
        Utils.abs(partitionCounter.getAndIncrement()) % numPartitions
      else
        partitioner.partition(key, numPartitions)
    if(partition < 0 || partition >= numPartitions)
      throw new UnknownTopicOrPartitionException("Invalid partition id : " + partition +
        "\n Valid values are in the range inclusive [0, " + (numPartitions-1) + "]")
    partition
  }

  /**
   * Constructs and sends the produce request based on a map from (topic, partition) -> messages
   *
   * @param brokerId the broker that will receive the request
   * @param messagesPerTopic the messages as a map from (topic, partition) -> messages
   * @return the set (topic, partitions) messages which incurred an error sending or processing
   */
  private def send(brokerId: Int, messagesPerTopic: Map[TopicAndPartition, ByteBufferMessageSet]) = {
    if(brokerId < 0) {
      warn("Failed to send to broker %d with data %s".format(brokerId, messagesPerTopic))
      messagesPerTopic.keys.toSeq
    } else if(messagesPerTopic.size > 0) {
      val producerRequest = new ProducerRequest(correlationCounter.getAndIncrement(), config.clientId, config.requiredAcks,
        config.requestTimeoutMs, messagesPerTopic)
      try {
        val syncProducer = producerPool.getProducer(brokerId)
        val response = syncProducer.send(producerRequest)
        debug("Producer sent messages for topics %s to broker %d on %s:%d"
          .format(messagesPerTopic, brokerId, syncProducer.config.host, syncProducer.config.port))
        if (response.status.size != producerRequest.data.size)
          throw new KafkaException("Incomplete response (%s) for producer request (%s)"
            .format(response, producerRequest))
        if (logger.isTraceEnabled) {
          val successfullySentData = response.status.filter(_._2.error == ErrorMapping.NoError)
          successfullySentData.foreach(m => messagesPerTopic(m._1).foreach(message =>
            trace("Successfully sent message: %s".format(Utils.readString(message.message.payload)))))
        }
        response.status.filter(_._2.error != ErrorMapping.NoError).toSeq
          .map(partitionStatus => partitionStatus._1)
      } catch {
        case t: Throwable =>
          warn("failed to send to broker %d with data %s".format(brokerId, messagesPerTopic), t)
          messagesPerTopic.keys.toSeq
      }
    } else {
      List.empty
    }
  }

  private def groupMessagesToSet(messagesPerTopicAndPartition: Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]) = {
    /** enforce the compressed.topics config here.
      *  If the compression codec is anything other than NoCompressionCodec,
      *    Enable compression only for specified topics if any
      *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
      *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
      */

    val messagesPerTopicPartition = messagesPerTopicAndPartition.map { case (topicAndPartition, messages) =>
      val rawMessages = messages.map(_.message)
      ( topicAndPartition,
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
                if(config.compressedTopics.contains(topicAndPartition.topic)) {
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
    messagesPerTopicPartition
  }

  def close() {
    if (producerPool != null)
      producerPool.close
  }
}
