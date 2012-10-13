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
import scala.collection.mutable.{ListBuffer, HashMap}
import kafka.api.{TopicMetadata, ProducerRequest}


class DefaultEventHandler[K,V](config: ProducerConfig,
                               private val partitioner: Partitioner[K],
                               private val encoder: Encoder[V],
                               private val producerPool: ProducerPool,
                               private val topicPartitionInfos: HashMap[String, TopicMetadata] = new HashMap[String, TopicMetadata])
  extends EventHandler[K,V] with Logging {
  val isSync = ("sync" == config.producerType)

  val brokerPartitionInfo = new BrokerPartitionInfo(config, producerPool, topicPartitionInfos)

  private val lock = new Object()

  def handle(events: Seq[ProducerData[K,V]]) {
    lock synchronized {
      val serializedData = serialize(events)
      serializedData.foreach{
        pd => val dataSize = pd.data.foldLeft(0)(_ + _.payloadSize)
              ProducerTopicStat.getProducerTopicStat(pd.topic).byteRate.mark(dataSize)
              ProducerTopicStat.getProducerAllTopicStat.byteRate.mark(dataSize)
      }
      var outstandingProduceRequests = serializedData
      var remainingRetries = config.producerRetries + 1
      while (remainingRetries > 0 && outstandingProduceRequests.size > 0) {
        outstandingProduceRequests = dispatchSerializedData(outstandingProduceRequests)
        if (outstandingProduceRequests.size > 0)  {
          // back off and update the topic metadata cache before attempting another send operation
          Thread.sleep(config.producerRetryBackoffMs)
          // get topics of the outstanding produce requests and refresh metadata for those
          Utils.swallowError(brokerPartitionInfo.updateInfo(outstandingProduceRequests.map(_.getTopic).toSet))
          remainingRetries -= 1
          ProducerStats.resendRate.mark()
        }
      }
      if(outstandingProduceRequests.size > 0) {
        ProducerStats.failedSendRate.mark()
        error("Failed to send the following requests: " + outstandingProduceRequests)
        throw new FailedToSendMessageException("Failed to send messages after " + config.producerRetries + " tries.", null)
      }
    }
  }

  private def dispatchSerializedData(messages: Seq[ProducerData[K,Message]]): Seq[ProducerData[K, Message]] = {
    val partitionedDataOpt = partitionAndCollate(messages)
    partitionedDataOpt match {
      case Some(partitionedData) =>
        val failedProduceRequests = new ListBuffer[ProducerData[K,Message]]
        try {
          for ((brokerid, eventsPerBrokerMap) <- partitionedData) {
            if (logger.isTraceEnabled)
              eventsPerBrokerMap.foreach(partitionAndEvent => trace("Handling event for Topic: %s, Broker: %d, Partitions: %s"
                .format(partitionAndEvent._1, brokerid, partitionAndEvent._2)))
            val messageSetPerBroker = groupMessagesToSet(eventsPerBrokerMap)

            val failedTopicPartitions = send(brokerid, messageSetPerBroker)
            failedTopicPartitions.foreach(topicPartition => {
              eventsPerBrokerMap.get(topicPartition) match {
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

  def serialize(events: Seq[ProducerData[K,V]]): Seq[ProducerData[K,Message]] = {
    val serializedProducerData = new ListBuffer[ProducerData[K,Message]]
    events.foreach {e =>
      val serializedMessages = new ListBuffer[Message]
      for (d <- e.getData) {
        try {
          serializedMessages += encoder.toMessage(d)
        } catch {
          case t =>
            ProducerStats.serializationErrorRate.mark()
            if (isSync)
              throw t
            else {
              // currently, if in async mode, we just log the serialization error. We need to revisit
              // this when doing kafka-496
              error("Error serializing message ", t)
            }
        }
      }
      if (serializedMessages.size > 0)
        serializedProducerData += new ProducerData[K,Message](e.getTopic, e.getKey, serializedMessages)
    }
    serializedProducerData
  }

  def partitionAndCollate(events: Seq[ProducerData[K,Message]]): Option[Map[Int, Map[TopicAndPartition, Seq[ProducerData[K,Message]]]]] = {
    val ret = new HashMap[Int, Map[TopicAndPartition, Seq[ProducerData[K,Message]]]]
    try {
      for (event <- events) {
        val topicPartitionsList = getPartitionListForTopic(event)
        val totalNumPartitions = topicPartitionsList.length

        val partitionIndex = getPartition(event.getKey, totalNumPartitions)
        val brokerPartition = topicPartitionsList(partitionIndex)

        // postpone the failure until the send operation, so that requests for other brokers are handled correctly
        val leaderBrokerId = brokerPartition.leaderBrokerIdOpt.getOrElse(-1)

        var dataPerBroker: HashMap[TopicAndPartition, Seq[ProducerData[K,Message]]] = null
        ret.get(leaderBrokerId) match {
          case Some(element) =>
            dataPerBroker = element.asInstanceOf[HashMap[TopicAndPartition, Seq[ProducerData[K,Message]]]]
          case None =>
            dataPerBroker = new HashMap[TopicAndPartition, Seq[ProducerData[K,Message]]]
            ret.put(leaderBrokerId, dataPerBroker)
        }

        val topicAndPartition = TopicAndPartition(event.getTopic, brokerPartition.partitionId)
        var dataPerTopicPartition: ListBuffer[ProducerData[K,Message]] = null
        dataPerBroker.get(topicAndPartition) match {
          case Some(element) =>
            dataPerTopicPartition = element.asInstanceOf[ListBuffer[ProducerData[K,Message]]]
          case None =>
            dataPerTopicPartition = new ListBuffer[ProducerData[K,Message]]
            dataPerBroker.put(topicAndPartition, dataPerTopicPartition)
        }
        dataPerTopicPartition.append(event)
      }
      Some(ret)
    }catch {    // Swallow recoverable exceptions and return None so that they can be retried.
      case ute: UnknownTopicException => warn("Failed to collate messages by topic,partition due to", ute); None
      case lnae: LeaderNotAvailableException => warn("Failed to collate messages by topic,partition due to", lnae); None
      case oe => error("Failed to collate messages by topic, partition due to", oe); throw oe
    }
  }

  private def getPartitionListForTopic(pd: ProducerData[K,Message]): Seq[PartitionAndLeader] = {
    debug("Getting the number of broker partitions registered for topic: " + pd.getTopic)
    val topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(pd.getTopic)
    debug("Broker partitions registered for topic: %s are %s"
      .format(pd.getTopic, topicPartitionsList.map(p => p.partitionId).mkString(",")))
    val totalNumPartitions = topicPartitionsList.length
    if(totalNumPartitions == 0) throw new NoBrokersForPartitionException("Partition = " + pd.getKey)
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
    val partition = if(key == null) Utils.getNextRandomInt(numPartitions)
    else partitioner.partition(key, numPartitions)
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
      val topicPartitionDataPairs = messagesPerTopic.toSeq.map {
        case (topicAndPartition, messages) =>
          (topicAndPartition, messages)
      }
      val producerRequest = new ProducerRequest(config.correlationId, config.clientId, config.requiredAcks,
        config.requestTimeoutMs, Map(topicPartitionDataPairs:_*))
      try {
        val syncProducer = producerPool.getProducer(brokerId)
        val response = syncProducer.send(producerRequest)
        trace("Producer sent messages for topics %s to broker %d on %s:%d"
          .format(messagesPerTopic, brokerId, syncProducer.config.host, syncProducer.config.port))
        if (response.status.size != producerRequest.data.size)
          throw new KafkaException("Incomplete response (%s) for producer request (%s)"
                                           .format(response, producerRequest))
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

  private def groupMessagesToSet(eventsPerTopicAndPartition: Map[TopicAndPartition, Seq[ProducerData[K,Message]]]) = {
    /** enforce the compressed.topics config here.
     *  If the compression codec is anything other than NoCompressionCodec,
     *    Enable compression only for specified topics if any
     *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
     *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
     */

    val messagesPerTopicPartition = eventsPerTopicAndPartition.map { e =>
      val topicAndPartition = e._1
      val produceData = e._2
      val messages = new ListBuffer[Message]
      produceData.map(p => messages.appendAll(p.getData))
      ( topicAndPartition,
        config.compressionCodec match {
          case NoCompressionCodec =>
            trace("Sending %d messages with no compression to %s".format(messages.size, topicAndPartition))
            new ByteBufferMessageSet(NoCompressionCodec, messages: _*)
          case _ =>
            config.compressedTopics.size match {
              case 0 =>
                trace("Sending %d messages with compression codec %d to %s"
                  .format(messages.size, config.compressionCodec.codec, topicAndPartition))
                new ByteBufferMessageSet(config.compressionCodec, messages: _*)
              case _ =>
                if(config.compressedTopics.contains(topicAndPartition.topic)) {
                  trace("Sending %d messages with compression codec %d to %s"
                    .format(messages.size, config.compressionCodec.codec, topicAndPartition))
                  new ByteBufferMessageSet(config.compressionCodec, messages: _*)
                }
                else {
                  trace("Sending %d messages to %s with no compression as it is not in compressed.topics - %s"
                    .format(messages.size, topicAndPartition, config.compressedTopics.toString))
                  new ByteBufferMessageSet(NoCompressionCodec, messages: _*)
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
