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

import kafka.api.{ProducerRequest, TopicData, PartitionData}
import kafka.cluster.Partition
import kafka.message.{Message, NoCompressionCodec, ByteBufferMessageSet}
import kafka.producer._
import kafka.serializer.Encoder
import scala.collection.Map
import scala.collection.mutable.{ListBuffer, HashMap}
import kafka.utils.{Utils, Logging}
import kafka.common.{FailedToSendMessageException, NoLeaderForPartitionException, InvalidPartitionException, NoBrokersForPartitionException}

class DefaultEventHandler[K,V](config: ProducerConfig,                               // this api is for testing
                               private val partitioner: Partitioner[K],              // use the other constructor
                               private val encoder: Encoder[V],
                               private val producerPool: ProducerPool)
  extends EventHandler[K,V] with Logging {

  val brokerPartitionInfo = new BrokerPartitionInfo(producerPool)

  // add producers to the producer pool
  producerPool.addProducers(config)

  private val lock = new Object()

  def handle(events: Seq[ProducerData[K,V]]) {
    lock synchronized {
      val serializedData = serialize(events)
      var outstandingProduceRequests = serializedData
      var remainingRetries = config.producerRetries
      while (remainingRetries > 0 && outstandingProduceRequests.size > 0) {
        outstandingProduceRequests = dispatchSerializedData(outstandingProduceRequests)
        if (outstandingProduceRequests.size > 0)  {
          // back off and update the topic metadata cache before attempting another send operation
          Thread.sleep(config.producerRetryBackoffMs)
          Utils.swallowError(brokerPartitionInfo.updateInfo())
          remainingRetries -= 1
        }
      }
      if(outstandingProduceRequests.size > 0) {
        error("Failed to send the following reqeusts: " + outstandingProduceRequests)
        throw new FailedToSendMessageException("Failed to send messages after " + config.producerRetries + " tries.", null)
      }
    }
  }

  private def dispatchSerializedData(messages: Seq[ProducerData[K,Message]]): Seq[ProducerData[K, Message]] = {
    val partitionedData = partitionAndCollate(messages)
    val failedProduceRequests = new ListBuffer[ProducerData[K,Message]]
    try {
      for ((brokerid, eventsPerBrokerMap) <- partitionedData) {
        if (logger.isTraceEnabled)
          eventsPerBrokerMap.foreach(partitionAndEvent => trace("Handling event for Topic: %s, Broker: %d, Partitions: %s"
            .format(partitionAndEvent._1, brokerid, partitionAndEvent._2)))
        val messageSetPerBroker = groupMessagesToSet(eventsPerBrokerMap)

        if((brokerid < 0) || (!send(brokerid, messageSetPerBroker)))
          failedProduceRequests.appendAll(eventsPerBrokerMap.map(r => r._2).flatten)
      }
    } catch {
      case t: Throwable => error("Failed to send messages")
    }
    failedProduceRequests
  }

  def serialize(events: Seq[ProducerData[K,V]]): Seq[ProducerData[K,Message]] = {
    events.map(e => new ProducerData[K,Message](e.getTopic, e.getKey, e.getData.map(m => encoder.toMessage(m))))
  }

  def partitionAndCollate(events: Seq[ProducerData[K,Message]]): Map[Int, Map[(String, Int), Seq[ProducerData[K,Message]]]] = {
    val ret = new HashMap[Int, Map[(String, Int), Seq[ProducerData[K,Message]]]]
    for (event <- events) {
      val topicPartitionsList = getPartitionListForTopic(event)
      val totalNumPartitions = topicPartitionsList.length

      val partitionIndex = getPartition(event.getKey, totalNumPartitions)
      val brokerPartition = topicPartitionsList(partitionIndex)

      val leaderBrokerId = brokerPartition.leader match {
        case Some(leader) => leader.brokerId
        case None => -1
        // postpone the failure until the send operation, so that requests for other brokers are handled correctly
      }

      var dataPerBroker: HashMap[(String, Int), Seq[ProducerData[K,Message]]] = null
      ret.get(leaderBrokerId) match {
        case Some(element) =>
          dataPerBroker = element.asInstanceOf[HashMap[(String, Int), Seq[ProducerData[K,Message]]]]
        case None =>
          dataPerBroker = new HashMap[(String, Int), Seq[ProducerData[K,Message]]]
          ret.put(leaderBrokerId, dataPerBroker)
      }

      val topicAndPartition = (event.getTopic, brokerPartition.partId)
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
    ret
  }

  private def getPartitionListForTopic(pd: ProducerData[K,Message]): Seq[Partition] = {
    debug("Getting the number of broker partitions registered for topic: " + pd.getTopic)
    val topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(pd.getTopic)
    debug("Broker partitions registered for topic: %s are %s"
      .format(pd.getTopic, topicPartitionsList.map(p => p.partId).mkString(",")))
    val totalNumPartitions = topicPartitionsList.length
    if(totalNumPartitions == 0) throw new NoBrokersForPartitionException("Partition = " + pd.getKey)
    topicPartitionsList
  }

  /**
   * Retrieves the partition id and throws an InvalidPartitionException if
   * the value of partition is not between 0 and numPartitions-1
   * @param key the partition key
   * @param numPartitions the total number of available partitions
   * @return the partition id
   */
  private def getPartition(key: K, numPartitions: Int): Int = {
    if(numPartitions <= 0)
      throw new InvalidPartitionException("Invalid number of partitions: " + numPartitions +
              "\n Valid values are > 0")
    val partition = if(key == null) Utils.getNextRandomInt(numPartitions)
                    else partitioner.partition(key, numPartitions)
    if(partition < 0 || partition >= numPartitions)
      throw new InvalidPartitionException("Invalid partition id : " + partition +
              "\n Valid values are in the range inclusive [0, " + (numPartitions-1) + "]")
    partition
  }

  /**
   * Constructs and sends the produce request based on a map from (topic, partition) -> messages
   *
   * @param brokerId the broker that will receive the request
   * @param messagesPerTopic the messages as a map from (topic, partition) -> messages
   */
  private def send(brokerId: Int, messagesPerTopic: Map[(String, Int), ByteBufferMessageSet]): Boolean = {
    try {
      if(brokerId < 0)
        throw new NoLeaderForPartitionException("No leader for some partition(s) on broker %d".format(brokerId))
      if(messagesPerTopic.size > 0) {
        val topics = new HashMap[String, ListBuffer[PartitionData]]()
        for(((topicName, partitionId), messagesSet) <- messagesPerTopic) {
          topics.get(topicName) match {
            case Some(x) => trace("found " + topicName)
            case None => topics += topicName -> new ListBuffer[PartitionData]() //create a new listbuffer for this topic
          }
          topics(topicName).append(new PartitionData(partitionId, messagesSet))
        }
        val topicData = topics.map(kv => new TopicData(kv._1, kv._2.toArray))
        val producerRequest = new ProducerRequest(config.correlationId, config.clientId, config.requiredAcks, config.ackTimeout, topicData.toArray)
        val syncProducer = producerPool.getProducer(brokerId)
        val response = syncProducer.send(producerRequest)
        // TODO: possibly send response to response callback handler
        trace("kafka producer sent messages for topics %s to broker %d on %s:%d"
          .format(messagesPerTopic, brokerId, syncProducer.config.host, syncProducer.config.port))
      }
      true
    }catch {
      case t: Throwable => false
    }
  }

  private def groupMessagesToSet(eventsPerTopicAndPartition: Map[(String,Int), Seq[ProducerData[K,Message]]]): Map[(String, Int), ByteBufferMessageSet] = {
    /** enforce the compressed.topics config here.
     *  If the compression codec is anything other than NoCompressionCodec,
     *    Enable compression only for specified topics if any
     *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
     *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
     */

    val messagesPerTopicPartition = eventsPerTopicAndPartition.map { e =>
      {
        val topicAndPartition = e._1
        val produceData = e._2
        val messages = new ListBuffer[Message]
        produceData.map(p => messages.appendAll(p.getData))

        ( topicAndPartition,
          config.compressionCodec match {
            case NoCompressionCodec =>
              trace("Sending %d messages with no compression to topic %s on partition %d"
                  .format(messages.size, topicAndPartition._1, topicAndPartition._2))
              new ByteBufferMessageSet(NoCompressionCodec, messages: _*)
            case _ =>
              config.compressedTopics.size match {
                case 0 =>
                  trace("Sending %d messages with compression codec %d to topic %s on partition %d"
                      .format(messages.size, config.compressionCodec.codec, topicAndPartition._1, topicAndPartition._2))
                  new ByteBufferMessageSet(config.compressionCodec, messages: _*)
                case _ =>
                  if(config.compressedTopics.contains(topicAndPartition._1)) {
                    trace("Sending %d messages with compression codec %d to topic %s on partition %d"
                        .format(messages.size, config.compressionCodec.codec, topicAndPartition._1, topicAndPartition._2))
                    new ByteBufferMessageSet(config.compressionCodec, messages: _*)
                  }
                  else {
                    trace("Sending %d messages to topic %s and partition %d with no compression as %s is not in compressed.topics - %s"
                        .format(messages.size, topicAndPartition._1, topicAndPartition._2, topicAndPartition._1,
                        config.compressedTopics.toString))
                    new ByteBufferMessageSet(NoCompressionCodec, messages: _*)
                  }
              }
          }
        )
      }
    }
    messagesPerTopicPartition
  }

  def close() {
    if (producerPool != null)
      producerPool.close    
  }
}
