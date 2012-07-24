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
package kafka.producer

import collection.mutable.HashMap
import kafka.api.{TopicMetadataRequest, TopicMetadata}
import kafka.common.KafkaException
import kafka.utils.Logging
import kafka.cluster.{Replica, Partition}
import kafka.common.{LeaderNotAvailableException, ErrorMapping, UnknownTopicException}

class BrokerPartitionInfo(producerPool: ProducerPool) extends Logging {
  val topicPartitionInfo = new HashMap[String, TopicMetadata]()
  val zkClient = producerPool.getZkClient

  /**
   * Return a sequence of (brokerId, numPartitions).
   * @param topic the topic for which this information is to be returned
   * @return a sequence of (brokerId, numPartitions). Returns a zero-length
   * sequence if no brokers are available.
   */
  def getBrokerPartitionInfo(topic: String): Seq[Partition] = {
    debug("Getting broker partition info for topic %s".format(topic))
    // check if the cache has metadata for this topic
    val topicMetadata = topicPartitionInfo.get(topic)
    val metadata: TopicMetadata =
      topicMetadata match {
        case Some(m) => m
        case None =>
          // refresh the topic metadata cache
          updateInfo(List(topic))
          val topicMetadata = topicPartitionInfo.get(topic)
          topicMetadata match {
            case Some(m) => m
            case None => throw new KafkaException("Failed to fetch topic metadata for topic: " + topic)
          }
      }
    val partitionMetadata = metadata.partitionsMetadata
    partitionMetadata.map { m =>
      val partition = new Partition(topic, m.partitionId)
      m.leader match {
        case Some(leader) =>
          val leaderReplica = new Replica(leader.id, partition, topic)
          partition.leaderId(Some(leaderReplica.brokerId))
          debug("Topic %s partition %d has leader %d".format(topic, m.partitionId, leader.id))
          partition
        case None =>
          debug("Topic %s partition %d does not have a leader yet".format(topic, m.partitionId))
          partition
      }
    }.sortWith((s, t) => s.partitionId < t.partitionId)
  }

  /**
   * It updates the cache by issuing a get topic metadata request to a random broker.
   * @param topic the topic for which the metadata is to be fetched
   */
  def updateInfo(topics: Seq[String] = Seq.empty[String]) = {
    val producer = producerPool.getAnyProducer
    val topicList = if(topics.size > 0) topics else topicPartitionInfo.keySet.toList
    topicList.foreach { topic =>
      info("Fetching metadata for topic %s".format(topic))
      val topicMetadataRequest = new TopicMetadataRequest(List(topic))
      var topicMetaDataResponse: Seq[TopicMetadata] = Nil
      try {
        topicMetaDataResponse = producer.send(topicMetadataRequest)
        // throw topic specific exception
        topicMetaDataResponse.foreach(metadata => ErrorMapping.maybeThrowException(metadata.errorCode))
        // throw partition specific exception
        topicMetaDataResponse.foreach(metadata =>
          metadata.partitionsMetadata.foreach(partitionMetadata => ErrorMapping.maybeThrowException(partitionMetadata.errorCode)))
      }catch {
        case te: UnknownTopicException => throw te
        case e: LeaderNotAvailableException => throw e
        case oe => warn("Ignoring non leader related error while fetching metadata", oe)  // swallow non leader related errors
      }
      val topicMetadata:Option[TopicMetadata] = if(topicMetaDataResponse.size > 0) Some(topicMetaDataResponse.head) else None
      topicMetadata match {
        case Some(metadata) =>
          info("Fetched metadata for topics %s".format(topic))
          topicMetadata.foreach(metadata => trace("Metadata for topic %s is %s".format(metadata.topic, metadata.toString)))
          topicPartitionInfo += (topic -> metadata)
        case None =>
      }
    }
  }
}
