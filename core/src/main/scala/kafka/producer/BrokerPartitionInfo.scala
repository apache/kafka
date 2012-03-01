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

import kafka.cluster.{Broker, Partition}
import collection.mutable.HashMap
import kafka.api.{TopicMetadataRequest, TopicMetadata}
import java.lang.IllegalStateException
import kafka.common.NoLeaderForPartitionException
import kafka.utils.Logging

class BrokerPartitionInfo(producerPool: ProducerPool) extends Logging {
  val topicPartitionInfo = new HashMap[String, TopicMetadata]()
  val zkClient = producerPool.getZkClient

  /**
   * Return a sequence of (brokerId, numPartitions).
   * @param topic the topic for which this information is to be returned
   * @return a sequence of (brokerId, numPartitions). Returns a zero-length
   * sequence if no brokers are available.
   */  
  def getBrokerPartitionInfo(topic: String): Seq[(Partition, Broker)] = {
    // check if the cache has metadata for this topic
    val topicMetadata = topicPartitionInfo.get(topic)
    val metadata: TopicMetadata =
    topicMetadata match {
      case Some(m) => m
      case None =>
        // refresh the topic metadata cache
        info("Fetching metadata for topic %s".format(topic))
        updateInfo(topic)
        val topicMetadata = topicPartitionInfo.get(topic)
        topicMetadata match {
          case Some(m) => m
          case None => throw new IllegalStateException("Failed to fetch topic metadata for topic: " + topic)
        }
    }
    val partitionMetadata = metadata.partitionsMetadata
    partitionMetadata.map { m =>
      m.leader match {
        case Some(leader) => (new Partition(leader.id, m.partitionId, topic) -> leader)
        case None =>  throw new NoLeaderForPartitionException("No leader for topic %s, partition %d".format(topic, m.partitionId))
      }
    }.sortWith((s, t) => s._1.partId < t._1.partId)
  }

  /**
   * It updates the cache by issuing a get topic metadata request to a random broker.
   * @param topic the topic for which the metadata is to be fetched
   */
  def updateInfo(topic: String = null) = {
    val producer = producerPool.getAnyProducer
    if(topic != null) {
      val topicMetadataRequest = new TopicMetadataRequest(List(topic))
      val topicMetadataList = producer.send(topicMetadataRequest)
      val topicMetadata:Option[TopicMetadata] = if(topicMetadataList.size > 0) Some(topicMetadataList.head) else None
      topicMetadata match {
        case Some(metadata) =>
          info("Fetched metadata for topics %s".format(topic))
          topicPartitionInfo += (topic -> metadata)
        case None =>
      }
    }else {
      // refresh cache for all topics
      val topics = topicPartitionInfo.keySet.toList
      val topicMetadata = producer.send(new TopicMetadataRequest(topics))
      info("Fetched metadata for topics %s".format(topicMetadata.mkString(",")))
      topicMetadata.foreach(metadata => topicPartitionInfo += (metadata.topic -> metadata))
    }
  }
}
