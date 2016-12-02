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

import org.apache.kafka.common.protocol.Errors

import collection.mutable.HashMap
import kafka.api.TopicMetadata
import kafka.common.KafkaException
import kafka.utils.Logging
import kafka.client.ClientUtils

@deprecated("This class has been deprecated and will be removed in a future release.", "0.10.0.0")
class BrokerPartitionInfo(producerConfig: ProducerConfig,
                          producerPool: ProducerPool,
                          topicPartitionInfo: HashMap[String, TopicMetadata])
        extends Logging {
  val brokerList = producerConfig.brokerList
  val brokers = ClientUtils.parseBrokerList(brokerList)

  /**
   * Return a sequence of (brokerId, numPartitions).
   * @param topic the topic for which this information is to be returned
   * @return a sequence of (brokerId, numPartitions). Returns a zero-length
   * sequence if no brokers are available.
   */
  def getBrokerPartitionInfo(topic: String, correlationId: Int): Seq[PartitionAndLeader] = {
    debug("Getting broker partition info for topic %s".format(topic))
    // check if the cache has metadata for this topic
    val topicMetadata = topicPartitionInfo.get(topic)
    val metadata: TopicMetadata =
      topicMetadata match {
        case Some(m) => m
        case None =>
          // refresh the topic metadata cache
          updateInfo(Set(topic), correlationId)
          val topicMetadata = topicPartitionInfo.get(topic)
          topicMetadata match {
            case Some(m) => m
            case None => throw new KafkaException("Failed to fetch topic metadata for topic: " + topic)
          }
      }
    val partitionMetadata = metadata.partitionsMetadata
    if(partitionMetadata.isEmpty) {
      if(metadata.errorCode != Errors.NONE.code) {
        throw new KafkaException(Errors.forCode(metadata.errorCode).exception)
      } else {
        throw new KafkaException("Topic metadata %s has empty partition metadata and no error code".format(metadata))
      }
    }
    partitionMetadata.map { m =>
      m.leader match {
        case Some(leader) =>
          debug("Partition [%s,%d] has leader %d".format(topic, m.partitionId, leader.id))
          new PartitionAndLeader(topic, m.partitionId, Some(leader.id))
        case None =>
          debug("Partition [%s,%d] does not have a leader yet".format(topic, m.partitionId))
          new PartitionAndLeader(topic, m.partitionId, None)
      }
    }.sortWith((s, t) => s.partitionId < t.partitionId)
  }

  /**
   * It updates the cache by issuing a get topic metadata request to a random broker.
   * @param topics the topics for which the metadata is to be fetched
   */
  def updateInfo(topics: Set[String], correlationId: Int) {
    var topicsMetadata: Seq[TopicMetadata] = Nil
    val topicMetadataResponse = ClientUtils.fetchTopicMetadata(topics, brokers, producerConfig, correlationId)
    topicsMetadata = topicMetadataResponse.topicsMetadata
    // throw partition specific exception
    topicsMetadata.foreach(tmd =>{
      trace("Metadata for topic %s is %s".format(tmd.topic, tmd))
      if(tmd.errorCode == Errors.NONE.code) {
        topicPartitionInfo.put(tmd.topic, tmd)
      } else
        warn("Error while fetching metadata [%s] for topic [%s]: %s ".format(tmd, tmd.topic, Errors.forCode(tmd.errorCode).exception.getClass))
      tmd.partitionsMetadata.foreach(pmd =>{
        if (pmd.errorCode != Errors.NONE.code && pmd.errorCode == Errors.LEADER_NOT_AVAILABLE.code) {
          warn("Error while fetching metadata %s for topic partition [%s,%d]: [%s]".format(pmd, tmd.topic, pmd.partitionId,
            Errors.forCode(pmd.errorCode).exception.getClass))
        } // any other error code (e.g. ReplicaNotAvailable) can be ignored since the producer does not need to access the replica and isr metadata
      })
    })
    producerPool.updateProducer(topicsMetadata)
  }

}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.10.0.0")
case class PartitionAndLeader(topic: String, partitionId: Int, leaderBrokerIdOpt: Option[Int])
