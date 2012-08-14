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
import kafka.utils.{Logging, Utils}
import kafka.common.ErrorMapping
import kafka.cluster.{Replica, Partition}


class BrokerPartitionInfo(producerConfig: ProducerConfig,
                          producerPool: ProducerPool,
                          topicPartitionInfo: HashMap[String, TopicMetadata])
        extends Logging {
  val brokerList = producerConfig.brokerList
  val brokers = Utils.getAllBrokersFromBrokerList(brokerList)

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
  def updateInfo(topics: Seq[String]) = {
    var fetchMetaDataSucceeded: Boolean = false
    var i: Int = 0
    val topicMetadataRequest = new TopicMetadataRequest(topics)
    var topicMetaDataResponse: Seq[TopicMetadata] = Nil
    var t: Throwable = null
    while(i < brokers.size && !fetchMetaDataSucceeded) {
      val producer: SyncProducer = ProducerPool.createSyncProducer(producerConfig, brokers(i))
      info("Fetching metadata for topic %s".format(brokers))
      try {
        topicMetaDataResponse = producer.send(topicMetadataRequest)
        fetchMetaDataSucceeded = true
        // throw partition specific exception
        topicMetaDataResponse.foreach(tmd =>{
          trace("Metadata for topic %s is %s".format(tmd.topic, tmd))
          if(tmd.errorCode == ErrorMapping.NoError){
            topicPartitionInfo.put(tmd.topic, tmd)
          } else
            warn("Metadata for topic [%s] is erronous: [%s]".format(tmd.topic, tmd), ErrorMapping.exceptionFor(tmd.errorCode))
          tmd.partitionsMetadata.foreach(pmd =>{
            if (pmd.errorCode != ErrorMapping.NoError){
              debug("Metadata for topic partition [%s, %d] is errornous: [%s]".format(tmd.topic, pmd.partitionId, pmd), ErrorMapping.exceptionFor(pmd.errorCode))
            }
          })
        })
        producerPool.updateProducer(topicMetaDataResponse)
      } catch {
        case e =>
          warn("fetching broker partition metadata for topics [%s] from broker [%s] failed".format(topics, brokers(i).toString), e)
          t = e
      } finally {
        i = i + 1
        producer.close()
      }
    }
    if(!fetchMetaDataSucceeded){
      throw new KafkaException("fetching broker partition metadata for topics [%s] from broker [%s] failed".format(topics, brokers), t)
    }
  }
}
