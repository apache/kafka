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

package kafka.consumer

import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.javaapi.consumer.ConsumerRebalanceListener

import scala.collection._
import kafka.utils.Logging
import kafka.serializer._

/**
 *  Main interface for consumer
 */
trait ConsumerConnector {
  
  /**
   *  Create a list of MessageStreams for each topic.
   *
   *  @param topicCountMap  a map of (topic, #streams) pair
   *  @return a map of (topic, list of  KafkaStream) pairs.
   *          The number of items in the list is #streams. Each stream supports
   *          an iterator over message/metadata pairs.
   */
  def createMessageStreams(topicCountMap: Map[String,Int]): Map[String, List[KafkaStream[Array[Byte],Array[Byte]]]]
  
  /**
   *  Create a list of MessageStreams for each topic.
   *
   *  @param topicCountMap  a map of (topic, #streams) pair
   *  @param keyDecoder Decoder to decode the key portion of the message
   *  @param valueDecoder Decoder to decode the value portion of the message
   *  @return a map of (topic, list of  KafkaStream) pairs.
   *          The number of items in the list is #streams. Each stream supports
   *          an iterator over message/metadata pairs.
   */
  def createMessageStreams[K,V](topicCountMap: Map[String,Int],
                                keyDecoder: Decoder[K],
                                valueDecoder: Decoder[V])
    : Map[String,List[KafkaStream[K,V]]]
  
  /**
   *  Create a list of message streams for all topics that match a given filter.
   *
   *  @param topicFilter Either a Whitelist or Blacklist TopicFilter object.
   *  @param numStreams Number of streams to return
   *  @param keyDecoder Decoder to decode the key portion of the message
   *  @param valueDecoder Decoder to decode the value portion of the message
   *  @return a list of KafkaStream each of which provides an
   *          iterator over message/metadata pairs over allowed topics.
   */
  def createMessageStreamsByFilter[K,V](topicFilter: TopicFilter,
                                        numStreams: Int = 1,
                                        keyDecoder: Decoder[K] = new DefaultDecoder(),
                                        valueDecoder: Decoder[V] = new DefaultDecoder())
    : Seq[KafkaStream[K,V]]

  /**
   *  Commit the offsets of all broker partitions connected by this connector.
   */
  def commitOffsets(retryOnFailure: Boolean)
  
  /**
   * KAFKA-1743: This method added for backward compatibility.
   */
  def commitOffsets

  /**
   * Commit offsets from an external offsets map.
   * @param offsetsToCommit the offsets to be committed.
   */
  def commitOffsets(offsetsToCommit: immutable.Map[TopicAndPartition, OffsetAndMetadata], retryOnFailure: Boolean)

  /**
   * Wire in a consumer rebalance listener to be executed when consumer rebalance occurs.
   * @param listener The consumer rebalance listener to wire in
   */
  def setConsumerRebalanceListener(listener: ConsumerRebalanceListener)
  
  /**
   *  Shut down the connector
   */
  def shutdown()
}

object Consumer extends Logging {
  /**
   *  Create a ConsumerConnector
   *
   *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
   *                 connection string zookeeper.connect.
   */
  def create(config: ConsumerConfig): ConsumerConnector = {
    val consumerConnect = new ZookeeperConsumerConnector(config)
    consumerConnect
  }

  /**
   *  Create a ConsumerConnector
   *
   *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
   *                 connection string zookeeper.connect.
   */
  def createJavaConsumerConnector(config: ConsumerConfig): kafka.javaapi.consumer.ConsumerConnector = {
    val consumerConnect = new kafka.javaapi.consumer.ZookeeperConsumerConnector(config)
    consumerConnect
  }
}
