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

import scala.collection._
import kafka.utils.{Utils, Logging}
import kafka.serializer.{DefaultDecoder, Decoder}

/**
 *  Main interface for consumer
 */
trait ConsumerConnector {
  /**
   *  Create a list of MessageStreams for each topic.
   *
   *  @param topicCountMap  a map of (topic, #streams) pair
   *  @param decoder Decoder to decode each Message to type T
   *  @return a map of (topic, list of  KafkaStream) pairs.
   *          The number of items in the list is #streams. Each stream supports
   *          an iterator over message/metadata pairs.
   */
  def createMessageStreams[T](topicCountMap: Map[String,Int],
                              decoder: Decoder[T] = new DefaultDecoder)
    : Map[String,List[KafkaStream[T]]]

  /**
   *  Create a list of message streams for all topics that match a given filter.
   *
   *  @param topicFilter Either a Whitelist or Blacklist TopicFilter object.
   *  @param numStreams Number of streams to return
   *  @param decoder Decoder to decode each Message to type T
   *  @return a list of KafkaStream each of which provides an
   *          iterator over message/metadata pairs over allowed topics.
   */
  def createMessageStreamsByFilter[T](topicFilter: TopicFilter,
                                      numStreams: Int = 1,
                                      decoder: Decoder[T] = new DefaultDecoder)
    : Seq[KafkaStream[T]]

  /**
   *  Commit the offsets of all broker partitions connected by this connector.
   */
  def commitOffsets
  
  /**
   *  Shut down the connector
   */
  def shutdown()
}

object Consumer extends Logging {
  private val consumerStatsMBeanName = "kafka:type=kafka.ConsumerStats"

  /**
   *  Create a ConsumerConnector
   *
   *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
   *                 connection string zk.connect.
   */
  def create(config: ConsumerConfig): ConsumerConnector = {
    val consumerConnect = new ZookeeperConsumerConnector(config)
    Utils.registerMBean(consumerConnect, consumerStatsMBeanName)
    consumerConnect
  }

  /**
   *  Create a ConsumerConnector
   *
   *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
   *                 connection string zk.connect.
   */
  def createJavaConsumerConnector(config: ConsumerConfig): kafka.javaapi.consumer.ConsumerConnector = {
    val consumerConnect = new kafka.javaapi.consumer.ZookeeperConsumerConnector(config)
    Utils.registerMBean(consumerConnect.underlying, consumerStatsMBeanName)
    consumerConnect
  }
}
