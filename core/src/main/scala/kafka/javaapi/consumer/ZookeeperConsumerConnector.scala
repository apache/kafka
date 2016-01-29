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
package kafka.javaapi.consumer

import kafka.serializer._
import kafka.consumer._
import kafka.common.{OffsetAndMetadata, TopicAndPartition, MessageStreamsExistException}
import scala.collection.{immutable, mutable, JavaConversions}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._

/**
 * This class handles the consumers interaction with zookeeper
 *
 * Directories:
 * 1. Consumer id registry:
 * /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/[0...N] --> { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
*/

private[kafka] class ZookeeperConsumerConnector(val config: ConsumerConfig,
                                                val enableFetcher: Boolean) // for testing only
    extends ConsumerConnector {

  private val underlying = new kafka.consumer.ZookeeperConsumerConnector(config, enableFetcher)
  private val messageStreamCreated = new AtomicBoolean(false)

  def this(config: ConsumerConfig) = this(config, true)

 // for java client
  def createMessageStreams[K,V](
        topicCountMap: java.util.Map[String,java.lang.Integer],
        keyDecoder: Decoder[K],
        valueDecoder: Decoder[V])
      : java.util.Map[String,java.util.List[KafkaStream[K,V]]] = {

    if (messageStreamCreated.getAndSet(true))
      throw new MessageStreamsExistException(this.getClass.getSimpleName +
                                   " can create message streams at most once",null)
    val scalaTopicCountMap: Map[String, Int] = {
      import JavaConversions._
      Map.empty[String, Int] ++ (topicCountMap.asInstanceOf[java.util.Map[String, Int]]: mutable.Map[String, Int])
    }
    val scalaReturn = underlying.consume(scalaTopicCountMap, keyDecoder, valueDecoder)
    val ret = new java.util.HashMap[String,java.util.List[KafkaStream[K,V]]]
    for ((topic, streams) <- scalaReturn) {
      var javaStreamList = new java.util.ArrayList[KafkaStream[K,V]]
      for (stream <- streams)
        javaStreamList.add(stream)
      ret.put(topic, javaStreamList)
    }
    ret
  }

  def createMessageStreams(topicCountMap: java.util.Map[String,java.lang.Integer]): java.util.Map[String,java.util.List[KafkaStream[Array[Byte],Array[Byte]]]] =
    createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder())

  def createMessageStreamsByFilter[K,V](topicFilter: TopicFilter, numStreams: Int, keyDecoder: Decoder[K], valueDecoder: Decoder[V]) = {
    import JavaConversions._
    underlying.createMessageStreamsByFilter(topicFilter, numStreams, keyDecoder, valueDecoder)
  }

  def createMessageStreamsByFilter(topicFilter: TopicFilter, numStreams: Int) =
    createMessageStreamsByFilter(topicFilter, numStreams, new DefaultDecoder(), new DefaultDecoder())

  def createMessageStreamsByFilter(topicFilter: TopicFilter) =
    createMessageStreamsByFilter(topicFilter, 1, new DefaultDecoder(), new DefaultDecoder())

  def commitOffsets() {
    underlying.commitOffsets(true)
  }

  def commitOffsets(retryOnFailure: Boolean) {
    underlying.commitOffsets(retryOnFailure)
  }

  def commitOffsets(offsetsToCommit: java.util.Map[TopicAndPartition, OffsetAndMetadata], retryOnFailure: Boolean) {
    underlying.commitOffsets(offsetsToCommit.asScala.toMap, retryOnFailure)
  }

  def setConsumerRebalanceListener(consumerRebalanceListener: ConsumerRebalanceListener) {
    underlying.setConsumerRebalanceListener(consumerRebalanceListener)
  }

  def shutdown() {
    underlying.shutdown
  }
}
