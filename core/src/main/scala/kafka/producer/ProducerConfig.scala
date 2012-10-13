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

import async.AsyncProducerConfig
import java.util.Properties
import kafka.utils.{Utils, VerifiableProperties}
import kafka.message.{CompressionCodec, NoCompressionCodec}

class ProducerConfig private (val props: VerifiableProperties)
        extends AsyncProducerConfig with SyncProducerConfigShared {

  def this(originalProps: Properties) {
    this(new VerifiableProperties(originalProps))
    props.verify()
  }

  /** This is for bootstrapping and the producer will only use it for getting metadata
   * (topics, partitions and replicas). The socket connections for sending the actual data
   * will be established based on the broker information returned in the metadata. The
   * format is host1:por1,host2:port2, and the list can be a subset of brokers or
   * a VIP pointing to a subset of brokers.
   */
  val brokerList = props.getString("broker.list")

  /**
   * If DefaultEventHandler is used, this specifies the number of times to
   * retry if an error is encountered during send.
   */
  val numRetries = props.getInt("num.retries", 0)

  /** the partitioner class for partitioning events amongst sub-topics */
  val partitionerClass = props.getString("partitioner.class", "kafka.producer.DefaultPartitioner")

  /** this parameter specifies whether the messages are sent asynchronously *
   * or not. Valid values are - async for asynchronous send                 *
   *                            sync for synchronous send                   */
  val producerType = props.getString("producer.type", "sync")

  /**
   * This parameter allows you to specify the compression codec for all data generated *
   * by this producer. The default is NoCompressionCodec
   */
  val compressionCodec = {
    val prop = props.getString("compression.codec", NoCompressionCodec.name)
    try {
      CompressionCodec.getCompressionCodec(prop.toInt)
    }
    catch {
      case nfe: NumberFormatException =>
        CompressionCodec.getCompressionCodec(prop)
    }
  }

  /** This parameter allows you to set whether compression should be turned *
   *  on for particular topics
   *
   *  If the compression codec is anything other than NoCompressionCodec,
   *
   *    Enable compression only for specified topics if any
   *
   *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
   *
   *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
   */
  val compressedTopics = Utils.parseCsvList(props.getString("compressed.topics", null))

  /**
   * The producer using the zookeeper software load balancer maintains a ZK cache that gets
   * updated by the zookeeper watcher listeners. During some events like a broker bounce, the
   * producer ZK cache can get into an inconsistent state, for a small time period. In this time
   * period, it could end up picking a broker partition that is unavailable. When this happens, the
   * ZK cache needs to be updated.
   * This parameter specifies the number of times the producer attempts to refresh this ZK cache.
   */
  val producerRetries = props.getInt("producer.num.retries", 3)

  val producerRetryBackoffMs = props.getInt("producer.retry.backoff.ms", 100)
}
