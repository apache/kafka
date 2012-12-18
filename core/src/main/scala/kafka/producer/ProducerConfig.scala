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
import kafka.common.{InvalidConfigException, Config}

object ProducerConfig extends Config {
  def validate(config: ProducerConfig) {
    validateClientId(config.clientId)
    validateBatchSize(config.batchSize, config.queueSize)
    validateProducerType(config.producerType)
  }

  def validateClientId(clientId: String) {
    validateChars("clientid", clientId)
  }

  def validateBatchSize(batchSize: Int, queueSize: Int) {
    if (batchSize > queueSize)
      throw new InvalidConfigException("Batch size = " + batchSize + " can't be larger than queue size = " + queueSize)
  }

  def validateProducerType(producerType: String) {
    producerType match {
      case "sync" =>
      case "async"=>
      case _ => throw new InvalidConfigException("Invalid value " + producerType + " for producer.type, valid values are sync/async")
    }
  }
}

class ProducerConfig private (val props: VerifiableProperties)
        extends AsyncProducerConfig with SyncProducerConfigShared {
  import ProducerConfig._

  def this(originalProps: Properties) {
    this(new VerifiableProperties(originalProps))
    props.verify()
  }

  /** This is for bootstrapping and the producer will only use it for getting metadata
   * (topics, partitions and replicas). The socket connections for sending the actual data
   * will be established based on the broker information returned in the metadata. The
   * format is host1:port1,host2:port2, and the list can be a subset of brokers or
   * a VIP pointing to a subset of brokers.
   */
  val brokerList = props.getString("broker.list")

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
   * If a request fails it is possible to have the producer automatically retry. This is controlled by this setting.
   * Note that not all errors mean that the message was lost--for example if the network connection is lost we will
   * get a socket exception--in this case enabling retries can result in duplicate messages.
   */
  val producerRetries = props.getInt("producer.num.retries", 3)

  /**
   * The amount of time to wait in between retries
   */
  val producerRetryBackoffMs = props.getInt("producer.retry.backoff.ms", 100)

  validate(this)
}
