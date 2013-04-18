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

import kafka.api._
import kafka.network._
import kafka.utils._
import kafka.utils.ZkUtils._
import collection.immutable
import kafka.common.{ErrorMapping, TopicAndPartition, KafkaException}
import org.I0Itec.zkclient.ZkClient
import kafka.cluster.Broker

/**
 * A consumer of kafka messages
 */
@threadsafe
class SimpleConsumer(val host: String,
                     val port: Int,
                     val soTimeout: Int,
                     val bufferSize: Int,
                     val clientId: String) extends Logging {

  ConsumerConfig.validateClientId(clientId)
  private val lock = new Object()
  private val blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.UseDefaultBufferSize, soTimeout)
  val brokerInfo = "host_%s-port_%s".format(host, port)
  private val fetchRequestAndResponseStats = FetchRequestAndResponseStatsRegistry.getFetchRequestAndResponseStats(clientId)

  private def connect(): BlockingChannel = {
    close
    blockingChannel.connect()
    blockingChannel
  }

  private def disconnect() = {
    if(blockingChannel.isConnected) {
      debug("Disconnecting from " + host + ":" + port)
      blockingChannel.disconnect()
    }
  }

  private def reconnect() {
    disconnect()
    connect()
  }

  def close() {
    lock synchronized {
        disconnect()
    }
  }
  
  private def sendRequest(request: RequestOrResponse): Receive = {
    lock synchronized {
      getOrMakeConnection()
      var response: Receive = null
      try {
        blockingChannel.send(request)
        response = blockingChannel.receive()
      } catch {
        case e : java.io.IOException =>
          info("Reconnect due to socket error: ", e)
          // retry once
          try {
            reconnect()
            blockingChannel.send(request)
            response = blockingChannel.receive()
          } catch {
            case ioe: java.io.IOException =>
              disconnect()
              throw ioe
          }
        case e => throw e
      }
      response
    }
  }

  def send(request: TopicMetadataRequest): TopicMetadataResponse = {
    val response = sendRequest(request)
    TopicMetadataResponse.readFrom(response.buffer)
  }

  /**
   *  Fetch a set of messages from a topic.
   *
   *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
   *  @return a set of fetched messages
   */
  def fetch(request: FetchRequest): FetchResponse = {
    var response: Receive = null
    val specificTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseStats(brokerInfo).requestTimer
    val aggregateTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats.requestTimer
    aggregateTimer.time {
      specificTimer.time {
        response = sendRequest(request)
      }
    }
    val fetchResponse = FetchResponse.readFrom(response.buffer)
    val fetchedSize = fetchResponse.sizeInBytes
    fetchRequestAndResponseStats.getFetchRequestAndResponseStats(brokerInfo).requestSizeHist.update(fetchedSize)
    fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats.requestSizeHist.update(fetchedSize)
    fetchResponse
  }

  /**
   *  Get a list of valid offsets (up to maxSize) before the given time.
   *  @param request a [[kafka.api.OffsetRequest]] object.
   *  @return a [[kafka.api.OffsetResponse]] object.
   */
  def getOffsetsBefore(request: OffsetRequest) = OffsetResponse.readFrom(sendRequest(request).buffer)

  /**
   * Commit offsets for a topic
   * @param request a [[kafka.api.OffsetCommitRequest]] object.
   * @return a [[kafka.api.OffsetCommitResponse]] object.
   */
  def commitOffsets(request: OffsetCommitRequest) = OffsetCommitResponse.readFrom(sendRequest(request).buffer)

  /**
   * Fetch offsets for a topic
   * @param request a [[kafka.api.OffsetFetchRequest]] object.
   * @return a [[kafka.api.OffsetFetchResponse]] object.
   */
  def fetchOffsets(request: OffsetFetchRequest) = OffsetFetchResponse.readFrom(sendRequest(request).buffer)

  private def getOrMakeConnection() {
    if(!blockingChannel.isConnected) {
      connect()
    }
  }

  /**
   * Get the earliest or latest offset of a given topic, partition.
   * @param topicAndPartition Topic and partition of which the offset is needed.
   * @param earliestOrLatest A value to indicate earliest or latest offset.
   * @param consumerId Id of the consumer which could be a consumer client, SimpleConsumerShell or a follower broker.
   * @return Requested offset.
   */
  def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val request = OffsetRequest(requestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(earliestOrLatest, 1)),
                                clientId = clientId,
                                replicaId = consumerId)
    val partitionErrorAndOffset = getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition)
    val offset = partitionErrorAndOffset.error match {
      case ErrorMapping.NoError => partitionErrorAndOffset.offsets.head
      case _ => throw ErrorMapping.exceptionFor(partitionErrorAndOffset.error)
    }
    offset
  }
}

