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
import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import kafka.utils.ZkUtils._
import collection.immutable
import kafka.common.{TopicAndPartition, KafkaException}
import org.I0Itec.zkclient.ZkClient
import kafka.cluster.Broker


object SimpleConsumer extends Logging {
  def earliestOrLatestOffset(broker: Broker, topic: String, partitionId: Int, earliestOrLatest: Long,
                             clientId: String, isFromOrdinaryConsumer: Boolean): Long = {
    var simpleConsumer: SimpleConsumer = null
    var producedOffset: Long = -1L
    try {
      simpleConsumer = new SimpleConsumer(broker.host, broker.port, ConsumerConfig.SocketTimeout,
                                          ConsumerConfig.SocketBufferSize, clientId)
      val topicAndPartition = TopicAndPartition(topic, partitionId)
      val request = if(isFromOrdinaryConsumer)
        new OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(earliestOrLatest, 1)))
      else
        new OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(earliestOrLatest, 1)),
                          Request.DebuggingConsumerId)
      producedOffset = simpleConsumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
    } catch {
      case e =>
        error("error in earliestOrLatestOffset() ", e)
    }
    finally {
      if (simpleConsumer != null)
        simpleConsumer.close()
    }
    producedOffset
  }

  def earliestOrLatestOffset(zkClient: ZkClient, topic: String, brokerId: Int, partitionId: Int,
                             earliestOrLatest: Long, clientId: String, isFromOrdinaryConsumer: Boolean = true): Long = {
    val cluster = getCluster(zkClient)
    val broker = cluster.getBroker(brokerId) match {
      case Some(b) => b
      case None => throw new KafkaException("Broker " + brokerId + " is unavailable. Cannot issue " +
                                                    "getOffsetsBefore request")
    }
    earliestOrLatestOffset(broker, topic, partitionId, earliestOrLatest, clientId, isFromOrdinaryConsumer)
  }
}


/**
 * A consumer of kafka messages
 */
@threadsafe
class SimpleConsumer(val host: String,
                     val port: Int,
                     val soTimeout: Int,
                     val bufferSize: Int,
                     val clientId: String) extends Logging {

  ClientId.validate(clientId)
  private val lock = new Object()
  private val blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.UseDefaultBufferSize, soTimeout)
  private val fetchRequestAndResponseStats = new FetchRequestAndResponseStats(clientId + "-host_%s-port_%s".format(host, port))

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
    fetchRequestAndResponseStats.requestTimer.time {
      response = sendRequest(request)
    }
    val fetchResponse = FetchResponse.readFrom(response.buffer)
    val fetchedSize = fetchResponse.sizeInBytes
    fetchRequestAndResponseStats.respondSizeHist.update(fetchedSize)
    fetchResponse
  }

  /**
   *  Get a list of valid offsets (up to maxSize) before the given time.
   *  @param request a [[kafka.api.OffsetRequest]] object.
   *  @return a [[kafka.api.OffsetResponse]] object.
   */
  def getOffsetsBefore(request: OffsetRequest) = OffsetResponse.readFrom(sendRequest(request).buffer)

  private def getOrMakeConnection() {
    if(!blockingChannel.isConnected) {
      connect()
    }
  }
}

class FetchRequestAndResponseStats(clientId: String) extends KafkaMetricsGroup {
  val requestTimer = new KafkaTimer(newTimer(clientId + "-FetchRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
  val respondSizeHist = newHistogram(clientId + "-FetchResponseSize")
}
