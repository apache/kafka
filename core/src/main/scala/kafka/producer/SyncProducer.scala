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

import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import kafka.api._
import kafka.common.MessageSizeTooLargeException
import kafka.message.MessageSet
import kafka.network.{BoundedByteBufferSend, Request, Receive}
import kafka.utils._
import kafka.utils.Utils._

/*
 * Send a message set.
 */
@threadsafe
class SyncProducer(val config: SyncProducerConfig) extends Logging {
  
  private val MaxConnectBackoffMs = 60000
  private var channel : SocketChannel = null
  private var sentOnConnection = 0
  private val lock = new Object()
  @volatile
  private var shutdown: Boolean = false

  debug("Instantiating Scala Sync Producer")

  private def verifyRequest(request: Request) = {
    if (logger.isTraceEnabled) {
      val buffer = new BoundedByteBufferSend(request).buffer
      trace("verifying sendbuffer of size " + buffer.limit)
      val requestTypeId = buffer.getShort()
      if(requestTypeId == RequestKeys.Produce) {
        val request = ProducerRequest.readFrom(buffer)
        trace(request.toString)
      }
    }
  }

  /**
   * Common functionality for the public send methods
   */
  private def doSend(request: Request): Tuple2[Receive, Int] = {
    lock synchronized {
      verifyRequest(request)
      val startTime = SystemTime.nanoseconds
      getOrMakeConnection()

      var response: Tuple2[Receive, Int] = null
      try {
        sendRequest(request, channel)
        response = getResponse(channel)
      } catch {
        case e: java.io.IOException =>
          // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
          disconnect()
          println("sdfsdfsdf")
          throw e
        case e => println("other sdfsdfsdfs"); throw e
      }
      // TODO: do we still need this?
      sentOnConnection += 1
      if(sentOnConnection >= config.reconnectInterval) {
        disconnect()
        channel = connect()
        sentOnConnection = 0
      }
      SyncProducerStats.recordProduceRequest(SystemTime.nanoseconds - startTime)
      response
    }
  }

  /**
   * Send a message
   */
  def send(producerRequest: ProducerRequest): ProducerResponse = {
    for( topicData <- producerRequest.data ) {
      for( partitionData <- topicData.partitionData ) {
	      verifyMessageSize(partitionData.messages)
        val setSize = partitionData.messages.sizeInBytes.asInstanceOf[Int]
        trace("Got message set with " + setSize + " bytes to send")
      }
    }
    val response = doSend(producerRequest)
    ProducerResponse.deserializeResponse(response._1.buffer)
  }

  def send(request: TopicMetadataRequest): Seq[TopicMetadata] = {
    val response = doSend(request)
    TopicMetadataRequest.deserializeTopicsMetadataResponse(response._1.buffer)
  }

  def close() = {
    lock synchronized {
      disconnect()
      shutdown = true
    }
  }

  private def verifyMessageSize(messages: MessageSet) {
    for (messageAndOffset <- messages)
      if (messageAndOffset.message.payloadSize > config.maxMessageSize)
        throw new MessageSizeTooLargeException
  }

  /**
   * Disconnect from current channel, closing connection.
   * Side effect: channel field is set to null on successful disconnect
   */
  private def disconnect() {
    try {
      if(channel != null) {
        info("Disconnecting from " + config.host + ":" + config.port)
        swallow(channel.close())
        swallow(channel.socket.close())
        channel = null
      }
    } catch {
      case e: Exception => error("Error on disconnect: ", e)
    }
  }
    
  private def connect(): SocketChannel = {
    var connectBackoffMs = 1
    val beginTimeMs = SystemTime.milliseconds
    while(channel == null && !shutdown) {
      try {
        channel = SocketChannel.open()
        channel.socket.setSendBufferSize(config.bufferSize)
        channel.configureBlocking(true)
        channel.socket.setSoTimeout(config.socketTimeoutMs)
        channel.socket.setKeepAlive(true)
        channel.connect(new InetSocketAddress(config.host, config.port))
        info("Connected to " + config.host + ":" + config.port + " for producing")
      }
      catch {
        case e: Exception => {
          disconnect()
          val endTimeMs = SystemTime.milliseconds
          if ( (endTimeMs - beginTimeMs + connectBackoffMs) > config.connectTimeoutMs) {
            error("Producer connection to " +  config.host + ":" + config.port + " timing out after " + config.connectTimeoutMs + " ms", e)
            throw e
          }
          error("Connection attempt to " +  config.host + ":" + config.port + " failed, next attempt in " + connectBackoffMs + " ms", e)
          SystemTime.sleep(connectBackoffMs)
          connectBackoffMs = math.min(10 * connectBackoffMs, MaxConnectBackoffMs)
        }
      }
    }
    channel
  }

  private def getOrMakeConnection() {
    if(channel == null) {
      channel = connect()
    }
  }
}

trait SyncProducerStatsMBean {
  def getProduceRequestsPerSecond: Double
  def getAvgProduceRequestMs: Double
  def getMaxProduceRequestMs: Double
  def getNumProduceRequests: Long
}

@threadsafe
class SyncProducerStats extends SyncProducerStatsMBean {
  private val produceRequestStats = new SnapshotStats

  def recordProduceRequest(requestNs: Long) = produceRequestStats.recordRequestMetric(requestNs)

  def getProduceRequestsPerSecond: Double = produceRequestStats.getRequestsPerSecond

  def getAvgProduceRequestMs: Double = produceRequestStats.getAvgMetric / (1000.0 * 1000.0)

  def getMaxProduceRequestMs: Double = produceRequestStats.getMaxMetric / (1000.0 * 1000.0)

  def getNumProduceRequests: Long = produceRequestStats.getNumRequests
}

object SyncProducerStats extends Logging {
  private val kafkaProducerstatsMBeanName = "kafka:type=kafka.KafkaProducerStats"
  private val stats = new SyncProducerStats
  swallow(Utils.registerMBean(stats, kafkaProducerstatsMBeanName))

  def recordProduceRequest(requestMs: Long) = stats.recordProduceRequest(requestMs)
}
