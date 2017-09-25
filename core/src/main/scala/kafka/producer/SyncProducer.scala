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

import java.util.Random
import java.util.concurrent.TimeUnit

import kafka.api._
import kafka.network.{RequestOrResponseSend, BlockingChannel}
import kafka.utils._
import org.apache.kafka.common.network.NetworkReceive
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.utils.Utils._

@deprecated("This object has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.KafkaProducer instead.", "0.10.0.0")
object SyncProducer {
  val RequestKey: Short = 0
  val randomGenerator = new Random
}

/*
 * Send a message set.
 */
@threadsafe
@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.KafkaProducer instead.", "0.10.0.0")
class SyncProducer(val config: SyncProducerConfig) extends Logging {

  private val lock = new Object()
  @volatile private var shutdown: Boolean = false
  private val blockingChannel = new BlockingChannel(config.host, config.port, BlockingChannel.UseDefaultBufferSize,
    config.sendBufferBytes, config.requestTimeoutMs)
  val producerRequestStats = ProducerRequestStatsRegistry.getProducerRequestStats(config.clientId)

  trace("Instantiating Scala Sync Producer with properties: %s".format(config.props))

  private def verifyRequest(request: RequestOrResponse) = {
    /**
     * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
     * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
     * data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
     */
    if (logger.isDebugEnabled) {
      val buffer = new RequestOrResponseSend("", request).buffer
      trace("verifying sendbuffer of size " + buffer.limit())
      val requestTypeId = buffer.getShort()
      if(requestTypeId == ApiKeys.PRODUCE.id) {
        val request = ProducerRequest.readFrom(buffer)
        trace(request.toString)
      }
    }
  }

  /**
   * Common functionality for the public send methods
   */
  private def doSend(request: RequestOrResponse, readResponse: Boolean = true): NetworkReceive = {
    lock synchronized {
      verifyRequest(request)
      getOrMakeConnection()

      var response: NetworkReceive = null
      try {
        blockingChannel.send(request)
        if(readResponse)
          response = blockingChannel.receive()
        else
          trace("Skipping reading response")
      } catch {
        case e: java.io.IOException =>
          // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
          disconnect()
          throw e
        case e: Throwable => throw e
      }
      response
    }
  }

  /**
   * Send a message. If the producerRequest had required.request.acks=0, then the
   * returned response object is null
   */
  def send(producerRequest: ProducerRequest): ProducerResponse = {
    val requestSize = producerRequest.sizeInBytes
    producerRequestStats.getProducerRequestStats(config.host, config.port).requestSizeHist.update(requestSize)
    producerRequestStats.getProducerRequestAllBrokersStats.requestSizeHist.update(requestSize)

    var response: NetworkReceive = null
    val specificTimer = producerRequestStats.getProducerRequestStats(config.host, config.port).requestTimer
    val aggregateTimer = producerRequestStats.getProducerRequestAllBrokersStats.requestTimer
    aggregateTimer.time {
      specificTimer.time {
        response = doSend(producerRequest, producerRequest.requiredAcks != 0)
      }
    }
    if(producerRequest.requiredAcks != 0) {
      val producerResponse = ProducerResponse.readFrom(response.payload)
      producerRequestStats.getProducerRequestStats(config.host, config.port).throttleTimeStats.update(producerResponse.throttleTime, TimeUnit.MILLISECONDS)
      producerRequestStats.getProducerRequestAllBrokersStats.throttleTimeStats.update(producerResponse.throttleTime, TimeUnit.MILLISECONDS)
      producerResponse
    }
    else
      null
  }

  def send(request: TopicMetadataRequest): TopicMetadataResponse = {
    val response = doSend(request)
    TopicMetadataResponse.readFrom(response.payload)
  }

  def close() = {
    lock synchronized {
      disconnect()
      shutdown = true
    }
  }

  /**
   * Disconnect from current channel, closing connection.
   * Side effect: channel field is set to null on successful disconnect
   */
  private def disconnect() {
    try {
      info("Disconnecting from " + formatAddress(config.host, config.port))
      blockingChannel.disconnect()
    } catch {
      case e: Exception => error("Error on disconnect: ", e)
    }
  }

  private def connect(): BlockingChannel = {
    if (!blockingChannel.isConnected && !shutdown) {
      try {
        blockingChannel.connect()
        info("Connected to " + formatAddress(config.host, config.port) + " for producing")
      } catch {
        case e: Exception => {
          disconnect()
          error("Producer connection to " + formatAddress(config.host, config.port) + " unsuccessful", e)
          throw e
        }
      }
    }
    blockingChannel
  }

  private def getOrMakeConnection() {
    if(!blockingChannel.isConnected) {
      connect()
    }
  }
}
