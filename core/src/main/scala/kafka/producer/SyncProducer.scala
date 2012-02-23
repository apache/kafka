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

import java.net._
import java.nio.channels._
import kafka.message._
import kafka.network._
import kafka.utils._
import kafka.api._
import scala.math._
import java.nio.ByteBuffer
import java.util.Random

object SyncProducer {
  val RequestKey: Short = 0
  val randomGenerator = new Random
}

/*
 * Send a message set.
 */
@threadsafe
class SyncProducer(val config: SyncProducerConfig) extends Logging {
  
  private val MaxConnectBackoffMs = 60000
  private var channel : SocketChannel = null
  private var sentOnConnection = 0
  /** make time-based reconnect starting at a random time **/
  private var lastConnectionTime = System.currentTimeMillis - SyncProducer.randomGenerator.nextDouble() * config.reconnectInterval

  private val lock = new Object()
  @volatile
  private var shutdown: Boolean = false

  trace("Instantiating Scala Sync Producer")

  private def verifySendBuffer(buffer : ByteBuffer) = {
    /**
     * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
     * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
     * data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
     */
    if (logger.isDebugEnabled) {
      trace("verifying sendbuffer of size " + buffer.limit)
      val requestTypeId = buffer.getShort()
      if (requestTypeId == RequestKeys.MultiProduce) {
        try {
          val request = MultiProducerRequest.readFrom(buffer)
          for (produce <- request.produces) {
            try {
              for (messageAndOffset <- produce.messages)
                if (!messageAndOffset.message.isValid)
                  throw new InvalidMessageException("Message for topic " + produce.topic + " is invalid")
            }
            catch {
              case e: Throwable =>
                error("error iterating messages ", e)
            }
          }
        }
        catch {
          case e: Throwable =>
            error("error verifying sendbuffer ", e)
        }
      }
    }
  }

  /**
   * Common functionality for the public send methods
   */
  private def send(send: BoundedByteBufferSend) {
    lock synchronized {
      verifySendBuffer(send.buffer.slice)
      val startTime = SystemTime.nanoseconds
      getOrMakeConnection()

      try {
        send.writeCompletely(channel)
      } catch {
        case e : java.io.IOException =>
          // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
          disconnect()
          throw e
        case e2 =>
          throw e2
      }
      // TODO: do we still need this?
      sentOnConnection += 1

      if(sentOnConnection >= config.reconnectInterval || (config.reconnectTimeInterval >= 0 && System.currentTimeMillis - lastConnectionTime >= config.reconnectTimeInterval)) {
        disconnect()
        channel = connect()
        sentOnConnection = 0
        lastConnectionTime = System.currentTimeMillis
      }
      val endTime = SystemTime.nanoseconds
      SyncProducerStats.recordProduceRequest(endTime - startTime)
    }
  }

  /**
   * Send a message
   */
  def send(topic: String, partition: Int, messages: ByteBufferMessageSet) {
    messages.verifyMessageSize(config.maxMessageSize)
    val setSize = messages.sizeInBytes.asInstanceOf[Int]
    trace("Got message set with " + setSize + " bytes to send")
    send(new BoundedByteBufferSend(new ProducerRequest(topic, partition, messages)))
  }
 
  def send(topic: String, messages: ByteBufferMessageSet): Unit = send(topic, ProducerRequest.RandomPartition, messages)

  def multiSend(produces: Array[ProducerRequest]) {
    for (request <- produces)
      request.messages.verifyMessageSize(config.maxMessageSize)
    val setSize = produces.foldLeft(0L)(_ + _.messages.sizeInBytes)
    trace("Got multi message sets with " + setSize + " bytes to send")
    send(new BoundedByteBufferSend(new MultiProducerRequest(produces)))
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
      if(channel != null) {
        info("Disconnecting from " + config.host + ":" + config.port)
        Utils.swallow(logger.warn, channel.close())
        Utils.swallow(logger.warn, channel.socket.close())
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
          if ( (endTimeMs - beginTimeMs + connectBackoffMs) > config.connectTimeoutMs)
          {
            error("Producer connection to " +  config.host + ":" + config.port + " timing out after " + config.connectTimeoutMs + " ms", e)
            throw e
          }
          error("Connection attempt to " +  config.host + ":" + config.port + " failed, next attempt in " + connectBackoffMs + " ms", e)
          SystemTime.sleep(connectBackoffMs)
          connectBackoffMs = min(10 * connectBackoffMs, MaxConnectBackoffMs)
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
  Utils.swallow(logger.warn, Utils.registerMBean(stats, kafkaProducerstatsMBeanName))

  def recordProduceRequest(requestMs: Long) = stats.recordProduceRequest(requestMs)
}
