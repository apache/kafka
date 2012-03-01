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

import java.net._
import java.nio.channels._
import kafka.api._
import kafka.network._
import kafka.utils._
import kafka.utils.Utils._

/**
 * A consumer of kafka messages
 */
@threadsafe
class SimpleConsumer(val host: String,
                     val port: Int,
                     val soTimeout: Int,
                     val bufferSize: Int) extends Logging {
  private var channel : SocketChannel = null
  private val lock = new Object()

  private def connect(): SocketChannel = {
    close
    val address = new InetSocketAddress(host, port)

    val channel = SocketChannel.open
    debug("Connected to " + address + " for fetching.")
    channel.configureBlocking(true)
    channel.socket.setReceiveBufferSize(bufferSize)
    channel.socket.setSoTimeout(soTimeout)
    channel.socket.setKeepAlive(true)
    channel.connect(address)
    trace("requested receive buffer size=" + bufferSize + " actual receive buffer size= " + channel.socket.getReceiveBufferSize)
    trace("soTimeout=" + soTimeout + " actual soTimeout= " + channel.socket.getSoTimeout)
    
    channel
  }

  private def close(channel: SocketChannel) = {
    debug("Disconnecting from " + channel.socket.getRemoteSocketAddress())
    swallow(channel.close())
    swallow(channel.socket.close())
  }

  def close() {
    lock synchronized {
      if (channel != null)
        close(channel)
      channel = null
    }
  }

  /**
   *  Fetch a set of messages from a topic.
   *
   *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
   *  @return a set of fetched messages
   */
  def fetch(request: FetchRequest): FetchResponse = {
    lock synchronized {
      val startTime = SystemTime.nanoseconds
      getOrMakeConnection()
      var response: Tuple2[Receive,Int] = null
      try {
        sendRequest(request, channel)
        response = getResponse(channel)
      } catch {
        case e : java.io.IOException =>
          info("Reconnect in fetch request due to socket error: ", e)
          // retry once
          try {
            channel = connect
            sendRequest(request, channel)
            response = getResponse(channel)
          } catch {
            case ioe: java.io.IOException => channel = null; throw ioe;
          }
        case e => throw e
      }
      val fetchResponse = FetchResponse.readFrom(response._1.buffer)
      val fetchedSize = fetchResponse.sizeInBytes

      val endTime = SystemTime.nanoseconds
      SimpleConsumerStats.recordFetchRequest(endTime - startTime)
      SimpleConsumerStats.recordConsumptionThroughput(fetchedSize)

      fetchResponse
    }
  }

  /**
   *  Get a list of valid offsets (up to maxSize) before the given time.
   *  The result is a list of offsets, in descending order.
   *
   *  @param time: time in millisecs (-1, from the latest offset available, -2 from the smallest offset available)
   *  @return an array of offsets
   */
  def getOffsetsBefore(topic: String, partition: Int, time: Long, maxNumOffsets: Int): Array[Long] = {
    lock synchronized {
      getOrMakeConnection()
      var response: Tuple2[Receive,Int] = null
      try {
        sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets), channel)
        response = getResponse(channel)
      } catch {
        case e : java.io.IOException =>
          info("Reconnect in get offetset request due to socket error: ", e)
          // retry once
          try {
            channel = connect
            sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets), channel)
            response = getResponse(channel)
          } catch {
            case ioe: java.io.IOException => channel = null; throw ioe;
          }
      }
      OffsetRequest.deserializeOffsetArray(response._1.buffer)
    }
  }

  private def getOrMakeConnection() {
    if(channel == null) {
      channel = connect()
    }
  }
}

trait SimpleConsumerStatsMBean {
  def getFetchRequestsPerSecond: Double
  def getAvgFetchRequestMs: Double
  def getMaxFetchRequestMs: Double
  def getNumFetchRequests: Long  
  def getConsumerThroughput: Double
}

@threadsafe
class SimpleConsumerStats extends SimpleConsumerStatsMBean {
  private val fetchRequestStats = new SnapshotStats

  def recordFetchRequest(requestNs: Long) = fetchRequestStats.recordRequestMetric(requestNs)

  def recordConsumptionThroughput(data: Long) = fetchRequestStats.recordThroughputMetric(data)

  def getFetchRequestsPerSecond: Double = fetchRequestStats.getRequestsPerSecond

  def getAvgFetchRequestMs: Double = fetchRequestStats.getAvgMetric / (1000.0 * 1000.0)

  def getMaxFetchRequestMs: Double = fetchRequestStats.getMaxMetric / (1000.0 * 1000.0)

  def getNumFetchRequests: Long = fetchRequestStats.getNumRequests

  def getConsumerThroughput: Double = fetchRequestStats.getThroughput
}

object SimpleConsumerStats extends Logging {
  private val simpleConsumerstatsMBeanName = "kafka:type=kafka.SimpleConsumerStats"
  private val stats = new SimpleConsumerStats
  Utils.registerMBean(stats, simpleConsumerstatsMBeanName)

  def recordFetchRequest(requestMs: Long) = stats.recordFetchRequest(requestMs)
  def recordConsumptionThroughput(data: Long) = stats.recordConsumptionThroughput(data)
}

