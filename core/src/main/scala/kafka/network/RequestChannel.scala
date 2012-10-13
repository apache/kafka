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

package kafka.network

import java.util.concurrent._
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import java.nio.ByteBuffer
import kafka.api._
import kafka.common.TopicAndPartition
import kafka.utils.{Logging, SystemTime}
import kafka.message.ByteBufferMessageSet


object RequestChannel extends Logging {
  val AllDone = new Request(1, 2, getShutdownReceive(), 0)

  def getShutdownReceive() = {
    val emptyProducerRequest = new ProducerRequest(0, 0, "", 0, 0, Map[TopicAndPartition, ByteBufferMessageSet]())
    val byteBuffer = ByteBuffer.allocate(emptyProducerRequest.sizeInBytes + 2)
    byteBuffer.putShort(RequestKeys.ProduceKey)
    emptyProducerRequest.writeTo(byteBuffer)
    byteBuffer.rewind()
    byteBuffer
  }

  case class Request(processor: Int, requestKey: Any, buffer: ByteBuffer, startTimeNs: Long) {
    var dequeueTimeNs = -1L
    var apiLocalCompleteTimeNs = -1L
    var responseCompleteTimeNs = -1L
    val requestId = buffer.getShort()
    val requestObj: RequestOrResponse = RequestKeys.deserializerForKey(requestId)(buffer)
    buffer.rewind()
    trace("Received request: %s".format(requestObj))

    def updateRequestMetrics() {
      val endTimeNs = SystemTime.nanoseconds
      val queueTime = (dequeueTimeNs - startTimeNs).max(0L)
      val apiLocalTime = (apiLocalCompleteTimeNs - dequeueTimeNs).max(0L)
      val apiRemoteTime = (responseCompleteTimeNs - apiLocalCompleteTimeNs).max(0L)
      val responseSendTime = (endTimeNs - responseCompleteTimeNs).max(0L)
      val totalTime = endTimeNs - startTimeNs
      var metricsList = List(RequestMetrics.metricsMap(RequestKeys.nameForKey(requestId)))
      if (requestId == RequestKeys.FetchKey) {
        val isFromFollower = requestObj.asInstanceOf[FetchRequest].isFromFollower
        metricsList ::= ( if (isFromFollower)
                            RequestMetrics.metricsMap(RequestMetrics.followFetchMetricName)
                          else
                            RequestMetrics.metricsMap(RequestMetrics.consumerFetchMetricName) )
      }
      metricsList.foreach{
        m => m.requestRate.mark()
             m.queueTimeHist.update(queueTime)
             m.localTimeHist.update(apiLocalTime)
             m.remoteTimeHist.update(apiRemoteTime)
             m.responseSendTimeHist.update(responseSendTime)
             m.totalTimeHist.update(totalTime)
      }
    }
    trace("Completed request: %s".format(requestObj))
  }
  
  case class Response(processor: Int, request: Request, responseSend: Send) {
    request.responseCompleteTimeNs = SystemTime.nanoseconds

    def this(request: Request, send: Send) =
      this(request.processor, request, send)
  }
}

class RequestChannel(val numProcessors: Int, val queueSize: Int) extends KafkaMetricsGroup {
  private var responseListeners: List[(Int) => Unit] = Nil
  private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)
  private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)
  for(i <- 0 until numProcessors)
    responseQueues(i) = new ArrayBlockingQueue[RequestChannel.Response](queueSize)

  newGauge(
    "RequestQueueSize",
    new Gauge[Int] {
      def getValue = requestQueue.size
    }
  )

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request) {
    requestQueue.put(request)
  }
  
  /** Send a response back to the socket server to be sent over the network */ 
  def sendResponse(response: RequestChannel.Response) {
    responseQueues(response.processor).put(response)
    for(onResponse <- responseListeners)
      onResponse(response.processor)
  }

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.Request = 
    requestQueue.take()

  /** Get a response for the given processor if there is one */
  def receiveResponse(processor: Int): RequestChannel.Response =
    responseQueues(processor).poll()

  def addResponseListener(onResponse: Int => Unit) { 
    responseListeners ::= onResponse
  }
}

object RequestMetrics {
  val metricsMap = new scala.collection.mutable.HashMap[String, RequestMetrics]
  val consumerFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "-Consumer"
  val followFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "-Follower"
  (RequestKeys.keyToNameAndDeserializerMap.values.map(e => e._1)
    ++ List(consumerFetchMetricName, followFetchMetricName)).foreach(name => metricsMap.put(name, new RequestMetrics(name)))
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {
  val requestRate = newMeter(name + "-RequestsPerSec",  "requests", TimeUnit.SECONDS)
  // time a request spent in a request queue
  val queueTimeHist = newHistogram(name + "-QueueTimeNs")
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(name + "-LocalTimeNs")
  // time a request takes to wait on remote brokers (only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(name + "-RemoteTimeNs")
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram(name + "-ResponseSendTimeNs")
  val totalTimeHist = newHistogram(name + "-TotalTimeNs")
}

