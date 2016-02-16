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

import java.net.InetAddress
import java.nio.ByteBuffer
import java.security.Principal
import java.util.concurrent._

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Logging, SystemTime}
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.{AbstractRequest, RequestHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.log4j.Logger


object RequestChannel extends Logging {
  val AllDone = new Request(processor = 1, connectionId = "2", new Session(KafkaPrincipal.ANONYMOUS, InetAddress.getLocalHost()), buffer = getShutdownReceive(), startTimeMs = 0, securityProtocol = SecurityProtocol.PLAINTEXT)

  def getShutdownReceive() = {
    val emptyProducerRequest = new ProducerRequest(0, 0, "", 0, 0, collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]())
    val byteBuffer = ByteBuffer.allocate(emptyProducerRequest.sizeInBytes + 2)
    byteBuffer.putShort(RequestKeys.ProduceKey)
    emptyProducerRequest.writeTo(byteBuffer)
    byteBuffer.rewind()
    byteBuffer
  }

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress)

  case class Request(processor: Int, connectionId: String, session: Session, private var buffer: ByteBuffer, startTimeMs: Long, securityProtocol: SecurityProtocol) {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeMs = -1L
    @volatile var apiLocalCompleteTimeMs = -1L
    @volatile var responseCompleteTimeMs = -1L
    @volatile var responseDequeueTimeMs = -1L
    @volatile var apiRemoteCompleteTimeMs = -1L

    val requestId = buffer.getShort()
    // for server-side request / response format
    // TODO: this will be removed once we migrated to client-side format
    val requestObj =
      if ( RequestKeys.keyToNameAndDeserializerMap.contains(requestId))
        RequestKeys.deserializerForKey(requestId)(buffer)
      else
        null
    // if we failed to find a server-side mapping, then try using the
    // client-side request / response format
    val header: RequestHeader =
      if (requestObj == null) {
        buffer.rewind
        RequestHeader.parse(buffer)
      } else
        null
    val body: AbstractRequest =
      if (requestObj == null)
        try {
          AbstractRequest.getRequest(header.apiKey, header.apiVersion, buffer)
        } catch {
          case ex: Throwable =>
            throw new InvalidRequestException(s"Error getting request for apiKey: ${header.apiKey} and apiVersion: ${header.apiVersion}", ex)
        }
      else
        null

    buffer = null
    private val requestLogger = Logger.getLogger("kafka.request.logger")

    private def requestDesc(details: Boolean): String = {
      if (requestObj != null)
        requestObj.describe(details)
      else
        header.toString + " -- " + body.toString
    }

    trace("Processor %d received request : %s".format(processor, requestDesc(true)))

    def updateRequestMetrics() {
      val endTimeMs = SystemTime.milliseconds
      // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes if the remote
      // processing time is really small. This value is set in KafkaApis from a request handling thread.
      // This may be read in a network thread before the actual update happens in KafkaApis which will cause us to
      // see a negative value here. In that case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.
      if (apiLocalCompleteTimeMs < 0)
        apiLocalCompleteTimeMs = responseCompleteTimeMs
      // If the apiRemoteCompleteTimeMs is not set (i.e., for requests that do not go through a purgatory), then it is
      // the same as responseCompleteTimeMs.
      if (apiRemoteCompleteTimeMs < 0)
        apiRemoteCompleteTimeMs = responseCompleteTimeMs

      val requestQueueTime = (requestDequeueTimeMs - startTimeMs).max(0L)
      val apiLocalTime = (apiLocalCompleteTimeMs - requestDequeueTimeMs).max(0L)
      val apiRemoteTime = (apiRemoteCompleteTimeMs - apiLocalCompleteTimeMs).max(0L)
      val apiThrottleTime = (responseCompleteTimeMs - apiRemoteCompleteTimeMs).max(0L)
      val responseQueueTime = (responseDequeueTimeMs - responseCompleteTimeMs).max(0L)
      val responseSendTime = (endTimeMs - responseDequeueTimeMs).max(0L)
      val totalTime = endTimeMs - startTimeMs
      var metricsList = List(RequestMetrics.metricsMap(ApiKeys.forId(requestId).name))
      if (requestId == RequestKeys.FetchKey) {
        val isFromFollower = requestObj.asInstanceOf[FetchRequest].isFromFollower
        metricsList ::= ( if (isFromFollower)
                            RequestMetrics.metricsMap(RequestMetrics.followFetchMetricName)
                          else
                            RequestMetrics.metricsMap(RequestMetrics.consumerFetchMetricName) )
      }
      metricsList.foreach{
        m => m.requestRate.mark()
             m.requestQueueTimeHist.update(requestQueueTime)
             m.localTimeHist.update(apiLocalTime)
             m.remoteTimeHist.update(apiRemoteTime)
             m.throttleTimeHist.update(apiThrottleTime)
             m.responseQueueTimeHist.update(responseQueueTime)
             m.responseSendTimeHist.update(responseSendTime)
             m.totalTimeHist.update(totalTime)
      }

      if(requestLogger.isTraceEnabled)
        requestLogger.trace("Completed request:%s from connection %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d,securityProtocol:%s,principal:%s"
          .format(requestDesc(true), connectionId, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime, securityProtocol, session.principal))
      else if(requestLogger.isDebugEnabled)
        requestLogger.debug("Completed request:%s from connection %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d,securityProtocol:%s,principal:%s"
          .format(requestDesc(false), connectionId, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime, securityProtocol, session.principal))
    }
  }
  
  case class Response(processor: Int, request: Request, responseSend: Send, responseAction: ResponseAction) {
    request.responseCompleteTimeMs = SystemTime.milliseconds

    def this(processor: Int, request: Request, responseSend: Send) =
      this(processor, request, responseSend, if (responseSend == null) NoOpAction else SendAction)

    def this(request: Request, send: Send) =
      this(request.processor, request, send)
  }

  trait ResponseAction
  case object SendAction extends ResponseAction
  case object NoOpAction extends ResponseAction
  case object CloseConnectionAction extends ResponseAction
}

class RequestChannel(val numProcessors: Int, val queueSize: Int) extends KafkaMetricsGroup {
  private var responseListeners: List[(Int) => Unit] = Nil
  private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)
  private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)
  for(i <- 0 until numProcessors)
    responseQueues(i) = new LinkedBlockingQueue[RequestChannel.Response]()

  newGauge(
    "RequestQueueSize",
    new Gauge[Int] {
      def value = requestQueue.size
    }
  )

  newGauge("ResponseQueueSize", new Gauge[Int]{
    def value = responseQueues.foldLeft(0) {(total, q) => total + q.size()}
  })

  for (i <- 0 until numProcessors) {
    newGauge("ResponseQueueSize",
      new Gauge[Int] {
        def value = responseQueues(i).size()
      },
      Map("processor" -> i.toString)
    )
  }

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

  /** No operation to take for the request, need to read more over the network */
  def noOperation(processor: Int, request: RequestChannel.Request) {
    responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.NoOpAction))
    for(onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Close the connection for the request */
  def closeConnection(processor: Int, request: RequestChannel.Request) {
    responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.CloseConnectionAction))
    for(onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.Request =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.Request =
    requestQueue.take()

  /** Get a response for the given processor if there is one */
  def receiveResponse(processor: Int): RequestChannel.Response = {
    val response = responseQueues(processor).poll()
    if (response != null)
      response.request.responseDequeueTimeMs = SystemTime.milliseconds
    response
  }

  def addResponseListener(onResponse: Int => Unit) { 
    responseListeners ::= onResponse
  }

  def shutdown() {
    requestQueue.clear
  }
}

object RequestMetrics {
  val metricsMap = new scala.collection.mutable.HashMap[String, RequestMetrics]
  val consumerFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "Consumer"
  val followFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "Follower"
  (ApiKeys.values().toList.map(e => e.name)
    ++ List(consumerFetchMetricName, followFetchMetricName)).foreach(name => metricsMap.put(name, new RequestMetrics(name)))
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {
  val tags = Map("request" -> name)
  val requestRate = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags)
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram("RequestQueueTimeMs", biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram("LocalTimeMs", biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram("RemoteTimeMs", biased = true, tags)
  // time a request is throttled (only relevant to fetch and produce requests)
  val throttleTimeHist = newHistogram("ThrottleTimeMs", biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram("ResponseQueueTimeMs", biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram("ResponseSendTimeMs", biased = true, tags)
  val totalTimeHist = newHistogram("TotalTimeMs", biased = true, tags)
}

