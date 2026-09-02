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
import kafka.utils.Logging
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.metrics.internals.MetricsUtils
import org.apache.kafka.network.{BaseRequest, CallbackRequest, CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, Request, Response, SendResponse, ShutdownRequest, StartThrottlingResponse, WakeupRequest}
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util.OptionalLong
import scala.jdk.CollectionConverters._

object RequestChannel extends Logging {

  private val RequestQueueSizeMetric = "RequestQueueSize"
  private val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"
}

class RequestChannel(val queueSize: Int,
                     time: Time,
                     val metrics: RequestChannelMetrics) {
  import RequestChannel._

  // Changing the package or class name may cause incompatibility with existing code and metrics configuration
  private val metricsPackage = "kafka.network"
  private val metricsClassName = "RequestChannel"
  private val metricsGroup = new KafkaMetricsGroup(metricsPackage, metricsClassName)

  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  private val processors = new ConcurrentHashMap[Int, Processor]()
  private val callbackQueue = new ArrayBlockingQueue[BaseRequest](queueSize)

  metricsGroup.newGauge(RequestQueueSizeMetric, () => requestQueue.size)

  metricsGroup.newGauge(ResponseQueueSizeMetric, () => {
    processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })

  def addProcessor(processor: Processor): Unit = {
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    metricsGroup.newGauge(ResponseQueueSizeMetric, () => processor.responseQueueSize,
      MetricsUtils.getTags(ProcessorMetricTag, processor.id.toString))
  }

  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)
    metricsGroup.removeMetric(ResponseQueueSizeMetric,
      MetricsUtils.getTags(ProcessorMetricTag, processorId.toString))
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: Request): Unit = {
    requestQueue.put(request)
  }

  def closeConnection(
    request: Request,
    errorCounts: java.util.Map[Errors, Integer]
  ): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    sendResponse(new CloseConnectionResponse(request))
  }

  def sendResponse(
    request: Request,
    response: AbstractResponse
  ): Unit = {
    updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala)
    sendResponse(new SendResponse(
      request,
      request.buildResponseSend(response),
      request.responseNode(response)
    ))
  }

  def sendNoOpResponse(request: Request): Unit = {
    sendResponse(new NoOpResponse(request))
  }

  def startThrottling(request: Request): Unit = {
    sendResponse(new StartThrottlingResponse(request))
  }

  def endThrottling(request: Request): Unit = {
    sendResponse(new EndThrottlingResponse(request))
  }

  /** Send a response back to the socket server to be sent over the network */
  private[network] def sendResponse(response: Response): Unit = {
    if (isTraceEnabled) {
      val requestHeader = response.request.headerForLoggingOrThrottling()
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }

    response match {
      // We should only send one of the following per request
      case _: SendResponse | _: NoOpResponse | _: CloseConnectionResponse =>
        val request = response.request
        val timeNanos = time.nanoseconds()
        request.responseCompleteTimeNanos(timeNanos)
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos(timeNanos)
        // If this callback was executed after KafkaApis returned we will need to adjust the callback completion time here.
        if (request.callbackRequestDequeueTimeNanos.isPresent && request.callbackRequestCompleteTimeNanos.isEmpty)
          request.callbackRequestCompleteTimeNanos(OptionalLong.of(time.nanoseconds()))
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    val processor = processors.get(response.request.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    if (processor != null) {
      processor.enqueueResponse(response)
    }
  }

  /** Get the next request or block until specified time has elapsed
   *  Check the callback queue and execute first if present since these
   *  requests have already waited in line. */
  def receiveRequest(timeout: Long): BaseRequest = {
    val callbackRequest = callbackQueue.poll()
    if (callbackRequest != null)
      callbackRequest
    else {
      val request = requestQueue.poll(timeout, TimeUnit.MILLISECONDS)
      request match {
        case _: WakeupRequest => callbackQueue.poll()
        case _ => request
      }
    }
  }

  /** Get the next request or block until there is one */
  def receiveRequest(): BaseRequest =
    requestQueue.take()

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.foreachEntry { (error, count) =>
      metrics.get(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
    callbackQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest.INSTANCE)

  def sendCallbackRequest(request: CallbackRequest): Unit = {
    callbackQueue.put(request)
    if (!requestQueue.offer(WakeupRequest.INSTANCE))
      trace("Wakeup request could not be added to queue. This means queue is full, so we will still process callback.")
  }

}
