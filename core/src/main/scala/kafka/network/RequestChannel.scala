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
import java.util.concurrent._

import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.{Gauge, Meter}
import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel.{BaseRequest, SendAction, ShutdownRequest, NoOpAction, CloseConnectionAction}
import kafka.utils.{Logging, NotNothing}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled

  sealed trait BaseRequest
  case object ShutdownRequest extends BaseRequest

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser = Sanitizer.sanitize(principal.getName)
  }

  class Metrics {

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    (ApiKeys.values.toSeq.map(_.name) ++
        Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName)).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name))
    }

    def apply(metricName: String) = metricsMap(metricName)

    def close(): Unit = {
       metricsMap.values.foreach(_.removeMetrics())
    }
  }

  class Request(val processor: Int,
                val context: RequestContext,
                startTimeNanos: Long,
                memoryPool: MemoryPool,
                @volatile private var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics) extends BaseRequest {
    val session = Session(context.principal, context.clientAddress)
    lazy val timers: RequestTimers = new RequestTimers
    lazy val bodyAndSize: RequestAndSize = context.parseRequest(buffer)
    lazy val description: String = s"$header -- ${bodyAndSize.request.toString(requestLogger.underlying.isTraceEnabled)}"

    /** Called only to provide the necessary bits of the Request to the Response **/
    def toSummary: RequestSummary = new RequestSummary(
      processor,
      metrics,
      context,
      session,
      startTimeNanos,
      description,
      timers,
      requestBodySizeInBytes = bodyAndSize.size,
      isFromFollower = bodyAndSize.request match {
        case r:FetchRequest => r.isFromFollower
        case _ => false
      }
    )

    def header: RequestHeader = context.header

    trace(s"Processor $processor received request: $description")

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def releaseBuffer(): Unit = {
      if (buffer != null) {
        // force evaluation of bodyAndSize and request parsing before buffer is released
        bodyAndSize
        memoryPool.release(buffer)
        buffer = null
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer)"
  }

  class RequestTimers {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var apiRemoteCompleteTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var temporaryMemoryBytes = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    def requestThreadTimeNanos = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }
  }

  /** Summary of Request without the parsed request **/
  class RequestSummary(
    val processor: Int,
    val metrics: Metrics,
    val context: RequestContext,
    val session: Session,
    startTimeNanos: Long,
    val description: String,
    val timers: RequestTimers,
    requestBodySizeInBytes: Int,
    isFromFollower: Boolean
  ) {
    def header: RequestHeader = context.header
    def sizeInBytes: Int = requestBodySizeInBytes

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response) {
      import timers._

      val endTimeNanos = Time.SYSTEM.nanoseconds
      // In some corner cases, apiLocalCompleteTimeNanos may not be set when the request completes if the remote
      // processing time is really small. This value is set in KafkaApis from a request handling thread.
      // This may be read in a network thread before the actual update happens in KafkaApis which will cause us to
      // see a negative value here. In that case, use responseCompleteTimeNanos as apiLocalCompleteTimeNanos.
      if (apiLocalCompleteTimeNanos < 0)
        apiLocalCompleteTimeNanos = responseCompleteTimeNanos
      // If the apiRemoteCompleteTimeNanos is not set (i.e., for requests that do not go through a purgatory), then it is
      // the same as responseCompleteTimeNanos.
      if (apiRemoteCompleteTimeNanos < 0)
        apiRemoteCompleteTimeNanos = responseCompleteTimeNanos

      /**
        * Converts nanos to millis with micros precision as additional decimal places in the request log have low
        * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
        * to the nearest long.
        */
      def nanosToMs(nanos: Long): Double = {
        val positiveNanos = math.max(nanos, 0)
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
      val apiRemoteTimeMs = nanosToMs(apiRemoteCompleteTimeNanos - apiLocalCompleteTimeNanos)
      val apiThrottleTimeMs = nanosToMs(responseCompleteTimeNanos - apiRemoteCompleteTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val fetchMetricNames: Seq[String] =
        if (header.apiKey == ApiKeys.FETCH) {
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ header.apiKey.name
      metricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate.mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(Math.round(apiThrottleTimeMs))
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.requestBytesHist.update(sizeInBytes)
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (isRequestLoggingEnabled) {
        val responseString =
          if (response.responseSend.isDefined)
            response.responseAsString.getOrElse(
              throw new IllegalStateException("responseAsString should always be defined if request logging is enabled"))
          else ""

        val builder = new StringBuilder(256)
        builder.append("Completed request:").append(description)
          .append(",response:").append(responseString)
          .append(" from connection ").append(context.connectionId)
          .append(";totalTime:").append(totalTimeMs)
          .append(",requestQueueTime:").append(requestQueueTimeMs)
          .append(",localTime:").append(apiLocalTimeMs)
          .append(",remoteTime:").append(apiRemoteTimeMs)
          .append(",throttleTime:").append(apiThrottleTimeMs)
          .append(",responseQueueTime:").append(responseQueueTimeMs)
          .append(",sendTime:").append(responseSendTimeMs)
          .append(",securityProtocol:").append(context.securityProtocol)
          .append(",principal:").append(session.principal)
          .append(",listener:").append(context.listenerName.value)
        if (temporaryMemoryBytes > 0)
          builder.append(",temporaryMemoryBytes:").append(temporaryMemoryBytes)
        if (messageConversionsTimeMs > 0)
          builder.append(",messageConversionsTime:").append(messageConversionsTimeMs)
        requestLogger.debug(builder.toString)
      }
    }
  }

  /** responseAsString should only be defined if request logging is enabled */
  class Response(val request: RequestSummary, val responseSend: Option[Send], val responseAction: ResponseAction,
                 val responseAsString: Option[String]) {
    request.timers.responseCompleteTimeNanos = Time.SYSTEM.nanoseconds
    if (request.timers.apiLocalCompleteTimeNanos == -1L)
      request.timers.apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds

    def processor: Int = request.processor

    override def toString =
      s"Response(request=$request, responseSend=$responseSend, responseAction=$responseAction), responseAsString=$responseAsString"
  }

  sealed trait ResponseAction
  case object SendAction extends ResponseAction
  case object NoOpAction extends ResponseAction
  case object CloseConnectionAction extends ResponseAction
}

class RequestChannel(val queueSize: Int) extends KafkaMetricsGroup {
  val metrics = new RequestChannel.Metrics
  private var responseListeners: List[(Int) => Unit] = Nil
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  private val responseQueues = new ConcurrentHashMap[Int, BlockingQueue[RequestChannel.Response]]()

  newGauge(
    "RequestQueueSize",
    new Gauge[Int] {
      def value = requestQueue.size
    }
  )

  newGauge("ResponseQueueSize", new Gauge[Int]{
    def value = responseQueues.values.asScala.foldLeft(0) {(total, q) => total + q.size()}
  })

  def addProcessor(processorId: Int): Unit = {
    val responseQueue = new LinkedBlockingQueue[RequestChannel.Response]()
    if (responseQueues.putIfAbsent(processorId, responseQueue) != null)
      warn(s"Unexpected processor with processorId $processorId")
    newGauge("ResponseQueueSize",
      new Gauge[Int] {
        def value = responseQueue.size()
      },
      Map("processor" -> processorId.toString)
    )
  }

  def removeProcessor(processorId: Int): Unit = {
    removeMetric("ResponseQueueSize", Map("processor" -> processorId.toString))
    responseQueues.remove(processorId)
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request) {
    requestQueue.put(request)
  }

  /** Send a response back to the socket server to be sent over the network */
  def sendResponse(response: RequestChannel.Response) {
    if (isTraceEnabled) {
      val requestHeader = response.request.header
      val message = response.responseAction match {
        case SendAction =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${response.responseSend.get.size} bytes."
        case NoOpAction =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case CloseConnectionAction =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
      }
      trace(message)
    }

    val responseQueue = responseQueues.get(response.processor)
    // `responseQueue` may be null if the processor was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    if (responseQueue != null) {
      responseQueue.put(response)
      for (onResponse <- responseListeners)
        onResponse(response.processor)
    }
  }

  /** Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.BaseRequest =
    requestQueue.take()

  /** Get a response for the given processor if there is one */
  def receiveResponse(processor: Int): RequestChannel.Response = {
    val responseQueue = responseQueues.get(processor)
    if (responseQueue == null)
      throw new IllegalStateException(s"receiveResponse with invalid processor $processor: processors=${responseQueues.keySet}")
    val response = responseQueue.poll()
    if (response != null)
      response.request.timers.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  def addResponseListener(onResponse: Int => Unit) {
    responseListeners ::= onResponse
  }

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]) {
    errors.foreach { case (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear() {
    requestQueue.clear()
  }

  def shutdown() {
    clear()
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

}

object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"

  val RequestsPerSec = "RequestsPerSec"
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  val LocalTimeMs = "LocalTimeMs"
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {
  import RequestMetrics._

  val tags = Map("request" -> name)
  val requestRate = newMeter(RequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram(RequestQueueTimeMs, biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(LocalTimeMs, biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(RemoteTimeMs, biased = true, tags)
  // time a request is throttled
  val throttleTimeHist = newHistogram(ThrottleTimeMs, biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram(ResponseQueueTimeMs, biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram(ResponseSendTimeMs, biased = true, tags)
  val totalTimeHist = newHistogram(TotalTimeMs, biased = true, tags)
  // request size in bytes
  val requestBytesHist = newHistogram(RequestBytes, biased = true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(MessageConversionsTimeMs, biased = true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(TemporaryMemoryBytes, biased = true, tags))
    else
      None

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name)

    @volatile private var meter: Meter = null

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int) {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    removeMetric(RequestsPerSec, tags)
    removeMetric(RequestQueueTimeMs, tags)
    removeMetric(LocalTimeMs, tags)
    removeMetric(RemoteTimeMs, tags)
    removeMetric(RequestsPerSec, tags)
    removeMetric(ThrottleTimeMs, tags)
    removeMetric(ResponseQueueTimeMs, tags)
    removeMetric(TotalTimeMs, tags)
    removeMetric(ResponseSendTimeMs, tags)
    removeMetric(RequestBytes, tags)
    removeMetric(ResponseSendTimeMs, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      removeMetric(MessageConversionsTimeMs, tags)
      removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
