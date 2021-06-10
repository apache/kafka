/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils.{Logging, NotNothing, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData._
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors, ObjectSerializationCache}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag


object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  val RequestQueueSizeMetric = "RequestQueueSize"
  val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"

  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled

  sealed trait BaseRequest

  case object ShutdownRequest extends BaseRequest

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser: String = Sanitizer.sanitize(principal.getName)
  }

  class Metrics(enabledApis: Iterable[ApiKeys]) {
    def this(scope: ListenerType) = {
      this(ApiKeys.apisForListener(scope).asScala)
    }

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    (enabledApis.map(_.name) ++
      Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName)).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name))
    }

    def apply(metricName: String): RequestMetrics = metricsMap(metricName)

    def close(): Unit = {
      metricsMap.values.foreach(_.removeMetrics())
    }
  }

  /**
   * 请求的实现类，有丰富的属性
   *
   * @param processor      这个完整的请求是由哪个{@link Processor}创建的。Kafka Broker端的网络模型属于Main-Sub Reactor：
   *                       {@link Acceptor} 只用于接收新的TCP连接（即{@link java.nio.channels.SocketChannel}），
   *                       再通过Round-robin算法分配一个{@link Processor}用于接收和发送数据。{@link Processor}会从底层的SocketChannel接收数据，
   *                       组装成完成的{@link Request}对象。
   *                       Processor线程的数量是由「num.network.threads」配置。每个监听器都是单独一套Main-Sub Reactor模型。
   * @param context        标识请求上下文信息。其中有一个非常重要的操作：解析从SocketChannel得到的二进制数据（{@link RequestContext# parseRequest}）
   * @param startTimeNanos 记录Requet对象被创建的时间，主要是用于各种时间统计指标的计算
   * @param memoryPool     非阻塞的内存池
   * @param buffer         从SocketChannel接收到的完整二进制数据。里面保存Request对象内存的字节缓冲区。
   * @param metrics        Request相关的各种监控指标管理类，内部构建一个Map封装所有JMX指标
   * @param envelope
   */
  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                val memoryPool: MemoryPool,
                @volatile var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics,
                val envelope: Option[RequestChannel.Request] = None) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var apiThrottleTimeMs = 0L
    @volatile var temporaryMemoryBytes = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    val session = Session(context.principal, context.clientAddress)

    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    // This is constructed on creation of a Request so that the JSON representation is computed before the request is
    // processed by the api layer. Otherwise, a ProduceRequest can occur without its data (ie. it goes into purgatory).
    val requestLog: Option[JsonNode] =
    if (RequestChannel.isRequestLoggingEnabled) Some(RequestConvertToJson.request(loggableRequest))
    else None

    def header: RequestHeader = context.header

    def sizeOfBodyInBytes: Int = bodyAndSize.size

    def sizeInBytes: Int = header.size(new ObjectSerializationCache) + sizeOfBodyInBytes

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def isForwarded: Boolean = envelope.isDefined

    def buildResponseSend(abstractResponse: AbstractResponse): Send = {
      envelope match {
        case Some(request) =>
          val responseBytes = context.buildResponseEnvelopePayload(abstractResponse)
          val envelopeResponse = new EnvelopeResponse(responseBytes, Errors.NONE)
          request.context.buildResponseSend(envelopeResponse)
        case None =>
          context.buildResponseSend(abstractResponse)
      }
    }

    def responseNode(response: AbstractResponse): Option[JsonNode] = {
      if (RequestChannel.isRequestLoggingEnabled)
        Some(RequestConvertToJson.response(response, context.apiVersion))
      else
        None
    }

    def headerForLoggingOrThrottling(): RequestHeader = {
      envelope match {
        case Some(request) =>
          request.context.header
        case None =>
          context.header
      }
    }

    def requestDesc(details: Boolean): String = {
      val forwardDescription = envelope.map { request =>
        s"Forwarded request: ${request.context} "
      }.getOrElse("")
      s"$forwardDescription$header -- ${loggableRequest.toString(details)}"
    }

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def loggableRequest: AbstractRequest = {

      bodyAndSize.request match {
        case alterConfigs: AlterConfigsRequest =>
          val loggableConfigs = alterConfigs.configs().asScala.map { case (resource, config) =>
            val loggableEntries = new AlterConfigsRequest.Config(config.entries.asScala.map { entry =>
              new AlterConfigsRequest.ConfigEntry(entry.name, KafkaConfig.loggableValue(resource.`type`, entry.name, entry.value))
            }.asJavaCollection)
            (resource, loggableEntries)
          }.asJava
          new AlterConfigsRequest.Builder(loggableConfigs, alterConfigs.validateOnly).build(alterConfigs.version())

        case alterConfigs: IncrementalAlterConfigsRequest =>
          val resources = new AlterConfigsResourceCollection(alterConfigs.data.resources.size)
          alterConfigs.data.resources.forEach { resource =>
            val newResource = new AlterConfigsResource()
              .setResourceName(resource.resourceName)
              .setResourceType(resource.resourceType)
            resource.configs.forEach { config =>
              newResource.configs.add(new AlterableConfig()
                .setName(config.name)
                .setValue(KafkaConfig.loggableValue(ConfigResource.Type.forId(resource.resourceType), config.name, config.value))
                .setConfigOperation(config.configOperation))
            }
            resources.add(newResource)
          }
          val data = new IncrementalAlterConfigsRequestData()
            .setValidateOnly(alterConfigs.data().validateOnly())
            .setResources(resources)
          new IncrementalAlterConfigsRequest.Builder(data).build(alterConfigs.version)

        case _ =>
          bodyAndSize.request
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos: Long = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response): Unit = {
      val endTimeNanos = Time.SYSTEM.nanoseconds

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
      val apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val fetchMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val isFromFollower = body[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ header.apiKey.name
      metricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(apiThrottleTimeMs)
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.requestBytesHist.update(sizeOfBodyInBytes)
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
        val desc = RequestConvertToJson.requestDescMetrics(header, requestLog, response.responseLog,
          context, session, isForwarded,
          totalTimeMs, requestQueueTimeMs, apiLocalTimeMs,
          apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs,
          responseSendTimeMs, temporaryMemoryBytes,
          messageConversionsTimeMs)
        requestLogger.debug("Completed request:" + desc.toString)
      }
    }

    def releaseBuffer(): Unit = {
      envelope match {
        case Some(request) =>
          request.releaseBuffer()
        case None =>
          if (buffer != null) {
            memoryPool.release(buffer)
            buffer = null
          }
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer, " +
      s"envelope=$envelope)"

  }

  /**
   * 响应体
   *
   * @param request 该响应体对应的请求体。请求体和响应体一般是成对出现的
   */
  sealed abstract class Response(val request: Request) {

    /**
     * 处理器
     *
     * @return
     */
    def processor: Int = request.processor

    def responseLog: Option[JsonNode] = None

    /**
     * 响应体中最重要的方法：定义当Response被处理后所要执行的收尾逻辑（方法）
     *
     * @return
     */
    def onComplete: Option[Send => Unit] = None
  }

  /**
   * 这个类用来保存响应结果
   *
   * @param request            该响应体对应的请求体。请求体和响应体一般是成对出现的
   * @param responseSend       只需要知道里面包含响应的二进制数据，它封装了简易的方法可以将数据写入底层的SocketChannel
   * @param responseLogValue   只有在启用日志记录请求的情况下才应定义该值
   * @param onCompleteCallback Response处理完后的回调方法
   */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseLogValue: Option[JsonNode],
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseLog: Option[JsonNode] = responseLogValue

    /**
     * 在Scala世界中，Unit和Java的Void等同。
     * Send=>Unit 表示一个方法，这个方法接收一个Send类型实例，然后执行一段代码逻辑。
     * 你可以把一个函数作为一个参数传给另一个函数，也可以把函数作为结果返回。
     *
     * @return
     */
    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String = s"Response(type=Send, request=$request, send=$responseSend, asString=$responseLogValue)"
  }

  /**
   * 为无需执行回调的请求而准备的
   *
   * @param request 该响应体对应的请求体。请求体和响应体一般是成对出现的
   */
  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String = s"Response(type=NoOp, request=$request)"
  }

  /**
   * 由于接收端内部出现不可重试的错误，需要关闭TCP连接才有可能解决问题。
   * 此时接收方法返回这类响应给发送方以显式关闭TCP连接。
   *
   * @param request 该响应体对应的请求体。请求体和响应体一般是成对出现的
   */
  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String = s"Response(type=CloseConnection, request=$request)"
  }

  /**
   * 用来通知Socket Server，此TCP已经开始被限流了。
   *
   * @param request 该响应体对应的请求体。请求体和响应体一般是成对出现的
   */
  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String = s"Response(type=StartThrottling, request=$request)"
  }

  /**
   * 用来通知Socket Server，此TCP已经解除限流了。
   *
   * @param request 该响应体对应的请求体。请求体和响应体一般是成对出现的
   */
  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String = s"Response(type=EndThrottling, request=$request)"
  }
}


/**
 * 传输Request的通道。不要与底层的SocketChannel概念相混合，
 * Kafka使用这个类充当生产者/消费者的中介：存储待处理的Request请求。
 * 从而解耦Processor线程和I/O处理线程。
 * 这个通道是被 {@link Processor} 和 {@link kafka.server.KafkaRequestHandlerPool} I/O线程处理器共享。
 * 这三个类构成一个微小的生产者/消费者模型。
 *
 * @param queueSize        队列长度
 * @param metricNamePrefix 监控指标前缀
 * @param time             定时工具类
 * @param metrics          监控指标类
 */
class RequestChannel(val queueSize: Int,
                     val metricNamePrefix: String,
                     time: Time,
                     val metrics: RequestChannel.Metrics) extends KafkaMetricsGroup {

  import RequestChannel._

  /**
   * 存放待处理请求的并发缓存队列，底层是ArrayBlockingQueue阻塞队列
   */
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)

  /**
   * 使用并发Map保存请求处理器。
   * key为Processor唯一标识ID，这里通道保存Processor的引用是因为
   */
  private val processors = new ConcurrentHashMap[Int, Processor]()

  val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)

  newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) { (total, processor) =>
      total + processor.responseQueueSize
    }
  })

  /**
   * 添加新的Processor处理器，该处理器主要用于在发送响应时获取之前绑定的处理器
   *
   * @param processor
   */
  def addProcessor(processor: Processor): Unit = {
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      Map(ProcessorMetricTag -> processor.id.toString))
  }

  /**
   * 移除处理器
   * 这个方法的使用场景是Processor的数量可以动态修改（通过kafka-configs命令完成），
   * 因此，当需要动态移除时，就会调用此方法移除Processor
   * @param processorId
   */
  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)
    removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString))
  }

  /**
   * 将待处理的请求加入缓存队列中。
   * 如果队列容量不够，则会阻塞调用线程
   *
   * @param request
   */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  /**
   * 将响应交给对应的Processor发送。
   * 当I/O线程 {@link kafka.server.KafkaRequestHandlerPool} 处理完后生成对应的Response，
   * 这个响应需要通过Processor发送，而且必须是接收请求相同的Processer。所以RequestChannel需要使用Map保存Processor对象的引用
   *
   * @param response
   */
  def sendResponse(response: RequestChannel.Response): Unit = {

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
        request.responseCompleteTimeNanos = timeNanos
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos = timeNanos
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    // 从并发Map中获取Processor。请求的接收和发送响应都应是同一个Processor线程。
    val processor = processors.get(response.processor)
    if (processor != null) {
      // 找到对应的Processor线程，那就将响应体写入Processor内部的缓存队列中并等待发送
      processor.enqueueResponse(response)
    }
    // 如果对应的Processor丢失，说明底层TCP连接已关闭，响应体直接丢弃即可
  }

  /** Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.BaseRequest =
    requestQueue.take()

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.forKeyValue { (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

}

object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"

  // 每秒处理Request数量，可以用来评估Broker繁忙程度
  val RequestsPerSec = "RequestsPerSec"

  // 计算Request在缓存队列中平均等待时间（单位毫秒）
  val RequestQueueTimeMs = "RequestQueueTimeMs"

  // 计算Request实际被处理的时间（单位毫秒）
  val LocalTimeMs = "LocalTimeMs"

  // 适用于Kafka的PRODUCE和FETCH请求，因为需要等待其它Broker的响应。
  // 这个监控十分重要
  val RemoteTimeMs = "RemoteTimeMs"

  // 完成 请求->处理请求->生成响应->发送响应 完整流程所花费的时间
  val TotalTimeMs = "TotalTimeMs"

  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {

  import RequestMetrics._

  val tags = Map("request" -> name)
  val requestRateInternal = new Pool[Short, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram(RequestQueueTimeMs, biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(LocalTimeMs, biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(RemoteTimeMs, biased = true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
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

  def requestRate(version: Short): Meter = {
    requestRateInternal.getAndMaybePut(version, newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags + ("version" -> version.toString)))
  }

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

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) removeMetric(RequestsPerSec, tags + ("version" -> version.toString))
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
