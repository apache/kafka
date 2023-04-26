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
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.network
import kafka.server.KafkaConfig
import kafka.utils.{Logging, NotNothing, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.EnvelopeResponseData
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util
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

  sealed trait BaseRequest //Scala 语言中的“trait”关键字，大致类似于 Java 中的 interface（接口）

  //ShutdownRequest 仅仅起到一个标志位的作用
  //当 Broker 进程关闭时，请求处理器（RequestHandler）会发送 ShutdownRequest 到专属的请求处理线程。
  // 该线程接收到此请求后，会主动触发一系列的 Broker 关闭逻辑。
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


  //Request 则是真正定义各类 Clients 端或 Broker 端请求的实现类。
  //它定义的属性包括 processor、context、startTimeNanos、memoryPool、buffer 和 metrics。
  //processor 是 Processor 线程的序号，即这个请求是由哪个 Processor 线程接收处理的。Broker 端参数 num.network.threads 控制了 Broker 每个监听器上创建的 Processor 线程数。
  //请求对象中的很多 JMX 指标，特别是时间类的统计指标，都需要使用 startTimeNanos 字段。你要注意的是，它是以纳秒为单位的时间戳信息，可以实现非常细粒度的时间统计精度。
  class Request(val processor: Int, //是 Processor 线程的序号
                val context: RequestContext, //context 是用来标识请求上下文信息的
                val startTimeNanos: Long, //startTimeNanos 记录了 Request 对象被创建的时间，主要用于各种时间统计指标的计算。
                val memoryPool: MemoryPool,//memoryPool 表示源码定义的一个非阻塞式的内存缓冲区，主要作用是避免 Request 对象无限使用内存。
                @volatile var buffer: ByteBuffer,//buffer 是真正保存 Request 对象内容的字节缓冲区。Request 发送方必须按照 Kafka RPC 协议规定的格式向该缓冲区写入字节，否则将抛出 InvalidRequestException 异常。这个逻辑主要是由 RequestContext 的 parseRequest 方法实现的。
                metrics: RequestChannel.Metrics,//metrics 是 Request 相关的各种监控指标的一个管理类。它里面构建了一个 Map，封装了所有的请求 JMX 指标。
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

    def sizeInBytes: Int = header.size + sizeOfBodyInBytes

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def isForwarded: Boolean = envelope.isDefined

    private def shouldReturnNotController(response: AbstractResponse): Boolean = {
      response match {
        case _: DescribeQuorumResponse => response.errorCounts.containsKey(Errors.NOT_LEADER_OR_FOLLOWER)
        case _ => response.errorCounts.containsKey(Errors.NOT_CONTROLLER)
      }
    }

    def buildResponseSend(abstractResponse: AbstractResponse): Send = {
      envelope match {
        case Some(request) =>
          val envelopeResponse = if (shouldReturnNotController(abstractResponse)) {
            // Since it's a NOT_CONTROLLER error response, we need to make envelope response with NOT_CONTROLLER error
            // to notify the requester (i.e. BrokerToControllerRequestThread) to update active controller
            new EnvelopeResponse(new EnvelopeResponseData()
              .setErrorCode(Errors.NOT_CONTROLLER.code()))
          } else {
            val responseBytes = context.buildResponseEnvelopePayload(abstractResponse)
            new EnvelopeResponse(responseBytes, Errors.NONE)
          }
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
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new AlterConfigsRequest(newData, alterConfigs.version())

        case alterConfigs: IncrementalAlterConfigsRequest =>
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new IncrementalAlterConfigsRequest.Builder(newData).build(alterConfigs.version())

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

  //定义 Response 的抽象基类。每个 Response 对象都包含了对应的 Request 对象。
  // 这个类里最重要的方法是 onComplete 方法，用来实现每类 Response 被处理后需要执行的回调逻辑。
  sealed abstract class Response(val request: Request) {//每个 Response 对象都要保存它对应的 Request 对象。

    def processor: Int = request.processor

    def responseLog: Option[JsonNode] = None

    // Scala 中的 Unit 类似于 Java 中的 void，而“Send => Unit”表示一个方法。
    // 这个方法接收一个 Send 类实例，然后执行一段代码逻辑。
    // 因此，你可以把一个函数作为一个参数传给另一个函数，也可以把函数作为结果返回。
    // 这里的 onComplete 方法就应用了第二种用法，也就是把函数赋值给另一个函数，并作为结果返回。
    // 这样做的好处在于，你可以灵活地变更 onCompleteCallback 来实现不同的回调逻辑。
    def onComplete: Option[Send => Unit] = None
  }

  /** responseLogValue should only be defined if request logging is enabled */
  // Kafka 大多数 Request 处理完成后都需要执行一段回调逻辑，SendResponse 就是保存返回结果的 Response 子类。
  // 里面最重要的字段是 onCompletionCallback，即指定处理完成之后的回调逻辑。
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseLogValue: Option[JsonNode],
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseLog: Option[JsonNode] = responseLogValue

    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseLogValue)"
  }

  // 有些 Request 处理完成后无需单独执行额外的回调逻辑。NoResponse 就是为这类 Response 准备的。
  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }
  // 用于出错后需要关闭 TCP 连接的场景，此时返回 CloseConnectionResponse 给 Request 发送方，显式地通知它关闭连接
  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  // StartThrottlingResponse：用于通知 Broker 的 Socket Server 组件（后面几节课我会讲到它）某个 TCP 连接通信通道开始被限流（throttling）。
  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  // EndThrottlingResponse：与 StartThrottlingResponse 对应，通知 Broker 的 SocketServer 组件某个 TCP 连接通信通道的限流已结束。
  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}

//传输 Request/Response 的通道。有了 Request 和 Response 的基础，下面我们可以学习 RequestChannel 类的实现了。
//RequestChannel 类实现了 KafkaMetricsGroup trait，后者封装了许多实用的指标监控方法，比如，newGauge 方法用于创建数值型的监控指标，newHistogram 方法用于创建直方图型的监控指标。
// 除了 Processor 的管理之外，RequestChannel 的另一个重要功能，是处理 Request 和 Response，具体表现为收发 Request 和发送 Response。
// 比如，收发 Request 的方法有 sendRequest 和 receiveRequest
class RequestChannel(val queueSize: Int,
                     val metricNamePrefix: String,
                     time: Time,
                     val metrics: RequestChannel.Metrics) {
  import RequestChannel._

  // 就 RequestChannel 类本身的主体功能而言，它定义了最核心的 3 个属性：requestQueue、queueSize 和 processors。
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  // 每个 RequestChannel 对象实例创建时，会定义一个队列来保存 Broker 接收到的各类请求，这个队列被称为请求队列或 Request 队列。
  // Kafka 使用 Java 提供的阻塞队列 ArrayBlockingQueue 实现这个请求队列，并利用它天然提供的线程安全性来保证多个线程能够并发安全高效地访问请求队列。
  // 在代码中，这个队列由变量requestQueue定义。而字段 queueSize 就是 Request 队列的最大长度。
  // 当 Broker 启动时，SocketServer 组件会创建 RequestChannel 对象，并把 Broker 端参数 queued.max.requests 赋值给 queueSize。
  // 因此，在默认情况下，每个 RequestChannel 上的队列长度是 500。
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)

  // 字段 processors 封装的是 RequestChannel 下辖的 Processor 线程池。
  // 每个 Processor 线程负责具体的请求处理逻辑。下面我详细说说 Processor 的管理。
  // Map 中的 Key 就是前面我们说的 processor 序号，而 Value 则对应具体的 Processor 线程对象。
  // 这个线程池的存在告诉了我们一个事实：当前 Kafka Broker 端所有网络线程都是在 RequestChannel 中维护的。
  // 既然创建了线程池，代码中必然要有管理线程池的操作。RequestChannel 中的 addProcessor 和 removeProcessor 方法就是做这些事的。
  private val processors = new ConcurrentHashMap[Int, Processor]()
  val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)

  metricsGroup.newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  metricsGroup.newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })

  //调用 ConcurrentHashMap 的 putIfAbsent 和 remove 方法分别实现增加和移除线程。
  // 每当 Broker 启动时，它都会调用 addProcessor 方法，向 RequestChannel 对象添加 num.network.threads 个 Processor 线程。
  // Kafka 官方文档的话，你就会发现，num.network.threads 这个参数的更新模式（Update Mode）是 Cluster-wide。这就说明，Kafka 允许你动态地修改此参数值。
  // 比如，Broker 启动时指定 num.network.threads 为 8，之后你通过 kafka-configs 命令将其修改为 3。显然，这个操作会减少 Processor 线程池中的线程数量。
  // 在这个场景下，removeProcessor 方法会被调用。
  def addProcessor(processor: Processor): Unit = {
    // 添加Processor到Processor线程池
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    metricsGroup.newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      // 为给定Processor对象创建对应的监控指标
      Map(ProcessorMetricTag -> processor.id.toString).asJava)
  }

  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)// 从Processor线程池中移除给定Processor线程
    metricsGroup.removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString).asJava)
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  def closeConnection(
    request: RequestChannel.Request,
    errorCounts: java.util.Map[Errors, Integer]
  ): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  // 对于 Response 而言，则没有所谓的接收 Response，只有发送 Response，即 sendResponse 方法。sendResponse 是啥意思呢？
  // 其实就是把 Response 对象发送出去，也就是将 Response 添加到 Response 队列的过程。
  def sendResponse(
    request: RequestChannel.Request,
    response: AbstractResponse,
    onComplete: Option[Send => Unit]
  ): Unit = {
    updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala)
    sendResponse(new RequestChannel.SendResponse(
      request,
      request.buildResponseSend(response),
      request.responseNode(response),
      onComplete
    ))
  }

  def sendNoOpResponse(request: RequestChannel.Request): Unit = {
    sendResponse(new network.RequestChannel.NoOpResponse(request))
  }

  def startThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new RequestChannel.StartThrottlingResponse(request))
  }

  def endThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new EndThrottlingResponse(request))
  }

  /** Send a response back to the socket server to be sent over the network */
  private[network] def sendResponse(response: RequestChannel.Response): Unit = {
    if (isTraceEnabled) {// 构造Trace日志输出字符串
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

    // 找出response对应的Processor线程，即request当初是由哪个Processor线程处理的
    // 代码会找出 Response 对象对应的 Processor 线程。
    // 当 Processor 处理完某个 Request 后，会把自己的序号封装进对应的 Response 对象。
    // 一旦找出了之前是由哪个 Processor 线程处理的，代码直接调用该 Processor 的 enqueueResponse 方法，将 Response 放入 Response 队列中，等待后续发送。
    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    // 将response对象放置到对应Processor线程的Response队列中
    if (processor != null) {
      processor.enqueueResponse(response)
    }
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

  // 每秒处理的 Request 数，用来评估 Broker 的繁忙状态。
  val RequestsPerSec = "RequestsPerSec"
  // RequestQueueTimeMs：计算 Request 在 Request 队列中的平均等候时间，单位是毫秒。
  // 倘若 Request 在队列的等待时间过长，你通常需要增加后端 I/O 线程的数量，来加快队列中 Request 的拿取速度。
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  // LocalTimeMs：计算 Request 实际被处理的时间，单位是毫秒。
  // local time是指本地broker写入磁盘的时间
  // 一旦定位到这个监控项的值很大，你就需要进一步研究 Request 被处理的逻辑了，具体分析到底是哪一步消耗了过多的时间。
  val LocalTimeMs = "LocalTimeMs"
  // RemoteTimeMs：Kafka 的读写请求（PRODUCE 请求和 FETCH 请求）逻辑涉及等待其他 Broker 操作的步骤。
  // RemoteTimeMs 计算的，就是等待其他 Broker 完成指定逻辑的时间。因为等待的是其他 Broker，因此被称为 Remote Time。
  // 这个监控项非常重要！Kafka 生产环境中设置 acks=all 的 Producer 程序发送消息延时高的主要原因，往往就是 Remote Time 高。
  // 因此，如果你也碰到了这样的问题，不妨先定位一下 Remote Time 是不是瓶颈。
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  // TotalTimeMs：计算 Request 被处理的完整流程时间。这是最实用的监控指标，没有之一！
  // 毕竟，我们通常都是根据 TotalTimeMs 来判断系统是否出现问题的。
  // 一旦发现了问题，我们才会利用前面的几个监控项进一步定位问题的原因。
  // TotalTimeMs = RequestQueueTimeMs + LocalTimeMs + RemoteTimeMs
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) {

  import RequestMetrics._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val tags = Map("request" -> name).asJava
  val requestRateInternal = new Pool[Short, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist = metricsGroup.newHistogram(RequestQueueTimeMs, true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = metricsGroup.newHistogram(LocalTimeMs, true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = metricsGroup.newHistogram(RemoteTimeMs, true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
  val throttleTimeHist = metricsGroup.newHistogram(ThrottleTimeMs, true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = metricsGroup.newHistogram(ResponseQueueTimeMs, true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = metricsGroup.newHistogram(ResponseSendTimeMs, true, tags)
  val totalTimeHist = metricsGroup.newHistogram(TotalTimeMs, true, tags)
  // request size in bytes
  val requestBytesHist = metricsGroup.newHistogram(RequestBytes, true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(metricsGroup.newHistogram(MessageConversionsTimeMs, true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(metricsGroup.newHistogram(TemporaryMemoryBytes, true, tags))
    else
      None

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter =
    requestRateInternal.getAndMaybePut(version, metricsGroup.newMeter(RequestsPerSec, "requests", TimeUnit.SECONDS, tagsWithVersion(version)))

  private def tagsWithVersion(version: Short): java.util.Map[String, String] = {
    val nameAndVersionTags = new util.HashMap[String, String](tags.size() + 1)
    nameAndVersionTags.putAll(tags)
    nameAndVersionTags.put("version", version.toString)
    nameAndVersionTags
  }

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name).asJava

    @volatile private var meter: Meter = _

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = metricsGroup.newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          metricsGroup.removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) {
      metricsGroup.removeMetric(RequestsPerSec, tagsWithVersion(version))
    }
    metricsGroup.removeMetric(RequestQueueTimeMs, tags)
    metricsGroup.removeMetric(LocalTimeMs, tags)
    metricsGroup.removeMetric(RemoteTimeMs, tags)
    metricsGroup.removeMetric(RequestsPerSec, tags)
    metricsGroup.removeMetric(ThrottleTimeMs, tags)
    metricsGroup.removeMetric(ResponseQueueTimeMs, tags)
    metricsGroup.removeMetric(TotalTimeMs, tags)
    metricsGroup.removeMetric(ResponseSendTimeMs, tags)
    metricsGroup.removeMetric(RequestBytes, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      metricsGroup.removeMetric(MessageConversionsTimeMs, tags)
      metricsGroup.removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
