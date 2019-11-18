package kafka.server

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.Collections

import kafka.cluster.Broker
import kafka.utils.{Logging, ShutdownableThread}
import org.apache.kafka.clients._
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, MetadataRequest, MetadataResponse}
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{KafkaFuture, Node}
import org.apache.kafka.common.internals.{KafkaFutureImpl, Topic}
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.JaasContext

import scala.collection.JavaConverters._

class BrokerToControllerChannelManager(metadataCache: MetadataCache,
                                       time: Time,
                                       metrics: Metrics,
                                       config: KafkaConfig,
                                       threadNamePrefix: Option[String] = None) extends Logging {

  private val requestQueue = new LinkedBlockingQueue[BrokerToControllerQueueItem]
  private val logContext = new LogContext(s"[broker-${config.brokerId}-to-controller] ")
  private val manualMetadataUpdater = new ManualMetadataUpdater()
  private lazy val requestThread = newRequestThread

  def shutdown(): Unit = {
    requestThread.shutdown()
    requestThread.awaitShutdown()
  }

  def getPartitionCount(topicName: String): KafkaFuture[Int] = {
    val topic = new MetadataRequestTopic().setName(Topic.GROUP_METADATA_TOPIC_NAME)
    val completionFuture = new KafkaFutureImpl[Int]
    sendRequest(new MetadataRequest.Builder(
      new MetadataRequestData().setTopics(Collections.singletonList(topic))), response => {
      val metadataResponse = response.asInstanceOf[MetadataResponse]
      val topicOpt = metadataResponse.topicMetadata().asScala.find(t => topicName.equals(t.topic))
      if (topicOpt.isEmpty) {
        completionFuture.completeExceptionally(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception)
      } else if (topicOpt.get.error != Errors.NONE) {
        completionFuture.completeExceptionally(topicOpt.get.error.exception)
      } else {
        completionFuture.complete(topicOpt.get.partitionMetadata.size)
      }
    })
    completionFuture
  }

  private[server] def newRequestThread = {
    val brokerToControllerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val brokerToControllerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)

    val networkClient = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        brokerToControllerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        brokerToControllerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "BrokerToControllerChannel",
        Map("BrokerId" -> config.brokerId.toString).asJava,
        false,
        channelBuilder,
        logContext
      )
      new NetworkClient(
        selector,
        manualMetadataUpdater,
        config.brokerId.toString,
        1,
        0,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        ClientDnsLookup.DEFAULT,
        time,
        false,
        new ApiVersions,
        logContext
      )
    }
    val threadName = threadNamePrefix match {
      case None => s"broker-${config.brokerId}-to-controller-send-thread"
      case Some(name) => s"$name:broker-${config.brokerId}-to-controller-send-thread"
    }

    new BrokerToControllerRequestThread(networkClient, manualMetadataUpdater, requestQueue, metadataCache, config,
      brokerToControllerListenerName, time, threadName)
  }

  private[server] def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    // create and start the thread lazily
    if (!requestThread.isAlive) {
      info(s"Starting ${requestThread.name}")
      requestThread.start()
    }
    requestQueue.put(BrokerToControllerQueueItem(request, callback))
  }
}

case class BrokerToControllerQueueItem(request: AbstractRequest.Builder[_ <: AbstractRequest],
                     callback: AbstractResponse => Unit)

class BrokerToControllerRequestThread(networkClient: KafkaClient,
                                      metadataUpdater: ManualMetadataUpdater,
                                      requestQueue: BlockingQueue[BrokerToControllerQueueItem],
                                      metadataCache: MetadataCache,
                                      config: KafkaConfig,
                                      listenerName: ListenerName,
                                      time: Time,
                                      threadName: String) extends ShutdownableThread(threadName) {

  private val socketTimeoutMs = config.controllerSocketTimeoutMs
  private var activeController: Option[Node] = None

  private[server] def maybeGetActiveController(): Option[Broker] = {
    metadataCache.getControllerId.flatMap(b => metadataCache.getAliveBroker(b))
  }

  private[server] def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

  override def doWork(): Unit = {
    // just peek the top, will take if the request was successful
    if (requestQueue.peek() == null) {
      backoff()
    } else {
      val BrokerToControllerQueueItem(request, callback) = requestQueue.peek()
      try {
        var clientResponse: ClientResponse = null
        if (controllerReady()) {
          trace(s"Sending request: $request")
          val clientRequest = networkClient.newClientRequest(activeController.get.idString, request,
            time.milliseconds(), true)
          clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
          if (!clientResponse.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
            requestQueue.take()
          } else { // TODO: test this case
            networkClient.close(activeController.get.idString)
            activeController = None
          }
        } else {
          info("Controller isn't cached, looking for local metadata changes")
          val controllerOpt = metadataCache.getControllerId.flatMap(metadataCache.getAliveBroker)
          if (controllerOpt.isDefined) {
            if (activeController.isEmpty || activeController.exists(_.id != controllerOpt.get.id))
              info(s"Recorded new controller, from now on will use broker ${controllerOpt.get.id}")
            else if (activeController.isDefined) // close the old controller connection
              networkClient.close(activeController.get.idString)
            activeController = Option(controllerOpt.get.node(listenerName))
            metadataUpdater.setNodes(metadataCache.getAliveBrokers.map(_.node(listenerName)).asJava)
          } else {
            warn("No controller defined in metadata cache, retrying after backoff")
            backoff()
          }
        }
        if (clientResponse != null) {
          if (callback != null) {
            callback(clientResponse.responseBody())
          }
        }
      } catch {
        case e: Throwable =>
          activeController match {
            case Some(controller) => {}
              warn(s"Broker ${config.brokerId}'s connection to active controller $controller was unsuccessful", e)
              networkClient.close(controller.idString)
            case None =>
              warn(s"No connection with controller", e)
          }
      }
    }

  }

  private[server] def controllerReady(): Boolean = {
    activeController.isDefined && NetworkClientUtils.awaitReady(networkClient, activeController.get, time, socketTimeoutMs)
  }
}
