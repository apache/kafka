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

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{Mode, Selector => KSelector, ChannelBuilders, InvalidReceiveException}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{NonFatal, ControlThrowable}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time) extends Logging with KafkaMetricsGroup {

  private val endpoints = config.listeners
  private val numProcessorThreads = config.numNetworkThreads
  private val maxQueuedRequests = config.queuedMaxRequests
  private val totalProcessorThreads = numProcessorThreads * endpoints.size

  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)
  private val processors = new Array[Processor](totalProcessorThreads)

  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
  private var connectionQuotas: ConnectionQuotas = _

  private val allMetricNames = (0 until totalProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    new MetricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /**
   * Start the socket server
   */
  def startup() {
    this.synchronized {

      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

      val channelConfigs = config.channelConfigs
      val sendBufferSize = config.socketSendBufferBytes
      val recvBufferSize = config.socketReceiveBufferBytes
      val maxRequestSize = config.socketRequestMaxBytes
      val connectionsMaxIdleMs = config.connectionsMaxIdleMs
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      endpoints.values.foreach { endpoint =>
        val protocol = endpoint.protocolType
        val processorEndIndex = processorBeginIndex + numProcessorThreads

        for (i <- processorBeginIndex until processorEndIndex) {
          processors(i) = new Processor(i,
            time,
            maxRequestSize,
            requestChannel,
            connectionQuotas,
            connectionsMaxIdleMs,
            protocol,
            channelConfigs,
            metrics
          )
        }

        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
        acceptors.put(endpoint, acceptor)
        Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString, endpoint.port), acceptor, false).start()
        acceptor.awaitStartup()

        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map( metricName =>
          metrics.metrics().get(metricName).value()).sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      acceptors.values.foreach(_.shutdown)
      processors.foreach(_.shutdown)
    }
    info("Shutdown completed")
  }

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
    try {
      acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(true)

  def wakeup()

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete() = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   */
  def close(selector: KSelector, connectionId: String) {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel) {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private val nioSelector = NSelector.open()
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  this.synchronized {
    processors.foreach { processor =>
      Utils.newThread("kafka-network-thread-%d-%s-%d".format(brokerId, endPoint.protocolType.toString, processor.id), processor, false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          val ready = nioSelector.select(500)
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                if (key.isAcceptable)
                  accept(key, processors(currentProcessor))
                else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want the
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket().setReceiveBufferSize(recvBufferSize)
    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostName, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostName, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s. sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getInetAddress, socketChannel.socket.getLocalSocketAddress,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               protocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, Object],
                               metrics: Metrics) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val channelBuilder = ChannelBuilders.create(protocol, Mode.SERVER, channelConfigs)
  private val metricTags = new util.HashMap[String, String]()
  metricTags.put("networkProcessor", id.toString)


  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        metrics.metrics().get(new MetricName("io-wait-ratio", "socket-server-metrics", metricTags)).value()
      }
    },
    metricTags.asScala
  )

  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    channelBuilder)

  override def run() {
    startupComplete()
    while(isRunning) {
      try {
        // setup any new connections that have been queued up
        configureNewConnections()
        // register any new responses for writing
        processNewResponses()

        try {
          selector.poll(300)
        } catch {
          case e @ (_: IllegalStateException | _: IOException) =>
            error("Closing processor %s due to illegal state or IO exception".format(id))
            swallow(closeAll())
            shutdownComplete()
            throw e
          case e: InvalidReceiveException =>
            // Log warning and continue since Selector already closed the connection
            warn("Connection was closed due to invalid receive. Processor will continue handling other connections")
        }
        selector.completedReceives.asScala.foreach { receive =>
          try {
            val channel = selector.channel(receive.source)
            val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal().getName), channel.socketDescription)
            val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session, buffer = receive.payload, startTimeMs = time.milliseconds, securityProtocol = protocol)
            requestChannel.sendRequest(req)
          } catch {
            case e @ (_: InvalidRequestException | _: SchemaException) =>
              // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
              error("Closing socket for " + receive.source + " because of error", e)
              close(selector, receive.source)
          }
          selector.mute(receive.source)
        }

        selector.completedSends.asScala.foreach { send =>
          val resp = inflightResponses.remove(send.destination()).getOrElse {
            throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
          }
          resp.request.updateRequestMetrics()
          selector.unmute(send.destination())
        }

        selector.disconnected.asScala.foreach { connectionId =>
          val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
            throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
          }.remoteHost
          // the channel has been closed by the selector but the quotas still need to be updated
          connectionQuotas.dec(InetAddress.getByName(remoteHost))
        }

      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e : ControlThrowable => throw e
        case e : Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()
  }

  private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while(curr != null) {
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            selector.unmute(curr.request.connectionId)
          case RequestChannel.SendAction =>
            trace("Socket server received response to send, registering for write and sending data: " + curr)
            selector.send(curr.responseSend)
            inflightResponses += (curr.request.connectionId -> curr)
          case RequestChannel.CloseConnectionAction =>
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while(!newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {
        debug("Processor " + id + " listening to new connection from " + channel.socket.getRemoteSocketAddress)
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        selector.register(connectionId, channel)
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid socket leak. The other
        // throwables will be caught in processor and logged as uncaught exception.
        case NonFatal(e) =>
          // need to close the channel here to avoid socket leak.
          close(channel)
          error("Processor " + id + " closed connection from " + channel.getRemoteAddress, e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }


  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
