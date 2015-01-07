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

import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.net._
import java.io._
import java.nio.channels._

import scala.collection._

import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import com.yammer.metrics.core.{Gauge, Meter}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val brokerId: Int,
                   val host: String,
                   val port: Int,
                   val numProcessorThreads: Int,
                   val maxQueuedRequests: Int,
                   val sendBufferSize: Int,
                   val recvBufferSize: Int,
                   val maxRequestSize: Int = Int.MaxValue,
                   val maxConnectionsPerIp: Int = Int.MaxValue,
                   val connectionsMaxIdleMs: Long,
                   val maxConnectionsPerIpOverrides: Map[String, Int] ) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Socket Server on Broker " + brokerId + "], "
  private val time = SystemTime
  private val processors = new Array[Processor](numProcessorThreads)
  @volatile private var acceptor: Acceptor = null
  val requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests)

  /* a meter to track the average free capacity of the network processors */
  private val aggregateIdleMeter = newMeter("NetworkProcessorAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

  /**
   * Start the socket server
   */
  def startup() {
    val quotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
    for(i <- 0 until numProcessorThreads) {
      processors(i) = new Processor(i, 
                                    time, 
                                    maxRequestSize, 
                                    aggregateIdleMeter,
                                    newMeter("IdlePercent", "percent", TimeUnit.NANOSECONDS, Map("networkProcessor" -> i.toString)),
                                    numProcessorThreads, 
                                    requestChannel,
                                    quotas,
                                    connectionsMaxIdleMs)
      Utils.newThread("kafka-network-thread-%d-%d".format(port, i), processors(i), false).start()
    }

    newGauge("ResponsesBeingSent", new Gauge[Int] {
      def value = processors.foldLeft(0) { (total, p) => total + p.countInterestOps(SelectionKey.OP_WRITE) }
    })

    // register the processor threads for notification of responses
    requestChannel.addResponseListener((id:Int) => processors(id).wakeup())
   
    // start accepting connections
    this.acceptor = new Acceptor(host, port, processors, sendBufferSize, recvBufferSize, quotas)
    Utils.newThread("kafka-socket-acceptor", acceptor, false).start()
    acceptor.awaitStartup
    info("Started")
  }

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    if(acceptor != null)
      acceptor.shutdown()
    for(processor <- processors)
      processor.shutdown()
    info("Shutdown completed")
  }
}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  protected val selector = Selector.open();
  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(true)

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    selector.wakeup()
    shutdownLatch.await
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    startupLatch.countDown
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete() = shutdownLatch.countDown

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get
  
  /**
   * Wakeup the thread for selection.
   */
  def wakeup() = selector.wakeup()
  
  /**
   * Close the given key and associated socket
   */
  def close(key: SelectionKey) {
    if(key != null) {
      key.attach(null)
      close(key.channel.asInstanceOf[SocketChannel])
      swallowError(key.cancel())
    }
  }
  
  def close(channel: SocketChannel) {
    if(channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
  
  /**
   * Close all open connections
   */
  def closeAll() {
    // removes cancelled keys from selector.keys set
    this.selector.selectNow() 
    val iter = this.selector.keys().iterator()
    while (iter.hasNext) {
      val key = iter.next()
      close(key)
    }
  }

  def countInterestOps(ops: Int): Int = {
    var count = 0
    val it = this.selector.keys().iterator()
    while (it.hasNext) {
      if ((it.next().interestOps() & ops) != 0) {
        count += 1
      }
    }
    count
  }
}

/**
 * Thread that accepts and configures new connections. There is only need for one of these
 */
private[kafka] class Acceptor(val host: String, 
                              val port: Int, 
                              private val processors: Array[Processor],
                              val sendBufferSize: Int, 
                              val recvBufferSize: Int,
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) {
  val serverChannel = openServerSocket(host, port)

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    startupComplete()
    var currentProcessor = 0
    while(isRunning) {
      val ready = selector.select(500)
      if(ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            if(key.isAcceptable)
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
    debug("Closing server socket and selector.")
    swallowError(serverChannel.close())
    swallowError(selector.close())
    shutdownComplete()
  }
  
  /*
   * Create a server socket to listen for connections on.
   */
  def openServerSocket(host: String, port: Int): ServerSocketChannel = {
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
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostName, port))
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

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
private[kafka] class Processor(val id: Int,
                               val time: Time,
                               val maxRequestSize: Int,
                               val aggregateIdleMeter: Meter,
                               val idleMeter: Meter,
                               val totalProcessorThreads: Int,
                               val requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               val connectionsMaxIdleMs: Long) extends AbstractServerThread(connectionQuotas) {

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000
  private var currentTimeNanos = SystemTime.nanoseconds
  private val lruConnections = new util.LinkedHashMap[SelectionKey, Long]
  private var nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos

  override def run() {
    startupComplete()
    while(isRunning) {
      // setup any new connections that have been queued up
      configureNewConnections()
      // register any new responses for writing
      processNewResponses()
      val startSelectTime = SystemTime.nanoseconds
      val ready = selector.select(300)
      currentTimeNanos = SystemTime.nanoseconds
      val idleTime = currentTimeNanos - startSelectTime
      idleMeter.mark(idleTime)
      // We use a single meter for aggregate idle percentage for the thread pool.
      // Since meter is calculated as total_recorded_value / time_window and
      // time_window is independent of the number of threads, each recorded idle
      // time should be discounted by # threads.
      aggregateIdleMeter.mark(idleTime / totalProcessorThreads)

      trace("Processor id " + id + " selection time = " + idleTime + " ns")
      if(ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            if(key.isReadable)
              read(key)
            else if(key.isWritable)
              write(key)
            else if(!key.isValid)
              close(key)
            else
              throw new IllegalStateException("Unrecognized key state for processor thread.")
          } catch {
            case e: EOFException => {
              info("Closing socket connection to %s.".format(channelFor(key).socket.getInetAddress))
              close(key)
            } case e: InvalidRequestException => {
              info("Closing socket connection to %s due to invalid request: %s".format(channelFor(key).socket.getInetAddress, e.getMessage))
              close(key)
            } case e: Throwable => {
              error("Closing socket for " + channelFor(key).socket.getInetAddress + " because of error", e)
              close(key)
            }
          }
        }
      }
      maybeCloseOldestConnection
    }
    debug("Closing selector.")
    closeAll()
    swallowError(selector.close())
    shutdownComplete()
  }

  /**
   * Close the given key and associated socket
   */
  override def close(key: SelectionKey): Unit = {
    lruConnections.remove(key)
    super.close(key)
  }

  private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while(curr != null) {
      val key = curr.request.requestKey.asInstanceOf[SelectionKey]
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction => {
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            key.interestOps(SelectionKey.OP_READ)
            key.attach(null)
          }
          case RequestChannel.SendAction => {
            trace("Socket server received response to send, registering for write: " + curr)
            key.interestOps(SelectionKey.OP_WRITE)
            key.attach(curr)
          }
          case RequestChannel.CloseConnectionAction => {
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(key)
          }
          case responseCode => throw new KafkaException("No mapping found for response code " + responseCode)
        }
      } catch {
        case e: CancelledKeyException => {
          debug("Ignoring response for closed socket.")
          close(key)
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
    while(newConnections.size() > 0) {
      val channel = newConnections.poll()
      debug("Processor " + id + " listening to new connection from " + channel.socket.getRemoteSocketAddress)
      channel.register(selector, SelectionKey.OP_READ)
    }
  }

  /*
   * Process reads from ready sockets
   */
  def read(key: SelectionKey) {
    lruConnections.put(key, currentTimeNanos)
    val socketChannel = channelFor(key)
    var receive = key.attachment.asInstanceOf[Receive]
    if(key.attachment == null) {
      receive = new BoundedByteBufferReceive(maxRequestSize)
      key.attach(receive)
    }
    val read = receive.readFrom(socketChannel)
    val address = socketChannel.socket.getRemoteSocketAddress();
    trace(read + " bytes read from " + address)
    if(read < 0) {
      close(key)
    } else if(receive.complete) {
      val req = RequestChannel.Request(processor = id, requestKey = key, buffer = receive.buffer, startTimeMs = time.milliseconds, remoteAddress = address)
      requestChannel.sendRequest(req)
      key.attach(null)
      // explicitly reset interest ops to not READ, no need to wake up the selector just yet
      key.interestOps(key.interestOps & (~SelectionKey.OP_READ))
    } else {
      // more reading to be done
      trace("Did not finish reading, registering for read again on connection " + socketChannel.socket.getRemoteSocketAddress())
      key.interestOps(SelectionKey.OP_READ)
      wakeup()
    }
  }

  /*
   * Process writes to ready sockets
   */
  def write(key: SelectionKey) {
    val socketChannel = channelFor(key)
    val response = key.attachment().asInstanceOf[RequestChannel.Response]
    val responseSend = response.responseSend
    if(responseSend == null)
      throw new IllegalStateException("Registered for write interest but no response attached to key.")
    val written = responseSend.writeTo(socketChannel)
    trace(written + " bytes written to " + socketChannel.socket.getRemoteSocketAddress() + " using key " + key)
    if(responseSend.complete) {
      response.request.updateRequestMetrics()
      key.attach(null)
      trace("Finished writing, registering for read on connection " + socketChannel.socket.getRemoteSocketAddress())
      key.interestOps(SelectionKey.OP_READ)
    } else {
      trace("Did not finish writing, registering for write again on connection " + socketChannel.socket.getRemoteSocketAddress())
      key.interestOps(SelectionKey.OP_WRITE)
      wakeup()
    }
  }

  private def channelFor(key: SelectionKey) = key.channel().asInstanceOf[SocketChannel]

  private def maybeCloseOldestConnection {
    if(currentTimeNanos > nextIdleCloseCheckTime) {
      if(lruConnections.isEmpty) {
        nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos
      } else {
        val oldestConnectionEntry = lruConnections.entrySet.iterator().next()
        val connectionLastActiveTime = oldestConnectionEntry.getValue
        nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos
        if(currentTimeNanos > nextIdleCloseCheckTime) {
          val key: SelectionKey = oldestConnectionEntry.getKey
          trace("About to close the idle connection from " + key.channel.asInstanceOf[SocketChannel].socket.getRemoteSocketAddress
            + " due to being idle for " + (currentTimeNanos - connectionLastActiveTime) / 1000 / 1000 + " millis")
          close(key)
        }
      }
    }
  }

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {
  private val overrides = overrideQuotas.map(entry => (InetAddress.getByName(entry._1), entry._2))
  private val counts = mutable.Map[InetAddress, Int]()
  
  def inc(addr: InetAddress) {
    counts synchronized {
      val count = counts.getOrElse(addr, 0)
      counts.put(addr, count + 1)
      val max = overrides.getOrElse(addr, defaultMax)
      if(count >= max)
        throw new TooManyConnectionsException(addr, max)
    }
  }
  
  def dec(addr: InetAddress) {
    counts synchronized {
      val count = counts.get(addr).get
      if(count == 1)
        counts.remove(addr)
      else
        counts.put(addr, count - 1)
    }
  }
  
}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
