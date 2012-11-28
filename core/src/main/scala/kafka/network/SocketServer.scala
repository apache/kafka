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
import java.util.concurrent.atomic._
import java.net._
import java.io._
import java.nio.channels._

import kafka.utils._

import org.apache.log4j.Logger
import kafka.api.RequestKeys

/**
 * An NIO socket server. The thread model is
 *   1 Acceptor thread that handles new connections
 *   N Processor threads that each have their own selectors and handle all requests from their connections synchronously
 */
class SocketServer(val port: Int,
                   val numProcessorThreads: Int,
                   monitoringPeriodSecs: Int,
                   private val handlerFactory: Handler.HandlerMapping,
                   val sendBufferSize: Int,
                   val receiveBufferSize: Int,
                   val maxRequestSize: Int = Int.MaxValue) {

  private val time = SystemTime
  private val processors = new Array[Processor](numProcessorThreads)
  private var acceptor: Acceptor = new Acceptor(port, processors, sendBufferSize, receiveBufferSize)
  val stats: SocketServerStats = new SocketServerStats(1000L * 1000L * 1000L * monitoringPeriodSecs)

  /**
   * Start the socket server
   */
  def startup() {
    for(i <- 0 until numProcessorThreads) {
      processors(i) = new Processor(handlerFactory, time, stats, maxRequestSize)
      Utils.newThread("kafka-processor-" + i, processors(i), false).start()
    }
    Utils.newThread("kafka-acceptor", acceptor, false).start()
    acceptor.awaitStartup
  }

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    acceptor.shutdown
    for(processor <- processors)
      processor.shutdown
  }

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread extends Runnable {

  protected val selector = Selector.open();
  protected val logger = Logger.getLogger(getClass())
  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(false)

  /**
   * Initiates a graceful shutdown by signeling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    selector.wakeup
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
    alive.set(true)
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

}

/**
 * Thread that accepts and configures new connections. There is only need for one of these
 */
private[kafka] class Acceptor(val port: Int, private val processors: Array[Processor], val sendBufferSize: Int, val receiveBufferSize: Int) extends AbstractServerThread {

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket.bind(new InetSocketAddress(port))
    serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    logger.info("Awaiting connections on port " + port)
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
            case e: Throwable => logger.error("Error in acceptor", e)
          }
        }
      }
    }
    logger.debug("Closing server socket and selector.")
    Utils.swallow(logger.error, serverChannel.close())
    Utils.swallow(logger.error, selector.close())
    shutdownComplete()
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    serverSocketChannel.socket().setReceiveBufferSize(receiveBufferSize)
    
    val socketChannel = serverSocketChannel.accept()
    socketChannel.configureBlocking(false)
    socketChannel.socket().setTcpNoDelay(true)
    socketChannel.socket().setSendBufferSize(sendBufferSize)

    if (logger.isDebugEnabled()) {
      logger.debug("sendBufferSize: [" + socketChannel.socket().getSendBufferSize() 
          + "] receiveBufferSize: [" + socketChannel.socket().getReceiveBufferSize() + "]")
    }

    processor.accept(socketChannel)
  }
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
private[kafka] class Processor(val handlerMapping: Handler.HandlerMapping,
                               val time: Time,
                               val stats: SocketServerStats,
                               val maxRequestSize: Int) extends AbstractServerThread {

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]();
  private val requestLogger = Logger.getLogger("kafka.request.logger")

  override def run() {
    startupComplete()
    while(isRunning) {
      // setup any new connections that have been queued up
      configureNewConnections()

      val ready = selector.select(500)
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
              logger.info("Closing socket connection to %s.".format(channelFor(key).socket.getInetAddress))
              close(key)
        }
        case e: InvalidRequestException => {
          logger.info("Closing socket connection to %s due to invalid request: %s".format(channelFor(key).socket.getInetAddress, e.getMessage))
          close(key)
            } case e: Throwable => {
              logger.error("Closing socket for " + channelFor(key).socket.getInetAddress + " because of error", e)
              close(key)
            }
          }
        }
      }
    }
    logger.debug("Closing selector.")
    Utils.swallow(logger.info, selector.close())
    shutdownComplete()
  }

  private def close(key: SelectionKey) {
    val channel = key.channel.asInstanceOf[SocketChannel]
    if(logger.isDebugEnabled)
      logger.debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
    Utils.swallow(logger.info, channel.socket().close())
    Utils.swallow(logger.info, channel.close())
    key.attach(null)
    Utils.swallow(logger.info, key.cancel())
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    selector.wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while(newConnections.size() > 0) {
      val channel = newConnections.poll()
      if(logger.isDebugEnabled())
        logger.debug("Listening to new connection from " + channel.socket.getRemoteSocketAddress)
      channel.register(selector, SelectionKey.OP_READ)
    }
  }

  /**
   * Handle a completed request producing an optional response
   */
  private def handle(key: SelectionKey, request: Receive): Option[Send] = {
    val requestTypeId = request.buffer.getShort()
    if(requestLogger.isTraceEnabled) {
      requestTypeId match {
        case RequestKeys.Produce =>
          requestLogger.trace("Handling produce request from " + channelFor(key).socket.getRemoteSocketAddress())
        case RequestKeys.Fetch =>
          requestLogger.trace("Handling fetch request from " + channelFor(key).socket.getRemoteSocketAddress())
        case RequestKeys.MultiFetch =>
          requestLogger.trace("Handling multi-fetch request from " + channelFor(key).socket.getRemoteSocketAddress())
        case RequestKeys.MultiProduce =>
          requestLogger.trace("Handling multi-produce request from " + channelFor(key).socket.getRemoteSocketAddress())
        case RequestKeys.Offsets =>
          requestLogger.trace("Handling offset request from " + channelFor(key).socket.getRemoteSocketAddress())
        case _ => throw new InvalidRequestException("No mapping found for handler id " + requestTypeId)
      }
    }
    val handler = handlerMapping(requestTypeId, request)
    if(handler == null)
      throw new InvalidRequestException("No handler found for request")
    val start = time.nanoseconds
    val maybeSend = handler(request)
    stats.recordRequest(requestTypeId, time.nanoseconds - start)
    maybeSend
  }

  /*
   * Process reads from ready sockets
   */
  def read(key: SelectionKey) {
    val socketChannel = channelFor(key)
    var request = key.attachment.asInstanceOf[Receive]
    if(key.attachment == null) {
      request = new BoundedByteBufferReceive(maxRequestSize)
      key.attach(request)
    }
    val read = request.readFrom(socketChannel)
    stats.recordBytesRead(read)
    if(logger.isTraceEnabled)
      logger.trace(read + " bytes read from " + socketChannel.socket.getRemoteSocketAddress())
    if(read < 0) {
      close(key)
      return
    } else if(request.complete) {
      val maybeResponse = handle(key, request)
      key.attach(null)
      // if there is a response, send it, otherwise do nothing
      if(maybeResponse.isDefined) {
        key.attach(maybeResponse.getOrElse(None))
        key.interestOps(SelectionKey.OP_WRITE)
      }
    } else {
      // more reading to be done
      key.interestOps(SelectionKey.OP_READ)
      selector.wakeup()
    }
  }

  /*
   * Process writes to ready sockets
   */
  def write(key: SelectionKey) {
    val response = key.attachment().asInstanceOf[Send]
    val socketChannel = channelFor(key)
    val written = response.writeTo(socketChannel)
    stats.recordBytesWritten(written)
    if(logger.isTraceEnabled)
      logger.trace(written + " bytes written to " + socketChannel.socket.getRemoteSocketAddress())
    if(response.complete) {
      key.attach(null)
      key.interestOps(SelectionKey.OP_READ)
    } else {
      key.interestOps(SelectionKey.OP_WRITE)
      selector.wakeup()
    }
  }

  private def channelFor(key: SelectionKey) = key.channel().asInstanceOf[SocketChannel]

}
