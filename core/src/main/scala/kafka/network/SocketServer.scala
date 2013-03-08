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

import kafka.common.KafkaException
import kafka.utils._

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
                   val maxRequestSize: Int = Int.MaxValue) extends Logging {
  this.logIdent = "[Socket Server on Broker " + brokerId + "], "
  private val time = SystemTime
  private val processors = new Array[Processor](numProcessorThreads)
  @volatile private var acceptor: Acceptor = null
  val requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests)

  /**
   * Start the socket server
   */
  def startup() {
    for(i <- 0 until numProcessorThreads) {
      processors(i) = new Processor(i, time, maxRequestSize, requestChannel)
      Utils.newThread("kafka-processor-%d-%d".format(port, i), processors(i), false).start()
    }
    // register the processor threads for notification of responses
    requestChannel.addResponseListener((id:Int) => processors(id).wakeup())
   
    // start accepting connections
    this.acceptor = new Acceptor(host, port, processors)
    Utils.newThread("kafka-acceptor", acceptor, false).start()
    acceptor.awaitStartup
    info("started")
  }

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("shutting down")
    if(acceptor != null)
      acceptor.shutdown()
    for(processor <- processors)
      processor.shutdown()
    requestChannel.shutdown
    info("shut down completely")
  }
}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread extends Runnable with Logging {

  protected val selector = Selector.open();
  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(false)

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
  
  /**
   * Wakeup the thread for selection.
   */
  def wakeup() = selector.wakeup()
  
}

/**
 * Thread that accepts and configures new connections. There is only need for one of these
 */
private[kafka] class Acceptor(val host: String, val port: Int, private val processors: Array[Processor]) extends AbstractServerThread {
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
            case e: Throwable => error("Error in acceptor", e)
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
    val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept()
    debug("Accepted connection from " + socketChannel.socket.getInetAddress() + " on " + socketChannel.socket.getLocalSocketAddress)
    socketChannel.configureBlocking(false)
    socketChannel.socket().setTcpNoDelay(true)
    processor.accept(socketChannel)
  }

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
private[kafka] class Processor(val id: Int,
                               val time: Time,
                               val maxRequestSize: Int,
                               val requestChannel: RequestChannel) extends AbstractServerThread {
  
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]();

  override def run() {
    startupComplete()
    while(isRunning) {
      // setup any new connections that have been queued up
      configureNewConnections()
      // register any new responses for writing
      processNewResponses()
      val startSelectTime = SystemTime.milliseconds
      val ready = selector.select(300)
      trace("Processor id " + id + " selection time = " + (SystemTime.milliseconds - startSelectTime) + " ms")
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
    }
    debug("Closing selector.")
    swallowError(selector.close())
    shutdownComplete()
  }

  private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while(curr != null) {
      val key = curr.request.requestKey.asInstanceOf[SelectionKey]
      try {
        if(curr.responseSend == null) {
          // a null response send object indicates that there is no response to send to the client.
          // In this case, we just want to turn the interest ops to READ to be able to read more pipelined requests
          // that are sitting in the server's socket buffer
          trace("Socket server received empty response to send, registering for read: " + curr)
          key.interestOps(SelectionKey.OP_READ)
          key.attach(null)
          curr.request.updateRequestMetrics
        } else {
          trace("Socket server received response to send, registering for write: " + curr)
          key.interestOps(SelectionKey.OP_WRITE)
          key.attach(curr)
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
  
  private def close(key: SelectionKey) {
    val channel = key.channel.asInstanceOf[SocketChannel]
    debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
    swallowError(channel.socket().close())
    swallowError(channel.close())
    key.attach(null)
    swallowError(key.cancel())
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

}
