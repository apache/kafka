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

import java.net.InetSocketAddress
import java.nio.channels._

import kafka.api.RequestOrResponse
import kafka.utils.{Logging, nonthreadsafe}
import org.apache.kafka.common.network.NetworkReceive


object BlockingChannel{
  val UseDefaultBufferSize = -1
}

/**
 *  A simple blocking channel with timeouts correctly enabled.
 *
 */
@nonthreadsafe
class BlockingChannel( val host: String, 
                       val port: Int, 
                       val readBufferSize: Int, 
                       val writeBufferSize: Int, 
                       val readTimeoutMs: Int ) extends Logging {
  private var connected = false
  private var channel: SocketChannel = null
  private var readChannel: ReadableByteChannel = null
  private var writeChannel: GatheringByteChannel = null
  private val lock = new Object()
  private val connectTimeoutMs = readTimeoutMs
  private var connectionId: String = ""

  def connect() = lock synchronized  {
    if(!connected) {
      try {
        channel = SocketChannel.open()
        if(readBufferSize > 0)
          channel.socket.setReceiveBufferSize(readBufferSize)
        if(writeBufferSize > 0)
          channel.socket.setSendBufferSize(writeBufferSize)
        channel.configureBlocking(true)
        channel.socket.setSoTimeout(readTimeoutMs)
        channel.socket.setKeepAlive(true)
        channel.socket.setTcpNoDelay(true)
        channel.socket.connect(new InetSocketAddress(host, port), connectTimeoutMs)

        writeChannel = channel
        // Need to create a new ReadableByteChannel from input stream because SocketChannel doesn't implement read with timeout
        // See: http://stackoverflow.com/questions/2866557/timeout-for-socketchannel-doesnt-work
        readChannel = Channels.newChannel(channel.socket().getInputStream)
        connected = true
        val localHost = channel.socket.getLocalAddress.getHostAddress
        val localPort = channel.socket.getLocalPort
        val remoteHost = channel.socket.getInetAddress.getHostAddress
        val remotePort = channel.socket.getPort
        connectionId = localHost + ":" + localPort + "-" + remoteHost + ":" + remotePort
        // settings may not match what we requested above
        val msg = "Created socket with SO_TIMEOUT = %d (requested %d), SO_RCVBUF = %d (requested %d), SO_SNDBUF = %d (requested %d), connectTimeoutMs = %d."
        debug(msg.format(channel.socket.getSoTimeout,
                         readTimeoutMs,
                         channel.socket.getReceiveBufferSize, 
                         readBufferSize,
                         channel.socket.getSendBufferSize,
                         writeBufferSize,
                         connectTimeoutMs))

      } catch {
        case e: Throwable => disconnect()
      }
    }
  }
  
  def disconnect() = lock synchronized {
    if(channel != null) {
      swallow(channel.close())
      swallow(channel.socket.close())
      channel = null
      writeChannel = null
    }
    // closing the main socket channel *should* close the read channel
    // but let's do it to be sure.
    if(readChannel != null) {
      swallow(readChannel.close())
      readChannel = null
    }
    connected = false
  }

  def isConnected = connected

  def send(request: RequestOrResponse): Long = {
    if(!connected)
      throw new ClosedChannelException()

    val send = new RequestOrResponseSend(connectionId, request)
    send.writeCompletely(writeChannel)
  }
  
  def receive(): NetworkReceive = {
    if(!connected)
      throw new ClosedChannelException()

    val response = readCompletely(readChannel)
    response.payload().rewind()

    response
  }

  private def readCompletely(channel: ReadableByteChannel): NetworkReceive = {
    val response = new NetworkReceive
    while (!response.complete())
      response.readFromReadableChannel(channel)
    response
  }

}
