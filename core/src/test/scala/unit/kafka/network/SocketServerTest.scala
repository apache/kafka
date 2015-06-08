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

package kafka.network;

import java.net._
import java.io._
import kafka.cluster.EndPoint
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.NetworkSend
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.utils.SystemTime
import org.junit._
import org.scalatest.junit.JUnitSuite
import java.util.Random
import junit.framework.Assert._
import kafka.producer.SyncProducerConfig
import kafka.api.ProducerRequest
import java.nio.ByteBuffer
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import java.nio.channels.SelectionKey
import kafka.utils.TestUtils
import scala.collection.Map

class SocketServerTest extends JUnitSuite {

  val server: SocketServer = new SocketServer(0,
                                              Map(SecurityProtocol.PLAINTEXT -> EndPoint(null, 0, SecurityProtocol.PLAINTEXT),
                                                  SecurityProtocol.TRACE -> EndPoint(null, 0, SecurityProtocol.TRACE)),
                                              numProcessorThreads = 1,
                                              maxQueuedRequests = 50,
                                              sendBufferSize = 300000,
                                              recvBufferSize = 300000,
                                              maxRequestSize = 50,
                                              maxConnectionsPerIp = 5,
                                              connectionsMaxIdleMs = 60*1000,
                                              maxConnectionsPerIpOverrides = Map.empty[String,Int],
                                              new SystemTime(),
                                              new Metrics())
  server.startup()

  def sendRequest(socket: Socket, id: Short, request: Array[Byte]) {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length + 2)
    outgoing.writeShort(id)
    outgoing.write(request)
    outgoing.flush()
  }

  def receiveResponse(socket: Socket): Array[Byte] = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    response
  }

  /* A simple request handler that just echos back the response */
  def processRequest(channel: RequestChannel) {
    val request = channel.receiveRequest
    val byteBuffer = ByteBuffer.allocate(request.requestObj.sizeInBytes)
    request.requestObj.writeTo(byteBuffer)
    byteBuffer.rewind()
    val send = new NetworkSend(request.connectionId, byteBuffer)
    channel.sendResponse(new RequestChannel.Response(request.processor, request, send))
  }

  def connect(s:SocketServer = server, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT) = {
    new Socket("localhost", server.boundPort(protocol))
  }


  @After
  def cleanup() {
    server.shutdown()
  }
  @Test
  def simpleRequest() {
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    val traceSocket = connect(protocol = SecurityProtocol.TRACE)
    val correlationId = -1
    val clientId = SyncProducerConfig.DefaultClientId
    val ackTimeoutMs = SyncProducerConfig.DefaultAckTimeoutMs
    val ack = SyncProducerConfig.DefaultRequiredAcks
    val emptyRequest =
      new ProducerRequest(correlationId, clientId, ack, ackTimeoutMs, collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]())

    val byteBuffer = ByteBuffer.allocate(emptyRequest.sizeInBytes)
    emptyRequest.writeTo(byteBuffer)
    byteBuffer.rewind()
    val serializedBytes = new Array[Byte](byteBuffer.remaining)
    byteBuffer.get(serializedBytes)

    // Test PLAINTEXT socket
    sendRequest(plainSocket, 0, serializedBytes)
    processRequest(server.requestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(plainSocket).toSeq)

    // Test TRACE socket
    sendRequest(traceSocket, 0, serializedBytes)
    processRequest(server.requestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(traceSocket).toSeq)
  }

  @Test
  def tooBigRequestIsRejected() {
    val tooManyBytes = new Array[Byte](server.maxRequestSize + 1)
    new Random().nextBytes(tooManyBytes)
    val socket = connect()
    sendRequest(socket, 0, tooManyBytes)
    try {
      receiveResponse(socket)
    } catch {
      case e: IOException => // thats fine
    }
  }

  @Test
  def testSocketsCloseOnShutdown() {
    // open a connection
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    val traceSocket = connect(protocol = SecurityProtocol.TRACE)
    val bytes = new Array[Byte](40)
    // send a request first to make sure the connection has been picked up by the socket server
    sendRequest(plainSocket, 0, bytes)
    sendRequest(traceSocket, 0, bytes)
    processRequest(server.requestChannel)

    // make sure the sockets are open
    server.acceptors.values.map(acceptor => assertFalse(acceptor.serverChannel.socket.isClosed))
    // then shutdown the server
    server.shutdown()

    val largeChunkOfBytes = new Array[Byte](1000000)
    // doing a subsequent send should throw an exception as the connection should be closed.
    // send a large chunk of bytes to trigger a socket flush
    try {
      sendRequest(plainSocket, 0, largeChunkOfBytes)
      fail("expected exception when writing to closed plain socket")
    } catch {
      case e: IOException => // expected
    }

    try {
      sendRequest(traceSocket, 0, largeChunkOfBytes)
      fail("expected exception when writing to closed trace socket")
    } catch {
      case e: IOException => // expected
    }
  }

  @Test
  def testMaxConnectionsPerIp() {
    // make the maximum allowable number of connections and then leak them
    val conns = (0 until server.maxConnectionsPerIp).map(i => connect())
    // now try one more (should fail)
    val conn = connect()
    conn.setSoTimeout(3000)
    assertEquals(-1, conn.getInputStream().read())
  }

  @Test
  def testMaxConnectionsPerIPOverrides(): Unit = {
    val overrideNum = 6
    val overrides: Map[String, Int] = Map("localhost" -> overrideNum)
    val overrideServer: SocketServer = new SocketServer(0,
                                                Map(SecurityProtocol.PLAINTEXT -> EndPoint(null, 0, SecurityProtocol.PLAINTEXT)),
                                                numProcessorThreads = 1,
                                                maxQueuedRequests = 50,
                                                sendBufferSize = 300000,
                                                recvBufferSize = 300000,
                                                maxRequestSize = 50,
                                                maxConnectionsPerIp = 5,
                                                connectionsMaxIdleMs = 60*1000,
                                                maxConnectionsPerIpOverrides = overrides,
                                                new SystemTime(),
                                                new Metrics())
    overrideServer.startup()
    // make the maximum allowable number of connections and then leak them
    val conns = ((0 until overrideNum).map(i => connect(overrideServer)))
    // now try one more (should fail)
    val conn = connect(overrideServer)
    conn.setSoTimeout(3000)
    assertEquals(-1, conn.getInputStream.read())
    overrideServer.shutdown()
  }
}
