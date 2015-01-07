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
                                              host = null,
                                              port = kafka.utils.TestUtils.choosePort,
                                              numProcessorThreads = 1,
                                              maxQueuedRequests = 50,
                                              sendBufferSize = 300000,
                                              recvBufferSize = 300000,
                                              maxRequestSize = 50,
                                              maxConnectionsPerIp = 5,
                                              connectionsMaxIdleMs = 60*1000,
                                              maxConnectionsPerIpOverrides = Map.empty[String,Int])
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
    val send = new BoundedByteBufferSend(byteBuffer)
    channel.sendResponse(new RequestChannel.Response(request.processor, request, send))
  }

  def connect(s:SocketServer = server) = new Socket("localhost", s.port)

  @After
  def cleanup() {
    server.shutdown()
  }
  @Test
  def simpleRequest() {
    val socket = connect()
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

    sendRequest(socket, 0, serializedBytes)
    processRequest(server.requestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(socket).toSeq)
  }

  @Test(expected = classOf[IOException])
  def tooBigRequestIsRejected() {
    val tooManyBytes = new Array[Byte](server.maxRequestSize + 1)
    new Random().nextBytes(tooManyBytes)
    val socket = connect()
    sendRequest(socket, 0, tooManyBytes)
    receiveResponse(socket)
  }

  @Test
  def testNullResponse() {
    val socket = connect()
    val bytes = new Array[Byte](40)
    sendRequest(socket, 0, bytes)

    val request = server.requestChannel.receiveRequest
    // Since the response is not sent yet, the selection key should not be readable.
    TestUtils.waitUntilTrue(
      () => { (request.requestKey.asInstanceOf[SelectionKey].interestOps & SelectionKey.OP_READ) != SelectionKey.OP_READ },
      "Socket key shouldn't be available for read")

    server.requestChannel.sendResponse(new RequestChannel.Response(0, request, null))

    // After the response is sent to the client (which is async and may take a bit of time), the socket key should be available for reads.
    TestUtils.waitUntilTrue(
      () => { (request.requestKey.asInstanceOf[SelectionKey].interestOps & SelectionKey.OP_READ) == SelectionKey.OP_READ },
      "Socket key should be available for reads")
  }

  @Test(expected = classOf[IOException])
  def testSocketsCloseOnShutdown() {
    // open a connection
    val socket = connect()
    val bytes = new Array[Byte](40)
    // send a request first to make sure the connection has been picked up by the socket server
    sendRequest(socket, 0, bytes)
    processRequest(server.requestChannel)
    // then shutdown the server
    server.shutdown()
    // doing a subsequent send should throw an exception as the connection should be closed.
    sendRequest(socket, 0, bytes)
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
                                                host = null,
                                                port = kafka.utils.TestUtils.choosePort,
                                                numProcessorThreads = 1,
                                                maxQueuedRequests = 50,
                                                sendBufferSize = 300000,
                                                recvBufferSize = 300000,
                                                maxRequestSize = 50,
                                                maxConnectionsPerIp = 5,
                                                connectionsMaxIdleMs = 60*1000,
                                                maxConnectionsPerIpOverrides = overrides)
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
