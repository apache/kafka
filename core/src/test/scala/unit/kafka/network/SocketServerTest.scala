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


class SocketServerTest extends JUnitSuite {

  val server: SocketServer = new SocketServer(0,
                                              host = null,
                                              port = kafka.utils.TestUtils.choosePort,
                                              numProcessorThreads = 1,
                                              maxQueuedRequests = 50,
                                              maxRequestSize = 50)
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

  def connect() = new Socket("localhost", server.port)

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

  @Test(expected=classOf[IOException])
  def tooBigRequestIsRejected() {
    val tooManyBytes = new Array[Byte](server.maxRequestSize + 1)
    new Random().nextBytes(tooManyBytes)
    val socket = connect()
    sendRequest(socket, 0, tooManyBytes)
    receiveResponse(socket)
  }

  @Test
  def testPipelinedRequestOrdering() {
    val socket = connect()
    val correlationId = -1
    val clientId = SyncProducerConfig.DefaultClientId
    val ackTimeoutMs = SyncProducerConfig.DefaultAckTimeoutMs
    val ack: Short = 0
    val emptyRequest =
      new ProducerRequest(correlationId, clientId, ack, ackTimeoutMs, collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]())

    val byteBuffer = ByteBuffer.allocate(emptyRequest.sizeInBytes)
    emptyRequest.writeTo(byteBuffer)
    byteBuffer.rewind()
    val serializedBytes = new Array[Byte](byteBuffer.remaining)
    byteBuffer.get(serializedBytes)

    sendRequest(socket, 0, serializedBytes)
    sendRequest(socket, 0, serializedBytes)

    // here the socket server should've read only the first request completely and since the response is not sent yet
    // the selection key should not be readable
    val request = server.requestChannel.receiveRequest
    Assert.assertFalse((request.requestKey.asInstanceOf[SelectionKey].interestOps & SelectionKey.OP_READ) == SelectionKey.OP_READ)

    server.requestChannel.sendResponse(new RequestChannel.Response(0, request, null))

    // if everything is working correctly, until you send a response for the first request,
    // the 2nd request will not be read by the socket server
    val request2 = server.requestChannel.receiveRequest
    server.requestChannel.sendResponse(new RequestChannel.Response(0, request2, null))
    Assert.assertFalse((request.requestKey.asInstanceOf[SelectionKey].interestOps & SelectionKey.OP_READ) == SelectionKey.OP_READ)
  }
}
