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
import java.nio._
import java.nio.channels._
import org.junit._
import junit.framework.Assert._
import org.scalatest.junit.JUnitSuite
import kafka.utils.TestUtils
import kafka.network._
import java.util.Random
import org.apache.log4j._

class SocketServerTest extends JUnitSuite {

  val server: SocketServer = new SocketServer(port = TestUtils.choosePort, 
                                              numProcessorThreads = 1, 
                                              monitoringPeriodSecs = 30, 
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
    val id = request.request.buffer.getShort
    val send = new BoundedByteBufferSend(request.request.buffer.slice)
    channel.sendResponse(new RequestChannel.Response(request.processor, request.requestKey, send, request.start, 15))
  }

  def connect() = new Socket("localhost", server.port)

  @After
  def cleanup() {
    server.shutdown()
  }

  @Test
  def simpleRequest() {
    val socket = connect()
    sendRequest(socket, 0, "hello".getBytes)
    processRequest(server.requestChannel)
    val response = new String(receiveResponse(socket))
    assertEquals("hello", response)
  }

  @Test(expected=classOf[IOException])
  def tooBigRequestIsRejected() {
    val tooManyBytes = new Array[Byte](server.maxRequestSize + 1)
    new Random().nextBytes(tooManyBytes)
    val socket = connect()
    sendRequest(socket, 0, tooManyBytes)
    receiveResponse(socket)
  }

}
