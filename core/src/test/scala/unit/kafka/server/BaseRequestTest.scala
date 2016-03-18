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

package kafka.server

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer

import kafka.integration.KafkaServerTestHarness
import kafka.network.SocketServer
import kafka.utils._
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.{ResponseHeader, RequestHeader, AbstractRequest}
import org.junit.Before

abstract class BaseRequestTest extends KafkaServerTestHarness {
  val numBrokers = 3
  private var correlationId = 0

  @Before
  override def setUp() {
    super.setUp()
    TestUtils.waitUntilTrue(() => servers.head.metadataCache.getAliveBrokers.size == numBrokers, "Wait for cache to update")
  }

  def socketServer = {
    servers.head.socketServer
  }

  private def connect(s: SocketServer = socketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Socket = {
    new Socket("localhost", s.boundPort(protocol))
  }

  private def sendRequest(socket: Socket, request: Array[Byte]) {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length)
    outgoing.write(request)
    outgoing.flush()
  }

  private def receiveResponse(socket: Socket): Array[Byte] = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    response
  }

  private def requestAndReceive(request: Array[Byte]): Array[Byte] = {
    val plainSocket = connect()
    try {
      sendRequest(plainSocket, request)
      receiveResponse(plainSocket)
    } finally {
      plainSocket.close()
    }
  }

  /**
    * Serializes and send the request to the given api. A ByteBuffer containing the response is returned.
    */
  def send(request: AbstractRequest, apiKey: ApiKeys, version: Short): ByteBuffer = {
    correlationId = correlationId + 1
    val serializedBytes = {
      val header = new RequestHeader(apiKey.id, version, "", correlationId)
      val byteBuffer = ByteBuffer.allocate(header.sizeOf() + request.sizeOf)
      header.writeTo(byteBuffer)
      request.writeTo(byteBuffer)
      byteBuffer.array()
    }

    val response = requestAndReceive(serializedBytes)

    val responseBuffer = ByteBuffer.wrap(response)
    val responseHeader = ResponseHeader.parse(responseBuffer)
    responseBuffer
  }
}
