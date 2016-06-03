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
import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.network.SocketServer
import kafka.utils._
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.{AbstractRequest, RequestHeader, ResponseHeader}
import org.junit.Before

abstract class BaseRequestTest extends KafkaServerTestHarness {
  private var correlationId = 0

  // If required, set number of brokers
  protected def numBrokers: Int = 3

  // If required, override properties by mutating the passed Properties object
  protected def propertyOverrides(properties: Properties) {}

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, enableControlledShutdown = false,
      interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = saslProperties)
    props.foreach(propertyOverrides)
    props.map(KafkaConfig.fromProps)
  }

  @Before
  override def setUp() {
    super.setUp()
    TestUtils.waitUntilTrue(() => servers.head.metadataCache.getAliveBrokers.size == numBrokers, "Wait for cache to update")
  }

  def socketServer = {
    servers.find { server =>
      val state = server.brokerState.currentState
      state != NotRunning.state && state != BrokerShuttingDown.state
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No live broker is available"))
  }

  def connect(s: SocketServer = socketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Socket = {
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

  def requestAndReceive(socket: Socket, request: Array[Byte]): Array[Byte] = {
    sendRequest(socket, request)
    receiveResponse(socket)
  }

  def send(request: AbstractRequest, apiKey: ApiKeys, version: Short): ByteBuffer = {
    val socket = connect()
    try {
      send(socket, request, apiKey, version)
    } finally {
      socket.close()
    }
  }

  /**
    * Serializes and send the request to the given api. A ByteBuffer containing the response is returned.
    */
  def send(socket: Socket, request: AbstractRequest, apiKey: ApiKeys, version: Short): ByteBuffer = {
    correlationId += 1
    val serializedBytes = {
      val header = new RequestHeader(apiKey.id, version, "", correlationId)
      val byteBuffer = ByteBuffer.allocate(header.sizeOf() + request.sizeOf)
      header.writeTo(byteBuffer)
      request.writeTo(byteBuffer)
      byteBuffer.array()
    }

    val response = requestAndReceive(socket, serializedBytes)

    val responseBuffer = ByteBuffer.wrap(response)
    ResponseHeader.parse(responseBuffer) // Parse the header to ensure its valid and move the buffer forward
    responseBuffer
  }
}
