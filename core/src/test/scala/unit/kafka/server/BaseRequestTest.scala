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
import org.apache.kafka.common.protocol.{ApiKeys, ProtoUtils, SecurityProtocol}
import org.apache.kafka.common.requests.{AbstractRequest, RequestHeader, ResponseHeader}
import org.junit.Before

abstract class BaseRequestTest extends KafkaServerTestHarness {
  private var correlationId = 0

  // If required, set number of brokers
  protected def numBrokers: Int = 3

  // If required, override properties by mutating the passed Properties object
  protected def propertyOverrides(properties: Properties) {}

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect,
      enableControlledShutdown = false, enableDeleteTopic = true,
      interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = saslProperties)
    props.foreach(propertyOverrides)
    props.map(KafkaConfig.fromProps)
  }

  def anySocketServer = {
    servers.find { server =>
      val state = server.brokerState.currentState
      state != NotRunning.state && state != BrokerShuttingDown.state
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No live broker is available"))
  }

  def controllerSocketServer = {
    servers.find { server =>
      server.kafkaController.isActive()
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No controller broker is available"))
  }

  def notControllerSocketServer = {
    servers.find { server =>
      !server.kafkaController.isActive()
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No non-controller broker is available"))
  }

  def brokerSocketServer(brokerId: Int) = {
    servers.find { server =>
      server.config.brokerId == brokerId
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"Could not find broker with id $brokerId"))
  }

  def connect(s: SocketServer = anySocketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Socket = {
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

  /**
    *
    * @param request
    * @param apiKey
    * @param version An optional version to use when sending the request. If not set, the latest known version is used
    * @param destination An optional SocketServer ot send the request to. If not set, any available server is used.
    * @param protocol An optional SecurityProtocol to use. If not set, PLAINTEXT is used.
    * @return
    */
  def send(request: AbstractRequest, apiKey: ApiKeys, version: Option[Short] = None,
           destination: SocketServer = anySocketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): ByteBuffer = {
    val requestVersion = version.getOrElse(ProtoUtils.latestVersion(apiKey.id))
    val socket = connect(destination, protocol)
    try {
      send(request, apiKey, requestVersion, socket)
    } finally {
      socket.close()
    }
  }

  /**
    * Serializes and send the request to the given api.
    * A ByteBuffer containing the response is returned.
    */
  def send(request: AbstractRequest, apiKey: ApiKeys, version: Short, socket: Socket): ByteBuffer = {
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
