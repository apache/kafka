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

import kafka.api.IntegrationTestHarness
import kafka.network.SocketServer
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractRequestResponse, RequestHeader, ResponseHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol

abstract class BaseRequestTest extends IntegrationTestHarness {
  private var correlationId = 0

  // If required, set number of brokers
  override def brokerCount: Int = 3

  // If required, override properties by mutating the passed Properties object
  protected def brokerPropertyOverrides(properties: Properties) {}

  override def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach { p =>
      p.put(KafkaConfig.ControlledShutdownEnableProp, "false")
      brokerPropertyOverrides(p)
    }
  }

  def anySocketServer = {
    servers.find { server =>
      val state = server.brokerState.currentState
      state != NotRunning.state && state != BrokerShuttingDown.state
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No live broker is available"))
  }

  def controllerSocketServer = {
    servers.find { server =>
      server.kafkaController.isActive
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No controller broker is available"))
  }

  def notControllerSocketServer = {
    servers.find { server =>
      !server.kafkaController.isActive
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No non-controller broker is available"))
  }

  def brokerSocketServer(brokerId: Int) = {
    servers.find { server =>
      server.config.brokerId == brokerId
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"Could not find broker with id $brokerId"))
  }

  def connect(s: SocketServer = anySocketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Socket = {
    new Socket("localhost", s.boundPort(ListenerName.forSecurityProtocol(protocol)))
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
    * @param destination An optional SocketServer ot send the request to. If not set, any available server is used.
    * @param protocol An optional SecurityProtocol to use. If not set, PLAINTEXT is used.
    * @return A ByteBuffer containing the response (without the response header)
    */
  def connectAndSend(request: AbstractRequest, apiKey: ApiKeys,
                     destination: SocketServer = anySocketServer,
                     apiVersion: Option[Short] = None,
                     protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): ByteBuffer = {
    val socket = connect(destination, protocol)
    try sendAndReceive(request, apiKey, socket, apiVersion)
    finally socket.close()
  }

  /**
    * @param destination An optional SocketServer ot send the request to. If not set, any available server is used.
    * @param protocol An optional SecurityProtocol to use. If not set, PLAINTEXT is used.
    * @return A ByteBuffer containing the response (without the response header).
    */
  def connectAndSendStruct(requestStruct: Struct, apiKey: ApiKeys, apiVersion: Short,
                           destination: SocketServer = anySocketServer,
                           protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): ByteBuffer = {
    val socket = connect(destination, protocol)
    try sendStructAndReceive(requestStruct, apiKey, socket, apiVersion)
    finally socket.close()
  }

  /**
    * Serializes and sends the request to the given api.
    */
  def send(request: AbstractRequest, apiKey: ApiKeys, socket: Socket, apiVersion: Option[Short] = None): Unit = {
    val header = nextRequestHeader(apiKey, apiVersion.getOrElse(request.version))
    val serializedBytes = request.serialize(header).array
    sendRequest(socket, serializedBytes)
  }

  /**
   * Receive response and return a ByteBuffer containing response without the header
   */
  def receive(socket: Socket): ByteBuffer = {
    val response = receiveResponse(socket)
    skipResponseHeader(response)
  }

  /**
    * Serializes and sends the request to the given api.
    * A ByteBuffer containing the response is returned.
    */
  def sendAndReceive(request: AbstractRequest, apiKey: ApiKeys, socket: Socket, apiVersion: Option[Short] = None): ByteBuffer = {
    send(request, apiKey, socket, apiVersion)
    val response = receiveResponse(socket)
    skipResponseHeader(response)
  }

  /**
   * Sends a request built by the builder, waits for the response and parses it 
   */
  def requestResponse(socket: Socket, clientId: String, correlationId: Int, requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): Struct = {
    val apiKey = requestBuilder.apiKey
    val request = requestBuilder.build()
    val header = new RequestHeader(apiKey, request.version, clientId, correlationId)
    val response = requestAndReceive(socket, request.serialize(header).array)
    val responseBuffer = skipResponseHeader(response)
    apiKey.parseResponse(request.version, responseBuffer)
  }
  
  /**
    * Serializes and sends the requestStruct to the given api.
    * A ByteBuffer containing the response (without the response header) is returned.
    */
  def sendStructAndReceive(requestStruct: Struct, apiKey: ApiKeys, socket: Socket, apiVersion: Short): ByteBuffer = {
    val header = nextRequestHeader(apiKey, apiVersion)
    val serializedBytes = AbstractRequestResponse.serialize(header.toStruct, requestStruct).array
    val response = requestAndReceive(socket, serializedBytes)
    skipResponseHeader(response)
  }

  protected def skipResponseHeader(response: Array[Byte]): ByteBuffer = {
    val responseBuffer = ByteBuffer.wrap(response)
    // Parse the header to ensure its valid and move the buffer forward
    ResponseHeader.parse(responseBuffer)
    responseBuffer
  }

  def nextRequestHeader(apiKey: ApiKeys, apiVersion: Short): RequestHeader = {
    correlationId += 1
    new RequestHeader(apiKey, apiVersion, "client-id", correlationId)
  }

}
