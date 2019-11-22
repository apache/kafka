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
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, RequestHeader, ResponseHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.Seq

abstract class BaseRequestTest extends IntegrationTestHarness {
  private var correlationId = 0

  // If required, set number of brokers
  override def brokerCount: Int = 3

  // If required, override properties by mutating the passed Properties object
  protected def brokerPropertyOverrides(properties: Properties): Unit = {}

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

  private def sendRequest(socket: Socket, request: Array[Byte]): Unit = {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length)
    outgoing.write(request)
    outgoing.flush()
  }

  def receive(socket: Socket, apiKey: ApiKeys, version: Short): AbstractResponse = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()

    val response = new Array[Byte](len)
    incoming.readFully(response)

    val responseBuffer = ByteBuffer.wrap(response)
    ResponseHeader.parse(responseBuffer, apiKey.responseHeaderVersion(version))

    val responseStruct = apiKey.parseResponse(version, responseBuffer)
    AbstractResponse.parseResponse(apiKey, responseStruct, version)
  }

  def sendAndReceive(request: AbstractRequest,
                     socket: Socket,
                     clientId: String = "client-id",
                     correlationId: Option[Int] = None): AbstractResponse = {
    send(request, socket, clientId, correlationId)
    receive(socket, request.api, request.version)
  }

  def connectAndReceive(request: AbstractRequest,
                        destination: SocketServer = anySocketServer,
                        protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): AbstractResponse = {
    val socket = connect(destination, protocol)
    try sendAndReceive(request, socket)
    finally socket.close()
  }

  /**
    * Serializes and sends the request to the given api.
    */
  def send(request: AbstractRequest,
           socket: Socket,
           clientId: String = "client-id",
           correlationId: Option[Int] = None): Unit = {
    val header = nextRequestHeader(request.api, request.version, clientId, correlationId)
    sendWithHeader(request, header, socket)
  }

  def sendWithHeader(request: AbstractRequest, header: RequestHeader, socket: Socket): Unit = {
    val serializedBytes = request.serialize(header).array
    sendRequest(socket, serializedBytes)
  }

  def nextRequestHeader(apiKey: ApiKeys,
                        apiVersion: Short,
                        clientId: String = "client-id",
                        correlationIdOpt: Option[Int] = None): RequestHeader = {
    val correlationId = correlationIdOpt.getOrElse {
      this.correlationId += 1
      this.correlationId
    }
    new RequestHeader(apiKey, apiVersion, clientId, correlationId)
  }

}
