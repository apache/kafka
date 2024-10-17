/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.network.SocketServer
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, RequestHeader, ResponseHeader}
import org.apache.kafka.common.utils.Utils

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer
import scala.reflect.ClassTag

object IntegrationTestUtils {

  def sendRequest(socket: Socket, request: Array[Byte]): Unit = {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length)
    outgoing.write(request)
    outgoing.flush()
  }

  private def sendWithHeader(request: AbstractRequest, header: RequestHeader, socket: Socket): Unit = {
    val serializedBytes = Utils.toArray(request.serializeWithHeader(header))
    sendRequest(socket, serializedBytes)
  }

  def nextRequestHeader[T <: AbstractResponse](apiKey: ApiKeys,
                                               apiVersion: Short,
                                               clientId: String = "client-id",
                                               correlationIdOpt: Option[Int] = None): RequestHeader = {
    val correlationId = correlationIdOpt.getOrElse {
      this.correlationId += 1
      this.correlationId
    }
    new RequestHeader(apiKey, apiVersion, clientId, correlationId)
  }

  def send(request: AbstractRequest,
           socket: Socket,
           clientId: String = "client-id",
           correlationId: Option[Int] = None): Unit = {
    val header = nextRequestHeader(request.apiKey, request.version, clientId, correlationId)
    sendWithHeader(request, header, socket)
  }

  def receive[T <: AbstractResponse](socket: Socket, apiKey: ApiKeys, version: Short)
                                    (implicit classTag: ClassTag[T]): T = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()

    val responseBytes = new Array[Byte](len)
    incoming.readFully(responseBytes)

    val responseBuffer = ByteBuffer.wrap(responseBytes)
    ResponseHeader.parse(responseBuffer, apiKey.responseHeaderVersion(version))

    AbstractResponse.parseResponse(apiKey, responseBuffer, version) match {
      case response: T => response
      case response =>
        throw new ClassCastException(s"Expected response with type ${classTag.runtimeClass}, but found ${response.getClass}")
    }
  }

  def sendAndReceive[T <: AbstractResponse](request: AbstractRequest,
                                            socket: Socket,
                                            clientId: String = "client-id",
                                            correlationId: Option[Int] = None)
                                           (implicit classTag: ClassTag[T]): T = {
    send(request, socket, clientId, correlationId)
    receive[T](socket, request.apiKey, request.version)
  }

  def connectAndReceive[T <: AbstractResponse](request: AbstractRequest,
                                               destination: SocketServer,
                                               listenerName: ListenerName)
                                              (implicit classTag: ClassTag[T]): T = {
    val socket = connect(destination, listenerName)
    try sendAndReceive[T](request, socket)
    finally socket.close()
  }

  private var correlationId = 0

  def connect(socketServer: SocketServer,
              listenerName: ListenerName): Socket = {
    new Socket("localhost", socketServer.boundPort(listenerName))
  }
}
