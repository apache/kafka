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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.types.Type
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.{ProduceResponse, ResponseHeader, ProduceRequest}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class EdgeCaseRequestTest extends KafkaServerTestHarness {

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, "false")
    List(KafkaConfig.fromProps(props))
  }

  private def socketServer = servers.head.socketServer

  private def connect(s: SocketServer = socketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Socket = {
    new Socket("localhost", s.boundPort(protocol))
  }

  private def sendRequest(socket: Socket, request: Array[Byte], id: Option[Short] = None) {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    id match {
      case Some(id) =>
        outgoing.writeInt(request.length + 2)
        outgoing.writeShort(id)
      case None =>
        outgoing.writeInt(request.length)
    }
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

  private def requestAndReceive(request: Array[Byte], id: Option[Short] = None): Array[Byte] = {
    val plainSocket = connect()
    try {
      sendRequest(plainSocket, request, id)
      receiveResponse(plainSocket)
    } finally {
      plainSocket.close()
    }
  }

  // Custom header serialization so that protocol assumptions are not forced
  private def requestHeaderBytes(apiKey: Short, apiVersion: Short, clientId: String = "", correlationId: Int = -1): Array[Byte] = {
    val size = {
      2 /* apiKey */ +
        2 /* version id */ +
        4 /* correlation id */ +
        Type.NULLABLE_STRING.sizeOf(clientId) /* client id */
    }

    val buffer = ByteBuffer.allocate(size)
    buffer.putShort(apiKey)
    buffer.putShort(apiVersion)
    buffer.putInt(correlationId)
    Type.NULLABLE_STRING.write(buffer, clientId)
    buffer.array()
  }

  private def verifyDisconnect(request: Array[Byte]) {
    val plainSocket = connect()
    try {
      sendRequest(plainSocket, requestHeaderBytes(-1, 0))
      assertEquals("The server should disconnect", -1, plainSocket.getInputStream.read())
    } finally {
      plainSocket.close()
    }
  }

  @Test
  def testProduceRequestWithNullClientId() {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val correlationId = -1
    TestUtils.createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 1, servers = servers)

    val serializedBytes = {
      val headerBytes = requestHeaderBytes(ApiKeys.PRODUCE.id, 2, null, correlationId)
      val messageBytes = "message".getBytes
      val request = new ProduceRequest(1, 10000, Map(topicPartition -> ByteBuffer.wrap(messageBytes)).asJava)
      val byteBuffer = ByteBuffer.allocate(headerBytes.length + request.sizeOf)
      byteBuffer.put(headerBytes)
      request.writeTo(byteBuffer)
      byteBuffer.array()
    }

    val response = requestAndReceive(serializedBytes)

    val responseBuffer = ByteBuffer.wrap(response)
    val responseHeader = ResponseHeader.parse(responseBuffer)
    val produceResponse = ProduceResponse.parse(responseBuffer)

    assertEquals("The response should parse completely", 0, responseBuffer.remaining())
    assertEquals("The correlationId should match request", correlationId, responseHeader.correlationId())
    assertEquals("One partition response should be returned", 1, produceResponse.responses().size())

    val partitionResponse = produceResponse.responses().get(topicPartition)
    assertNotNull(partitionResponse)
    assertEquals("There should be no error", 0, partitionResponse.errorCode)
  }

  @Test
  def testHeaderOnlyRequest() {
    verifyDisconnect(requestHeaderBytes(ApiKeys.PRODUCE.id, 1))
  }

  @Test
  def testInvalidApiKeyRequest() {
    verifyDisconnect(requestHeaderBytes(-1, 0))
  }

  @Test
  def testInvalidApiVersionRequest() {
    verifyDisconnect(requestHeaderBytes(ApiKeys.PRODUCE.id, -1))
  }

  @Test
  def testMalformedHeaderRequest() {
    val serializedBytes = {
      // Only send apiKey and apiVersion
      val buffer = ByteBuffer.allocate(
        2 /* apiKey */ +
          2 /* apiVersion */
      )
      buffer.putShort(ApiKeys.PRODUCE.id)
      buffer.putShort(1)
      buffer.array()
    }

    verifyDisconnect(serializedBytes)
  }
}
