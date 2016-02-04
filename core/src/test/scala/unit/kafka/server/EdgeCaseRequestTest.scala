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

import java.io.{DataInputStream, DataOutputStream, File}
import java.net.Socket
import java.nio.ByteBuffer
import java.util.{HashMap, Properties}

import kafka.api.ApiUtils._

import kafka.network.SocketServer
import kafka.utils.TestUtils._
import kafka.utils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.{RequestHeader, ProduceResponse, ResponseHeader, ProduceRequest}
import org.apache.log4j.{Level, Logger}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class EdgeCaseRequestTest extends ZooKeeperTestHarness {
  var logDir: File = null
  var server: KafkaServer = null
  var socketServer: SocketServer = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
  private val kafkaApisLogger = Logger.getLogger(classOf[KafkaApis])

  @Before
  override def setUp() {
    super.setUp()
    val config: Properties = createBrokerConfig(1, zkConnect)
    config.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, "false")
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)
    server = TestUtils.createServer(KafkaConfig.fromProps(config))
    socketServer = server.socketServer
    requestHandlerLogger.setLevel(Level.DEBUG)
    kafkaApisLogger.setLevel(Level.DEBUG)
  }

  @After
  override def tearDown() {
    server.shutdown()
    CoreUtils.rm(logDir)
    super.tearDown()
  }

  private def connect(s: SocketServer = socketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Socket = {
    new Socket("localhost", server.boundPort(protocol))
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
    socket.isInputShutdown
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    response
  }

  private def requestHeaderBytes(apiKey: Short, apiVersion: Short, clientId: String = "", correlationId: Int = -1): Array[Byte] = {
    val header = new RequestHeader(apiKey, apiVersion, clientId, correlationId)
    val buffer = ByteBuffer.allocate(header.sizeOf())
    header.writeTo(buffer)
    buffer.array()
  }

  @Test
  def testProduceRequestWithNullClientId() {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val correlationId = -1
    TestUtils.createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 1, servers = Seq(server))

    val serializedBytes = {
      val headerBytes = requestHeaderBytes(ApiKeys.PRODUCE.id, 1, null, correlationId)
      val messageBytes = "message".getBytes
      val request = new ProduceRequest(1, 10000, Map(topicPartition -> ByteBuffer.wrap(messageBytes)).asJava)
      val byteBuffer = ByteBuffer.allocate(headerBytes.length + request.sizeOf)
      byteBuffer.put(headerBytes)
      request.writeTo(byteBuffer)
      byteBuffer.array()
    }

    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    sendRequest(plainSocket, serializedBytes)
    val response = receiveResponse(plainSocket)
    plainSocket.close()

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
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    sendRequest(plainSocket, requestHeaderBytes(ApiKeys.PRODUCE.id, 1))

    assertEquals("The server should disconnect", -1, plainSocket.getInputStream.read())
    plainSocket.close()
  }

  @Test
  def testInvalidApiKeyRequest() {
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    sendRequest(plainSocket, requestHeaderBytes(-1, 0))

    assertEquals("The server should disconnect", -1, plainSocket.getInputStream.read())
    plainSocket.close()
  }

  @Test
  def testInvalidApiVersionRequest() {
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    sendRequest(plainSocket, requestHeaderBytes(ApiKeys.PRODUCE.id, -1))

    assertEquals("The server should disconnect", -1, plainSocket.getInputStream.read())
    plainSocket.close()
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

    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    sendRequest(plainSocket, serializedBytes)

    assertEquals("The server should disconnect", -1, plainSocket.getInputStream.read())
    plainSocket.close()
  }
}
