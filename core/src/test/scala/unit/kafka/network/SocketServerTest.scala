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

import java.io._
import java.net._
import javax.net.ssl._
import java.io._
import java.nio.ByteBuffer
import java.util.Random

import kafka.api.ProducerRequest
import kafka.cluster.EndPoint
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import kafka.producer.SyncProducerConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.NetworkSend
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.utils.SystemTime
import org.junit.Assert._
import org.junit._
import org.scalatest.junit.JUnitSuite
import java.util.Random
import kafka.producer.SyncProducerConfig
import kafka.api.ProducerRequest
import java.nio.ByteBuffer
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import kafka.server.KafkaConfig
import java.nio.channels.SelectionKey
import kafka.utils.TestUtils

import scala.collection.Map

class SocketServerTest extends JUnitSuite {
  val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
  props.put("listeners", "PLAINTEXT://localhost:0,TRACE://localhost:0")
  props.put("num.network.threads", "1")
  props.put("socket.send.buffer.bytes", "300000")
  props.put("socket.receive.buffer.bytes", "300000")
  props.put("queued.max.requests", "50")
  props.put("socket.request.max.bytes", "50")
  props.put("max.connections.per.ip", "5")
  props.put("connections.max.idle.ms", "60000")
  val config: KafkaConfig = KafkaConfig.fromProps(props)
  val server: SocketServer = new SocketServer(config, new Metrics(), new SystemTime())
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
    val send = new NetworkSend(request.connectionId, byteBuffer)
    channel.sendResponse(new RequestChannel.Response(request.processor, request, send))
  }

  def connect(s:SocketServer = server, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT) = {
    new Socket("localhost", server.boundPort(protocol))
  }

  @After
  def cleanup() {
    server.shutdown()
  }

  @Test
  def simpleRequest() {
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    val traceSocket = connect(protocol = SecurityProtocol.TRACE)
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

    // Test PLAINTEXT socket
    sendRequest(plainSocket, 0, serializedBytes)
    processRequest(server.requestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(plainSocket).toSeq)

    // Test TRACE socket
    sendRequest(traceSocket, 0, serializedBytes)
    processRequest(server.requestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(traceSocket).toSeq)
  }

  @Test
  def tooBigRequestIsRejected() {
    val tooManyBytes = new Array[Byte](server.maxRequestSize + 1)
    new Random().nextBytes(tooManyBytes)
    val socket = connect()
    sendRequest(socket, 0, tooManyBytes)
    try {
      receiveResponse(socket)
    } catch {
      case e: IOException => // thats fine
    }
  }

  @Test
  def testSocketsCloseOnShutdown() {
    // open a connection
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    val traceSocket = connect(protocol = SecurityProtocol.TRACE)
    val bytes = new Array[Byte](40)
    // send a request first to make sure the connection has been picked up by the socket server
    sendRequest(plainSocket, 0, bytes)
    sendRequest(traceSocket, 0, bytes)
    processRequest(server.requestChannel)

    // make sure the sockets are open
    server.acceptors.values.map(acceptor => assertFalse(acceptor.serverChannel.socket.isClosed))
    // then shutdown the server
    server.shutdown()

    val largeChunkOfBytes = new Array[Byte](1000000)
    // doing a subsequent send should throw an exception as the connection should be closed.
    // send a large chunk of bytes to trigger a socket flush
    try {
      sendRequest(plainSocket, 0, largeChunkOfBytes)
      fail("expected exception when writing to closed plain socket")
    } catch {
      case e: IOException => // expected
    }

    try {
      sendRequest(traceSocket, 0, largeChunkOfBytes)
      fail("expected exception when writing to closed trace socket")
    } catch {
      case e: IOException => // expected
    }
  }

  @Test
  def testMaxConnectionsPerIp() {
    // make the maximum allowable number of connections and then leak them
    val conns = (0 until server.maxConnectionsPerIp).map(i => connect())
    // now try one more (should fail)
    val conn = connect()
    conn.setSoTimeout(3000)
    assertEquals(-1, conn.getInputStream().read())
  }

  @Test
  def testMaxConnectionsPerIPOverrides() {
    val overrideNum = 6
    val overrides: Map[String, Int] = Map("localhost" -> overrideNum)
    val overrideprops = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    val overrideServer: SocketServer = new SocketServer(KafkaConfig.fromProps(overrideprops), new Metrics(), new SystemTime())
    overrideServer.startup()
    // make the maximum allowable number of connections and then leak them
    val conns = ((0 until overrideNum).map(i => connect(overrideServer)))
    // now try one more (should fail)
    val conn = connect(overrideServer)
    conn.setSoTimeout(3000)
    assertEquals(-1, conn.getInputStream.read())
    overrideServer.shutdown()
  }

  @Test
  def testSSLSocketServer(): Unit = {
    val trustStoreFile = File.createTempFile("truststore", ".jks")
    val overrideprops = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0, enableSSL = true, trustStoreFile = Some(trustStoreFile))
    overrideprops.put("listeners", "SSL://localhost:0")

    val overrideServer: SocketServer = new SocketServer(KafkaConfig.fromProps(overrideprops), new Metrics(), new SystemTime())
    overrideServer.startup()
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(null, Array(TestUtils.trustAllCerts), new java.security.SecureRandom())
    val socketFactory = sslContext.getSocketFactory
    val sslSocket = socketFactory.createSocket("localhost", overrideServer.boundPort(SecurityProtocol.SSL)).asInstanceOf[SSLSocket]
    sslSocket.setNeedClientAuth(false)

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

    sendRequest(sslSocket, 0, serializedBytes)
    processRequest(overrideServer.requestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(sslSocket).toSeq)
    overrideServer.shutdown()
  }
}
