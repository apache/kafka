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
import javax.net.ssl._
import java.io._

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.NetworkSend
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.auth.KafkaPrincipal
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
  val config = KafkaConfig.fromProps(props)
  val metrics = new Metrics
  val server = new SocketServer(config, metrics, new SystemTime)
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

  def connect(s: SocketServer = server, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT) =
    new Socket("localhost", server.boundPort(protocol))

  @After
  def cleanup() {
    metrics.close()
    server.shutdown()
  }

  private def producerRequestBytes: Array[Byte] = {
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
    serializedBytes
  }

  @Test
  def simpleRequest() {
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    val traceSocket = connect(protocol = SecurityProtocol.TRACE)
    val serializedBytes = producerRequestBytes

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
    val tooManyBytes = new Array[Byte](server.config.socketRequestMaxBytes + 1)
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
    val conns = (0 until server.config.maxConnectionsPerIp).map(_ => connect())
    // now try one more (should fail)
    val conn = connect()
    conn.setSoTimeout(3000)
    assertEquals(-1, conn.getInputStream().read())
    conn.close()

    // it should succeed after closing one connection
    val address = conns.head.getInetAddress
    conns.head.close()
    TestUtils.waitUntilTrue(() => server.connectionCount(address) < conns.length,
      "Failed to decrement connection count after close")
    val conn2 = connect()
    val serializedBytes = producerRequestBytes
    sendRequest(conn2, 0, serializedBytes)
    val request = server.requestChannel.receiveRequest(2000)
    assertNotNull(request)
    conn2.close()
    conns.tail.foreach(_.close())
  }

  @Test
  def testMaxConnectionsPerIPOverrides() {
    val overrideNum = 6
    val overrides = Map("localhost" -> overrideNum)
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    val serverMetrics = new Metrics()
    val overrideServer: SocketServer = new SocketServer(KafkaConfig.fromProps(overrideProps), serverMetrics, new SystemTime())
    try {
      overrideServer.startup()
      // make the maximum allowable number of connections and then leak them
      val conns = ((0 until overrideNum).map(i => connect(overrideServer)))
      // now try one more (should fail)
      val conn = connect(overrideServer)
      conn.setSoTimeout(3000)
      assertEquals(-1, conn.getInputStream.read())
      conn.close()
      conns.foreach(_.close())
    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  @Test
  def testSslSocketServer(): Unit = {
    val trustStoreFile = File.createTempFile("truststore", ".jks")
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, interBrokerSecurityProtocol = Some(SecurityProtocol.SSL),
      trustStoreFile = Some(trustStoreFile))
    overrideProps.put(KafkaConfig.ListenersProp, "SSL://localhost:0")

    val serverMetrics = new Metrics
    val overrideServer: SocketServer = new SocketServer(KafkaConfig.fromProps(overrideProps), serverMetrics, new SystemTime)
    overrideServer.startup()
    try {
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
      sslSocket.close()
    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  @Test
  def testSessionPrincipal(): Unit = {
    val socket = connect()
    val bytes = new Array[Byte](40)
    sendRequest(socket, 0, bytes)
    assertEquals(KafkaPrincipal.ANONYMOUS, server.requestChannel.receiveRequest().session.principal)
    socket.close()
  }

}
