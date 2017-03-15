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

package kafka.network

import java.io._
import java.net._
import java.nio.ByteBuffer
import java.util.{HashMap, Random}
import javax.net.ssl._

import com.yammer.metrics.core.Gauge
import com.yammer.metrics.{Metrics => YammerMetrics}
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ListenerName, NetworkSend}
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{ProduceRequest, RequestHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit._
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer

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
  val credentialProvider = new CredentialProvider(config.saslEnabledMechanisms)
  val server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider)
  server.startup()
  val sockets = new ArrayBuffer[Socket]

  def sendRequest(socket: Socket, request: Array[Byte], id: Option[Short] = None) {
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

  def receiveResponse(socket: Socket): Array[Byte] = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    response
  }

  /* A simple request handler that just echos back the response */
  def processRequest(channel: RequestChannel) {
    val request = channel.receiveRequest(2000)
    assertNotNull("receiveRequest timed out", request)
    processRequest(channel, request)
  }

  def processRequest(channel: RequestChannel, request: RequestChannel.Request) {
    val byteBuffer = ByteBuffer.allocate(request.header.sizeOf + request.body.sizeOf)
    request.header.writeTo(byteBuffer)
    request.body.writeTo(byteBuffer)
    byteBuffer.rewind()

    val send = new NetworkSend(request.connectionId, byteBuffer)
    channel.sendResponse(new RequestChannel.Response(request.processor, request, send))
  }

  def connect(s: SocketServer = server, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT) = {
    val socket = new Socket("localhost", s.boundPort(ListenerName.forSecurityProtocol(protocol)))
    sockets += socket
    socket
  }

  @After
  def tearDown() {
    metrics.close()
    server.shutdown()
    sockets.foreach(_.close())
    sockets.clear()
  }

  private def producerRequestBytes: Array[Byte] = {
    val apiKey: Short = 0
    val correlationId = -1
    val clientId = ""
    val ackTimeoutMs = 10000
    val ack = 0: Short

    val emptyRequest = new ProduceRequest.Builder(
        ack, ackTimeoutMs, new HashMap[TopicPartition, MemoryRecords]()).build()
    val emptyHeader = new RequestHeader(apiKey, emptyRequest.version, clientId, correlationId)

    val byteBuffer = ByteBuffer.allocate(emptyHeader.sizeOf + emptyRequest.sizeOf)
    emptyHeader.writeTo(byteBuffer)
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
    sendRequest(plainSocket, serializedBytes)
    processRequest(server.requestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(plainSocket).toSeq)

    // Test TRACE socket
    sendRequest(traceSocket, serializedBytes)
    processRequest(server.requestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(traceSocket).toSeq)
  }

  @Test
  def tooBigRequestIsRejected() {
    val tooManyBytes = new Array[Byte](server.config.socketRequestMaxBytes + 1)
    new Random().nextBytes(tooManyBytes)
    val socket = connect()
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(tooManyBytes.length)
    try {
      // Server closes client connection when it processes the request length because
      // it is too big. The write of request body may fail if the connection has been closed.
      outgoing.write(tooManyBytes)
      outgoing.flush()
      receiveResponse(socket)
    } catch {
      case _: IOException => // thats fine
    }
  }

  @Test
  def testGracefulClose() {
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    val serializedBytes = producerRequestBytes

    for (i <- 0 until 10)
      sendRequest(plainSocket, serializedBytes)
    plainSocket.close()
    for (i <- 0 until 10) {
      val request = server.requestChannel.receiveRequest(2000)
      assertNotNull("receiveRequest timed out", request)
      server.requestChannel.noOperation(request.processor, request)
    }
  }

  @Test
  def testSocketsCloseOnShutdown() {
    // open a connection
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    val traceSocket = connect(protocol = SecurityProtocol.TRACE)
    val bytes = new Array[Byte](40)
    // send a request first to make sure the connection has been picked up by the socket server
    sendRequest(plainSocket, bytes, Some(0))
    sendRequest(traceSocket, bytes, Some(0))
    processRequest(server.requestChannel)

    // make sure the sockets are open
    server.acceptors.values.map(acceptor => assertFalse(acceptor.serverChannel.socket.isClosed))
    // then shutdown the server
    server.shutdown()

    val largeChunkOfBytes = new Array[Byte](1000000)
    // doing a subsequent send should throw an exception as the connection should be closed.
    // send a large chunk of bytes to trigger a socket flush
    try {
      sendRequest(plainSocket, largeChunkOfBytes, Some(0))
      fail("expected exception when writing to closed plain socket")
    } catch {
      case _: IOException => // expected
    }

    try {
      sendRequest(traceSocket, largeChunkOfBytes, Some(0))
      fail("expected exception when writing to closed trace socket")
    } catch {
      case _: IOException => // expected
    }
  }

  @Test
  def testMaxConnectionsPerIp() {
    // make the maximum allowable number of connections
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
    sendRequest(conn2, serializedBytes)
    val request = server.requestChannel.receiveRequest(2000)
    assertNotNull(request)
  }

  @Test
  def testMaxConnectionsPerIpOverrides() {
    val overrideNum = server.config.maxConnectionsPerIp + 1
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    overrideProps.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, s"localhost:$overrideNum")
    val serverMetrics = new Metrics()
    val overrideServer = new SocketServer(KafkaConfig.fromProps(overrideProps), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      // make the maximum allowable number of connections
      val conns = (0 until overrideNum).map(_ => connect(overrideServer))

      // it should succeed
      val serializedBytes = producerRequestBytes
      sendRequest(conns.last, serializedBytes)
      val request = overrideServer.requestChannel.receiveRequest(2000)
      assertNotNull(request)

      // now try one more (should fail)
      val conn = connect(overrideServer)
      conn.setSoTimeout(3000)
      assertEquals(-1, conn.getInputStream.read())
    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  @Test
  def testSslSocketServer() {
    val trustStoreFile = File.createTempFile("truststore", ".jks")
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, interBrokerSecurityProtocol = Some(SecurityProtocol.SSL),
      trustStoreFile = Some(trustStoreFile))
    overrideProps.put(KafkaConfig.ListenersProp, "SSL://localhost:0")

    val serverMetrics = new Metrics
    val overrideServer = new SocketServer(KafkaConfig.fromProps(overrideProps), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      val sslContext = SSLContext.getInstance("TLSv1.2")
      sslContext.init(null, Array(TestUtils.trustAllCerts), new java.security.SecureRandom())
      val socketFactory = sslContext.getSocketFactory
      val sslSocket = socketFactory.createSocket("localhost",
        overrideServer.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SSL))).asInstanceOf[SSLSocket]
      sslSocket.setNeedClientAuth(false)

      val apiKey = ApiKeys.PRODUCE.id
      val correlationId = -1
      val clientId = ""
      val ackTimeoutMs = 10000
      val ack = 0: Short
      val emptyRequest = new ProduceRequest.Builder(
          ack, ackTimeoutMs, new HashMap[TopicPartition, MemoryRecords]()).build()
      val emptyHeader = new RequestHeader(apiKey, emptyRequest.version, clientId, correlationId)

      val byteBuffer = ByteBuffer.allocate(emptyHeader.sizeOf() + emptyRequest.sizeOf())
      emptyHeader.writeTo(byteBuffer)
      emptyRequest.writeTo(byteBuffer)
      byteBuffer.rewind()
      val serializedBytes = new Array[Byte](byteBuffer.remaining)
      byteBuffer.get(serializedBytes)

      sendRequest(sslSocket, serializedBytes)
      processRequest(overrideServer.requestChannel)
      assertEquals(serializedBytes.toSeq, receiveResponse(sslSocket).toSeq)
      sslSocket.close()
    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  @Test
  def testSessionPrincipal() {
    val socket = connect()
    val bytes = new Array[Byte](40)
    sendRequest(socket, bytes, Some(0))
    assertEquals(KafkaPrincipal.ANONYMOUS, server.requestChannel.receiveRequest(2000).session.principal)
  }

  /* Test that we update request metrics if the client closes the connection while the broker response is in flight. */
  @Test
  def testClientDisconnectionUpdatesRequestMetrics() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider) {
      override def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, requestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, listenerName, protocol, config, metrics, credentialProvider) {
          override protected[network] def sendResponse(response: RequestChannel.Response) {
            conn.close()
            super.sendResponse(response)
          }
        }
      }
    }
    try {
      overrideServer.startup()
      conn = connect(overrideServer)
      val serializedBytes = producerRequestBytes
      sendRequest(conn, serializedBytes)

      val channel = overrideServer.requestChannel
      val request = channel.receiveRequest(2000)

      val requestMetrics = RequestMetrics.metricsMap(ApiKeys.forId(request.requestId).name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val expectedTotalTimeCount = totalTimeHistCount() + 1

      // send a large buffer to ensure that the broker detects the client disconnection while writing to the socket channel.
      // On Mac OS X, the initial write seems to always succeed and it is able to write up to 102400 bytes on the initial
      // write. If the buffer is smaller than this, the write is considered complete and the disconnection is not
      // detected. If the buffer is larger than 102400 bytes, a second write is attempted and it fails with an
      // IOException.
      val send = new NetworkSend(request.connectionId, ByteBuffer.allocate(550000))
      channel.sendResponse(new RequestChannel.Response(request.processor, request, send))
      TestUtils.waitUntilTrue(() => totalTimeHistCount() == expectedTotalTimeCount,
        s"request metrics not updated, expected: $expectedTotalTimeCount, actual: ${totalTimeHistCount()}")

    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  /*
   * Test that we update request metrics if the channel has been removed from the selector when the broker calls
   * `selector.send` (selector closes old connections, for example).
   */
  @Test
  def testBrokerSendAfterChannelClosedUpdatesRequestMetrics() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    props.setProperty(KafkaConfig.ConnectionsMaxIdleMsProp, "100")
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      conn = connect(overrideServer)
      val serializedBytes = producerRequestBytes
      sendRequest(conn, serializedBytes)
      val channel = overrideServer.requestChannel
      val request = channel.receiveRequest(2000)

      TestUtils.waitUntilTrue(() => overrideServer.processor(request.processor).channel(request.connectionId).isEmpty,
        s"Idle connection `${request.connectionId}` was not closed by selector")

      val requestMetrics = RequestMetrics.metricsMap(ApiKeys.forId(request.requestId).name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val expectedTotalTimeCount = totalTimeHistCount() + 1

      processRequest(channel, request)

      TestUtils.waitUntilTrue(() => totalTimeHistCount() == expectedTotalTimeCount,
        s"request metrics not updated, expected: $expectedTotalTimeCount, actual: ${totalTimeHistCount()}")

    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }

  }

  @Test
  def testMetricCollectionAfterShutdown(): Unit = {
    server.shutdown()

    val sum = YammerMetrics
      .defaultRegistry
      .allMetrics.asScala
      .filterKeys(k => k.getName.endsWith("IdlePercent") || k.getName.endsWith("NetworkProcessorAvgIdlePercent"))
      .collect { case (_, metric: Gauge[_]) => metric.value.asInstanceOf[Double] }
      .sum

    assertEquals(0, sum, 0)
  }

}
