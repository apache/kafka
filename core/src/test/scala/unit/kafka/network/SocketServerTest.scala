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
import kafka.network.RequestChannel.SendAction
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{KafkaChannel, ListenerName, NetworkSend, Send}
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.{AbstractRequest, ProduceRequest, RequestHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{MockTime, Time}
import org.junit.Assert._
import org.junit._
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._
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

  // Clean-up any metrics left around by previous tests
  for (metricName <- YammerMetrics.defaultRegistry.allMetrics.keySet.asScala)
    YammerMetrics.defaultRegistry.removeMetric(metricName)

  val server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider)
  server.startup()
  val sockets = new ArrayBuffer[Socket]

  def sendRequest(socket: Socket, request: Array[Byte], id: Option[Short] = None, flush: Boolean = true) {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    id match {
      case Some(id) =>
        outgoing.writeInt(request.length + 2)
        outgoing.writeShort(id)
      case None =>
        outgoing.writeInt(request.length)
    }
    outgoing.write(request)
    if (flush)
      outgoing.flush()
  }

  def receiveResponse(socket: Socket): Array[Byte] = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    response
  }

  private def receiveRequest(channel: RequestChannel, timeout: Long = 2000L): RequestChannel.Request = {
    channel.receiveRequest(timeout) match {
      case request: RequestChannel.Request => request
      case RequestChannel.ShutdownRequest => fail("Unexpected shutdown received")
      case null => fail("receiveRequest timed out")
    }
  }

  /* A simple request handler that just echos back the response */
  def processRequest(channel: RequestChannel) {
    processRequest(channel, receiveRequest(channel))
  }

  def processRequest(channel: RequestChannel, request: RequestChannel.Request) {
    val byteBuffer = request.body[AbstractRequest].serialize(request.header)
    byteBuffer.rewind()

    val send = new NetworkSend(request.context.connectionId, byteBuffer)
    channel.sendResponse(new RequestChannel.Response(request, Some(send), SendAction))
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
    val correlationId = -1
    val clientId = ""
    val ackTimeoutMs = 10000
    val ack = 0: Short

    val emptyRequest = new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE, ack, ackTimeoutMs,
      new HashMap[TopicPartition, MemoryRecords]()).build()
    val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)
    val byteBuffer = emptyRequest.serialize(emptyHeader)
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

    for (_ <- 0 until 10)
      sendRequest(plainSocket, serializedBytes)
    plainSocket.close()
    for (_ <- 0 until 10) {
      val request = receiveRequest(server.requestChannel)
      assertNotNull("receiveRequest timed out", request)
      server.requestChannel.sendResponse(new RequestChannel.Response(request, None, RequestChannel.NoOpAction))
    }
  }

  @Test
  def testConnectionId() {
    val sockets = (1 to 5).map(_ => connect(protocol = SecurityProtocol.PLAINTEXT))
    val serializedBytes = producerRequestBytes

    val requests = sockets.map{socket =>
      sendRequest(socket, serializedBytes)
      receiveRequest(server.requestChannel)
    }
    requests.zipWithIndex.foreach { case (request, i) =>
      val index = request.context.connectionId.split("-").last
      assertEquals(i.toString, index)
    }

    sockets.foreach(_.close)
  }

  @Test
  def testIdleConnection() {
    val idleTimeMs = 60000
    val time = new MockTime()
    props.put(KafkaConfig.ConnectionsMaxIdleMsProp, idleTimeMs.toString)
    val serverMetrics = new Metrics
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, time, credentialProvider)

    def openChannel(request: RequestChannel.Request): Option[KafkaChannel] =
      overrideServer.processor(request.processor).channel(request.context.connectionId)
    def openOrClosingChannel(request: RequestChannel.Request): Option[KafkaChannel] =
      overrideServer.processor(request.processor).openOrClosingChannel(request.context.connectionId)

    try {
      overrideServer.startup()
      val serializedBytes = producerRequestBytes

      // Connection with no staged receives
      val socket1 = connect(overrideServer, protocol = SecurityProtocol.PLAINTEXT)
      sendRequest(socket1, serializedBytes)
      val request1 = receiveRequest(overrideServer.requestChannel)
      assertTrue("Channel not open", openChannel(request1).nonEmpty)
      assertEquals(openChannel(request1), openOrClosingChannel(request1))

      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request1).isEmpty, "Failed to close idle channel")
      assertTrue("Channel not removed", openChannel(request1).isEmpty)
      processRequest(overrideServer.requestChannel, request1)

      // Connection with staged receives
      val socket2 = connect(overrideServer, protocol = SecurityProtocol.PLAINTEXT)
      val request2 = sendRequestsUntilStagedReceive(overrideServer, socket2, serializedBytes)

      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openChannel(request2).isEmpty, "Failed to close idle channel")
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request2).nonEmpty, "Channel removed without processing staged receives")
      processRequest(overrideServer.requestChannel, request2) // this triggers a failed send since channel has been closed
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request2).isEmpty, "Failed to remove channel with failed sends")
      assertNull("Received request after failed send", overrideServer.requestChannel.receiveRequest(200))

    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  @Test
  def testConnectionIdReuse() {
    val idleTimeMs = 60000
    val time = new MockTime()
    props.put(KafkaConfig.ConnectionsMaxIdleMsProp, idleTimeMs.toString)
    val serverMetrics = new Metrics
    val overrideConnectionId = "127.0.0.1:1-127.0.0.1:2-0"
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, time, credentialProvider) {
      override def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, requestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, listenerName, protocol, config, metrics, credentialProvider, memoryPool) {
          override protected[network] def connectionId(socket: Socket): String = overrideConnectionId
        }
      }
    }

    def openChannel: Option[KafkaChannel] = overrideServer.processor(0).channel(overrideConnectionId)
    def openOrClosingChannel: Option[KafkaChannel] = overrideServer.processor(0).openOrClosingChannel(overrideConnectionId)
    def connectionCount = overrideServer.connectionCount(InetAddress.getByName("127.0.0.1"))

    try {
      overrideServer.startup()
      val socket1 = connect(overrideServer)
      TestUtils.waitUntilTrue(() => connectionCount == 1 && openChannel.isDefined, "Failed to create channel")
      val channel1 = openChannel.getOrElse(throw new RuntimeException("Channel not found"))

      // Create new connection with same id when `channel1` is still open and in Selector.channels
      // Check that new connection is closed and openChannel still contains `channel1`
      connect(overrideServer)
      TestUtils.waitUntilTrue(() => connectionCount == 1, "Failed to close channel")
      assertSame(channel1, openChannel.getOrElse(throw new RuntimeException("Channel not found")))

      // Send requests to `channel1` until a receive is staged and advance time beyond idle time so that `channel1` is
      // closed with staged receives and is in Selector.closingChannels
      val serializedBytes = producerRequestBytes
      val request = sendRequestsUntilStagedReceive(overrideServer, socket1, serializedBytes)
      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openChannel.isEmpty, "Idle channel not closed")
      TestUtils.waitUntilTrue(() => openOrClosingChannel.isDefined, "Channel removed without processing staged receives")

      // Create new connection with same id when when `channel1` is in Selector.closingChannels
      // Check that new connection is closed and openOrClosingChannel still contains `channel1`
      connect(overrideServer)
      TestUtils.waitUntilTrue(() => connectionCount == 1, "Failed to close channel")
      assertSame(channel1, openOrClosingChannel.getOrElse(throw new RuntimeException("Channel not found")))

      // Complete request with failed send so that `channel1` is removed from Selector.closingChannels
      processRequest(overrideServer.requestChannel, request)
      TestUtils.waitUntilTrue(() => connectionCount == 0 && openOrClosingChannel.isEmpty, "Failed to remove channel with failed send")

      // Check that new connections can be created with the same id since `channel1` is no longer in Selector
      connect(overrideServer)
      TestUtils.waitUntilTrue(() => connectionCount == 1 && openChannel.isDefined, "Failed to open new channel")
      val newChannel = openChannel.getOrElse(throw new RuntimeException("Channel not found"))
      assertNotSame(channel1, newChannel)
      newChannel.disconnect()

    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  private def sendRequestsUntilStagedReceive(server: SocketServer, socket: Socket, requestBytes: Array[Byte]): RequestChannel.Request = {
    def sendTwoRequestsReceiveOne(): RequestChannel.Request = {
      sendRequest(socket, requestBytes, flush = false)
      sendRequest(socket, requestBytes, flush = true)
      receiveRequest(server.requestChannel)
    }
    val (request, hasStagedReceives) = TestUtils.computeUntilTrue(sendTwoRequestsReceiveOne()) { req =>
      val connectionId = req.context.connectionId
      val hasStagedReceives = server.processor(0).numStagedReceives(connectionId) > 0
      if (!hasStagedReceives) {
        processRequest(server.requestChannel, req)
        processRequest(server.requestChannel)
      }
      hasStagedReceives
    }
    assertTrue(s"Receives not staged for ${org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS} ms", hasStagedReceives)
    request
  }

  @Test
  def testSocketsCloseOnShutdown() {
    // open a connection
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    plainSocket.setTcpNoDelay(true)
    val traceSocket = connect(protocol = SecurityProtocol.TRACE)
    traceSocket.setTcpNoDelay(true)
    val bytes = new Array[Byte](40)
    // send a request first to make sure the connection has been picked up by the socket server
    sendRequest(plainSocket, bytes, Some(0))
    sendRequest(traceSocket, bytes, Some(0))
    processRequest(server.requestChannel)
    // the following sleep is necessary to reliably detect the connection close when we send data below
    Thread.sleep(200L)
    // make sure the sockets are open
    server.acceptors.values.foreach(acceptor => assertFalse(acceptor.serverChannel.socket.isClosed))
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
    assertEquals(-1, conn.getInputStream.read())
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

      val correlationId = -1
      val clientId = ""
      val ackTimeoutMs = 10000
      val ack = 0: Short
      val emptyRequest = new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE, ack, ackTimeoutMs,
        new HashMap[TopicPartition, MemoryRecords]()).build()
      val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)

      val byteBuffer = emptyRequest.serialize(emptyHeader)
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
    assertEquals(KafkaPrincipal.ANONYMOUS, receiveRequest(server.requestChannel).session.principal)
  }

  /* Test that we update request metrics if the client closes the connection while the broker response is in flight. */
  @Test
  def testClientDisconnectionUpdatesRequestMetrics() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider) {
      override def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, requestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, listenerName, protocol, config, metrics, credentialProvider, MemoryPool.NONE) {
          override protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send) {
            conn.close()
            super.sendResponse(response, responseSend)
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
      val request = receiveRequest(channel)

      val requestMetrics = RequestMetrics.metricsMap(request.header.apiKey.name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val expectedTotalTimeCount = totalTimeHistCount() + 1

      // send a large buffer to ensure that the broker detects the client disconnection while writing to the socket channel.
      // On Mac OS X, the initial write seems to always succeed and it is able to write up to 102400 bytes on the initial
      // write. If the buffer is smaller than this, the write is considered complete and the disconnection is not
      // detected. If the buffer is larger than 102400 bytes, a second write is attempted and it fails with an
      // IOException.
      val send = new NetworkSend(request.context.connectionId, ByteBuffer.allocate(550000))
      channel.sendResponse(new RequestChannel.Response(request, Some(send), SendAction))
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
      val request = receiveRequest(channel)

      TestUtils.waitUntilTrue(() => overrideServer.processor(request.processor).channel(request.context.connectionId).isEmpty,
        s"Idle connection `${request.context.connectionId}` was not closed by selector")

      val requestMetrics = RequestMetrics.metricsMap(request.header.apiKey.name)
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

    val nonZeroMetricNamesAndValues = YammerMetrics
      .defaultRegistry
      .allMetrics.asScala
      .filterKeys(k => k.getName.endsWith("IdlePercent") || k.getName.endsWith("NetworkProcessorAvgIdlePercent"))
      .collect { case (k, metric: Gauge[_]) => (k, metric.value().asInstanceOf[Double]) }
      .filter { case (_, value) => value != 0.0 }

    assertEquals(Map.empty, nonZeroMetricNamesAndValues)
  }

  @Test
  def testProcessorMetricsTags(): Unit = {
    val kafkaMetricNames = metrics.metrics.keySet.asScala.filter(_.tags.asScala.get("listener").nonEmpty)
    assertFalse(kafkaMetricNames.isEmpty)

    val expectedListeners = Set("PLAINTEXT", "TRACE")
    kafkaMetricNames.foreach { kafkaMetricName =>
      assertTrue(expectedListeners.contains(kafkaMetricName.tags.get("listener")))
    }

    // legacy metrics not tagged
    val yammerMetricsNames = YammerMetrics.defaultRegistry.allMetrics.asScala
      .filterKeys(_.getType.equals("Processor"))
      .collect { case (k, _: Gauge[_]) => k }
    assertFalse(yammerMetricsNames.isEmpty)

    yammerMetricsNames.foreach { yammerMetricName =>
      assertFalse(yammerMetricName.getMBeanName.contains("listener="))
    }
  }

}
