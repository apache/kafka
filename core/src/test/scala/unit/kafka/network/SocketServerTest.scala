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
import java.nio.channels.SocketChannel
import java.util.{HashMap, Properties, Random}

import com.yammer.metrics.core.{Gauge, Meter}
import com.yammer.metrics.{Metrics => YammerMetrics}
import javax.net.ssl._
import kafka.security.CredentialProvider
import kafka.server.{KafkaConfig, ThrottledChannel}
import kafka.utils.Implicits._
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteState
import org.apache.kafka.common.network.{ChannelBuilder, ChannelState, KafkaChannel, ListenerName, NetworkReceive, NetworkSend, Selector, Send}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{AbstractRequest, ProduceRequest, RequestHeader, SaslAuthenticateRequest, SaslHandshakeRequest}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.utils.{LogContext, MockTime, Time}
import org.apache.log4j.Level
import org.junit.Assert._
import org.junit._
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

class SocketServerTest extends JUnitSuite {
  val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
  props.put("listeners", "PLAINTEXT://localhost:0")
  props.put("num.network.threads", "1")
  props.put("socket.send.buffer.bytes", "300000")
  props.put("socket.receive.buffer.bytes", "300000")
  props.put("queued.max.requests", "50")
  props.put("socket.request.max.bytes", "50")
  props.put("max.connections.per.ip", "5")
  props.put("connections.max.idle.ms", "60000")
  val config = KafkaConfig.fromProps(props)
  val metrics = new Metrics
  val credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, null)
  val localAddress = InetAddress.getLoopbackAddress

  // Clean-up any metrics left around by previous tests
  for (metricName <- YammerMetrics.defaultRegistry.allMetrics.keySet.asScala)
    YammerMetrics.defaultRegistry.removeMetric(metricName)

  val server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider)
  server.startup()
  val sockets = new ArrayBuffer[Socket]

  private val kafkaLogger = org.apache.log4j.LogManager.getLogger("kafka")
  private var logLevelToRestore: Level = _

  @Before
  def setUp(): Unit = {
    // Run the tests with TRACE logging to exercise request logging path
    logLevelToRestore = kafkaLogger.getLevel
    kafkaLogger.setLevel(Level.TRACE)
  }

  @After
  def tearDown() {
    shutdownServerAndMetrics(server)
    sockets.foreach(_.close())
    sockets.clear()
    kafkaLogger.setLevel(logLevelToRestore)
  }

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

  def sendApiRequest(socket: Socket, request: AbstractRequest, header: RequestHeader) = {
    val byteBuffer = request.serialize(header)
    byteBuffer.rewind()
    val serializedBytes = new Array[Byte](byteBuffer.remaining)
    byteBuffer.get(serializedBytes)
    sendRequest(socket, serializedBytes)
  }

  def receiveResponse(socket: Socket): Array[Byte] = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    response
  }

  private def receiveRequest(channel: RequestChannel, timeout: Long = 2000L): RequestChannel.Request = {
    channel.receiveRequest(0,timeout) match {
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
    channel.sendResponse(new RequestChannel.SendResponse(request, send, Some(request.header.toString), None))
  }

  def connect(s: SocketServer = server, listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), localAddr: InetAddress = null, port: Int = 0) = {
    val socket = new Socket("localhost", s.boundPort(listenerName), localAddr, port)
    sockets += socket
    socket
  }

  // Create a client connection, process one request and return (client socket, connectionId)
  def connectAndProcessRequest(s: SocketServer): (Socket, String) = {
    val socket = connect(s)
    val request = sendAndReceiveRequest(socket, s)
    processRequest(s.dataPlaneRequestChannel, request)
    (socket, request.context.connectionId)
  }

  def sendAndReceiveRequest(socket: Socket, server: SocketServer): RequestChannel.Request = {
    sendRequest(socket, producerRequestBytes())
    receiveRequest(server.dataPlaneRequestChannel)
  }

  def shutdownServerAndMetrics(server: SocketServer): Unit = {
    server.shutdown()
    server.metrics.close()
  }

  private def producerRequestBytes(ack: Short = 0): Array[Byte] = {
    val correlationId = -1
    val clientId = ""
    val ackTimeoutMs = 10000

    val emptyRequest = ProduceRequest.Builder.forCurrentMagic(ack, ackTimeoutMs,
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
    val plainSocket = connect()
    val serializedBytes = producerRequestBytes()

    // Test PLAINTEXT socket
    sendRequest(plainSocket, serializedBytes)
    processRequest(server.dataPlaneRequestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(plainSocket).toSeq)
    verifyAcceptorBlockedPercent("PLAINTEXT", expectBlocked = false)
  }

  @Test
  def testControlPlaneRequest(): Unit = {
    val testProps = new Properties
    testProps ++= props
    testProps.put("listeners", "PLAINTEXT://localhost:0,CONTROLLER://localhost:0")
    testProps.put("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")
    testProps.put("control.plane.listener.name", "CONTROLLER")
    val config = KafkaConfig.fromProps(testProps)
    withTestableServer(config, { testableServer =>
      val socket = connect(testableServer, config.controlPlaneListenerName.get,
        localAddr = InetAddress.getLocalHost)
      sendAndReceiveControllerRequest(socket, testableServer)
    })
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
    val plainSocket = connect()
    val serializedBytes = producerRequestBytes()

    for (_ <- 0 until 10)
      sendRequest(plainSocket, serializedBytes)
    plainSocket.close()
    for (_ <- 0 until 10) {
      val request = receiveRequest(server.dataPlaneRequestChannel)
      assertNotNull("receiveRequest timed out", request)
      server.dataPlaneRequestChannel.sendResponse(new RequestChannel.NoOpResponse(request))
    }
  }

  @Test
  def testNoOpAction(): Unit = {
    val plainSocket = connect()
    val serializedBytes = producerRequestBytes()

    for (_ <- 0 until 3)
      sendRequest(plainSocket, serializedBytes)
    for (_ <- 0 until 3) {
      val request = receiveRequest(server.dataPlaneRequestChannel)
      assertNotNull("receiveRequest timed out", request)
      server.dataPlaneRequestChannel.sendResponse(new RequestChannel.NoOpResponse(request))
    }
  }

  @Test
  def testConnectionId() {
    val sockets = (1 to 5).map(_ => connect())
    val serializedBytes = producerRequestBytes()

    val requests = sockets.map{socket =>
      sendRequest(socket, serializedBytes)
      receiveRequest(server.dataPlaneRequestChannel)
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
      overrideServer.dataPlaneProcessor(request.processor).channel(request.context.connectionId)
    def openOrClosingChannel(request: RequestChannel.Request): Option[KafkaChannel] =
      overrideServer.dataPlaneProcessor(request.processor).openOrClosingChannel(request.context.connectionId)

    try {
      overrideServer.startup()
      val serializedBytes = producerRequestBytes()

      // Connection with no staged receives
      val socket1 = connect(overrideServer)
      sendRequest(socket1, serializedBytes)
      val request1 = receiveRequest(overrideServer.dataPlaneRequestChannel)
      assertTrue("Channel not open", openChannel(request1).nonEmpty)
      assertEquals(openChannel(request1), openOrClosingChannel(request1))

      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request1).isEmpty, "Failed to close idle channel")
      assertTrue("Channel not removed", openChannel(request1).isEmpty)
      processRequest(overrideServer.dataPlaneRequestChannel, request1)

      // Connection with staged receives
      val socket2 = connect(overrideServer)
      val request2 = sendRequestsUntilStagedReceive(overrideServer, socket2, serializedBytes)

      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openChannel(request2).isEmpty, "Failed to close idle channel")
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request2).nonEmpty, "Channel removed without processing staged receives")
      processRequest(overrideServer.dataPlaneRequestChannel, request2) // this triggers a failed send since channel has been closed
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request2).isEmpty, "Failed to remove channel with failed sends")
      assertNull("Received request after failed send", overrideServer.dataPlaneRequestChannel.receiveRequest(200))

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testConnectionIdReuse() {
    val idleTimeMs = 60000
    val time = new MockTime()
    props.put(KafkaConfig.ConnectionsMaxIdleMsProp, idleTimeMs.toString)
    props.put("listeners", "PLAINTEXT://localhost:0")
    val serverMetrics = new Metrics
    @volatile var selector: TestableSelector = null
    val overrideConnectionId = "127.0.0.1:1-127.0.0.1:2-0"
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, time, credentialProvider) {
      override def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, dataPlaneRequestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, config.failedAuthenticationDelayMs, listenerName, protocol, config, metrics,
          credentialProvider, memoryPool, new LogContext()) {
            override protected[network] def connectionId(socket: Socket): String = overrideConnectionId
            override protected[network] def createSelector(channelBuilder: ChannelBuilder): Selector = {
             val testableSelector = new TestableSelector(config, channelBuilder, time, metrics)
             selector = testableSelector
             testableSelector
          }
        }
      }
    }

    def openChannel: Option[KafkaChannel] = overrideServer.dataPlaneProcessor(0).channel(overrideConnectionId)
    def openOrClosingChannel: Option[KafkaChannel] = overrideServer.dataPlaneProcessor(0).openOrClosingChannel(overrideConnectionId)
    def connectionCount = overrideServer.connectionCount(InetAddress.getByName("127.0.0.1"))

    // Create a client connection and wait for server to register the connection with the selector. For
    // test scenarios below where `Selector.register` fails, the wait ensures that checks are performed
    // only after `register` is processed by the server.
    def connectAndWaitForConnectionRegister(): Socket = {
      val connections = selector.operationCounts(SelectorOperation.Register)
      val socket = connect(overrideServer)
      TestUtils.waitUntilTrue(() =>
        selector.operationCounts(SelectorOperation.Register) == connections + 1, "Connection not registered")
      socket
    }

    try {
      overrideServer.startup()
      val socket1 = connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1 && openChannel.isDefined, "Failed to create channel")
      val channel1 = openChannel.getOrElse(throw new RuntimeException("Channel not found"))

      // Create new connection with same id when `channel1` is still open and in Selector.channels
      // Check that new connection is closed and openChannel still contains `channel1`
      connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1, "Failed to close channel")
      assertSame(channel1, openChannel.getOrElse(throw new RuntimeException("Channel not found")))

      // Send requests to `channel1` until a receive is staged and advance time beyond idle time so that `channel1` is
      // closed with staged receives and is in Selector.closingChannels
      val serializedBytes = producerRequestBytes()
      val request = sendRequestsUntilStagedReceive(overrideServer, socket1, serializedBytes)
      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openChannel.isEmpty, "Idle channel not closed")
      TestUtils.waitUntilTrue(() => openOrClosingChannel.isDefined, "Channel removed without processing staged receives")

      // Create new connection with same id when `channel1` is in Selector.closingChannels
      // Check that new connection is closed and openOrClosingChannel still contains `channel1`
      connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1, "Failed to close channel")
      assertSame(channel1, openOrClosingChannel.getOrElse(throw new RuntimeException("Channel not found")))

      // Complete request with failed send so that `channel1` is removed from Selector.closingChannels
      processRequest(overrideServer.dataPlaneRequestChannel, request)
      TestUtils.waitUntilTrue(() => connectionCount == 0 && openOrClosingChannel.isEmpty, "Failed to remove channel with failed send")

      // Check that new connections can be created with the same id since `channel1` is no longer in Selector
      connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1 && openChannel.isDefined, "Failed to open new channel")
      val newChannel = openChannel.getOrElse(throw new RuntimeException("Channel not found"))
      assertNotSame(channel1, newChannel)
      newChannel.disconnect()

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  private def sendRequestsUntilStagedReceive(server: SocketServer, socket: Socket, requestBytes: Array[Byte]): RequestChannel.Request = {
    def sendTwoRequestsReceiveOne(): RequestChannel.Request = {
      sendRequest(socket, requestBytes, flush = false)
      sendRequest(socket, requestBytes, flush = true)
      receiveRequest(server.dataPlaneRequestChannel)
    }
    val (request, hasStagedReceives) = TestUtils.computeUntilTrue(sendTwoRequestsReceiveOne()) { req =>
      val connectionId = req.context.connectionId
      val hasStagedReceives = server.dataPlaneProcessor(0).numStagedReceives(connectionId) > 0
      if (!hasStagedReceives) {
        processRequest(server.dataPlaneRequestChannel, req)
        processRequest(server.dataPlaneRequestChannel)
      }
      hasStagedReceives
    }
    assertTrue(s"Receives not staged for ${org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS} ms", hasStagedReceives)
    request
  }

  // Prepares test setup for throttled channel tests. throttlingDone controls whether or not throttling has completed
  // in quota manager.
  def throttledChannelTestSetUp(socket: Socket, serializedBytes: Array[Byte], noOpResponse: Boolean,
                                throttlingInProgress: Boolean): RequestChannel.Request = {
    sendRequest(socket, serializedBytes)

    // Mimic a primitive request handler that fetches the request from RequestChannel and place a response with a
    // throttled channel.
    val request = receiveRequest(server.dataPlaneRequestChannel)
    val byteBuffer = request.body[AbstractRequest].serialize(request.header)
    val send = new NetworkSend(request.context.connectionId, byteBuffer)
    def channelThrottlingCallback(response: RequestChannel.Response): Unit = {
      server.dataPlaneRequestChannel.sendResponse(response)
    }
    val throttledChannel = new ThrottledChannel(request, new MockTime(), 100, channelThrottlingCallback)
    val response =
      if (!noOpResponse)
        new RequestChannel.SendResponse(request, send, Some(request.header.toString), None)
      else
        new RequestChannel.NoOpResponse(request)
    server.dataPlaneRequestChannel.sendResponse(response)

    // Quota manager would call notifyThrottlingDone() on throttling completion. Simulate it if throttleingInProgress is
    // false.
    if (!throttlingInProgress)
      throttledChannel.notifyThrottlingDone()

    request
  }

  def openOrClosingChannel(request: RequestChannel.Request): Option[KafkaChannel] =
    server.dataPlaneProcessor(0).openOrClosingChannel(request.context.connectionId)

  @Test
  def testSendActionResponseWithThrottledChannelWhereThrottlingInProgress() {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, false, true)

    // receive response
    assertEquals(serializedBytes.toSeq, receiveResponse(socket).toSeq)
    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.MUTED_AND_THROTTLED), "fail")
    // Channel should still be muted.
    assertTrue(openOrClosingChannel(request).exists(c => c.isMute()))
  }

  @Test
  def testSendActionResponseWithThrottledChannelWhereThrottlingAlreadyDone() {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, false, false)

    // receive response
    assertEquals(serializedBytes.toSeq, receiveResponse(socket).toSeq)
    // Since throttling is already done, the channel can be unmuted after sending out the response.
    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.NOT_MUTED), "fail")
    // Channel is now unmuted.
    assertFalse(openOrClosingChannel(request).exists(c => c.isMute()))
  }

  @Test
  def testNoOpActionResponseWithThrottledChannelWhereThrottlingInProgress() {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, true, true)

    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.MUTED_AND_THROTTLED), "fail")
    // Channel should still be muted.
    assertTrue(openOrClosingChannel(request).exists(c => c.isMute()))
  }

  @Test
  def testNoOpActionResponseWithThrottledChannelWhereThrottlingAlreadyDone() {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, true, false)

    // Since throttling is already done, the channel can be unmuted.
    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.NOT_MUTED), "fail")
    // Channel is now unmuted.
    assertFalse(openOrClosingChannel(request).exists(c => c.isMute()))
  }

  @Test
  def testSocketsCloseOnShutdown() {
    // open a connection
    val plainSocket = connect()
    plainSocket.setTcpNoDelay(true)
    val bytes = new Array[Byte](40)
    // send a request first to make sure the connection has been picked up by the socket server
    sendRequest(plainSocket, bytes, Some(0))
    processRequest(server.dataPlaneRequestChannel)
    // the following sleep is necessary to reliably detect the connection close when we send data below
    Thread.sleep(200L)
    // make sure the sockets ar e open
    server.dataPlaneAcceptors.asScala.values.foreach(acceptor => assertFalse(acceptor.serverChannel.socket.isClosed))
    // then shutdown the server
    shutdownServerAndMetrics(server)

    val largeChunkOfBytes = new Array[Byte](1000000)
    // doing a subsequent send should throw an exception as the connection should be closed.
    // send a large chunk of bytes to trigger a socket flush
    try {
      sendRequest(plainSocket, largeChunkOfBytes, Some(0))
      fail("expected exception when writing to closed plain socket")
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
    val serializedBytes = producerRequestBytes()
    sendRequest(conn2, serializedBytes)
    val request = server.dataPlaneRequestChannel.receiveRequest(2000)
    assertNotNull(request)
  }

  @Test
  def testZeroMaxConnectionsPerIp() {
    val newProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    newProps.setProperty(KafkaConfig.MaxConnectionsPerIpProp, "0")
    newProps.setProperty(KafkaConfig.MaxConnectionsPerIpOverridesProp, "%s:%s".format("127.0.0.1", "5"))
    val server = new SocketServer(KafkaConfig.fromProps(newProps), new Metrics(), Time.SYSTEM, credentialProvider)
    try {
      server.startup()
      // make the maximum allowable number of connections
      val conns = (0 until 5).map(_ => connect(server))
      // now try one more (should fail)
      val conn = connect(server)
      conn.setSoTimeout(3000)
      assertEquals(-1, conn.getInputStream.read())
      conn.close()

      // it should succeed after closing one connection
      val address = conns.head.getInetAddress
      conns.head.close()
      TestUtils.waitUntilTrue(() => server.connectionCount(address) < conns.length,
        "Failed to decrement connection count after close")
      val conn2 = connect(server)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn2, serializedBytes)
      val request = server.dataPlaneRequestChannel.receiveRequest(2000)
      assertNotNull(request)

      // now try to connect from the external facing interface, which should fail
      val conn3 = connect(s = server, localAddr = InetAddress.getLocalHost)
      conn3.setSoTimeout(3000)
      assertEquals(-1, conn3.getInputStream.read())
      conn3.close()
    } finally {
      shutdownServerAndMetrics(server)
    }
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
      val serializedBytes = producerRequestBytes()
      sendRequest(conns.last, serializedBytes)
      val request = overrideServer.dataPlaneRequestChannel.receiveRequest(2000)
      assertNotNull(request)

      // now try one more (should fail)
      val conn = connect(overrideServer)
      conn.setSoTimeout(3000)
      assertEquals(-1, conn.getInputStream.read())
    } finally {
      shutdownServerAndMetrics(overrideServer)
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
      val emptyRequest = ProduceRequest.Builder.forCurrentMagic(ack, ackTimeoutMs,
        new HashMap[TopicPartition, MemoryRecords]()).build()
      val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)

      val byteBuffer = emptyRequest.serialize(emptyHeader)
      byteBuffer.rewind()
      val serializedBytes = new Array[Byte](byteBuffer.remaining)
      byteBuffer.get(serializedBytes)

      sendRequest(sslSocket, serializedBytes)
      processRequest(overrideServer.dataPlaneRequestChannel)
      assertEquals(serializedBytes.toSeq, receiveResponse(sslSocket).toSeq)
      sslSocket.close()
    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testSaslReauthenticationFailure(): Unit = {
    shutdownServerAndMetrics(server) // we will use our own instance because we require custom configs
    val username = "admin"
    val password = "admin-secret"
    val reauthMs = 1500
    val brokerProps = new Properties
    brokerProps.setProperty("listeners", "SASL_PLAINTEXT://localhost:0")
    brokerProps.setProperty("security.inter.broker.protocol", "SASL_PLAINTEXT")
    brokerProps.setProperty("listener.name.sasl_plaintext.plain.sasl.jaas.config",
      "org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"%s\" password=\"%s\" user_%s=\"%s\";".format(username, password, username, password))
    brokerProps.setProperty("sasl.mechanism.inter.broker.protocol", "PLAIN")
    brokerProps.setProperty("listener.name.sasl_plaintext.sasl.enabled.mechanisms", "PLAIN")
    brokerProps.setProperty("num.network.threads", "1")
    brokerProps.setProperty("connections.max.reauth.ms", reauthMs.toString)
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect,
      saslProperties = Some(brokerProps), enableSaslPlaintext = true)
    val time = new MockTime()
    val overrideServer = new TestableSocketServer(KafkaConfig.fromProps(overrideProps), time = time)
    try {
      overrideServer.startup()
      val socket = connect(overrideServer, ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT))

      val correlationId = -1
      val clientId = ""
      // send a SASL handshake request
      val saslHandshakeRequest = new SaslHandshakeRequest.Builder("PLAIN")
        .build()
      val saslHandshakeHeader = new RequestHeader(ApiKeys.SASL_HANDSHAKE, saslHandshakeRequest.version, clientId,
        correlationId)
      sendApiRequest(socket, saslHandshakeRequest, saslHandshakeHeader)
      receiveResponse(socket)

      // now send credentials within a SaslAuthenticateRequest
      val authBytes = "admin\u0000admin\u0000admin-secret".getBytes("UTF-8")
      val saslAuthenticateRequest = new SaslAuthenticateRequest.Builder(ByteBuffer.wrap(authBytes)).build()
      val saslAuthenticateHeader = new RequestHeader(ApiKeys.SASL_AUTHENTICATE, saslAuthenticateRequest.version,
        clientId, correlationId)
      sendApiRequest(socket, saslAuthenticateRequest, saslAuthenticateHeader)
      receiveResponse(socket)
      assertEquals(1, overrideServer.testableSelector.channels.size)

      // advance the clock long enough to cause server-side disconnection upon next send...
      time.sleep(reauthMs * 2)
      // ...and now send something to trigger the disconnection
      val ackTimeoutMs = 10000
      val ack = 0: Short
      val emptyRequest = ProduceRequest.Builder.forCurrentMagic(ack, ackTimeoutMs,
        new HashMap[TopicPartition, MemoryRecords]()).build()
      val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)
      sendApiRequest(socket, emptyRequest, emptyHeader)
      // wait a little bit for the server-side disconnection to occur since it happens asynchronously
      try {
        TestUtils.waitUntilTrue(() => overrideServer.testableSelector.channels.isEmpty,
          "Expired connection was not closed", 1000, 100)
      } finally {
        socket.close()
      }
    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testSessionPrincipal(): Unit = {
    val socket = connect()
    val bytes = new Array[Byte](40)
    sendRequest(socket, bytes, Some(0))
    assertEquals(KafkaPrincipal.ANONYMOUS, receiveRequest(server.dataPlaneRequestChannel).session.principal)
  }

  /* Test that we update request metrics if the client closes the connection while the broker response is in flight. */
  @Test
  def testClientDisconnectionUpdatesRequestMetrics() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider) {
      override def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, dataPlaneRequestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, config.failedAuthenticationDelayMs, listenerName, protocol, config, metrics,
          credentialProvider, MemoryPool.NONE, new LogContext()) {
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
      val serializedBytes = producerRequestBytes()
      sendRequest(conn, serializedBytes)

      val channel = overrideServer.dataPlaneRequestChannel
      val request = receiveRequest(channel)

      val requestMetrics = channel.metrics(request.header.apiKey.name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val expectedTotalTimeCount = totalTimeHistCount() + 1

      // send a large buffer to ensure that the broker detects the client disconnection while writing to the socket channel.
      // On Mac OS X, the initial write seems to always succeed and it is able to write up to 102400 bytes on the initial
      // write. If the buffer is smaller than this, the write is considered complete and the disconnection is not
      // detected. If the buffer is larger than 102400 bytes, a second write is attempted and it fails with an
      // IOException.
      val send = new NetworkSend(request.context.connectionId, ByteBuffer.allocate(550000))
      channel.sendResponse(new RequestChannel.SendResponse(request, send, None, None))
      TestUtils.waitUntilTrue(() => totalTimeHistCount() == expectedTotalTimeCount,
        s"request metrics not updated, expected: $expectedTotalTimeCount, actual: ${totalTimeHistCount()}")

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testClientDisconnectionWithStagedReceivesFullyProcessed() {
    val serverMetrics = new Metrics
    @volatile var selector: TestableSelector = null
    val overrideConnectionId = "127.0.0.1:1-127.0.0.1:2-0"
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider) {
      override def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, dataPlaneRequestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, config.failedAuthenticationDelayMs, listenerName, protocol, config, metrics,
          credentialProvider, memoryPool, new LogContext()) {
          override protected[network] def connectionId(socket: Socket): String = overrideConnectionId
          override protected[network] def createSelector(channelBuilder: ChannelBuilder): Selector = {
           val testableSelector = new TestableSelector(config, channelBuilder, time, metrics)
           selector = testableSelector
           testableSelector
        }
        }
      }
    }

    def openChannel: Option[KafkaChannel] = overrideServer.dataPlaneProcessor(0).channel(overrideConnectionId)
    def openOrClosingChannel: Option[KafkaChannel] = overrideServer.dataPlaneProcessor(0).openOrClosingChannel(overrideConnectionId)

    try {
      overrideServer.startup()
      val socket = connect(overrideServer)

      TestUtils.waitUntilTrue(() => openChannel.nonEmpty, "Channel not found")

      // Setup channel to client with staged receives so when client disconnects
      // it will be stored in Selector.closingChannels
      val serializedBytes = producerRequestBytes(1)
      val request = sendRequestsUntilStagedReceive(overrideServer, socket, serializedBytes)

      // Set SoLinger to 0 to force a hard disconnect via TCP RST
      socket.setSoLinger(true, 0)
      socket.close()

      // Complete request with socket exception so that the channel is removed from Selector.closingChannels
      processRequest(overrideServer.dataPlaneRequestChannel, request)
      TestUtils.waitUntilTrue(() => openOrClosingChannel.isEmpty, "Channel not closed after failed send")
      assertTrue("Unexpected completed send", selector.completedSends.isEmpty)
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
    props.setProperty(KafkaConfig.ConnectionsMaxIdleMsProp, "110")
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      conn = connect(overrideServer)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn, serializedBytes)
      val channel = overrideServer.dataPlaneRequestChannel
      val request = receiveRequest(channel)

      TestUtils.waitUntilTrue(() => overrideServer.dataPlaneProcessor(request.processor).channel(request.context.connectionId).isEmpty,
        s"Idle connection `${request.context.connectionId}` was not closed by selector")

      val requestMetrics = channel.metrics(request.header.apiKey.name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val expectedTotalTimeCount = totalTimeHistCount() + 1

      processRequest(channel, request)

      TestUtils.waitUntilTrue(() => totalTimeHistCount() == expectedTotalTimeCount,
        s"request metrics not updated, expected: $expectedTotalTimeCount, actual: ${totalTimeHistCount()}")

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testRequestMetricsAfterStop(): Unit = {
    server.stopProcessingRequests()
    val version = ApiKeys.PRODUCE.latestVersion
    val version2 = (version - 1).toShort
    for (_ <- 0 to 1) server.dataPlaneRequestChannel.metrics(ApiKeys.PRODUCE.name).requestRate(version).mark()
    server.dataPlaneRequestChannel.metrics(ApiKeys.PRODUCE.name).requestRate(version2).mark()
    assertEquals(2, server.dataPlaneRequestChannel.metrics(ApiKeys.PRODUCE.name).requestRate(version).count())
    server.dataPlaneRequestChannel.updateErrorMetrics(ApiKeys.PRODUCE, Map(Errors.NONE -> 1))
    val nonZeroMeters = Map(s"kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce,version=$version" -> 2,
        s"kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce,version=$version2" -> 1,
        "kafka.network:type=RequestMetrics,name=ErrorsPerSec,request=Produce,error=NONE" -> 1)

    def requestMetricMeters = YammerMetrics
      .defaultRegistry
      .allMetrics.asScala
      .filterKeys(k => k.getType == "RequestMetrics")
      .collect { case (k, metric: Meter) => (k.toString, metric.count) }

    assertEquals(nonZeroMeters, requestMetricMeters.filter { case (_, value) => value != 0 })
    server.shutdown()
    assertEquals(Map.empty, requestMetricMeters)
  }

  @Test
  def testMetricCollectionAfterShutdown(): Unit = {
    server.shutdown()

    val nonZeroMetricNamesAndValues = YammerMetrics
      .defaultRegistry
      .allMetrics.asScala
      .filterKeys(k => k.getName.endsWith("IdlePercent") || k.getName.endsWith("NetworkProcessorAvgIdlePercent"))
      .collect { case (k, metric: Gauge[_]) => (k, metric.value().asInstanceOf[Double]) }
      .filter { case (_, value) => value != 0.0 && !value.equals(Double.NaN) }

    assertEquals(Map.empty, nonZeroMetricNamesAndValues)
  }

  @Test
  def testProcessorMetricsTags(): Unit = {
    val kafkaMetricNames = metrics.metrics.keySet.asScala.filter(_.tags.asScala.get("listener").nonEmpty)
    assertFalse(kafkaMetricNames.isEmpty)

    val expectedListeners = Set("PLAINTEXT")
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

  /**
   * Tests exception handling in [[Processor.configureNewConnections]]. Exception is
   * injected into [[Selector.register]] which is used to register each new connection.
   * Test creates two connections in a single iteration by waking up the selector only
   * when two connections are ready.
   * Verifies that
   * - first failed connection is closed
   * - second connection is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def configureNewConnectionException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector

      testableSelector.updateMinWakeup(2)
      testableSelector.addFailure(SelectorOperation.Register)
      val sockets = (1 to 2).map(_ => connect(testableServer))
      testableSelector.waitForOperations(SelectorOperation.Register, 2)
      TestUtils.waitUntilTrue(() => testableServer.connectionCount(localAddress) == 1, "Failed channel not removed")

      assertProcessorHealthy(testableServer, testableSelector.notFailed(sockets))
    })
  }

  /**
   * Tests exception handling in [[Processor.processNewResponses]]. Exception is
   * injected into [[Selector.send]] which is used to send the new response.
   * Test creates two responses in a single iteration by waking up the selector only
   * when two responses are ready.
   * Verifies that
   * - first failed channel is closed
   * - second response is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def processNewResponseException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector
      testableSelector.updateMinWakeup(2)

      val sockets = (1 to 2).map(_ => connect(testableServer))
      sockets.foreach(sendRequest(_, producerRequestBytes()))

      testableServer.testableSelector.addFailure(SelectorOperation.Send)
      sockets.foreach(_ => processRequest(testableServer.dataPlaneRequestChannel))
      testableSelector.waitForOperations(SelectorOperation.Send, 2)
      testableServer.waitForChannelClose(testableSelector.allFailedChannels.head, locallyClosed = true)

      assertProcessorHealthy(testableServer, testableSelector.notFailed(sockets))
    })
  }

  /**
   * Tests exception handling in [[Processor.processNewResponses]] when [[Selector.send]]
   * fails with `CancelledKeyException`, which is handled by the selector using a different
   * code path. Test scenario is similar to [[SocketServerTest.processNewResponseException]].
   */
  @Test
  def sendCancelledKeyException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector
      testableSelector.updateMinWakeup(2)

      val sockets = (1 to 2).map(_ => connect(testableServer))
      sockets.foreach(sendRequest(_, producerRequestBytes()))
      val requestChannel = testableServer.dataPlaneRequestChannel

      val requests = sockets.map(_ => receiveRequest(requestChannel))
      val failedConnectionId = requests(0).context.connectionId
      // `KafkaChannel.disconnect()` cancels the selection key, triggering CancelledKeyException during send
      testableSelector.channel(failedConnectionId).disconnect()
      requests.foreach(processRequest(requestChannel, _))
      testableSelector.waitForOperations(SelectorOperation.Send, 2)
      testableServer.waitForChannelClose(failedConnectionId, locallyClosed = false)

      val successfulSocket = if (isSocketConnectionId(failedConnectionId, sockets(0))) sockets(1) else sockets(0)
      assertProcessorHealthy(testableServer, Seq(successfulSocket))
    })
  }

  /**
   * Tests exception handling in [[Processor.processNewResponses]] when [[Selector.send]]
   * to a channel in closing state throws an exception. Test scenario is similar to
   * [[SocketServerTest.processNewResponseException]].
   */
  @Test
  def closingChannelException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector
      testableSelector.updateMinWakeup(2)

      val sockets = (1 to 2).map(_ => connect(testableServer))
      val serializedBytes = producerRequestBytes()
      val request = sendRequestsUntilStagedReceive(testableServer, sockets(0), serializedBytes)
      sendRequest(sockets(1), serializedBytes)

      testableSelector.addFailure(SelectorOperation.Send)
      sockets(0).close()
      processRequest(testableServer.dataPlaneRequestChannel, request)
      processRequest(testableServer.dataPlaneRequestChannel) // Also process request from other channel
      testableSelector.waitForOperations(SelectorOperation.Send, 2)
      testableServer.waitForChannelClose(request.context.connectionId, locallyClosed = true)

      assertProcessorHealthy(testableServer, Seq(sockets(1)))
    })
  }

  /**
   * Tests exception handling in [[Processor.processCompletedReceives]]. Exception is
   * injected into [[Selector.mute]] which is used to mute the channel when a receive is complete.
   * Test creates two receives in a single iteration by caching completed receives until two receives
   * are complete.
   * Verifies that
   * - first failed channel is closed
   * - second receive is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def processCompletedReceiveException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val sockets = (1 to 2).map(_ => connect(testableServer))
      val testableSelector = testableServer.testableSelector
      val requestChannel = testableServer.dataPlaneRequestChannel

      testableSelector.cachedCompletedReceives.minPerPoll = 2
      testableSelector.addFailure(SelectorOperation.Mute)
      sockets.foreach(sendRequest(_, producerRequestBytes()))
      val requests = sockets.map(_ => receiveRequest(requestChannel))
      testableSelector.waitForOperations(SelectorOperation.Mute, 2)
      testableServer.waitForChannelClose(testableSelector.allFailedChannels.head, locallyClosed = true)
      requests.foreach(processRequest(requestChannel, _))

      assertProcessorHealthy(testableServer, testableSelector.notFailed(sockets))
    })
  }

  /**
   * Tests exception handling in [[Processor.processCompletedSends]]. Exception is
   * injected into [[Selector.unmute]] which is used to unmute the channel after send is complete.
   * Test creates two completed sends in a single iteration by caching completed sends until two
   * sends are complete.
   * Verifies that
   * - first failed channel is closed
   * - second send is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def processCompletedSendException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector
      val sockets = (1 to 2).map(_ => connect(testableServer))
      val requests = sockets.map(sendAndReceiveRequest(_, testableServer))

      testableSelector.addFailure(SelectorOperation.Unmute)
      requests.foreach(processRequest(testableServer.dataPlaneRequestChannel, _))
      testableSelector.waitForOperations(SelectorOperation.Unmute, 2)
      testableServer.waitForChannelClose(testableSelector.allFailedChannels.head, locallyClosed = true)

      assertProcessorHealthy(testableServer, testableSelector.notFailed(sockets))
    })
  }

  /**
   * Tests exception handling in [[Processor.processDisconnected]]. An invalid connectionId
   * is inserted to the disconnected list just before the actual valid one.
   * Verifies that
   * - first invalid connectionId is ignored
   * - second disconnected channel is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def processDisconnectedException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val (socket, connectionId) = connectAndProcessRequest(testableServer)
      val testableSelector = testableServer.testableSelector

      // Add an invalid connectionId to `Selector.disconnected` list before the actual disconnected channel
      // and check that the actual id is processed and the invalid one ignored.
      testableSelector.cachedDisconnected.minPerPoll = 2
      testableSelector.cachedDisconnected.deferredValues += "notAValidConnectionId" -> ChannelState.EXPIRED
      socket.close()
      testableSelector.operationCounts.clear()
      testableSelector.waitForOperations(SelectorOperation.Poll, 1)
      testableServer.waitForChannelClose(connectionId, locallyClosed = false)

      assertProcessorHealthy(testableServer)
    })
  }

  /**
   * Tests that `Processor` continues to function correctly after a failed [[Selector.poll]].
   */
  @Test
  def pollException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val (socket, _) = connectAndProcessRequest(testableServer)
      val testableSelector = testableServer.testableSelector

      testableSelector.addFailure(SelectorOperation.Poll)
      testableSelector.operationCounts.clear()
      testableSelector.waitForOperations(SelectorOperation.Poll, 2)

      assertProcessorHealthy(testableServer, Seq(socket))
    })
  }

  /**
   * Tests handling of `ControlThrowable`. Verifies that the selector is closed.
   */
  @Test
  def controlThrowable(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      connectAndProcessRequest(testableServer)
      val testableSelector = testableServer.testableSelector

      testableSelector.operationCounts.clear()
      testableSelector.addFailure(SelectorOperation.Poll,
          Some(new RuntimeException("ControlThrowable exception during poll()") with ControlThrowable))
      testableSelector.waitForOperations(SelectorOperation.Poll, 1)

      testableSelector.waitForOperations(SelectorOperation.CloseSelector, 1)
    })
  }

  @Test
  def testConnectionRateLimit(): Unit = {
    shutdownServerAndMetrics(server)
    val numConnections = 5
    props.put("max.connections.per.ip", numConnections.toString)
    val testableServer = new TestableSocketServer(KafkaConfig.fromProps(props), connectionQueueSize = 1)
    testableServer.startup()
    val testableSelector = testableServer.testableSelector
    val errors = new mutable.HashSet[String]

    def acceptorStackTraces: scala.collection.Map[Thread, String] = {
      Thread.getAllStackTraces.asScala.filterKeys(_.getName.contains("kafka-socket-acceptor"))
        .mapValues(_.toList.mkString("\n"))
    }

    def acceptorBlocked: Boolean = {
      val stackTraces = acceptorStackTraces
      if (stackTraces.isEmpty)
        errors.add(s"Acceptor thread not found, threads=${Thread.getAllStackTraces.keySet}")
      stackTraces.exists { case (thread, stackTrace) =>
          thread.getState == Thread.State.WAITING && stackTrace.contains("ArrayBlockingQueue")
      }
    }

    def registeredConnectionCount: Int = testableSelector.operationCounts.getOrElse(SelectorOperation.Register, 0)

    try {
      // Block selector until Acceptor is blocked while connections are pending
      testableSelector.pollCallback = () => {
        try {
          TestUtils.waitUntilTrue(() => errors.nonEmpty || registeredConnectionCount >= numConnections - 1 || acceptorBlocked,
            "Acceptor not blocked", waitTimeMs = 10000)
        } catch {
          case _: Throwable => errors.add(s"Acceptor not blocked: $acceptorStackTraces")
        }
      }
      testableSelector.operationCounts.clear()
      val sockets = (1 to numConnections).map(_ => connect(testableServer))
      TestUtils.waitUntilTrue(() => errors.nonEmpty || registeredConnectionCount == numConnections,
        "Connections not registered", waitTimeMs = 15000)
      assertEquals(Set.empty, errors)
      testableSelector.waitForOperations(SelectorOperation.Register, numConnections)

      // In each iteration, SocketServer processes at most connectionQueueSize (1 in this test)
      // new connections and then does poll() to process data from existing connections. So for
      // 5 connections, we expect 5 iterations. Since we stop when the 5th connection is processed,
      // we can safely check that there were atleast 4 polls prior to the 5th connection.
      val pollCount = testableSelector.operationCounts(SelectorOperation.Poll)
      assertTrue(s"Connections created too quickly: $pollCount", pollCount >= numConnections - 1)
      verifyAcceptorBlockedPercent("PLAINTEXT", expectBlocked = true)

      assertProcessorHealthy(testableServer, sockets)
    } finally {
      shutdownServerAndMetrics(testableServer)
    }
  }

  private def withTestableServer(config : KafkaConfig = config, testWithServer: TestableSocketServer => Unit): Unit = {
    props.put("listeners", "PLAINTEXT://localhost:0")
    val testableServer = new TestableSocketServer(config)
    testableServer.startup()
    try {
        testWithServer(testableServer)
    } finally {
      shutdownServerAndMetrics(testableServer)
    }
  }

  def sendAndReceiveControllerRequest(socket: Socket, server: SocketServer): RequestChannel.Request = {
    sendRequest(socket, producerRequestBytes())
    receiveRequest(server.controlPlaneRequestChannelOpt.get)
  }

  private def assertProcessorHealthy(testableServer: TestableSocketServer, healthySockets: Seq[Socket] = Seq.empty): Unit = {
    val selector = testableServer.testableSelector
    selector.reset()
    val requestChannel = testableServer.dataPlaneRequestChannel

    // Check that existing channels behave as expected
    healthySockets.foreach { socket =>
      val request = sendAndReceiveRequest(socket, testableServer)
      processRequest(requestChannel, request)
      socket.close()
    }
    TestUtils.waitUntilTrue(() => testableServer.connectionCount(localAddress) == 0, "Channels not removed")

    // Check new channel behaves as expected
    val (socket, connectionId) = connectAndProcessRequest(testableServer)
    assertArrayEquals(producerRequestBytes(), receiveResponse(socket))
    assertNotNull("Channel should not have been closed", selector.channel(connectionId))
    assertNull("Channel should not be closing", selector.closingChannel(connectionId))
    socket.close()
    TestUtils.waitUntilTrue(() => testableServer.connectionCount(localAddress) == 0, "Channels not removed")
  }

  // Since all sockets use the same local host, it is sufficient to check the local port
  def isSocketConnectionId(connectionId: String, socket: Socket): Boolean =
    connectionId.contains(s":${socket.getLocalPort}-")

  private def verifyAcceptorBlockedPercent(listenerName: String, expectBlocked: Boolean): Unit = {
    val blockedPercentMetricMBeanName = "kafka.network:type=Acceptor,name=AcceptorBlockedPercent,listener=PLAINTEXT"
    val blockedPercentMetrics = YammerMetrics.defaultRegistry.allMetrics.asScala
      .filterKeys(_.getMBeanName == blockedPercentMetricMBeanName).values
    assertEquals(1, blockedPercentMetrics.size)
    val blockedPercentMetric = blockedPercentMetrics.head.asInstanceOf[Meter]
    val blockedPercent = blockedPercentMetric.meanRate
    if (expectBlocked) {
      assertTrue(s"Acceptor blocked percent not recorded: $blockedPercent", blockedPercent > 0.0)
      assertTrue(s"Unexpected blocked percent in acceptor: $blockedPercent", blockedPercent <= 1.0)
    } else {
      assertEquals(0.0, blockedPercent, 0.001)
    }
  }

  class TestableSocketServer(config : KafkaConfig = config, val connectionQueueSize: Int = 20,
                             override val time: Time = Time.SYSTEM) extends SocketServer(config,
      new Metrics, time, credentialProvider) {

    @volatile var selector: Option[TestableSelector] = None

    override def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
      new Processor(id, time, config.socketRequestMaxBytes, requestChannel, connectionQuotas, config.connectionsMaxIdleMs,
        config.failedAuthenticationDelayMs, listenerName, protocol, config, metrics, credentialProvider,
        memoryPool, new LogContext(), connectionQueueSize) {

        override protected[network] def createSelector(channelBuilder: ChannelBuilder): Selector = {
           val testableSelector = new TestableSelector(config, channelBuilder, time, metrics, metricTags.asScala)
           selector = Some(testableSelector)
           testableSelector
        }
      }
    }

    def testableSelector: TestableSelector =
      selector.getOrElse(throw new IllegalStateException("Selector not created"))

    def waitForChannelClose(connectionId: String, locallyClosed: Boolean): Unit = {
      val selector = testableSelector
      if (locallyClosed) {
        TestUtils.waitUntilTrue(() => selector.allLocallyClosedChannels.contains(connectionId),
            s"Channel not closed: $connectionId")
        assertTrue("Unexpected disconnect notification", testableSelector.allDisconnectedChannels.isEmpty)
      } else {
        TestUtils.waitUntilTrue(() => selector.allDisconnectedChannels.contains(connectionId),
            s"Disconnect notification not received: $connectionId")
        assertTrue("Channel closed locally", testableSelector.allLocallyClosedChannels.isEmpty)
      }
      val openCount = selector.allChannels.size - 1 // minus one for the channel just closed above
      TestUtils.waitUntilTrue(() => connectionCount(localAddress) == openCount, "Connection count not decremented")
      TestUtils.waitUntilTrue(() =>
        dataPlaneProcessor(0).inflightResponseCount == 0, "Inflight responses not cleared")
      assertNull("Channel not removed", selector.channel(connectionId))
      assertNull("Closing channel not removed", selector.closingChannel(connectionId))
    }
  }

  sealed trait SelectorOperation
  object SelectorOperation {
    case object Register extends SelectorOperation
    case object Poll extends SelectorOperation
    case object Send extends SelectorOperation
    case object Mute extends SelectorOperation
    case object Unmute extends SelectorOperation
    case object Wakeup extends SelectorOperation
    case object Close extends SelectorOperation
    case object CloseSelector extends SelectorOperation
  }

  class TestableSelector(config: KafkaConfig, channelBuilder: ChannelBuilder, time: Time, metrics: Metrics, metricTags: mutable.Map[String, String] = mutable.Map.empty)
        extends Selector(config.socketRequestMaxBytes, config.connectionsMaxIdleMs, config.failedAuthenticationDelayMs,
            metrics, time, "socket-server", metricTags.asJava, false, true, channelBuilder, MemoryPool.NONE, new LogContext()) {

    val failures = mutable.Map[SelectorOperation, Exception]()
    val operationCounts = mutable.Map[SelectorOperation, Int]().withDefaultValue(0)
    val allChannels = mutable.Set[String]()
    val allLocallyClosedChannels = mutable.Set[String]()
    val allDisconnectedChannels = mutable.Set[String]()
    val allFailedChannels = mutable.Set[String]()

    // Enable data from `Selector.poll()` to be deferred to a subsequent poll() until
    // the number of elements of that type reaches `minPerPoll`. This enables tests to verify
    // that failed processing doesn't impact subsequent processing within the same iteration.
    class PollData[T] {
      var minPerPoll = 1
      val deferredValues = mutable.Buffer[T]()
      val currentPollValues = mutable.Buffer[T]()
      def update(newValues: mutable.Buffer[T]): Unit = {
        if (currentPollValues.nonEmpty || deferredValues.size + newValues.size >= minPerPoll) {
          if (deferredValues.nonEmpty) {
            currentPollValues ++= deferredValues
            deferredValues.clear()
          }
          currentPollValues ++= newValues
        } else
          deferredValues ++= newValues
      }
      def reset(): Unit = {
        currentPollValues.clear()
      }
    }
    val cachedCompletedReceives = new PollData[NetworkReceive]()
    val cachedCompletedSends = new PollData[Send]()
    val cachedDisconnected = new PollData[(String, ChannelState)]()
    val allCachedPollData = Seq(cachedCompletedReceives, cachedCompletedSends, cachedDisconnected)
    @volatile var minWakeupCount = 0
    @volatile var pollTimeoutOverride: Option[Long] = None
    @volatile var pollCallback: () => Unit = () => {}

    def addFailure(operation: SelectorOperation, exception: Option[Exception] = None) {
      failures += operation ->
        exception.getOrElse(new IllegalStateException(s"Test exception during $operation"))
    }

    private def onOperation(operation: SelectorOperation, connectionId: Option[String], onFailure: => Unit): Unit = {
      operationCounts(operation) += 1
      failures.remove(operation).foreach { e =>
        connectionId.foreach(allFailedChannels.add)
        onFailure
        throw e
      }
    }

    def waitForOperations(operation: SelectorOperation, minExpectedTotal: Int): Unit = {
      TestUtils.waitUntilTrue(() =>
        operationCounts.getOrElse(operation, 0) >= minExpectedTotal, "Operations not performed within timeout")
    }

    def runOp[T](operation: SelectorOperation, connectionId: Option[String],
        onFailure: => Unit = {})(code: => T): T = {
      // If a failure is set on `operation`, throw that exception even if `code` fails
      try code
      finally onOperation(operation, connectionId, onFailure)
    }

    override def register(id: String, socketChannel: SocketChannel): Unit = {
      runOp(SelectorOperation.Register, Some(id), onFailure = close(id)) {
        super.register(id, socketChannel)
      }
    }

    override def send(s: Send): Unit = {
      runOp(SelectorOperation.Send, Some(s.destination)) {
        super.send(s)
      }
    }

    override def poll(timeout: Long): Unit = {
      try {
        pollCallback.apply()
        allCachedPollData.foreach(_.reset)
        runOp(SelectorOperation.Poll, None) {
          super.poll(pollTimeoutOverride.getOrElse(timeout))
        }
      } finally {
        super.channels.asScala.foreach(allChannels += _.id)
        allDisconnectedChannels ++= super.disconnected.asScala.keys
        cachedCompletedReceives.update(super.completedReceives.asScala)
        cachedCompletedSends.update(super.completedSends.asScala)
        cachedDisconnected.update(super.disconnected.asScala.toBuffer)
      }
    }

    override def mute(id: String): Unit = {
      runOp(SelectorOperation.Mute, Some(id)) {
        super.mute(id)
      }
    }

    override def unmute(id: String): Unit = {
      runOp(SelectorOperation.Unmute, Some(id)) {
        super.unmute(id)
      }
    }

    override def wakeup(): Unit = {
      runOp(SelectorOperation.Wakeup, None) {
        if (minWakeupCount > 0)
          minWakeupCount -= 1
        if (minWakeupCount <= 0)
          super.wakeup()
      }
    }

    override def disconnected: java.util.Map[String, ChannelState] = cachedDisconnected.currentPollValues.toMap.asJava

    override def completedSends: java.util.List[Send] = cachedCompletedSends.currentPollValues.asJava

    override def completedReceives: java.util.List[NetworkReceive] = cachedCompletedReceives.currentPollValues.asJava

    override def close(id: String): Unit = {
      runOp(SelectorOperation.Close, Some(id)) {
        super.close(id)
        allLocallyClosedChannels += id
      }
    }

    override def close(): Unit = {
      runOp(SelectorOperation.CloseSelector, None) {
        super.close()
      }
    }

    def updateMinWakeup(count: Int): Unit = {
      minWakeupCount = count
      // For tests that ignore wakeup to process responses together, increase poll timeout
      // to ensure that poll doesn't complete before the responses are ready
      pollTimeoutOverride = Some(1000L)
      // Wakeup current poll to force new poll timeout to take effect
      super.wakeup()
    }

    def reset(): Unit = {
      failures.clear()
      allCachedPollData.foreach(_.minPerPoll = 1)
    }

    def notFailed(sockets: Seq[Socket]): Seq[Socket] = {
      // Each test generates failure for exactly one failed channel
      assertEquals(1, allFailedChannels.size)
      val failedConnectionId = allFailedChannels.head
      sockets.filterNot(socket => isSocketConnectionId(failedConnectionId, socket))
    }
  }
}
