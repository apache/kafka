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
import java.nio.channels.{SelectionKey, SocketChannel}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue, ExecutionException, Executors, TimeUnit}
import java.util.{Collections, Properties, Random}
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode, TextNode}
import com.yammer.metrics.core.{Gauge, Meter}

import javax.net.ssl._
import kafka.cluster.EndPoint
import kafka.security.CredentialProvider
import kafka.server.{ApiVersionManager, KafkaConfig, SimpleApiVersionManager, ThrottleCallback, ThrottledChannel}
import kafka.utils.Implicits._
import kafka.utils.TestUtils
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.{ProduceRequestData, SaslAuthenticateRequestData, SaslHandshakeRequestData, VoteRequestData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteState
import org.apache.kafka.common.network.{ClientInformation, _}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, MockTime, Time, Utils}
import org.apache.kafka.server.common.{Features, MetadataVersion}
import org.apache.kafka.test.{TestSslUtils, TestUtils => JTestUtils}
import org.apache.log4j.Level
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._

import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.server.metrics.KafkaYammerMetrics

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.ControlThrowable

class SocketServerTest {
  val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
  props.put("listeners", "PLAINTEXT://localhost:0")
  props.put("num.network.threads", "1")
  props.put("socket.send.buffer.bytes", "300000")
  props.put("socket.receive.buffer.bytes", "300000")
  props.put("queued.max.requests", "50")
  props.put("socket.request.max.bytes", "100")
  props.put("max.connections.per.ip", "5")
  props.put("connections.max.idle.ms", "60000")
  val config = KafkaConfig.fromProps(props)
  val metrics = new Metrics
  val credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, null)
  val localAddress = InetAddress.getLoopbackAddress

  // Clean-up any metrics left around by previous tests
  TestUtils.clearYammerMetrics()

  private val apiVersionManager = new SimpleApiVersionManager(ListenerType.BROKER, true, false,
    () => new Features(MetadataVersion.latestTesting(), Collections.emptyMap[String, java.lang.Short], 0, true))
  var server: SocketServer = _
  val sockets = new ArrayBuffer[Socket]

  private val kafkaLogger = org.apache.log4j.LogManager.getLogger("kafka")
  private var logLevelToRestore: Level = _
  def endpoint: EndPoint = {
    KafkaConfig.fromProps(props, doLog = false).dataPlaneListeners.head
  }
  def listener: String = endpoint.listenerName.value
  val uncaughtExceptions = new AtomicInteger(0)

  @BeforeEach
  def setUp(): Unit = {
    server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager);
    server.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
    // Run the tests with TRACE logging to exercise request logging path
    logLevelToRestore = kafkaLogger.getLevel
    kafkaLogger.setLevel(Level.TRACE)

    assertTrue(server.controlPlaneRequestChannelOpt.isEmpty)
  }

  @AfterEach
  def tearDown(): Unit = {
    shutdownServerAndMetrics(server)
    sockets.foreach(_.close())
    sockets.clear()
    kafkaLogger.setLevel(logLevelToRestore)
    TestUtils.clearYammerMetrics()
  }

  def sendRequest(socket: Socket, request: Array[Byte], id: Option[Short] = None, flush: Boolean = true): Unit = {
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

  def sendApiRequest(socket: Socket, request: AbstractRequest, header: RequestHeader): Unit = {
    val serializedBytes = Utils.toArray(request.serializeWithHeader(header))
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
    channel.receiveRequest(timeout) match {
      case request: RequestChannel.Request => request
      case RequestChannel.WakeupRequest => throw new AssertionError("Unexpected wakeup received")
      case request: RequestChannel.CallbackRequest => throw new AssertionError("Unexpected callback received")
      case RequestChannel.ShutdownRequest => throw new AssertionError("Unexpected shutdown received")
      case null => throw new AssertionError("receiveRequest timed out")
    }
  }

  /* A simple request handler that just echos back the response */
  def processRequest(channel: RequestChannel): Unit = {
    processRequest(channel, receiveRequest(channel))
  }

  def processRequest(channel: RequestChannel, request: RequestChannel.Request): Unit = {
    val byteBuffer = request.body[AbstractRequest].serializeWithHeader(request.header)
    val send = new NetworkSend(request.context.connectionId, ByteBufferSend.sizePrefixed(byteBuffer))
    val headerLog = RequestConvertToJson.requestHeaderNode(request.header)
    channel.sendResponse(new RequestChannel.SendResponse(request, send, Some(headerLog), None))
  }

  def processRequestNoOpResponse(channel: RequestChannel, request: RequestChannel.Request): Unit = {
    channel.sendNoOpResponse(request)
  }

  def connect(s: SocketServer = server,
              listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
              localAddr: InetAddress = null,
              port: Int = 0): Socket = {
    val boundPort = try {
      s.boundPort(listenerName)
    } catch {
      case e: Throwable => throw new RuntimeException("Unable to find bound port for listener " +
        s"${listenerName}", e)
    }
    val socket = try {
      new Socket("localhost", boundPort, localAddr, port)
    } catch {
      case e: Throwable => throw new RuntimeException(s"Unable to connect to remote port ${boundPort} " +
        s"with local port ${port} on listener ${listenerName}", e)
    }
    sockets += socket
    socket
  }

  def sslConnect(s: SocketServer = server): Socket = {
    val socket = sslClientSocket(s.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SSL)))
    sockets += socket
    socket
  }

  private def sslClientSocket(port: Int): Socket = {
    val sslContext = SSLContext.getInstance(TestSslUtils.DEFAULT_TLS_PROTOCOL_FOR_TESTS)
    sslContext.init(null, Array(TestUtils.trustAllCerts), new java.security.SecureRandom())
    val socketFactory = sslContext.getSocketFactory
    val socket = socketFactory.createSocket("localhost", port)
    socket.asInstanceOf[SSLSocket].setNeedClientAuth(false)
    socket
  }

  // Create a client connection, process one request and return (client socket, connectionId)
  def connectAndProcessRequest(s: SocketServer): (Socket, String) = {
    val securityProtocol = s.dataPlaneAcceptors.asScala.head._1.securityProtocol
    val socket = securityProtocol match {
      case SecurityProtocol.PLAINTEXT | SecurityProtocol.SASL_PLAINTEXT =>
        connect(s)
      case SecurityProtocol.SSL | SecurityProtocol.SASL_SSL =>
        sslConnect(s)
      case _ =>
        throw new IllegalStateException(s"Unexpected security protocol $securityProtocol")
    }
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

  private def producerRequestBytes(apiVersion: Short = ApiKeys.PRODUCE.latestVersion, ack: Short = 0): Array[Byte] = {
    val correlationId = -1
    val clientId = ""
    val ackTimeoutMs = 10000

    val emptyRequest = requests.ProduceRequest.forCurrentMagic(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection())
      .setAcks(ack)
      .setTimeoutMs(ackTimeoutMs)
      .setTransactionalId(null))
      .build(apiVersion)
    val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)
    Utils.toArray(emptyRequest.serializeWithHeader(emptyHeader))
  }

  private def apiVersionRequestBytes(clientId: String, version: Short): Array[Byte] = {
    val request = new ApiVersionsRequest.Builder().build(version)
    val header = new RequestHeader(ApiKeys.API_VERSIONS, request.version(), clientId, -1)
    Utils.toArray(request.serializeWithHeader(header))
  }

  @Test
  def simpleRequest(): Unit = {
    val plainSocket = connect()
    val serializedBytes = producerRequestBytes()

    // Test PLAINTEXT socket
    sendRequest(plainSocket, serializedBytes)
    processRequest(server.dataPlaneRequestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(plainSocket).toSeq)
    verifyAcceptorBlockedPercent("PLAINTEXT", expectBlocked = false)
  }


  private def testClientInformation(version: Short, expectedClientSoftwareName: String,
                                    expectedClientSoftwareVersion: String): Unit = {
    val plainSocket = connect()
    val address = plainSocket.getLocalAddress
    val clientId = "clientId"

    // Send ApiVersionsRequest - unknown expected
    sendRequest(plainSocket, apiVersionRequestBytes(clientId, version))
    var receivedReq = receiveRequest(server.dataPlaneRequestChannel)

    assertEquals(ClientInformation.UNKNOWN_NAME_OR_VERSION, receivedReq.context.clientInformation.softwareName)
    assertEquals(ClientInformation.UNKNOWN_NAME_OR_VERSION, receivedReq.context.clientInformation.softwareVersion)

    server.dataPlaneRequestChannel.sendNoOpResponse(receivedReq)

    // Send ProduceRequest - client info expected
    sendRequest(plainSocket, producerRequestBytes())
    receivedReq = receiveRequest(server.dataPlaneRequestChannel)

    assertEquals(expectedClientSoftwareName, receivedReq.context.clientInformation.softwareName)
    assertEquals(expectedClientSoftwareVersion, receivedReq.context.clientInformation.softwareVersion)

    server.dataPlaneRequestChannel.sendNoOpResponse(receivedReq)

    // Close the socket
    plainSocket.setSoLinger(true, 0)
    plainSocket.close()

    TestUtils.waitUntilTrue(() => server.connectionCount(address) == 0, msg = "Connection not closed")
  }

  @Test
  def testClientInformationWithLatestApiVersionsRequest(): Unit = {
    testClientInformation(
      ApiKeys.API_VERSIONS.latestVersion,
      "apache-kafka-java",
      AppInfoParser.getVersion
    )
  }

  @Test
  def testClientInformationWithOldestApiVersionsRequest(): Unit = {
    testClientInformation(
      ApiKeys.API_VERSIONS.oldestVersion,
      ClientInformation.UNKNOWN_NAME_OR_VERSION,
      ClientInformation.UNKNOWN_NAME_OR_VERSION
    )
  }

  @Test
  def testRequestPerSecAndDeprecatedRequestsPerSecMetrics(): Unit = {
    val clientName = "apache-kafka-java"
    val clientVersion = AppInfoParser.getVersion

    def deprecatedRequestsPerSec(requestVersion: Short): Option[Long] =
      TestUtils.meterCountOpt(s"${RequestMetrics.DeprecatedRequestsPerSec},request=Produce,version=$requestVersion," +
        s"clientSoftwareName=$clientName,clientSoftwareVersion=$clientVersion")

    def requestsPerSec(requestVersion: Short): Option[Long] =
      TestUtils.meterCountOpt(s"${RequestMetrics.RequestsPerSec},request=Produce,version=$requestVersion")

    val plainSocket = connect()
    val address = plainSocket.getLocalAddress
    val clientId = "clientId"

    sendRequest(plainSocket, apiVersionRequestBytes(clientId, ApiKeys.API_VERSIONS.latestVersion))
    var receivedReq = receiveRequest(server.dataPlaneRequestChannel)
    server.dataPlaneRequestChannel.sendNoOpResponse(receivedReq)

    var requestVersion = ApiKeys.PRODUCE.latestVersion
    sendRequest(plainSocket, producerRequestBytes(requestVersion))
    receivedReq = receiveRequest(server.dataPlaneRequestChannel)

    assertEquals(clientName, receivedReq.context.clientInformation.softwareName)
    assertEquals(clientVersion, receivedReq.context.clientInformation.softwareVersion)

    server.dataPlaneRequestChannel.sendNoOpResponse(receivedReq)
    TestUtils.waitUntilTrue(() => requestsPerSec(requestVersion).isDefined, "RequestsPerSec metric could not be found")
    assertTrue(requestsPerSec(requestVersion).getOrElse(0L) > 0, "RequestsPerSec should be higher than 0")
    assertEquals(None, deprecatedRequestsPerSec(requestVersion))

    requestVersion = 3
    sendRequest(plainSocket, producerRequestBytes(requestVersion))
    receivedReq = receiveRequest(server.dataPlaneRequestChannel)
    server.dataPlaneRequestChannel.sendNoOpResponse(receivedReq)
    TestUtils.waitUntilTrue(() => deprecatedRequestsPerSec(requestVersion).isDefined, "DeprecatedRequestsPerSec metric could not be found")
    assertTrue(deprecatedRequestsPerSec(requestVersion).getOrElse(0L) > 0, "DeprecatedRequestsPerSec should be higher than 0")

    plainSocket.setSoLinger(true, 0)
    plainSocket.close()

    TestUtils.waitUntilTrue(() => server.connectionCount(address) == 0, msg = "Connection not closed")

  }

  @Test
  def testStagedListenerStartup(): Unit = {
    shutdownServerAndMetrics(server)
    val testProps = new Properties
    testProps ++= props
    testProps.put("listeners", "EXTERNAL://localhost:0,INTERNAL://localhost:0,CONTROL_PLANE://localhost:0")
    testProps.put("listener.security.protocol.map", "EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROL_PLANE:PLAINTEXT")
    testProps.put("control.plane.listener.name", "CONTROL_PLANE")
    testProps.put("inter.broker.listener.name", "INTERNAL")
    val config = KafkaConfig.fromProps(testProps)
    val testableServer = new TestableSocketServer(config)

    val updatedEndPoints = config.effectiveAdvertisedListeners.map { endpoint =>
      endpoint.copy(port = testableServer.boundPort(endpoint.listenerName))
    }.map(_.toJava)

    val externalReadyFuture = new CompletableFuture[Void]()

    def controlPlaneListenerStarted() = {
      try {
        val socket = connect(testableServer, config.controlPlaneListenerName.get, localAddr = InetAddress.getLocalHost)
        sendAndReceiveControllerRequest(socket, testableServer)
        true
      } catch {
        case _: Throwable => false
      }
    }

    def listenerStarted(listenerName: ListenerName) = {
      try {
        val socket = connect(testableServer, listenerName, localAddr = InetAddress.getLocalHost)
        sendAndReceiveRequest(socket, testableServer)
        true
      } catch {
        case _: Throwable => false
      }
    }

    try {
      val externalListener = new ListenerName("EXTERNAL")
      val externalEndpoint = updatedEndPoints.find(e => e.listenerName.get == externalListener.value).get
      val controlPlaneListener = new ListenerName("CONTROL_PLANE")
      val controlPlaneEndpoint = updatedEndPoints.find(e => e.listenerName.get == controlPlaneListener.value).get
      val futures = Map(
        externalEndpoint -> externalReadyFuture,
        controlPlaneEndpoint -> CompletableFuture.completedFuture[Void](null))
      val requestProcessingFuture = testableServer.enableRequestProcessing(futures)
      TestUtils.waitUntilTrue(() => controlPlaneListenerStarted(), "Control plane listener not started")
      assertFalse(listenerStarted(config.interBrokerListenerName))
      assertFalse(listenerStarted(externalListener))
      externalReadyFuture.complete(null)
      TestUtils.waitUntilTrue(() => listenerStarted(config.interBrokerListenerName), "Inter-broker listener not started")
      TestUtils.waitUntilTrue(() => listenerStarted(externalListener), "External listener not started")
      requestProcessingFuture.get(1, TimeUnit.MINUTES)
    } finally {
      shutdownServerAndMetrics(testableServer)
    }
  }

  @Test
  def testStagedListenerShutdownWhenConnectionQueueIsFull(): Unit = {
    shutdownServerAndMetrics(server)
    val testProps = new Properties
    testProps ++= props
    testProps.put("listeners", "EXTERNAL://localhost:0,INTERNAL://localhost:0,CONTROLLER://localhost:0")
    testProps.put("listener.security.protocol.map", "EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
    testProps.put("control.plane.listener.name", "CONTROLLER")
    testProps.put("inter.broker.listener.name", "INTERNAL")
    val config = KafkaConfig.fromProps(testProps)
    val connectionQueueSize = 1
    val testableServer = new TestableSocketServer(config, connectionQueueSize)
    testableServer.enableRequestProcessing(Map()).get(1, TimeUnit.MINUTES)

    val socket1 = connect(testableServer, new ListenerName("EXTERNAL"), localAddr = InetAddress.getLocalHost)
    sendRequest(socket1, producerRequestBytes())
    val socket2 = connect(testableServer, new ListenerName("EXTERNAL"), localAddr = InetAddress.getLocalHost)
    sendRequest(socket2, producerRequestBytes())

    testableServer.shutdown()
  }

  @Test
  def testDisabledRequestIsRejected(): Unit = {
    val correlationId = 57
    val header = new RequestHeader(ApiKeys.VOTE, 0, "", correlationId)
    val request = new VoteRequest.Builder(new VoteRequestData()).build()
    val serializedBytes = Utils.toArray(request.serializeWithHeader(header))

    val socket = connect()

    val outgoing = new DataOutputStream(socket.getOutputStream)
    try {
      outgoing.writeInt(serializedBytes.length)
      outgoing.write(serializedBytes)
      outgoing.flush()
      receiveResponse(socket)
    } catch {
      case _: IOException => // we expect the server to close the socket
    } finally {
      outgoing.close()
    }
  }

  @Test
  def tooBigRequestIsRejected(): Unit = {
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
      case _: IOException => // that's fine
    }
  }

  @Test
  def testGracefulClose(): Unit = {
    val plainSocket = connect()
    val serializedBytes = producerRequestBytes()

    for (_ <- 0 until 10)
      sendRequest(plainSocket, serializedBytes)
    plainSocket.close()
    for (_ <- 0 until 10) {
      val request = receiveRequest(server.dataPlaneRequestChannel)
      assertNotNull(request, "receiveRequest timed out")
      processRequestNoOpResponse(server.dataPlaneRequestChannel, request)
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
      assertNotNull(request, "receiveRequest timed out")
      processRequestNoOpResponse(server.dataPlaneRequestChannel, request)
    }
  }

  @Test
  def testConnectionId(): Unit = {
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
  def testIdleConnection(): Unit = {
    val idleTimeMs = 60000
    val time = new MockTime()
    props.put(KafkaConfig.ConnectionsMaxIdleMsProp, idleTimeMs.toString)
    val serverMetrics = new Metrics
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics,
      time, credentialProvider, apiVersionManager)

    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
      val serializedBytes = producerRequestBytes()

      // Connection with no outstanding requests
      val socket0 = connect(overrideServer)
      sendRequest(socket0, serializedBytes)
      val request0 = receiveRequest(overrideServer.dataPlaneRequestChannel)
      processRequest(overrideServer.dataPlaneRequestChannel, request0)
      assertTrue(openChannel(request0, overrideServer).nonEmpty, "Channel not open")
      assertEquals(openChannel(request0, overrideServer), openOrClosingChannel(request0, overrideServer))
      // Receive response to make sure activity on socket server processor thread quiesces, otherwise
      // it may continue after the mock time sleep, so there would be events that would mark the
      // connection as "up-to-date" after the sleep and prevent connection from being idle.
      receiveResponse(socket0)
      TestUtils.waitUntilTrue(() => !openChannel(request0, overrideServer).get.isMuted, "Failed to unmute channel")
      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request0, overrideServer).isEmpty, "Failed to close idle channel")
      assertTrue(openChannel(request0, overrideServer).isEmpty, "Channel not removed")

      // Connection with one request being processed (channel is muted), no other in-flight requests
      val socket1 = connect(overrideServer)
      sendRequest(socket1, serializedBytes)
      val request1 = receiveRequest(overrideServer.dataPlaneRequestChannel)
      assertTrue(openChannel(request1, overrideServer).nonEmpty, "Channel not open")
      assertEquals(openChannel(request1, overrideServer), openOrClosingChannel(request1, overrideServer))
      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request1, overrideServer).isEmpty, "Failed to close idle channel")
      assertTrue(openChannel(request1, overrideServer).isEmpty, "Channel not removed")
      processRequest(overrideServer.dataPlaneRequestChannel, request1)

      // Connection with one request being processed (channel is muted), more in-flight requests
      val socket2 = connect(overrideServer)
      val request2 = sendRequestsReceiveOne(overrideServer, socket2, serializedBytes, 3)
      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request2, overrideServer).isEmpty, "Failed to close idle channel")
      assertTrue(openChannel(request1, overrideServer).isEmpty, "Channel not removed")
      processRequest(overrideServer.dataPlaneRequestChannel, request2) // this triggers a failed send since channel has been closed
      assertNull(overrideServer.dataPlaneRequestChannel.receiveRequest(200), "Received request on expired channel")

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testConnectionIdReuse(): Unit = {
    val idleTimeMs = 60000
    props.put(KafkaConfig.ConnectionsMaxIdleMsProp, idleTimeMs.toString)
    props ++= sslServerProps
    val overrideConnectionId = "127.0.0.1:1-127.0.0.1:2-0"
    val overrideServer = new TestableSocketServer(KafkaConfig.fromProps(props))

    def openChannel: Option[KafkaChannel] = overrideServer.dataPlaneAcceptor(listener).get.processors(0).channel(overrideConnectionId)
    def openOrClosingChannel: Option[KafkaChannel] = overrideServer.dataPlaneAcceptor(listener).get.processors(0).openOrClosingChannel(overrideConnectionId)
    def connectionCount = overrideServer.connectionCount(InetAddress.getByName("127.0.0.1"))

    // Create a client connection and wait for server to register the connection with the selector. For
    // test scenarios below where `Selector.register` fails, the wait ensures that checks are performed
    // only after `register` is processed by the server.
    def connectAndWaitForConnectionRegister(): Socket = {
      val connections = overrideServer.testableSelector.operationCounts(SelectorOperation.Register)
      val socket = sslConnect(overrideServer)
      TestUtils.waitUntilTrue(() =>
        overrideServer.testableSelector.operationCounts(SelectorOperation.Register) == connections + 1, "Connection not registered")
      socket
    }

    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
      overrideServer.testableProcessor.setConnectionId(overrideConnectionId)
      val socket1 = connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1 && openChannel.isDefined, "Failed to create channel")
      val channel1 = openChannel.getOrElse(throw new RuntimeException("Channel not found"))

      // Create new connection with same id when `channel1` is still open and in Selector.channels
      // Check that new connection is closed and openChannel still contains `channel1`
      connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1, "Failed to close channel")
      assertSame(channel1, openChannel.getOrElse(throw new RuntimeException("Channel not found")))
      socket1.close()
      TestUtils.waitUntilTrue(() => openChannel.isEmpty, "Channel not closed")

      // Create a channel with buffered receive and close remote connection
      val request = makeChannelWithBufferedRequestsAndCloseRemote(overrideServer, overrideServer.testableSelector)
      val channel2 = openChannel.getOrElse(throw new RuntimeException("Channel not found"))

      // Create new connection with same id when `channel2` is closing, but still in Selector.channels
      // Check that new connection is closed and openOrClosingChannel still contains `channel2`
      connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1, "Failed to close channel")
      assertSame(channel2, openOrClosingChannel.getOrElse(throw new RuntimeException("Channel not found")))

      // Complete request with failed send so that `channel2` is removed from Selector.channels
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

  private def makeSocketWithBufferedRequests(server: SocketServer,
                                             serverSelector: Selector,
                                             proxyServer: ProxyServer,
                                             numBufferedRequests: Int = 2): (Socket, RequestChannel.Request) = {

    val requestBytes = producerRequestBytes()
    val socket = sslClientSocket(proxyServer.localPort)
    sendRequest(socket, requestBytes)
    val request1 = receiveRequest(server.dataPlaneRequestChannel)

    val connectionId = request1.context.connectionId
    val channel = server.dataPlaneAcceptor(listener).get.processors(0).channel(connectionId).getOrElse(throw new IllegalStateException("Channel not found"))
    val transportLayer: SslTransportLayer = JTestUtils.fieldValue(channel, classOf[KafkaChannel], "transportLayer")
    val netReadBuffer: ByteBuffer = JTestUtils.fieldValue(transportLayer, classOf[SslTransportLayer], "netReadBuffer")

    proxyServer.enableBuffering(netReadBuffer)
    (1 to numBufferedRequests).foreach { _ => sendRequest(socket, requestBytes) }

    val keysWithBufferedRead: util.Set[SelectionKey] = JTestUtils.fieldValue(serverSelector, classOf[Selector], "keysWithBufferedRead")
    keysWithBufferedRead.add(channel.selectionKey)
    JTestUtils.setFieldValue(transportLayer, "hasBytesBuffered", true)

    (socket, request1)
  }

  /**
   * Create a channel with data in SSL buffers and close the remote connection.
   * The channel should remain open in SocketServer even if it detects that the peer has closed
   * the connection since there is pending data to be processed.
   */
  private def makeChannelWithBufferedRequestsAndCloseRemote(server: TestableSocketServer,
                                                            serverSelector: TestableSelector,
                                                            makeClosing: Boolean = false): RequestChannel.Request = {

    val proxyServer = new ProxyServer(server)
    try {
      val (socket, request1) = makeSocketWithBufferedRequests(server, serverSelector, proxyServer)

      socket.close()
      proxyServer.serverConnSocket.close()
      TestUtils.waitUntilTrue(() => proxyServer.clientConnSocket.isClosed, "Client socket not closed", waitTimeMs = 10000)

      processRequestNoOpResponse(server.dataPlaneRequestChannel, request1)
      val channel = openOrClosingChannel(request1, server).getOrElse(throw new IllegalStateException("Channel closed too early"))
      if (makeClosing)
        serverSelector.pendingClosingChannels.add(channel)

      receiveRequest(server.dataPlaneRequestChannel, timeout = 10000)
    } finally {
      proxyServer.close()
    }
  }

  def sendRequestsReceiveOne(server: SocketServer, socket: Socket, requestBytes: Array[Byte], numRequests: Int): RequestChannel.Request = {
    (1 to numRequests).foreach(i => sendRequest(socket, requestBytes, flush = i == numRequests))
    receiveRequest(server.dataPlaneRequestChannel)
  }

  private def closeSocketWithPendingRequest(server: SocketServer,
                                            createSocket: () => Socket): RequestChannel.Request = {

    def maybeReceiveRequest(): Option[RequestChannel.Request] = {
      try {
        Some(receiveRequest(server.dataPlaneRequestChannel, timeout = 1000))
      } catch {
        case _: Exception => None
      }
    }

    def closedChannelWithPendingRequest(): Option[RequestChannel.Request] = {
      val socket = createSocket.apply()
      val req1 = sendRequestsReceiveOne(server, socket, producerRequestBytes(), numRequests = 100)
      processRequestNoOpResponse(server.dataPlaneRequestChannel, req1)
      // Set SoLinger to 0 to force a hard disconnect via TCP RST
      socket.setSoLinger(true, 0)
      socket.close()

      maybeReceiveRequest().flatMap { req =>
        processRequestNoOpResponse(server.dataPlaneRequestChannel, req)
        maybeReceiveRequest()
      }
    }

    val (request, _) = TestUtils.computeUntilTrue(closedChannelWithPendingRequest()) { req => req.nonEmpty }
    request.getOrElse(throw new IllegalStateException("Could not create close channel with pending request"))
  }

  // Prepares test setup for throttled channel tests. throttlingDone controls whether or not throttling has completed
  // in quota manager.
  def throttledChannelTestSetUp(socket: Socket, serializedBytes: Array[Byte], noOpResponse: Boolean,
                                throttlingInProgress: Boolean): RequestChannel.Request = {
    sendRequest(socket, serializedBytes)

    // Mimic a primitive request handler that fetches the request from RequestChannel and place a response with a
    // throttled channel.
    val request = receiveRequest(server.dataPlaneRequestChannel)
    val byteBuffer = request.body[AbstractRequest].serializeWithHeader(request.header)
    val send = new NetworkSend(request.context.connectionId, ByteBufferSend.sizePrefixed(byteBuffer))

    val channelThrottlingCallback = new ThrottleCallback {
      override def startThrottling(): Unit = server.dataPlaneRequestChannel.startThrottling(request)
      override def endThrottling(): Unit = server.dataPlaneRequestChannel.endThrottling(request)
    }
    val throttledChannel = new ThrottledChannel(new MockTime(), 100, channelThrottlingCallback)
    val headerLog = RequestConvertToJson.requestHeaderNode(request.header)
    val response =
      if (!noOpResponse)
        new RequestChannel.SendResponse(request, send, Some(headerLog), None)
      else
        new RequestChannel.NoOpResponse(request)
    server.dataPlaneRequestChannel.sendResponse(response)

    // Quota manager would call notifyThrottlingDone() on throttling completion. Simulate it if throttlingInProgress is
    // false.
    if (!throttlingInProgress)
      throttledChannel.notifyThrottlingDone()

    request
  }

  def openChannel(request: RequestChannel.Request, server: SocketServer = this.server): Option[KafkaChannel] =
    server.dataPlaneAcceptor(listener).get.processors(0).channel(request.context.connectionId)

  def openOrClosingChannel(request: RequestChannel.Request, server: SocketServer = this.server): Option[KafkaChannel] =
    server.dataPlaneAcceptor(listener).get.processors(0).openOrClosingChannel(request.context.connectionId)

  @Test
  def testSendActionResponseWithThrottledChannelWhereThrottlingInProgress(): Unit = {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, noOpResponse = false, throttlingInProgress = true)

    // receive response
    assertEquals(serializedBytes.toSeq, receiveResponse(socket).toSeq)
    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.MUTED_AND_THROTTLED), "fail")
    // Channel should still be muted.
    assertTrue(openOrClosingChannel(request).exists(c => c.isMuted))
  }

  @Test
  def testSendActionResponseWithThrottledChannelWhereThrottlingAlreadyDone(): Unit = {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, noOpResponse = false, throttlingInProgress = false)

    // receive response
    assertEquals(serializedBytes.toSeq, receiveResponse(socket).toSeq)
    // Since throttling is already done, the channel can be unmuted after sending out the response.
    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.NOT_MUTED), "fail")
    // Channel is now unmuted.
    assertFalse(openOrClosingChannel(request).exists(c => c.isMuted))
  }

  @Test
  def testNoOpActionResponseWithThrottledChannelWhereThrottlingInProgress(): Unit = {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, noOpResponse = true, throttlingInProgress = true)

    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.MUTED_AND_THROTTLED), "fail")
    // Channel should still be muted.
    assertTrue(openOrClosingChannel(request).exists(c => c.isMuted))
  }

  @Test
  def testNoOpActionResponseWithThrottledChannelWhereThrottlingAlreadyDone(): Unit = {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, noOpResponse = true, throttlingInProgress = false)

    // Since throttling is already done, the channel can be unmuted.
    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.NOT_MUTED), "fail")
    // Channel is now unmuted.
    assertFalse(openOrClosingChannel(request).exists(c => c.isMuted))
  }

  @Test
  def testSocketsCloseOnShutdown(): Unit = {
    // open a connection
    val plainSocket = connect()
    plainSocket.setTcpNoDelay(true)
    val bytes = new Array[Byte](40)
    // send a request first to make sure the connection has been picked up by the socket server
    sendRequest(plainSocket, bytes, Some(0))
    processRequest(server.dataPlaneRequestChannel)
    // the following sleep is necessary to reliably detect the connection close when we send data below
    Thread.sleep(200L)
    // make sure the sockets are open
    server.dataPlaneAcceptors.asScala.values.foreach(acceptor => assertFalse(acceptor.serverChannel.socket.isClosed))
    // then shutdown the server
    shutdownServerAndMetrics(server)

    verifyRemoteConnectionClosed(plainSocket)
  }

  @Test
  def testMaxConnectionsPerIp(): Unit = {
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
  def testZeroMaxConnectionsPerIp(): Unit = {
    val newProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    newProps.setProperty(KafkaConfig.MaxConnectionsPerIpProp, "0")
    newProps.setProperty(KafkaConfig.MaxConnectionsPerIpOverridesProp, "%s:%s".format("127.0.0.1", "5"))
    val server = new SocketServer(KafkaConfig.fromProps(newProps), new Metrics(),
      Time.SYSTEM, credentialProvider, apiVersionManager)
    try {
      server.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
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
  def testMaxConnectionsPerIpOverrides(): Unit = {
    val overrideNum = server.config.maxConnectionsPerIp + 1
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    overrideProps.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, s"localhost:$overrideNum")
    val serverMetrics = new Metrics()
    val overrideServer = new SocketServer(KafkaConfig.fromProps(overrideProps), serverMetrics,
      Time.SYSTEM, credentialProvider, apiVersionManager)
    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
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
  def testExceptionInAcceptor(): Unit = {
    val serverMetrics = new Metrics()

    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics,
      Time.SYSTEM, credentialProvider, apiVersionManager) {

      // same as SocketServer.createAcceptor,
      // except the Acceptor overriding a method to inject the exception
      override protected def createDataPlaneAcceptor(endPoint: EndPoint, isPrivilegedListener: Boolean, requestChannel: RequestChannel): DataPlaneAcceptor = {

        new DataPlaneAcceptor(this, endPoint, this.config, nodeId, connectionQuotas, time, false, requestChannel, serverMetrics, this.credentialProvider, new LogContext(), MemoryPool.NONE, this.apiVersionManager) {
          override protected def configureAcceptedSocketChannel(socketChannel: SocketChannel): Unit = {
            assertEquals(1, connectionQuotas.get(socketChannel.socket.getInetAddress))
            throw new IOException("test injected IOException")
          }
        }
      }
    }

    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
      val conn = connect(overrideServer)
      conn.setSoTimeout(3000)
      assertEquals(-1, conn.getInputStream.read())
      assertEquals(0, overrideServer.connectionQuotas.get(conn.getInetAddress))
    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testConnectionRatePerIp(): Unit = {
    val defaultTimeoutMs = 2000
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    overrideProps.remove(KafkaConfig.MaxConnectionsPerIpProp)
    overrideProps.put(KafkaConfig.NumQuotaSamplesProp, String.valueOf(2))
    val connectionRate = 5
    val time = new MockTime()
    val overrideServer = new SocketServer(KafkaConfig.fromProps(overrideProps), new Metrics(),
      time, credentialProvider, apiVersionManager)
    // update the connection rate to 5
    overrideServer.connectionQuotas.updateIpConnectionRateQuota(None, Some(connectionRate))
    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
      // make the (maximum allowable number + 1) of connections
      (0 to connectionRate).map(_ => connect(overrideServer))

      val acceptors = overrideServer.dataPlaneAcceptors.asScala.values
      // waiting for 5 connections got accepted and 1 connection got throttled
      TestUtils.waitUntilTrue(
        () => acceptors.foldLeft(0)((accumulator, acceptor) => accumulator + acceptor.throttledSockets.size) == 1,
        "timeout waiting for 1 connection to get throttled",
        defaultTimeoutMs)

      // now try one more, so that we can make sure this connection will get throttled
      var conn = connect(overrideServer)
      // there should be total 2 connection got throttled now
      TestUtils.waitUntilTrue(
        () => acceptors.foldLeft(0)((accumulator, acceptor) => accumulator + acceptor.throttledSockets.size) == 2,
        "timeout waiting for 2 connection to get throttled",
        defaultTimeoutMs)
      // advance time to unthrottle connections
      time.sleep(defaultTimeoutMs)
      acceptors.foreach(_.wakeup())
      // make sure there are no connection got throttled now(and the throttled connections should be closed)
      TestUtils.waitUntilTrue(() => acceptors.forall(_.throttledSockets.isEmpty),
        "timeout waiting for connection to be unthrottled",
        defaultTimeoutMs)
      // verify the connection is closed now
      verifyRemoteConnectionClosed(conn)

      // new connection should succeed after previous connection closed, and previous samples have been expired
      conn = connect(overrideServer)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn, serializedBytes)
      val request = overrideServer.dataPlaneRequestChannel.receiveRequest(defaultTimeoutMs)
      assertNotNull(request)
    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testThrottledSocketsClosedOnShutdown(): Unit = {
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    overrideProps.remove("max.connections.per.ip")
    overrideProps.put(KafkaConfig.NumQuotaSamplesProp, String.valueOf(2))
    val connectionRate = 5
    val time = new MockTime()
    val overrideServer = new SocketServer(KafkaConfig.fromProps(overrideProps), new Metrics(),
      time, credentialProvider, apiVersionManager)
    overrideServer.connectionQuotas.updateIpConnectionRateQuota(None, Some(connectionRate))
    overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
    // make the maximum allowable number of connections
    (0 until connectionRate).map(_ => connect(overrideServer))
    // now try one more (should get throttled)
    val conn = connect(overrideServer)
    // don't advance time so that connection never gets unthrottled
    shutdownServerAndMetrics(overrideServer)
    verifyRemoteConnectionClosed(conn)
  }

  private def verifyRemoteConnectionClosed(connection: Socket): Unit = {
    val largeChunkOfBytes = new Array[Byte](1000000)
    // doing a subsequent send should throw an exception as the connection should be closed.
    // send a large chunk of bytes to trigger a socket flush
    assertThrows(classOf[IOException], () => sendRequest(connection, largeChunkOfBytes, Some(0)))
  }

  @Test
  def testSslSocketServer(): Unit = {
    val serverMetrics = new Metrics
    val overrideServer = new SocketServer(KafkaConfig.fromProps(sslServerProps), serverMetrics,
      Time.SYSTEM, credentialProvider, apiVersionManager)
    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
      val sslContext = SSLContext.getInstance(TestSslUtils.DEFAULT_TLS_PROTOCOL_FOR_TESTS)
      sslContext.init(null, Array(TestUtils.trustAllCerts), new java.security.SecureRandom())
      val socketFactory = sslContext.getSocketFactory
      val sslSocket = socketFactory.createSocket("localhost",
        overrideServer.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SSL))).asInstanceOf[SSLSocket]
      sslSocket.setNeedClientAuth(false)

      val correlationId = -1
      val clientId = ""
      val ackTimeoutMs = 10000
      val ack = 0: Short
      val emptyRequest = requests.ProduceRequest.forCurrentMagic(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection())
        .setAcks(ack)
        .setTimeoutMs(ackTimeoutMs)
        .setTransactionalId(null))
        .build()
      val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)
      val serializedBytes = Utils.toArray(emptyRequest.serializeWithHeader(emptyHeader))

      sendRequest(sslSocket, serializedBytes)
      processRequest(overrideServer.dataPlaneRequestChannel)
      assertEquals(serializedBytes.toSeq, receiveResponse(sslSocket).toSeq)
      sslSocket.close()
    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testSaslReauthenticationFailureWithKip152SaslAuthenticate(): Unit = {
    checkSaslReauthenticationFailure(true)
  }

  @Test
  def testSaslReauthenticationFailureNoKip152SaslAuthenticate(): Unit = {
    checkSaslReauthenticationFailure(false)
  }

  def checkSaslReauthenticationFailure(leverageKip152SaslAuthenticateRequest : Boolean): Unit = {
    shutdownServerAndMetrics(server) // we will use our own instance because we require custom configs
    val username = "admin"
    val password = "admin-secret"
    val reauthMs = 1500
    props.setProperty("listeners", "SASL_PLAINTEXT://localhost:0")
    props.setProperty("security.inter.broker.protocol", "SASL_PLAINTEXT")
    props.setProperty("listener.name.sasl_plaintext.plain.sasl.jaas.config",
      "org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"%s\" password=\"%s\" user_%s=\"%s\";".format(username, password, username, password))
    props.setProperty("sasl.mechanism.inter.broker.protocol", "PLAIN")
    props.setProperty("listener.name.sasl_plaintext.sasl.enabled.mechanisms", "PLAIN")
    props.setProperty("num.network.threads", "1")
    props.setProperty("connections.max.reauth.ms", reauthMs.toString)
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect,
      saslProperties = Some(props), enableSaslPlaintext = true)
    val time = new MockTime()
    val overrideServer = new TestableSocketServer(KafkaConfig.fromProps(overrideProps), time = time)
    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
      val socket = connect(overrideServer, ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT))

      val correlationId = -1
      val clientId = ""
      // send a SASL handshake request
      val version : Short = if (leverageKip152SaslAuthenticateRequest) ApiKeys.SASL_HANDSHAKE.latestVersion else 0
      val saslHandshakeRequest = new SaslHandshakeRequest.Builder(new SaslHandshakeRequestData().setMechanism("PLAIN"))
        .build(version)
      val saslHandshakeHeader = new RequestHeader(ApiKeys.SASL_HANDSHAKE, saslHandshakeRequest.version, clientId,
        correlationId)
      sendApiRequest(socket, saslHandshakeRequest, saslHandshakeHeader)
      receiveResponse(socket)

      // now send credentials
      val authBytes = "admin\u0000admin\u0000admin-secret".getBytes(StandardCharsets.UTF_8)
      if (leverageKip152SaslAuthenticateRequest) {
        // send credentials within a SaslAuthenticateRequest
        val saslAuthenticateRequest = new SaslAuthenticateRequest.Builder(new SaslAuthenticateRequestData()
          .setAuthBytes(authBytes)).build()
        val saslAuthenticateHeader = new RequestHeader(ApiKeys.SASL_AUTHENTICATE, saslAuthenticateRequest.version,
          clientId, correlationId)
        sendApiRequest(socket, saslAuthenticateRequest, saslAuthenticateHeader)
      } else {
        // send credentials directly, without a SaslAuthenticateRequest
        sendRequest(socket, authBytes)
      }
      receiveResponse(socket)
      assertEquals(1, overrideServer.testableSelector.channels.size)

      // advance the clock long enough to cause server-side disconnection upon next send...
      time.sleep(reauthMs * 2)
      // ...and now send something to trigger the disconnection
      val ackTimeoutMs = 10000
      val ack = 0: Short
      val emptyRequest = requests.ProduceRequest.forCurrentMagic(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection())
        .setAcks(ack)
        .setTimeoutMs(ackTimeoutMs)
        .setTransactionalId(null))
        .build()
      val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)
      sendApiRequest(socket, emptyRequest, emptyHeader)
      // wait a little bit for the server-side disconnection to occur since it happens asynchronously
      try {
        TestUtils.waitUntilTrue(() => overrideServer.testableSelector.channels.isEmpty,
          "Expired connection was not closed", 1000)
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
  def testClientDisconnectionUpdatesRequestMetrics(): Unit = {
    shutdownServerAndMetrics(server)
    // The way we detect a connection close from the client depends on the response size. If it's small, an
    // IOException ("Connection reset by peer") is thrown when the Selector reads from the socket. If
    // it's large, an IOException ("Broken pipe") is thrown when the Selector writes to the socket. We test
    // both paths to ensure they are handled correctly.
    checkClientDisconnectionUpdatesRequestMetrics(0)
    checkClientDisconnectionUpdatesRequestMetrics(550000)
  }

  private def checkClientDisconnectionUpdatesRequestMetrics(responseBufferSize: Int): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    val overrideServer = new TestableSocketServer(KafkaConfig.fromProps(props))

    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
      val conn: Socket = connect(overrideServer)
      overrideServer.testableProcessor.closeSocketOnSendResponse(conn)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn, serializedBytes)

      val channel = overrideServer.dataPlaneRequestChannel
      val request = receiveRequest(channel)

      val requestMetrics = channel.metrics(request.header.apiKey.name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val expectedTotalTimeCount = totalTimeHistCount() + 1
      val send = new NetworkSend(request.context.connectionId, ByteBufferSend.sizePrefixed(ByteBuffer.allocate(responseBufferSize)))
      val headerLog = new ObjectNode(JsonNodeFactory.instance)
      headerLog.set("response", new TextNode("someResponse"))
      channel.sendResponse(new RequestChannel.SendResponse(request, send, Some(headerLog), None))

      TestUtils.waitUntilTrue(() => totalTimeHistCount() == expectedTotalTimeCount,
        s"request metrics not updated, expected: $expectedTotalTimeCount, actual: ${totalTimeHistCount()}")

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testClientDisconnectionWithOutstandingReceivesProcessedUntilFailedSend(): Unit = {
    shutdownServerAndMetrics(server)
    val serverMetrics = new Metrics
    val overrideServer = new TestableSocketServer(KafkaConfig.fromProps(props))

    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
      val selector = overrideServer.testableSelector

      // Create a channel, send some requests and close socket. Receive one pending request after socket was closed.
      val request = closeSocketWithPendingRequest(overrideServer, () => connect(overrideServer))

      // Complete request with socket exception so that the channel is closed
      processRequest(overrideServer.dataPlaneRequestChannel, request)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request, overrideServer).isEmpty, "Channel not closed after failed send")
      assertTrue(selector.completedSends.isEmpty, "Unexpected completed send")
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
  def testBrokerSendAfterChannelClosedUpdatesRequestMetrics(): Unit = {
    props.setProperty(KafkaConfig.ConnectionsMaxIdleMsProp, "110")
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics,
      Time.SYSTEM, credentialProvider, apiVersionManager)
    try {
      overrideServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
      conn = connect(overrideServer)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn, serializedBytes)
      val channel = overrideServer.dataPlaneRequestChannel
      val request = receiveRequest(channel)

      TestUtils.waitUntilTrue(() => overrideServer.dataPlaneAcceptor(listener).get.processors(request.processor).channel(request.context.connectionId).isEmpty,
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

    def requestMetricMeters = KafkaYammerMetrics
      .defaultRegistry
      .allMetrics.asScala
      .collect { case (k, metric: Meter) if k.getType == "RequestMetrics" => (k.toString, metric.count) }

    assertEquals(nonZeroMeters, requestMetricMeters.filter { case (_, value) => value != 0 })
    server.shutdown()
    assertEquals(Map.empty, requestMetricMeters)
  }

  @Test
  def testMetricCollectionAfterShutdown(): Unit = {
    shutdownServerAndMetrics(server)

    val nonZeroMetricNamesAndValues = KafkaYammerMetrics
      .defaultRegistry
      .allMetrics.asScala
      .filter { case (k, _) => k.getName.endsWith("IdlePercent") || k.getName.endsWith("NetworkProcessorAvgIdlePercent") }
      .collect { case (k, metric: Gauge[_]) => (k, metric.value().asInstanceOf[Double]) }
      .filter { case (_, value) => value != 0.0 && !value.equals(Double.NaN) }

    assertEquals(Map.empty, nonZeroMetricNamesAndValues)
  }

  @Test
  def testProcessorMetricsTags(): Unit = {
    val kafkaMetricNames = metrics.metrics.keySet.asScala.filter(_.tags.asScala.contains("listener"))
    assertFalse(kafkaMetricNames.isEmpty)

    val expectedListeners = Set("PLAINTEXT")
    kafkaMetricNames.foreach { kafkaMetricName =>
      assertTrue(expectedListeners.contains(kafkaMetricName.tags.get("listener")))
    }

    // legacy metrics not tagged
    val yammerMetricsNames = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (k, _) => k.getType.equals("Processor") }
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
    shutdownServerAndMetrics(server)
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
    shutdownServerAndMetrics(server)
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
   * Tests channel send failure handling when send failure is triggered by [[Selector.send]]
   * to a channel whose peer has closed its connection.
   */
  @Test
  def remoteCloseSendFailure(): Unit = {
    verifySendFailureAfterRemoteClose(makeClosing = false)
  }

  /**
   * Tests channel send failure handling when send failure is triggered by [[Selector.send]]
   * to a channel whose peer has closed its connection and the channel is in `closingChannels`.
   */
  @Test
  def closingChannelSendFailure(): Unit = {
    verifySendFailureAfterRemoteClose(makeClosing = true)
  }

  private def verifySendFailureAfterRemoteClose(makeClosing: Boolean): Unit = {
    props ++= sslServerProps
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector

      val serializedBytes = producerRequestBytes()
      val request = makeChannelWithBufferedRequestsAndCloseRemote(testableServer, testableSelector, makeClosing)
      val otherSocket = sslConnect(testableServer)
      sendRequest(otherSocket, serializedBytes)

      processRequest(testableServer.dataPlaneRequestChannel, request)
      processRequest(testableServer.dataPlaneRequestChannel) // Also process request from other socket
      testableSelector.waitForOperations(SelectorOperation.Send, 2)
      testableServer.waitForChannelClose(request.context.connectionId, locallyClosed = false)

      assertProcessorHealthy(testableServer, Seq(otherSocket))
    })
  }

  /**
   * Verifies that all pending buffered receives are processed even if remote connection is closed.
   * The channel must be closed after pending receives are processed.
   */
  @Test
  def remoteCloseWithBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = false)
  }

  /**
   * Verifies that channel is closed when remote client closes its connection if there is no
   * buffered receive.
   */
  @Test
  @Disabled // TODO: re-enabled until KAFKA-13735 is fixed
  def remoteCloseWithoutBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 0, hasIncomplete = false)
  }

  /**
   * Verifies that channel is closed when remote client closes its connection if there is a pending
   * receive that is incomplete.
   */
  @Test
  def remoteCloseWithIncompleteBufferedReceive(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 0, hasIncomplete = true)
  }

  /**
   * Verifies that all pending buffered receives are processed even if remote connection is closed.
   * The channel must be closed after complete receives are processed, even if there is an incomplete
   * receive remaining in the buffers.
   */
  @Test
  def remoteCloseWithCompleteAndIncompleteBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = true)
  }

  /**
   * Verifies that pending buffered receives are processed when remote connection is closed
   * until a response send fails.
   */
  @Test
  def remoteCloseWithBufferedReceivesFailedSend(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = false, responseRequiredIndex = 1)
  }

  /**
   * Verifies that all pending buffered receives are processed for channel in closing state.
   * The channel must be closed after pending receives are processed.
   */
  @Test
  @Disabled // TODO: re-enable after KAFKA-13736 is fixed
  def closingChannelWithBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = false, makeClosing = true)
  }

  /**
   * Verifies that all pending buffered receives are processed for channel in closing state.
   * The channel must be closed after complete receives are processed, even if there is an incomplete
   * receive remaining in the buffers.
   */
  @Test
  def closingChannelWithCompleteAndIncompleteBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = true)
  }

  /**
   * Verifies that pending buffered receives are processed for a channel in closing state
   * until a response send fails.
   */
  @Test
  def closingChannelWithBufferedReceivesFailedSend(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = false, responseRequiredIndex = 1)
  }

  /**
   * Verifies handling of client disconnections when the server-side channel is in the state
   * specified using the parameters.
   *
   * @param numComplete Number of complete buffered requests
   * @param hasIncomplete If true, add an additional partial buffered request
   * @param responseRequiredIndex Index of the buffered request for which a response is sent. Previous requests
   *                              are completed without a response. If set to -1, all `numComplete` requests
   *                              are completed without a response.
   * @param makeClosing If true, put the channel into closing state in the server Selector.
   */
  private def verifyRemoteCloseWithBufferedReceives(numComplete: Int,
                                                    hasIncomplete: Boolean,
                                                    responseRequiredIndex: Int = -1,
                                                    makeClosing: Boolean = false): Unit = {
    shutdownServerAndMetrics(server)
    props ++= sslServerProps

    // Truncates the last request in the SSL buffers by directly updating the buffers to simulate partial buffered request
    def truncateBufferedRequest(channel: KafkaChannel): Unit = {
      val transportLayer: SslTransportLayer = JTestUtils.fieldValue(channel, classOf[KafkaChannel], "transportLayer")
      val netReadBuffer: ByteBuffer = JTestUtils.fieldValue(transportLayer, classOf[SslTransportLayer], "netReadBuffer")
      val appReadBuffer: ByteBuffer = JTestUtils.fieldValue(transportLayer, classOf[SslTransportLayer], "appReadBuffer")
      if (appReadBuffer.position() > 4) {
        appReadBuffer.position(4)
        netReadBuffer.position(0)
      } else {
        netReadBuffer.position(20)
      }
    }
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector

      val proxyServer = new ProxyServer(testableServer)
      try {
        // Step 1: Send client requests.
        //   a) request1 is sent by the client to ProxyServer and this is directly sent to the server. This
        //      ensures that server-side channel is in muted state until this request is processed in Step 3.
        //   b) `numComplete` requests are sent and buffered in the server-side channel's SSL buffers
        //   c) If `hasIncomplete=true`, an extra request is sent and buffered as in b). This will be truncated later
        //      when previous requests have been processed and only one request is remaining in the SSL buffer,
        //      making it easy to truncate.
        val numBufferedRequests = numComplete + (if (hasIncomplete) 1 else 0)
        val (socket, request1) = makeSocketWithBufferedRequests(testableServer, testableSelector, proxyServer, numBufferedRequests)
        val channel = openChannel(request1, testableServer).getOrElse(throw new IllegalStateException("Channel closed too early"))

        // Step 2: Close the client-side socket and the proxy socket to the server, triggering close notification in the
        // server when the client is unmuted in Step 3. Get the channel into its desired closing/buffered state.
        socket.close()
        proxyServer.serverConnSocket.close()
        TestUtils.waitUntilTrue(() => proxyServer.clientConnSocket.isClosed, "Client socket not closed")
        if (makeClosing)
          testableSelector.pendingClosingChannels.add(channel)
        if (numComplete == 0 && hasIncomplete)
          truncateBufferedRequest(channel)

        // Step 3: Process the first request. Verify that the channel is not removed since the channel
        // should be retained to process buffered data.
        processRequestNoOpResponse(testableServer.dataPlaneRequestChannel, request1)
        if (numComplete > 0) {
          assertSame(channel, openOrClosingChannel(request1, testableServer).getOrElse(throw new IllegalStateException("Channel closed too early")))
        }

        // Step 4: Process buffered data. if `responseRequiredIndex>=0`, the channel should be failed and removed when
        // attempting to send response. Otherwise, the channel should be removed when all completed buffers are processed.
        // Channel should be closed and removed even if there is a partial buffered request when `hasIncomplete=true`
        val numRequests = if (responseRequiredIndex >= 0) responseRequiredIndex + 1 else numComplete
        (0 until numRequests).foreach { i =>
          val request = receiveRequest(testableServer.dataPlaneRequestChannel)
          if (i == numComplete - 1 && hasIncomplete)
            truncateBufferedRequest(channel)
          if (responseRequiredIndex == i)
            processRequest(testableServer.dataPlaneRequestChannel, request)
          else
            processRequestNoOpResponse(testableServer.dataPlaneRequestChannel, request)
        }
        testableServer.waitForChannelClose(channel.id, locallyClosed = false)

        // Verify that SocketServer is healthy
        val anotherSocket = sslConnect(testableServer)
        assertProcessorHealthy(testableServer, Seq(anotherSocket))
      } finally {
        proxyServer.close()
      }
    })
  }

  /**
   * Tests idle channel expiry for SSL channels with buffered data. Muted channels are expired
   * immediately even if there is pending data to be processed. This is consistent with PLAINTEXT where
   * we expire muted channels even if there is data available on the socket. This scenario occurs if broker
   * takes longer than idle timeout to process a client request. In this case, typically client would have
   * expired its connection and would potentially reconnect to retry the request, so immediate expiry enables
   * the old connection and its associated resources to be freed sooner.
   */
  @Test
  def idleExpiryWithBufferedReceives(): Unit = {
    shutdownServerAndMetrics(server)
    val idleTimeMs = 60000
    val time = new MockTime()
    props.put(KafkaConfig.ConnectionsMaxIdleMsProp, idleTimeMs.toString)
    props ++= sslServerProps
    val testableServer = new TestableSocketServer(time = time)
    testableServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    assertTrue(testableServer.controlPlaneRequestChannelOpt.isEmpty)

    val proxyServer = new ProxyServer(testableServer)
    try {
      val testableSelector = testableServer.testableSelector
      testableSelector.updateMinWakeup(2)

      val sleepTimeMs = idleTimeMs / 2 + 1
      val (socket, request) = makeSocketWithBufferedRequests(testableServer, testableSelector, proxyServer)
      // advance mock time in increments to verify that muted sockets with buffered data don't have their idle time updated
      // additional calls to poll() should not update the channel last idle time
      for (_ <- 0 to 3) {
        time.sleep(sleepTimeMs)
        testableSelector.operationCounts.clear()
        testableSelector.waitForOperations(SelectorOperation.Poll, 1)
      }
      testableServer.waitForChannelClose(request.context.connectionId, locallyClosed = false)

      val otherSocket = sslConnect(testableServer)
      assertProcessorHealthy(testableServer, Seq(otherSocket))

      socket.close()
    } finally {
      proxyServer.close()
      shutdownServerAndMetrics(testableServer)
    }
  }

  @Test
  def testUnmuteChannelWithBufferedReceives(): Unit = {
    shutdownServerAndMetrics(server)
    val time = new MockTime()
    props ++= sslServerProps
    val testableServer = new TestableSocketServer(time = time)
    testableServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
    val proxyServer = new ProxyServer(testableServer)
    try {
      val testableSelector = testableServer.testableSelector
      val (socket, request) = makeSocketWithBufferedRequests(testableServer, testableSelector, proxyServer)
      testableSelector.operationCounts.clear()
      testableSelector.waitForOperations(SelectorOperation.Poll, 1)
      val keysWithBufferedRead: util.Set[SelectionKey] = JTestUtils.fieldValue(testableSelector, classOf[Selector], "keysWithBufferedRead")
      assertEquals(Set.empty, keysWithBufferedRead.asScala)
      processRequest(testableServer.dataPlaneRequestChannel, request)
      // buffered requests should be processed after channel is unmuted
      receiveRequest(testableServer.dataPlaneRequestChannel)
      socket.close()
    } finally {
      proxyServer.close()
      shutdownServerAndMetrics(testableServer)
    }
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
    shutdownServerAndMetrics(server)
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
    shutdownServerAndMetrics(server)
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
    shutdownServerAndMetrics(server)
    withTestableServer (testWithServer = { testableServer =>
      connectAndProcessRequest(testableServer)
      val testableSelector = testableServer.testableSelector

      testableSelector.operationCounts.clear()
      testableSelector.addFailure(SelectorOperation.Poll,
        Some(new ControlThrowable() {}))
      testableSelector.waitForOperations(SelectorOperation.Poll, 1)

      testableSelector.waitForOperations(SelectorOperation.CloseSelector, 1)
      assertEquals(1, uncaughtExceptions.get)
      uncaughtExceptions.set(0)
    })
  }

  @Test
  def testConnectionRateLimit(): Unit = {
    shutdownServerAndMetrics(server)
    val numConnections = 5
    props.put("max.connections.per.ip", numConnections.toString)
    val testableServer = new TestableSocketServer(KafkaConfig.fromProps(props), connectionQueueSize = 1)
    testableServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
    val testableSelector = testableServer.testableSelector
    val errors = new mutable.HashSet[String]

    def acceptorStackTraces: scala.collection.Map[Thread, String] = {
      Thread.getAllStackTraces.asScala.collect {
        case (thread, stacktraceElement) if thread.getName.contains("kafka-socket-acceptor") =>
          thread -> stacktraceElement.mkString("\n")
      }
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
      // we can safely check that there were at least 4 polls prior to the 5th connection.
      val pollCount = testableSelector.operationCounts(SelectorOperation.Poll)
      assertTrue(pollCount >= numConnections - 1, s"Connections created too quickly: $pollCount")
      verifyAcceptorBlockedPercent("PLAINTEXT", expectBlocked = true)

      assertProcessorHealthy(testableServer, sockets)
    } finally {
      shutdownServerAndMetrics(testableServer)
    }
  }


  @Test
  def testControlPlaneAsPrivilegedListener(): Unit = {
    val testProps = new Properties
    testProps ++= props
    testProps.put("listeners", "PLAINTEXT://localhost:0,CONTROLLER://localhost:0")
    testProps.put("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")
    testProps.put("control.plane.listener.name", "CONTROLLER")
    val config = KafkaConfig.fromProps(testProps)
    withTestableServer(config, { testableServer =>
      val controlPlaneSocket = connect(testableServer, config.controlPlaneListenerName.get,
        localAddr = InetAddress.getLocalHost)
      val sentRequest = sendAndReceiveControllerRequest(controlPlaneSocket, testableServer)
      assertTrue(sentRequest.context.fromPrivilegedListener)

      val plainSocket = connect(testableServer, localAddr = InetAddress.getLocalHost)
      val plainRequest = sendAndReceiveRequest(plainSocket, testableServer)
      assertFalse(plainRequest.context.fromPrivilegedListener)
    })
  }

  @Test
  def testInterBrokerListenerAsPrivilegedListener(): Unit = {
    val testProps = new Properties
    testProps ++= props
    testProps.put("listeners", "EXTERNAL://localhost:0,INTERNAL://localhost:0")
    testProps.put("listener.security.protocol.map", "EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT")
    testProps.put("inter.broker.listener.name", "INTERNAL")
    val config = KafkaConfig.fromProps(testProps)
    withTestableServer(config, { testableServer =>
      val interBrokerSocket = connect(testableServer, config.interBrokerListenerName,
        localAddr = InetAddress.getLocalHost)
      val sentRequest = sendAndReceiveRequest(interBrokerSocket, testableServer)
      assertTrue(sentRequest.context.fromPrivilegedListener)

      val externalSocket = connect(testableServer, new ListenerName("EXTERNAL"),
        localAddr = InetAddress.getLocalHost)
      val externalRequest = sendAndReceiveRequest(externalSocket, testableServer)
      assertFalse(externalRequest.context.fromPrivilegedListener)
    })
  }

  @Test
  def testControlPlaneTakePrecedenceOverInterBrokerListenerAsPrivilegedListener(): Unit = {
    val testProps = new Properties
    testProps ++= props
    testProps.put("listeners", "EXTERNAL://localhost:0,INTERNAL://localhost:0,CONTROLLER://localhost:0")
    testProps.put("listener.security.protocol.map", "EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
    testProps.put("control.plane.listener.name", "CONTROLLER")
    testProps.put("inter.broker.listener.name", "INTERNAL")
    val config = KafkaConfig.fromProps(testProps)
    withTestableServer(config, { testableServer =>
      val controlPlaneSocket = connect(testableServer, config.controlPlaneListenerName.get,
        localAddr = InetAddress.getLocalHost)
      val controlPlaneRequest = sendAndReceiveControllerRequest(controlPlaneSocket, testableServer)
      assertTrue(controlPlaneRequest.context.fromPrivilegedListener)

      val interBrokerSocket = connect(testableServer, config.interBrokerListenerName,
        localAddr = InetAddress.getLocalHost)
      val interBrokerRequest = sendAndReceiveRequest(interBrokerSocket, testableServer)
      assertFalse(interBrokerRequest.context.fromPrivilegedListener)

      val externalSocket = connect(testableServer, new ListenerName("EXTERNAL"),
        localAddr = InetAddress.getLocalHost)
      val externalRequest = sendAndReceiveRequest(externalSocket, testableServer)
      assertFalse(externalRequest.context.fromPrivilegedListener)
    })
  }

  @Test
  def testListenBacklogSize(): Unit = {
    val backlogSize = 128
    props.put("socket.listen.backlog.size", backlogSize.toString)

    // TCP listen backlog size is the max count of pending connections (i.e. connections such that
    // 3-way handshake is done at kernel level and waiting to be accepted by the server application.
    // From client perspective, such connections should be visible as already "connected")
    // Hence, we can check if listen backlog size is properly configured by trying to connect the server
    // without starting acceptor thread.
    withTestableServer(KafkaConfig.fromProps(props), { testableServer =>
      1 to backlogSize foreach { _ =>
        assertTrue(connect(testableServer).isConnected)
      }
    }, false)
  }

  /**
   * Test to ensure "Selector.poll()" does not block at "select(timeout)" when there is no data in the socket but there
   * is data in the buffer. This only happens when SSL protocol is used.
   */
  @Test
  def testLatencyWithBufferedDataAndNoSocketData(): Unit = {
    shutdownServerAndMetrics(server)

    props ++= sslServerProps
    val testableServer = new TestableSocketServer(KafkaConfig.fromProps(props))
    testableServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
    val testableSelector = testableServer.testableSelector
    val proxyServer = new ProxyServer(testableServer)
    val selectTimeoutMs = 5000
    // set pollTimeoutOverride to "selectTimeoutMs" to ensure poll() timeout is distinct and can be identified
    testableSelector.pollTimeoutOverride = Some(selectTimeoutMs)

    try {
      // initiate SSL connection by sending 1 request via socket, then send 2 requests directly into the netReadBuffer
      val (sslSocket, req1) = makeSocketWithBufferedRequests(testableServer, testableSelector, proxyServer)

      // force all data to be transferred to the kafka broker by closing the client connection to proxy server
      sslSocket.close()
      TestUtils.waitUntilTrue(() => proxyServer.clientConnSocket.isClosed, "proxyServer.clientConnSocket is still not closed after 60000 ms", 60000)

      // process the request and send the response
      processRequest(testableServer.dataPlaneRequestChannel, req1)

      // process the requests in the netReadBuffer, this should not block
      val req2 = receiveRequest(testableServer.dataPlaneRequestChannel)
      processRequest(testableServer.dataPlaneRequestChannel, req2)

    } finally {
      proxyServer.close()
      shutdownServerAndMetrics(testableServer)
    }
  }

  @Test
  def testAuthorizerFailureCausesEnableRequestProcessingFailure(): Unit = {
    shutdownServerAndMetrics(server)
    val newServer = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager)
    try {
      val failedFuture = new CompletableFuture[Void]()
      failedFuture.completeExceptionally(new RuntimeException("authorizer startup failed"))
      assertThrows(classOf[ExecutionException], () => {
        newServer.enableRequestProcessing(Map(endpoint.toJava -> failedFuture)).get()
      })
    } finally {
      shutdownServerAndMetrics(newServer)
    }
  }

  @Test
  def testFailedAcceptorStartupCausesEnableRequestProcessingFailure(): Unit = {
    shutdownServerAndMetrics(server)
    val newServer = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager)
    try {
      newServer.dataPlaneAcceptors.values().forEach(a => a.shouldRun.set(false))
      assertThrows(classOf[ExecutionException], () => {
        newServer.enableRequestProcessing(Map()).get()
      })
    } finally {
      shutdownServerAndMetrics(newServer)
    }
  }

  @Test
  def testAcceptorStartOpensPortIfNeeded(): Unit = {
    shutdownServerAndMetrics(server)
    val newServer = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager)
    try {
      newServer.dataPlaneAcceptors.values().forEach(a => {
        a.serverChannel.close()
        a.serverChannel = null
      })
      val authorizerFuture = new CompletableFuture[Void]()
      val enableFuture = newServer.enableRequestProcessing(
        newServer.dataPlaneAcceptors.keys().asScala.
          map(_.toJava).map(k => k -> authorizerFuture).toMap)
      assertFalse(authorizerFuture.isDone())
      assertFalse(enableFuture.isDone())
      newServer.dataPlaneAcceptors.values().forEach(a => assertNull(a.serverChannel))
      authorizerFuture.complete(null)
      enableFuture.get(1, TimeUnit.MINUTES)
      newServer.dataPlaneAcceptors.values().forEach(a => assertNotNull(a.serverChannel))
    } finally {
      shutdownServerAndMetrics(newServer)
    }
  }

  private def sslServerProps: Properties = {
    val trustStoreFile = TestUtils.tempFile("truststore", ".jks")
    val sslProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, interBrokerSecurityProtocol = Some(SecurityProtocol.SSL),
      trustStoreFile = Some(trustStoreFile))
    sslProps.put(KafkaConfig.ListenersProp, "SSL://localhost:0")
    sslProps.put(KafkaConfig.NumNetworkThreadsProp, "1")
    sslProps
  }

  private def withTestableServer(config : KafkaConfig = KafkaConfig.fromProps(props),
                                 testWithServer: TestableSocketServer => Unit,
                                 startProcessingRequests: Boolean = true): Unit = {
    shutdownServerAndMetrics(server)
    val testableServer = new TestableSocketServer(config)
    if (startProcessingRequests) {
      testableServer.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
    }
    try {
      testWithServer(testableServer)
    } finally {
      shutdownServerAndMetrics(testableServer)
      assertEquals(0, uncaughtExceptions.get)
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
    assertNotNull(selector.channel(connectionId), "Channel should not have been closed")
    assertNull(selector.closingChannel(connectionId), "Channel should not be closing")
    socket.close()
    TestUtils.waitUntilTrue(() => testableServer.connectionCount(localAddress) == 0, "Channels not removed")
  }

  // Since all sockets use the same local host, it is sufficient to check the local port
  def isSocketConnectionId(connectionId: String, socket: Socket): Boolean =
    connectionId.contains(s":${socket.getLocalPort}-")

  private def verifyAcceptorBlockedPercent(listenerName: String, expectBlocked: Boolean): Unit = {
    val blockedPercentMetricMBeanName = s"kafka.network:type=Acceptor,name=AcceptorBlockedPercent,listener=$listenerName"
    val blockedPercentMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == blockedPercentMetricMBeanName
    }.values
    assertEquals(1, blockedPercentMetrics.size)
    val blockedPercentMetric = blockedPercentMetrics.head.asInstanceOf[Meter]
    val blockedPercent = blockedPercentMetric.meanRate
    if (expectBlocked) {
      assertTrue(blockedPercent > 0.0, s"Acceptor blocked percent not recorded: $blockedPercent")
      assertTrue(blockedPercent <= 1.0, s"Unexpected blocked percent in acceptor: $blockedPercent")
    } else {
      assertEquals(0.0, blockedPercent, 0.001)
    }
  }

  class TestableAcceptor(socketServer: SocketServer,
                         endPoint: EndPoint,
                         cfg: KafkaConfig,
                         nodeId: Int,
                         connectionQuotas: ConnectionQuotas,
                         time: Time,
                         isPrivilegedListener: Boolean,
                         requestChannel: RequestChannel,
                         metrics: Metrics,
                         credentialProvider: CredentialProvider,
                         logContext: LogContext,
                         memoryPool: MemoryPool,
                         apiVersionManager: ApiVersionManager,
                         connectionQueueSize: Int) extends DataPlaneAcceptor(socketServer,
                                                                             endPoint,
                                                                             cfg,
                                                                             nodeId,
                                                                             connectionQuotas,
                                                                             time,
                                                                             isPrivilegedListener,
                                                                             requestChannel,
                                                                             metrics,
                                                                             credentialProvider,
                                                                             logContext,
                                                                             memoryPool,
                                                                             apiVersionManager) {

    override def newProcessor(id: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol): Processor = {
      new TestableProcessor(id, time, requestChannel, listenerName, securityProtocol, cfg, connectionQuotas, connectionQueueSize, isPrivilegedListener)
    }
  }

  class TestableProcessor(id: Int, time: Time, requestChannel: RequestChannel, listenerName: ListenerName, securityProtocol: SecurityProtocol, config: KafkaConfig, connectionQuotas: ConnectionQuotas, connectionQueueSize: Int, isPrivilegedListener: Boolean)
  extends Processor(id,
                    time,
                    10000,
                    requestChannel,
                    connectionQuotas,
                    300000L,
                    0,
                    listenerName,
                    securityProtocol,
                    config,
                    new Metrics(),
                    credentialProvider,
                    MemoryPool.NONE,
                    new LogContext(),
                    connectionQueueSize,
                    isPrivilegedListener,
                    apiVersionManager,
                    s"TestableProcessor${id}") {
    private var connectionId: Option[String] = None
    private var conn: Option[Socket] = None

    override protected[network] def createSelector(channelBuilder: ChannelBuilder): Selector = {
      new TestableSelector(config, channelBuilder, time, metrics, metricTags.asScala)
    }

    override private[network] def processException(errorMessage: String, throwable: Throwable): Unit = {
      if (errorMessage.contains("uncaught exception"))
        uncaughtExceptions.incrementAndGet()
      super.processException(errorMessage, throwable)
    }

    def setConnectionId(connectionId: String): Unit = {
      this.connectionId = Some(connectionId)
    }

    override protected[network] def connectionId(socket: Socket): String = {
      this.connectionId.getOrElse(super.connectionId(socket))
    }

    def closeSocketOnSendResponse(conn: Socket): Unit = {
      this.conn = Some(conn)
    }

    override protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send): Unit = {
      this.conn.foreach(_.close())
      super.sendResponse(response, responseSend)
    }
  }

  class TestableSocketServer(
    config : KafkaConfig = KafkaConfig.fromProps(props),
    connectionQueueSize: Int = 20,
    time: Time = Time.SYSTEM
  ) extends SocketServer(
    config, new Metrics, time, credentialProvider, apiVersionManager,
  ) {

    override def createDataPlaneAcceptor(endPoint: EndPoint, isPrivilegedListener: Boolean, requestChannel: RequestChannel) : DataPlaneAcceptor = {
      new TestableAcceptor(this, endPoint, this.config, 0, connectionQuotas, time, isPrivilegedListener, requestChannel, this.metrics, this.credentialProvider, new LogContext, MemoryPool.NONE, this.apiVersionManager, connectionQueueSize)
    }

    def testableSelector: TestableSelector =
      testableProcessor.selector.asInstanceOf[TestableSelector]

    def testableProcessor: TestableProcessor = {
      val endpoint = this.config.dataPlaneListeners.head
      dataPlaneAcceptors.get(endpoint).processors(0).asInstanceOf[TestableProcessor]
    }

    def waitForChannelClose(connectionId: String, locallyClosed: Boolean): Unit = {
      val selector = testableSelector
      if (locallyClosed) {
        TestUtils.waitUntilTrue(() => selector.allLocallyClosedChannels.contains(connectionId),
          s"Channel not closed: $connectionId")
        assertTrue(testableSelector.allDisconnectedChannels.isEmpty, "Unexpected disconnect notification")
      } else {
        TestUtils.waitUntilTrue(() => selector.allDisconnectedChannels.contains(connectionId),
          s"Disconnect notification not received: $connectionId")
        assertTrue(testableSelector.allLocallyClosedChannels.isEmpty, "Channel closed locally")
      }
      val openCount = selector.allChannels.size - 1 // minus one for the channel just closed above
      TestUtils.waitUntilTrue(() => connectionCount(localAddress) == openCount, "Connection count not decremented")
      TestUtils.waitUntilTrue(() =>
        dataPlaneAcceptor(listener).get.processors(0).inflightResponseCount == 0, "Inflight responses not cleared")
      assertNull(selector.channel(connectionId), "Channel not removed")
      assertNull(selector.closingChannel(connectionId), "Closing channel not removed")
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

    val failures = mutable.Map[SelectorOperation, Throwable]()
    val operationCounts = mutable.Map[SelectorOperation, Int]().withDefaultValue(0)
    val allChannels = mutable.Set[String]()
    val allLocallyClosedChannels = mutable.Set[String]()
    val allDisconnectedChannels = mutable.Set[String]()
    val allFailedChannels = mutable.Set[String]()

    // Enable data from `Selector.poll()` to be deferred to a subsequent poll() until
    // the number of elements of that type reaches `minPerPoll`. This enables tests to verify
    // that failed processing doesn't impact subsequent processing within the same iteration.
    abstract class PollData[T] {
      var minPerPoll = 1
      val deferredValues = mutable.Buffer[T]()

      /**
       * Process new results and return the results for the current poll if at least
       * `minPerPoll` results are available including any deferred results. Otherwise
       * add the provided values to the deferred set and return an empty buffer. This allows
       * tests to process `minPerPoll` elements as the results of a single poll iteration.
       */
      protected def update(newValues: mutable.Buffer[T]): mutable.Buffer[T] = {
        val currentPollValues = mutable.Buffer[T]()
        if (deferredValues.size + newValues.size >= minPerPoll) {
          if (deferredValues.nonEmpty) {
            currentPollValues ++= deferredValues
            deferredValues.clear()
          }
          currentPollValues ++= newValues
        } else
          deferredValues ++= newValues

        currentPollValues
      }

      /**
       * Process results from the appropriate buffer in Selector and update the buffer to either
       * defer and return nothing or return all results including previously deferred values.
       */
      def updateResults(): Unit
    }

    class CompletedReceivesPollData(selector: TestableSelector) extends PollData[NetworkReceive] {
      val completedReceivesMap: util.Map[String, NetworkReceive] = JTestUtils.fieldValue(selector, classOf[Selector], "completedReceives")

      override def updateResults(): Unit = {
        val currentReceives = update(selector.completedReceives.asScala.toBuffer)
        completedReceivesMap.clear()
        currentReceives.foreach { receive =>
          val channelOpt = Option(selector.channel(receive.source)).orElse(Option(selector.closingChannel(receive.source)))
          channelOpt.foreach { channel => completedReceivesMap.put(channel.id, receive) }
        }
      }
    }

    class CompletedSendsPollData(selector: TestableSelector) extends PollData[NetworkSend] {
      override def updateResults(): Unit = {
        val currentSends = update(selector.completedSends.asScala)
        selector.completedSends.clear()
        currentSends.foreach { selector.completedSends.add }
      }
    }

    class DisconnectedPollData(selector: TestableSelector) extends PollData[(String, ChannelState)] {
      override def updateResults(): Unit = {
        val currentDisconnected = update(selector.disconnected.asScala.toBuffer)
        selector.disconnected.clear()
        currentDisconnected.foreach { case (channelId, state) => selector.disconnected.put(channelId, state) }
      }
    }

    val cachedCompletedReceives = new CompletedReceivesPollData(this)
    val cachedCompletedSends = new CompletedSendsPollData(this)
    val cachedDisconnected = new DisconnectedPollData(this)
    val allCachedPollData = Seq(cachedCompletedReceives, cachedCompletedSends, cachedDisconnected)
    val pendingClosingChannels = new ConcurrentLinkedQueue[KafkaChannel]()
    @volatile var minWakeupCount = 0
    @volatile var pollTimeoutOverride: Option[Long] = None
    @volatile var pollCallback: () => Unit = () => {}

    def addFailure(operation: SelectorOperation, exception: Option[Throwable] = None): Unit = {
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

    override def send(s: NetworkSend): Unit = {
      runOp(SelectorOperation.Send, Some(s.destinationId)) {
        super.send(s)
      }
    }

    override def poll(timeout: Long): Unit = {
      try {
        assertEquals(0, super.completedReceives().size)
        assertEquals(0, super.completedSends().size)

        pollCallback.apply()
        while (!pendingClosingChannels.isEmpty) {
          makeClosing(pendingClosingChannels.poll())
        }
        runOp(SelectorOperation.Poll, None) {
          super.poll(pollTimeoutOverride.getOrElse(timeout))
        }
      } finally {
        super.channels.forEach(allChannels += _.id)
        allDisconnectedChannels ++= super.disconnected.asScala.keys

        cachedCompletedReceives.updateResults()
        cachedCompletedSends.updateResults()
        cachedDisconnected.updateResults()
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

    private def makeClosing(channel: KafkaChannel): Unit = {
      val channels: util.Map[String, KafkaChannel] = JTestUtils.fieldValue(this, classOf[Selector], "channels")
      val closingChannels: util.Map[String, KafkaChannel] = JTestUtils.fieldValue(this, classOf[Selector], "closingChannels")
      closingChannels.put(channel.id, channel)
      channels.remove(channel.id)
    }
  }

  /**
   * Proxy server used to intercept connections to SocketServer. This is used for testing SSL channels
   * with buffered data. A single SSL client is expected to be created by the test using this ProxyServer.
   * By default, data between the client and the server is simply transferred across to the destination by ProxyServer.
   * Tests can enable buffering in ProxyServer to directly copy incoming data from the client to the server-side
   * channel's `netReadBuffer` to simulate scenarios with SSL buffered data.
   */
  private class ProxyServer(socketServer: SocketServer) {
    val serverSocket = new ServerSocket(0)
    val localPort = serverSocket.getLocalPort
    val serverConnSocket = new Socket("localhost", socketServer.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SSL)))
    val executor = Executors.newFixedThreadPool(2)
    @volatile var clientConnSocket: Socket = _
    @volatile var buffer: Option[ByteBuffer] = None

    executor.submit((() => {
      try {
        clientConnSocket = serverSocket.accept()
        val serverOut = serverConnSocket.getOutputStream
        val clientIn = clientConnSocket.getInputStream
        var b: Int = -1
        while ({b = clientIn.read(); b != -1}) {
          buffer match {
            case Some(buf) =>
              buf.put(b.asInstanceOf[Byte])
            case None =>
              serverOut.write(b)
              serverOut.flush()
          }
        }
      } finally {
        clientConnSocket.close()
      }
    }): Runnable)

    executor.submit((() => {
      var b: Int = -1
      val serverIn = serverConnSocket.getInputStream
      while ({b = serverIn.read(); b != -1}) {
        clientConnSocket.getOutputStream.write(b)
      }
    }): Runnable)

    def enableBuffering(buffer: ByteBuffer): Unit = this.buffer = Some(buffer)

    def close(): Unit = {
      serverSocket.close()
      serverConnSocket.close()
      clientConnSocket.close()
      executor.shutdownNow()
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS))
    }

  }
}
