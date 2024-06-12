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

import java.util.{Collections, OptionalLong, Properties}
import kafka.utils.TestUtils
import org.apache.kafka.common.Node
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.{BrokerHeartbeatResponseData, BrokerRegistrationResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, BrokerHeartbeatRequest, BrokerHeartbeatResponse, BrokerRegistrationRequest, BrokerRegistrationResponse}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.config.{KRaftConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test, Timeout}

import java.util.concurrent.{CompletableFuture, Future}
import scala.jdk.CollectionConverters._

@Timeout(value = 12)
class BrokerLifecycleManagerTest {
  private var manager: BrokerLifecycleManager = null

  @AfterEach
  def tearDown(): Unit = {
    if (manager != null) {
      manager.close()
    }
  }

  def configProperties = {
    val properties = new Properties()
    properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/foo")
    properties.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    properties.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    properties.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, s"2@localhost:9093")
    properties.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    properties.setProperty(KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG, "300000")
    properties.setProperty(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, "100")
    properties
  }

  @Test
  def testCreateAndClose(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    manager = new BrokerLifecycleManager(context.config, context.time, "create-and-close-", isZkBroker = false, Set(Uuid.fromString("oFoTeS9QT0aAyCyH41v45A")))
    manager.close()
  }

  @Test
  def testCreateStartAndClose(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    manager = new BrokerLifecycleManager(context.config, context.time, "create-start-and-close-", isZkBroker = false, Set(Uuid.fromString("uiUADXZWTPixVvp6UWFWnw")))
    assertEquals(BrokerState.NOT_RUNNING, manager.state)
    manager.start(() => context.highestMetadataOffset.get(),
      context.mockChannelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap(), OptionalLong.empty())
    TestUtils.retry(60000) {
      assertEquals(BrokerState.STARTING, manager.state)
    }
    manager.close()
    assertEquals(BrokerState.SHUTTING_DOWN, manager.state)
  }

  @Test
  def testSuccessfulRegistration(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    manager = new BrokerLifecycleManager(context.config, context.time, "successful-registration-", isZkBroker = false, Set(Uuid.fromString("gCpDJgRlS2CBCpxoP2VMsQ")))
    val controllerNode = new Node(3000, "localhost", 8021)
    context.controllerNodeProvider.node.set(controllerNode)
    manager.start(() => context.highestMetadataOffset.get(),
      context.mockChannelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap(), OptionalLong.of(10L))
    TestUtils.retry(60000) {
      assertEquals(1, context.mockChannelManager.unsentQueue.size)
      assertEquals(10L, context.mockChannelManager.unsentQueue.getFirst.request.build().asInstanceOf[BrokerRegistrationRequest].data().previousBrokerEpoch())
    }
    context.mockClient.prepareResponseFrom(new BrokerRegistrationResponse(
      new BrokerRegistrationResponseData().setBrokerEpoch(1000)), controllerNode)
    TestUtils.retry(10000) {
      context.poll()
      assertEquals(1000L, manager.brokerEpoch)
    }
  }

  @Test
  def testRegistrationTimeout(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val controllerNode = new Node(3000, "localhost", 8021)
    manager = new BrokerLifecycleManager(context.config, context.time, "registration-timeout-", isZkBroker = false, Set(Uuid.fromString("9XBOAtr4T0Wbx2sbiWh6xg")))
    context.controllerNodeProvider.node.set(controllerNode)
    def newDuplicateRegistrationResponse(): Unit = {
      context.mockClient.prepareResponseFrom(new BrokerRegistrationResponse(
        new BrokerRegistrationResponseData().
          setErrorCode(Errors.DUPLICATE_BROKER_REGISTRATION.code())), controllerNode)
      context.mockChannelManager.poll()
    }
    newDuplicateRegistrationResponse()
    assertEquals(1, context.mockClient.futureResponses().size)
    manager.start(() => context.highestMetadataOffset.get(),
      context.mockChannelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap(), OptionalLong.empty())
    // We should send the first registration request and get a failure immediately
    TestUtils.retry(60000) {
      context.poll()
      assertEquals(0, context.mockClient.futureResponses().size)
    }
    // Verify that we resend the registration request.
    newDuplicateRegistrationResponse()
    TestUtils.retry(60000) {
      context.time.sleep(100)
      context.poll()
      manager.eventQueue.wakeup()
      assertEquals(0, context.mockClient.futureResponses().size)
    }
    // Verify that we time out eventually.
    context.time.sleep(300000)
    TestUtils.retry(60000) {
      context.poll()
      manager.eventQueue.wakeup()
      assertEquals(BrokerState.SHUTTING_DOWN, manager.state)
      assertTrue(manager.initialCatchUpFuture.isCompletedExceptionally)
      assertEquals(-1L, manager.brokerEpoch)
    }
  }

  @Test
  def testControlledShutdown(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    manager = new BrokerLifecycleManager(context.config, context.time, "controlled-shutdown-", isZkBroker = false, Set(Uuid.fromString("B4RtUz1ySGip3A7ZFYB2dg")))
    val controllerNode = new Node(3000, "localhost", 8021)
    context.controllerNodeProvider.node.set(controllerNode)
    context.mockClient.prepareResponseFrom(new BrokerRegistrationResponse(
      new BrokerRegistrationResponseData().setBrokerEpoch(1000)), controllerNode)
    context.mockClient.prepareResponseFrom(new BrokerHeartbeatResponse(
      new BrokerHeartbeatResponseData().setIsCaughtUp(true)), controllerNode)
    manager.start(() => context.highestMetadataOffset.get(),
      context.mockChannelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap(), OptionalLong.empty())
    TestUtils.retry(10000) {
      context.poll()
      manager.eventQueue.wakeup()
      assertEquals(BrokerState.RECOVERY, manager.state)
    }
    context.mockClient.prepareResponseFrom(new BrokerHeartbeatResponse(
      new BrokerHeartbeatResponseData().setIsFenced(false)), controllerNode)
    context.time.sleep(20)
    TestUtils.retry(10000) {
      context.poll()
      manager.eventQueue.wakeup()
      assertEquals(BrokerState.RUNNING, manager.state)
    }
    manager.beginControlledShutdown()
    TestUtils.retry(10000) {
      context.poll()
      manager.eventQueue.wakeup()
      assertEquals(BrokerState.PENDING_CONTROLLED_SHUTDOWN, manager.state)
      assertTrue(context.mockClient.hasInFlightRequests)
    }

    context.mockClient.respond(
      (body: AbstractRequest) => {
        body match {
          case heartbeatRequest: BrokerHeartbeatRequest =>
            assertTrue(heartbeatRequest.data.wantShutDown)
            true
          case _ =>
            false
        }
      },
      new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData().setShouldShutDown(true))
    )

    TestUtils.retry(10000) {
      context.poll()
      manager.eventQueue.wakeup()
      assertEquals(BrokerState.SHUTTING_DOWN, manager.state)
    }
    manager.controlledShutdownFuture.get()
  }

  def prepareResponse[T<:AbstractRequest](ctx: RegistrationTestContext, response: AbstractResponse): Future[T] = {
    val result = new CompletableFuture[T]()
    ctx.mockClient.prepareResponseFrom(
      (body: AbstractRequest) => result.complete(body.asInstanceOf[T]),
      response,
      ctx.controllerNodeProvider.node.get
    )
    result
  }

  def poll[T](ctx: RegistrationTestContext, manager: BrokerLifecycleManager, future: Future[T]): T = {
    while (ctx.mockChannelManager.unsentQueue.isEmpty) {
      // Cancel a potential timeout event, so it doesn't get activated if we advance the clock too much
      manager.eventQueue.cancelDeferred("initialRegistrationTimeout")

      // If the manager is idling until scheduled events we need to advance the clock
      if (manager.eventQueue.firstDeferredIfIdling().isPresent)
        ctx.time.sleep(5)
      manager.eventQueue.wakeup()
    }
    while (!future.isDone) {
      ctx.poll()
    }
    future.get
  }

  @Test
  def testAlwaysSendsAccumulatedOfflineDirs(): Unit = {
    val ctx = new RegistrationTestContext(configProperties)
    manager = new BrokerLifecycleManager(ctx.config, ctx.time, "offline-dirs-sent-in-heartbeat-", isZkBroker = false, Set(Uuid.fromString("0IbF1sjhSGG6FNvnrPbqQg")))
    val controllerNode = new Node(3000, "localhost", 8021)
    ctx.controllerNodeProvider.node.set(controllerNode)

    val registration = prepareResponse(ctx, new BrokerRegistrationResponse(new BrokerRegistrationResponseData().setBrokerEpoch(1000)))
    manager.start(() => ctx.highestMetadataOffset.get(),
      ctx.mockChannelManager, ctx.clusterId, ctx.advertisedListeners,
      Collections.emptyMap(), OptionalLong.empty())
    poll(ctx, manager, registration)

    def nextHeartbeatDirs(): Set[String] =
      poll(ctx, manager, prepareResponse[BrokerHeartbeatRequest](ctx, new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData())))
        .data().offlineLogDirs().asScala.map(_.toString).toSet
    assertEquals(Set.empty, nextHeartbeatDirs())
    manager.propagateDirectoryFailure(Uuid.fromString("h3sC4Yk-Q9-fd0ntJTocCA"), Integer.MAX_VALUE)
    assertEquals(Set("h3sC4Yk-Q9-fd0ntJTocCA"), nextHeartbeatDirs())
    manager.propagateDirectoryFailure(Uuid.fromString("ej8Q9_d2Ri6FXNiTxKFiow"), Integer.MAX_VALUE)
    assertEquals(Set("h3sC4Yk-Q9-fd0ntJTocCA", "ej8Q9_d2Ri6FXNiTxKFiow"), nextHeartbeatDirs())
    manager.propagateDirectoryFailure(Uuid.fromString("1iF76HVNRPqC7Y4r6647eg"), Integer.MAX_VALUE)
    assertEquals(Set("h3sC4Yk-Q9-fd0ntJTocCA", "ej8Q9_d2Ri6FXNiTxKFiow", "1iF76HVNRPqC7Y4r6647eg"), nextHeartbeatDirs())
  }

  @Test
  def testRegistrationIncludesDirs(): Unit = {
    val logDirs = Set("ad5FLIeCTnaQdai5vOjeng", "ybdzUKmYSLK6oiIpI6CPlw").map(Uuid.fromString)
    val ctx = new RegistrationTestContext(configProperties)
    manager = new BrokerLifecycleManager(ctx.config, ctx.time, "registration-includes-dirs-",
      isZkBroker = false, logDirs)
    val controllerNode = new Node(3000, "localhost", 8021)
    ctx.controllerNodeProvider.node.set(controllerNode)

    val registration = prepareResponse(ctx, new BrokerRegistrationResponse(new BrokerRegistrationResponseData().setBrokerEpoch(1000)))

    manager.start(() => ctx.highestMetadataOffset.get(),
      ctx.mockChannelManager, ctx.clusterId, ctx.advertisedListeners,
      Collections.emptyMap(), OptionalLong.empty())
    val request = poll(ctx, manager, registration).asInstanceOf[BrokerRegistrationRequest]

    assertEquals(logDirs, request.data.logDirs().asScala.toSet)
  }

  @Test
  def testKraftJBODMetadataVersionUpdateEvent(): Unit = {
    val ctx = new RegistrationTestContext(configProperties)
    manager = new BrokerLifecycleManager(ctx.config, ctx.time, "jbod-metadata-version-update", isZkBroker = false, Set(Uuid.fromString("gCpDJgRlS2CBCpxoP2VMsQ")))

    val controllerNode = new Node(3000, "localhost", 8021)
    ctx.controllerNodeProvider.node.set(controllerNode)

    manager.start(() => ctx.highestMetadataOffset.get(),
      ctx.mockChannelManager, ctx.clusterId, ctx.advertisedListeners,
      Collections.emptyMap(), OptionalLong.of(10L))

    def doPoll[T<:AbstractRequest](response: AbstractResponse) = poll(ctx, manager, prepareResponse[T](ctx, response))
    def nextHeartbeatRequest() = doPoll[AbstractRequest](new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData()))
    def nextRegistrationRequest(epoch: Long) =
      doPoll[BrokerRegistrationRequest](new BrokerRegistrationResponse(new BrokerRegistrationResponseData().setBrokerEpoch(epoch)))

    // Broker registers and response sets epoch to 1000L
    assertEquals(10L, nextRegistrationRequest(1000L).data().previousBrokerEpoch())

    nextHeartbeatRequest() // poll for next request as way to synchronize with the new value into brokerEpoch
    assertEquals(1000L, manager.brokerEpoch)

    // Trigger JBOD MV update
    manager.resendBrokerRegistrationUnlessZkMode()

    // Accept new registration, response sets epoch to 1200
    nextRegistrationRequest(1200L)

    nextHeartbeatRequest()
    assertEquals(1200L, manager.brokerEpoch)
  }
}
