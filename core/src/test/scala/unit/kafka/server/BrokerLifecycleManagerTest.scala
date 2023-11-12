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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import java.util.concurrent.{CompletableFuture, Future}
import scala.jdk.CollectionConverters._

@Timeout(value = 12)
class BrokerLifecycleManagerTest {
  def configProperties = {
    val properties = new Properties()
    properties.setProperty(KafkaConfig.LogDirsProp, "/tmp/foo")
    properties.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    properties.setProperty(KafkaConfig.NodeIdProp, "1")
    properties.setProperty(KafkaConfig.QuorumVotersProp, s"2@localhost:9093")
    properties.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    properties.setProperty(KafkaConfig.InitialBrokerRegistrationTimeoutMsProp, "300000")
    properties.setProperty(KafkaConfig.BrokerHeartbeatIntervalMsProp, "100")
    properties
  }

  @Test
  def testCreateAndClose(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, "create-and-close-", isZkBroker = false)
    manager.close()
  }

  @Test
  def testCreateStartAndClose(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, "create-start-and-close-", isZkBroker = false)
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
    val manager = new BrokerLifecycleManager(context.config, context.time, "successful-registration-", isZkBroker = false)
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
    manager.close()
  }

  @Test
  def testRegistrationTimeout(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val controllerNode = new Node(3000, "localhost", 8021)
    val manager = new BrokerLifecycleManager(context.config, context.time, "registration-timeout-", isZkBroker = false)
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
      assertTrue(manager.initialCatchUpFuture.isCompletedExceptionally())
      assertEquals(-1L, manager.brokerEpoch)
    }
    manager.close()
  }

  @Test
  def testControlledShutdown(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, "controlled-shutdown-", isZkBroker = false)
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
    manager.close()
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

  def poll[T](context: RegistrationTestContext, manager: BrokerLifecycleManager, future: Future[T]): T = {
    while (!future.isDone || context.mockClient.hasInFlightRequests) {
      context.poll()
      manager.eventQueue.wakeup()
      context.time.sleep(100)
    }
    future.get
  }

  @Test
  def testOfflineDirsSentUntilHeartbeatSuccess(): Unit = {
    val ctx = new RegistrationTestContext(configProperties)
    val manager = new BrokerLifecycleManager(ctx.config, ctx.time, "offline-dirs-sent-in-heartbeat-", isZkBroker = false)
    val controllerNode = new Node(3000, "localhost", 8021)
    ctx.controllerNodeProvider.node.set(controllerNode)

    val registration = prepareResponse(ctx, new BrokerRegistrationResponse(new BrokerRegistrationResponseData().setBrokerEpoch(1000)))
    val hb1 = prepareResponse[BrokerHeartbeatRequest](ctx, new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData()
      .setErrorCode(Errors.NOT_CONTROLLER.code())))
    val hb2 = prepareResponse[BrokerHeartbeatRequest](ctx, new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData()))
    val hb3 = prepareResponse[BrokerHeartbeatRequest](ctx, new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData()))

    val offlineDirs = Set(Uuid.fromString("h3sC4Yk-Q9-fd0ntJTocCA"), Uuid.fromString("ej8Q9_d2Ri6FXNiTxKFiow"))
    offlineDirs.foreach(manager.propagateDirectoryFailure)

    // start the manager late to prevent a race, and force expectations on the first heartbeat
    manager.start(() => ctx.highestMetadataOffset.get(),
      ctx.mockChannelManager, ctx.clusterId, ctx.advertisedListeners,
      Collections.emptyMap(), OptionalLong.empty())

    poll(ctx, manager, registration)
    val dirs1 = poll(ctx, manager, hb1).data().offlineLogDirs()
    val dirs2 = poll(ctx, manager, hb2).data().offlineLogDirs()
    val dirs3 = poll(ctx, manager, hb3).data().offlineLogDirs()

    assertEquals(offlineDirs, dirs1.asScala.toSet)
    assertEquals(offlineDirs, dirs2.asScala.toSet)
    assertEquals(Set.empty, dirs3.asScala.toSet)
    manager.close()
  }

  @Test
  def testRegistrationIncludesDirs(): Unit = {
    val logDirs = Set("ad5FLIeCTnaQdai5vOjeng", "ybdzUKmYSLK6oiIpI6CPlw").map(Uuid.fromString)
    val ctx = new RegistrationTestContext(configProperties)
    val manager = new BrokerLifecycleManager(ctx.config, ctx.time, "registration-includes-dirs-",
      isZkBroker = false, logDirs)
    val controllerNode = new Node(3000, "localhost", 8021)
    ctx.controllerNodeProvider.node.set(controllerNode)

    val registration = prepareResponse(ctx, new BrokerRegistrationResponse(new BrokerRegistrationResponseData().setBrokerEpoch(1000)))

    manager.start(() => ctx.highestMetadataOffset.get(),
      ctx.mockChannelManager, ctx.clusterId, ctx.advertisedListeners,
      Collections.emptyMap(), OptionalLong.empty())
    val request = poll(ctx, manager, registration).asInstanceOf[BrokerRegistrationRequest]

    assertEquals(logDirs, request.data.logDirs().asScala.toSet)

    manager.close()
  }
}
