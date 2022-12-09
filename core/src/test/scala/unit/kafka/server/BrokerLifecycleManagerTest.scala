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

import java.util.{Collections, Properties}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.clients.{Metadata, MockClient, NodeApiVersions}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.Node
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{Listener, ListenerCollection}
import org.apache.kafka.common.message.{BrokerHeartbeatResponseData, BrokerRegistrationResponseData}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys.{BROKER_HEARTBEAT, BROKER_REGISTRATION}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, BrokerHeartbeatRequest, BrokerHeartbeatResponse, BrokerRegistrationResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.metadata.BrokerState
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.api.Assertions._

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
    properties
  }

  class SimpleControllerNodeProvider extends ControllerNodeProvider {
    val node = new AtomicReference[Node](null)

    def listenerName: ListenerName = new ListenerName("PLAINTEXT")

    def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT;

    def saslMechanism: String = SaslConfigs.DEFAULT_SASL_MECHANISM

    override def getControllerInfo(): ControllerInformation = ControllerInformation(Option(node.get()),
      listenerName, securityProtocol, saslMechanism, isZkController = false)
  }

  class BrokerLifecycleManagerTestContext(properties: Properties) {
    val config = new KafkaConfig(properties)
    val time = new MockTime(1, 1)
    val highestMetadataOffset = new AtomicLong(0)
    val metadata = new Metadata(1000, 1000, new LogContext(), new ClusterResourceListeners())
    val mockClient = new MockClient(time, metadata)
    val controllerNodeProvider = new SimpleControllerNodeProvider()
    val nodeApiVersions = NodeApiVersions.create(Seq(BROKER_REGISTRATION, BROKER_HEARTBEAT).map {
      apiKey => new ApiVersion().setApiKey(apiKey.id).
        setMinVersion(apiKey.oldestVersion()).setMaxVersion(apiKey.latestVersion())
    }.toList.asJava)
    val mockChannelManager = new MockBrokerToControllerChannelManager(mockClient,
      time, controllerNodeProvider, nodeApiVersions)
    val clusterId = "x4AJGXQSRnephtTZzujw4w"
    val advertisedListeners = new ListenerCollection()
    config.effectiveAdvertisedListeners.foreach { ep =>
      advertisedListeners.add(new Listener().setHost(ep.host).
        setName(ep.listenerName.value()).
        setPort(ep.port.shortValue()).
        setSecurityProtocol(ep.securityProtocol.id))
    }

    def poll(): Unit = {
      mockClient.wakeup()
      mockChannelManager.poll()
    }
  }

  @Test
  def testCreateAndClose(): Unit = {
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
    manager.close()
  }

  @Test
  def testCreateStartAndClose(): Unit = {
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
    assertEquals(BrokerState.NOT_RUNNING, manager.state)
    manager.start(() => context.highestMetadataOffset.get(),
      context.mockChannelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap())
    TestUtils.retry(60000) {
      assertEquals(BrokerState.STARTING, manager.state)
    }
    manager.close()
    assertEquals(BrokerState.SHUTTING_DOWN, manager.state)
  }

  @Test
  def testSuccessfulRegistration(): Unit = {
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
    val controllerNode = new Node(3000, "localhost", 8021)
    context.controllerNodeProvider.node.set(controllerNode)
    context.mockClient.prepareResponseFrom(new BrokerRegistrationResponse(
      new BrokerRegistrationResponseData().setBrokerEpoch(1000)), controllerNode)
    manager.start(() => context.highestMetadataOffset.get(),
      context.mockChannelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap())
    TestUtils.retry(10000) {
      context.poll()
      assertEquals(1000L, manager.brokerEpoch)
    }
    manager.close()

  }

  @Test
  def testRegistrationTimeout(): Unit = {
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val controllerNode = new Node(3000, "localhost", 8021)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
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
      Collections.emptyMap())
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
    val context = new BrokerLifecycleManagerTestContext(configProperties)
    val manager = new BrokerLifecycleManager(context.config, context.time, None)
    val controllerNode = new Node(3000, "localhost", 8021)
    context.controllerNodeProvider.node.set(controllerNode)
    context.mockClient.prepareResponseFrom(new BrokerRegistrationResponse(
      new BrokerRegistrationResponseData().setBrokerEpoch(1000)), controllerNode)
    context.mockClient.prepareResponseFrom(new BrokerHeartbeatResponse(
      new BrokerHeartbeatResponseData().setIsCaughtUp(true)), controllerNode)
    manager.start(() => context.highestMetadataOffset.get(),
      context.mockChannelManager, context.clusterId, context.advertisedListeners,
      Collections.emptyMap())
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
}
