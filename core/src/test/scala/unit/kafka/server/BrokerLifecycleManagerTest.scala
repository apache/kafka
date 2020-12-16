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

import java.util.Properties
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.clients.{ManualMetadataUpdater, Metadata, MockClient}
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.BrokerRegistrationResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.BrokerRegistrationResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.metadata.BrokerState
import org.junit.rules.Timeout
import org.junit.{Assert, Rule, Test}

class BrokerLifecycleManagerTest {
  @Rule
  def globalTimeout = Timeout.millis(120000)

  def configProperties = {
    val properties = new Properties()
    properties.setProperty(KafkaConfig.LogDirsProp, "/tmp/foo")
    properties.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    properties.setProperty(KafkaConfig.BrokerIdProp, "1")
    properties.setProperty(KafkaConfig.InitialBrokerRegistrationTimeoutMs, "300000")
    properties
  }

  class SimpleControllerNodeProvider extends ControllerNodeProvider {
    val node = new AtomicReference[Node](null)
    def controllerNode(): Option[Node] = Option(node.get())
  }

  class BrokerLifecycleManagerTestContext(properties: Properties) {
    val config = new KafkaConfig(properties)
    val time = new MockTime(1, 1)
    val highestMetadataOffset = new AtomicLong(0)
    val metadata = new Metadata(1000, 1000, new LogContext(), new ClusterResourceListeners())
    val mockClient = new MockClient(time, metadata)
    mockClient.enableBlockingUntilWakeup(10)
    val metadataUpdater = new ManualMetadataUpdater()
    val controllerNodeProvider = new SimpleControllerNodeProvider()
    val channelManager = new BrokerToControllerChannelManager(mockClient,
      metadataUpdater, controllerNodeProvider, time, 60000, config, "channelManager", None)
    val clusterId = Uuid.fromString("x4AJGXQSRnephtTZzujw4w")
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
    Assert.assertEquals(BrokerState.NOT_RUNNING, manager.state())
    manager.start(() => context.highestMetadataOffset.get(),
      context.channelManager, context.clusterId)
    TestUtils.retry(60000) {
      Assert.assertEquals(BrokerState.STARTING, manager.state())
    }
    manager.close()
    Assert.assertEquals(BrokerState.SHUTTING_DOWN, manager.state())
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
      context.channelManager, context.clusterId)
    TestUtils.retry(10000) {
      context.mockClient.wakeup()
      Assert.assertEquals(1000L, manager.brokerEpoch())
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
    }
    newDuplicateRegistrationResponse()
    Assert.assertEquals(1, context.mockClient.futureResponses().size)
    manager.start(() => context.highestMetadataOffset.get(),
      context.channelManager, context.clusterId)
    // We should send the first registration request and get a failure immediately
    TestUtils.retry(60000) {
      context.mockClient.wakeup()
      Assert.assertEquals(0, context.mockClient.futureResponses().size)
    }
    // Verify that we resend the registration request.
    newDuplicateRegistrationResponse()
    TestUtils.retry(60000) {
      context.time.sleep(100)
      context.mockClient.wakeup()
      manager.eventQueue.wakeup()
      Assert.assertEquals(0, context.mockClient.futureResponses().size)
    }
    // Verify that we time out eventually.
    context.time.sleep(300000)
    TestUtils.retry(60000) {
      context.mockClient.wakeup()
      manager.eventQueue.wakeup()
      Assert.assertEquals(BrokerState.SHUTTING_DOWN, manager.state())
      Assert.assertTrue(manager.initialCatchUpFuture.isCompletedExceptionally())
      Assert.assertEquals(-1L, manager.brokerEpoch())
    }
    manager.close()
  }
}