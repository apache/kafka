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

package kafka.server.metadata

import kafka.coordinator.transaction.TransactionCoordinator

import java.util.Collections.{singleton, singletonList, singletonMap}
import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import kafka.log.LogManager
import kafka.server.{BrokerLifecycleManager, BrokerServer, KafkaConfig, ReplicaManager}
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry, NewTopic}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.BROKER
import org.apache.kafka.common.metadata.FeatureLevelRecord
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataImageTest, MetadataProvenance}
import org.apache.kafka.image.loader.LogDeltaManifest
import org.apache.kafka.raft.LeaderAndEpoch
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.fault.FaultHandler
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.{clearInvocations, doThrow, mock, times, verify, verifyNoInteractions}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class BrokerMetadataPublisherTest {
  val exitException = new AtomicReference[Throwable](null)

  @BeforeEach
  def setUp(): Unit = {
    Exit.setExitProcedure((code, _) => exitException.set(new RuntimeException(s"Exit ${code}")))
    Exit.setHaltProcedure((code, _) => exitException.set(new RuntimeException(s"Halt ${code}")))
  }

  @AfterEach
  def tearDown(): Unit = {
    Exit.resetExitProcedure();
    Exit.resetHaltProcedure();
    val exception = exitException.get()
    if (exception != null) {
      throw exception
    }
  }

  @Test
  def testGetTopicDelta(): Unit = {
    assert(BrokerMetadataPublisher.getTopicDelta(
      "not-a-topic",
      MetadataImageTest.IMAGE1,
      MetadataImageTest.DELTA1).isEmpty, "Expected no delta for unknown topic")

    assert(BrokerMetadataPublisher.getTopicDelta(
      "foo",
      MetadataImageTest.IMAGE1,
      MetadataImageTest.DELTA1).isEmpty, "Expected no delta for deleted topic")

    assert(BrokerMetadataPublisher.getTopicDelta(
      "bar",
      MetadataImageTest.IMAGE1,
      MetadataImageTest.DELTA1).isDefined, "Expected to see delta for changed topic")
  }

  private def newMockDynamicConfigPublisher(
    broker: BrokerServer,
    errorHandler: FaultHandler
  ): DynamicConfigPublisher = {
    Mockito.spy(new DynamicConfigPublisher(
      conf = broker.config,
      faultHandler = errorHandler,
      dynamicConfigHandlers = broker.dynamicConfigHandlers.toMap,
      nodeType = "broker"))
  }

  @Test
  def testReloadUpdatedFilesWithoutConfigChange(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val broker = cluster.brokers().values().iterator().next()
      val publisher = newMockDynamicConfigPublisher(broker, cluster.nonFatalFaultHandler())

      val numTimesReloadCalled = new AtomicInteger(0)
      Mockito.when(publisher.reloadUpdatedFilesWithoutConfigChange(any[Properties]())).
        thenAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock): Unit = numTimesReloadCalled.addAndGet(1)
        })
      broker.brokerMetadataPublisher.dynamicConfigPublisher = publisher
      val admin = Admin.create(cluster.clientProperties())
      try {
        assertEquals(0, numTimesReloadCalled.get())
        admin.incrementalAlterConfigs(singletonMap(
          new ConfigResource(BROKER, ""),
          singleton(new AlterConfigOp(new ConfigEntry(KafkaConfig.MaxConnectionsProp, "123"), SET)))).all().get()
        TestUtils.waitUntilTrue(() => numTimesReloadCalled.get() == 0,
          "numTimesConfigured never reached desired value")

        // Setting the foo.bar.test.configuration to 1 will still trigger reconfiguration because
        // reloadUpdatedFilesWithoutConfigChange will be called.
        admin.incrementalAlterConfigs(singletonMap(
          new ConfigResource(BROKER, broker.config.nodeId.toString),
          singleton(new AlterConfigOp(new ConfigEntry(KafkaConfig.MaxConnectionsProp, "123"), SET)))).all().get()
        TestUtils.waitUntilTrue(() => numTimesReloadCalled.get() == 1,
          "numTimesConfigured never reached desired value")
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testExceptionInUpdateCoordinator(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_7_IV1).
        build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val broker = cluster.brokers().values().iterator().next()
      TestUtils.retry(60000) {
        assertNotNull(broker.brokerMetadataPublisher)
      }
      val publisher = Mockito.spy(broker.brokerMetadataPublisher)
      doThrow(new RuntimeException("injected failure")).when(publisher).updateCoordinator(any(), any(), any(), any(), any())
      broker.sharedServer.loader.removeAndClosePublisher(broker.brokerMetadataPublisher).get(1, TimeUnit.MINUTES)
      broker.metadataPublishers.remove(broker.brokerMetadataPublisher)
      broker.sharedServer.loader.installPublishers(List(publisher).asJava).get(1, TimeUnit.MINUTES)
      val admin = Admin.create(cluster.clientProperties())
      try {
        admin.createTopics(singletonList(new NewTopic("foo", 1, 1.toShort))).all().get()
      } finally {
        admin.close()
      }
      TestUtils.retry(60000) {
        assertTrue(Option(cluster.nonFatalFaultHandler().firstException()).
          flatMap(e => Option(e.getMessage())).getOrElse("(none)").contains("injected failure"))
      }
    } finally {
      cluster.nonFatalFaultHandler().setIgnore(true)
      cluster.close()
    }
  }

  @Test
  def testNewImagePushedToGroupCoordinator(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, ""))
    val metadataCache = new KRaftMetadataCache(0)
    val logManager = mock(classOf[LogManager])
    val replicaManager = mock(classOf[ReplicaManager])
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val faultHandler = mock(classOf[FaultHandler])

    val metadataPublisher = new BrokerMetadataPublisher(
      config,
      metadataCache,
      logManager,
      replicaManager,
      groupCoordinator,
      mock(classOf[TransactionCoordinator]),
      mock(classOf[DynamicConfigPublisher]),
      mock(classOf[DynamicClientQuotaPublisher]),
      mock(classOf[ScramPublisher]),
      mock(classOf[DelegationTokenPublisher]),
      mock(classOf[AclPublisher]),
      faultHandler,
      faultHandler,
      mock(classOf[BrokerLifecycleManager]),
    )

    val image = MetadataImage.EMPTY
    val delta = new MetadataDelta.Builder()
      .setImage(image)
      .build()

    metadataPublisher.onMetadataUpdate(delta, image,
      LogDeltaManifest.newBuilder()
        .provenance(MetadataProvenance.EMPTY)
        .leaderAndEpoch(LeaderAndEpoch.UNKNOWN)
        .numBatches(1)
        .elapsedNs(100)
        .numBytes(42)
        .build());

    verify(groupCoordinator).onNewMetadataImage(image, delta)
  }

  @Test
  def testMetadataVersionUpdateToIBP_3_7_IV2OrAboveTriggersBrokerReRegistration(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, ""))
    val metadataCache = new KRaftMetadataCache(0)
    val logManager = mock(classOf[LogManager])
    val replicaManager = mock(classOf[ReplicaManager])
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val faultHandler = mock(classOf[FaultHandler])
    val brokerLifecycleManager = mock(classOf[BrokerLifecycleManager])

    val metadataPublisher = new BrokerMetadataPublisher(
      config,
      metadataCache,
      logManager,
      replicaManager,
      groupCoordinator,
      mock(classOf[TransactionCoordinator]),
      mock(classOf[DynamicConfigPublisher]),
      mock(classOf[DynamicClientQuotaPublisher]),
      mock(classOf[ScramPublisher]),
      mock(classOf[DelegationTokenPublisher]),
      mock(classOf[AclPublisher]),
      faultHandler,
      faultHandler,
      brokerLifecycleManager,
    )

    var image = MetadataImage.EMPTY
    var delta = new MetadataDelta.Builder()
      .setImage(image)
      .build()

    // We first upgrade metadata version to 3_6_IV2
    delta.replay(new FeatureLevelRecord().
      setName(MetadataVersion.FEATURE_NAME).
      setFeatureLevel(MetadataVersion.IBP_3_6_IV2.featureLevel()))
    var newImage = delta.apply(new MetadataProvenance(100, 4, 2000))

    metadataPublisher.onMetadataUpdate(delta, newImage,
      LogDeltaManifest.newBuilder()
        .provenance(MetadataProvenance.EMPTY)
        .leaderAndEpoch(LeaderAndEpoch.UNKNOWN)
        .numBatches(1)
        .elapsedNs(100)
        .numBytes(42)
        .build());

    // This should NOT trigger broker reregistration
    verifyNoInteractions(brokerLifecycleManager)

    // We then upgrade to IBP_3_7_IV2
    image = newImage
    delta = new MetadataDelta.Builder()
      .setImage(image)
      .build()
    delta.replay(new FeatureLevelRecord().
      setName(MetadataVersion.FEATURE_NAME).
      setFeatureLevel(MetadataVersion.IBP_3_7_IV2.featureLevel()))
    newImage = delta.apply(new MetadataProvenance(100, 4, 2000))

    metadataPublisher.onMetadataUpdate(delta, newImage,
      LogDeltaManifest.newBuilder()
        .provenance(MetadataProvenance.EMPTY)
        .leaderAndEpoch(LeaderAndEpoch.UNKNOWN)
        .numBatches(1)
        .elapsedNs(100)
        .numBytes(42)
        .build());

    // This SHOULD trigger a broker registration
    verify(brokerLifecycleManager, times(1)).handleKraftJBODMetadataVersionUpdate()
    clearInvocations(brokerLifecycleManager)

    // Finally upgrade to IBP_3_8_IV0
    image = newImage
    delta = new MetadataDelta.Builder()
      .setImage(image)
      .build()
    delta.replay(new FeatureLevelRecord().
      setName(MetadataVersion.FEATURE_NAME).
      setFeatureLevel(MetadataVersion.IBP_3_8_IV0.featureLevel()))
    newImage = delta.apply(new MetadataProvenance(200, 4, 3000))

    metadataPublisher.onMetadataUpdate(delta, newImage,
      LogDeltaManifest.newBuilder()
        .provenance(MetadataProvenance.EMPTY)
        .leaderAndEpoch(LeaderAndEpoch.UNKNOWN)
        .numBatches(1)
        .elapsedNs(100)
        .numBytes(42)
        .build());

    // This should NOT trigger broker reregistration
    verify(brokerLifecycleManager, times(0)).handleKraftJBODMetadataVersionUpdate()

    metadataPublisher.close()
  }
}
