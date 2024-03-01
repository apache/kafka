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

import kafka.server.metadata.ZkMetadataCache
import kafka.utils.TestUtils
import kafka.zk.{FeatureZNode, FeatureZNodeStatus, ZkVersion}
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.server.common.{Features => JFeatures}
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.IBP_3_2_IV0
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

import java.util.concurrent.{CountDownLatch, TimeoutException}
import scala.jdk.CollectionConverters._

class FinalizedFeatureChangeListenerTest extends QuorumTestHarness {
  var listener: FinalizedFeatureChangeListener = _
  case class FinalizedFeaturesAndEpoch(features: Map[String, Short], epoch: Long) {
    override def toString(): String = {
      s"FinalizedFeaturesAndEpoch(features=$features, epoch=$epoch)"
    }
  }

  def asJava(input: Map[String, Short]): java.util.Map[String, java.lang.Short] = {
    input.map(kv => kv._1 -> kv._2.asInstanceOf[java.lang.Short]).asJava
  }

  private def createBrokerFeatures(): BrokerFeatures = {
    val supportedFeaturesMap = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4),
      "feature_2" -> new SupportedVersionRange(1, 3))
    val brokerFeatures = BrokerFeatures.createDefault(true)
    brokerFeatures.setSupportedFeatures(Features.supportedFeatures(supportedFeaturesMap.asJava))
    brokerFeatures
  }

  private def createFinalizedFeatures(): FinalizedFeaturesAndEpoch = {
    val finalizedFeaturesMap = Map[String, Short]("feature_1" -> 3)
    zkClient.createFeatureZNode(FeatureZNode(IBP_3_2_IV0, FeatureZNodeStatus.Enabled, finalizedFeaturesMap))
    val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(version, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeBytes.isEmpty)
    FinalizedFeaturesAndEpoch(finalizedFeaturesMap, version)
  }

  private def createListener(
    cache: ZkMetadataCache,
    expectedCacheContent: Option[FinalizedFeaturesAndEpoch]
  ): FinalizedFeatureChangeListener = {
    val listener = new FinalizedFeatureChangeListener(cache, zkClient)
    assertFalse(listener.isListenerInitiated)
    assertTrue(cache.getFeatureOption.isEmpty)
    listener.initOrThrow(15000)
    assertTrue(listener.isListenerInitiated)
    if (expectedCacheContent.isDefined) {
      val mayBeNewCacheContent = cache.getFeatureOption
      assertFalse(mayBeNewCacheContent.isEmpty)
      val newCacheContent = mayBeNewCacheContent.get
      assertEquals(asJava(expectedCacheContent.get.features), newCacheContent.finalizedFeatures())
      assertEquals(expectedCacheContent.get.epoch, newCacheContent.finalizedFeaturesEpoch())
    } else {
      val mayBeNewCacheContent = cache.getFeatureOption
      assertTrue(mayBeNewCacheContent.isEmpty)
    }
    listener
  }

  @AfterEach
  def testTearDown(): Unit = {
    if (listener != null) {
      listener.close()
    }
  }

  /**
   * Tests that the listener can be initialized, and that it can listen to ZK notifications
   * successfully from an "Enabled" FeatureZNode (the ZK data has no feature incompatibilities).
   * Particularly the test checks if multiple notifications can be processed in ZK
   * (i.e. whether the FeatureZNode watch can be re-established).
   */
  @Test
  def testInitSuccessAndNotificationSuccess(): Unit = {
    val initialFinalizedFeatures = createFinalizedFeatures()
    val brokerFeatures = createBrokerFeatures()
    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    listener = createListener(cache, Some(initialFinalizedFeatures))

    def updateAndCheckCache(finalizedFeatures: Map[String, Short]): Unit = {
      zkClient.updateFeatureZNode(FeatureZNode(IBP_3_2_IV0, FeatureZNodeStatus.Enabled, finalizedFeatures))
      val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
      assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
      assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
      assertTrue(updatedVersion > initialFinalizedFeatures.epoch)

      cache.waitUntilFeatureEpochOrThrow(updatedVersion, JTestUtils.DEFAULT_MAX_WAIT_MS)
      assertEquals(new JFeatures(MetadataVersion.IBP_2_8_IV1,
        asJava(finalizedFeatures), updatedVersion, false),
          cache.getFeatureOption.get)
      assertTrue(listener.isListenerInitiated)
    }

    // Check if the write succeeds and a ZK notification is received that causes the feature cache
    // to be populated.
    updateAndCheckCache(Map[String, Short]("feature_1" -> 4))
    // Check if second write succeeds and a ZK notification is again received that causes the cache
    // to be populated. This check is needed to verify that the watch on the FeatureZNode was
    // re-established after the notification was received due to the first write above.
    updateAndCheckCache(
      Map[String, Short](
        "feature_1" -> 4,
        "feature_2" -> 3))
  }

  /**
   * Tests that the listener can be initialized, and that it can process FeatureZNode deletion
   * successfully.
   */
  @Test
  def testFeatureZNodeDeleteNotificationProcessing(): Unit = {
    val brokerFeatures = createBrokerFeatures()
    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    val initialFinalizedFeatures = createFinalizedFeatures()
    listener = createListener(cache, Some(initialFinalizedFeatures))

    zkClient.deleteFeatureZNode()
    val (mayBeFeatureZNodeDeletedBytes, deletedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertEquals(deletedVersion, ZkVersion.UnknownVersion)
    assertTrue(mayBeFeatureZNodeDeletedBytes.isEmpty)
    TestUtils.waitUntilTrue(() => {
      cache.getFeatureOption.isEmpty
    }, "Timed out waiting for FinalizedFeatureCache to become empty")
    assertTrue(listener.isListenerInitiated)
  }

  /**
   * Tests that the listener can be initialized, and that it can process disabling of a FeatureZNode
   * successfully.
   */
  @Test
  def testFeatureZNodeDisablingNotificationProcessing(): Unit = {
    val brokerFeatures = createBrokerFeatures()
    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    val initialFinalizedFeatures = createFinalizedFeatures()

    val updatedFinalizedFeaturesMap = Map[String, Short]()
    zkClient.updateFeatureZNode(FeatureZNode(IBP_3_2_IV0, FeatureZNodeStatus.Disabled, updatedFinalizedFeaturesMap))
    val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
    assertTrue(updatedVersion > initialFinalizedFeatures.epoch)
    assertTrue(cache.getFeatureOption.isEmpty)
  }

  /**
   * Tests that the wait operation on the cache fails (as expected) when an epoch can never be
   * reached. Also tests that the wait operation on the cache succeeds when an epoch is expected to
   * be reached.
   */
  @Test
  def testCacheUpdateWaitFailsForUnreachableVersion(): Unit = {
    val initialFinalizedFeatures = createFinalizedFeatures()
    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, createBrokerFeatures())
    listener = createListener(cache, Some(initialFinalizedFeatures))

    assertThrows(classOf[TimeoutException], () => cache.waitUntilFeatureEpochOrThrow(initialFinalizedFeatures.epoch + 1, JTestUtils.DEFAULT_MAX_WAIT_MS))

    val updatedFinalizedFeaturesMap = Map[String, Short]()
    zkClient.updateFeatureZNode(FeatureZNode(IBP_3_2_IV0, FeatureZNodeStatus.Disabled, updatedFinalizedFeaturesMap))
    val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
    assertTrue(updatedVersion > initialFinalizedFeatures.epoch)

    assertThrows(classOf[TimeoutException], () => cache.waitUntilFeatureEpochOrThrow(updatedVersion, JTestUtils.DEFAULT_MAX_WAIT_MS))
    assertTrue(cache.getFeatureOption.isEmpty)
    assertTrue(listener.isListenerInitiated)
  }

  /**
   * Tests that the listener initialization fails when it picks up a feature incompatibility from
   * ZK from an "Enabled" FeatureZNode.
   */
  @Test
  def testInitFailureDueToFeatureIncompatibility(): Unit = {
    val brokerFeatures = createBrokerFeatures()
    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    val incompatibleFinalizedFeaturesMap = Map[String, Short]("feature_1" -> 5)
    zkClient.createFeatureZNode(FeatureZNode(IBP_3_2_IV0, FeatureZNodeStatus.Enabled, incompatibleFinalizedFeaturesMap))
    val (mayBeFeatureZNodeBytes, initialVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(initialVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeBytes.isEmpty)

    val exitLatch = new CountDownLatch(1)
    Exit.setExitProcedure((_, _) => exitLatch.countDown())
    try {
      listener = new FinalizedFeatureChangeListener(cache, zkClient)
      assertFalse(listener.isListenerInitiated)
      assertTrue(cache.getFeatureOption.isEmpty)
      assertThrows(classOf[TimeoutException], () => listener.initOrThrow(5000))
      exitLatch.await()
      assertFalse(listener.isListenerInitiated)
      assertTrue(listener.isListenerDead)
      assertTrue(cache.getFeatureOption.isEmpty)
    } finally {
      Exit.resetExitProcedure()
    }
  }

  /**
   * Tests that the listener initialization fails when invalid wait time (<= 0) is provided as input.
   */
  @Test
  def testInitFailureDueToInvalidWaitTime(): Unit = {
    val brokerFeatures = createBrokerFeatures()
    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    listener = new FinalizedFeatureChangeListener(cache, zkClient)
    assertThrows(classOf[IllegalArgumentException], () => listener.initOrThrow(0))
    assertThrows(classOf[IllegalArgumentException], () => listener.initOrThrow(-1))
  }

  /**
   * Tests that after successful initialization, the listener fails when it picks up a feature
   * incompatibility from ZK.
   */
  @Test
  def testNotificationFailureDueToFeatureIncompatibility(): Unit = {
    val brokerFeatures = createBrokerFeatures()
    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    val initialFinalizedFeatures = createFinalizedFeatures()
    listener = createListener(cache, Some(initialFinalizedFeatures))

    val exitLatch = new CountDownLatch(1)
    Exit.setExitProcedure((_, _) => exitLatch.countDown())
    val incompatibleFinalizedFeaturesMap = Map[String, Short](
      "feature_1" -> (brokerFeatures.supportedFeatures.get("feature_1").max() + 1).asInstanceOf[Short])
    zkClient.updateFeatureZNode(FeatureZNode(IBP_3_2_IV0, FeatureZNodeStatus.Enabled, incompatibleFinalizedFeaturesMap))
    val (mayBeFeatureZNodeIncompatibleBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeIncompatibleBytes.isEmpty)

    try {
      TestUtils.waitUntilTrue(() => {
        // Make sure the custom exit procedure (defined above) was called.
        exitLatch.getCount == 0 &&
        // Make sure the listener is no longer initiated (because, it is dead).
        !listener.isListenerInitiated &&
        // Make sure the listener dies after hitting an exception when processing incompatible
        // features read from ZK.
        listener.isListenerDead &&
        // Make sure the cache contents are as expected, and, the incompatible features were not
        // applied.
          cache.getFeatureOption.get.equals(new JFeatures(MetadataVersion.IBP_2_8_IV1,
            asJava(initialFinalizedFeatures.features), initialFinalizedFeatures.epoch, false))
      }, "Timed out waiting for listener death and FinalizedFeatureCache to be updated")
    } finally {
      Exit.resetExitProcedure()
    }
  }
}
