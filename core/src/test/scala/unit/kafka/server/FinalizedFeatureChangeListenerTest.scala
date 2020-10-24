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

import java.util.concurrent.{CountDownLatch, TimeoutException}

import kafka.zk.{FeatureZNode, FeatureZNodeStatus, ZkVersion, ZooKeeperTestHarness}
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.feature.{Features, FinalizedVersionRange, SupportedVersionRange}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.Assert.{assertEquals, assertFalse, assertNotEquals, assertThrows, assertTrue}
import org.junit.Test

import scala.jdk.CollectionConverters._

class FinalizedFeatureChangeListenerTest extends ZooKeeperTestHarness {

  private def createBrokerFeatures(): BrokerFeatures = {
    val supportedFeaturesMap = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4),
      "feature_2" -> new SupportedVersionRange(1, 3))
    val brokerFeatures = BrokerFeatures.createDefault()
    brokerFeatures.setSupportedFeatures(Features.supportedFeatures(supportedFeaturesMap.asJava))
    brokerFeatures
  }

  private def createFinalizedFeatures(): FinalizedFeaturesAndEpoch = {
    val finalizedFeaturesMap = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 3))
    val finalizedFeatures = Features.finalizedFeatures(finalizedFeaturesMap.asJava)
    zkClient.createFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, finalizedFeatures))
    val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(version, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeBytes.isEmpty)
    FinalizedFeaturesAndEpoch(finalizedFeatures, version)
  }

  private def createListener(
    cache: FinalizedFeatureCache,
    expectedCacheContent: Option[FinalizedFeaturesAndEpoch]
  ): FinalizedFeatureChangeListener = {
    val listener = new FinalizedFeatureChangeListener(cache, zkClient)
    assertFalse(listener.isListenerInitiated)
    assertTrue(cache.isEmpty)
    listener.initOrThrow(15000)
    assertTrue(listener.isListenerInitiated)
    if (expectedCacheContent.isDefined) {
      val mayBeNewCacheContent = cache.get
      assertFalse(mayBeNewCacheContent.isEmpty)
      val newCacheContent = mayBeNewCacheContent.get
      assertEquals(expectedCacheContent.get.features, newCacheContent.features)
      assertEquals(expectedCacheContent.get.epoch, newCacheContent.epoch)
    } else {
      val mayBeNewCacheContent = cache.get
      assertTrue(mayBeNewCacheContent.isEmpty)
    }
    listener
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
    val cache = new FinalizedFeatureCache(brokerFeatures)
    val listener = createListener(cache, Some(initialFinalizedFeatures))

    def updateAndCheckCache(finalizedFeatures: Features[FinalizedVersionRange]): Unit = {
      zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, finalizedFeatures))
      val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
      assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
      assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
      assertTrue(updatedVersion > initialFinalizedFeatures.epoch)

      cache.waitUntilEpochOrThrow(updatedVersion, JTestUtils.DEFAULT_MAX_WAIT_MS)
      assertEquals(FinalizedFeaturesAndEpoch(finalizedFeatures, updatedVersion), cache.get.get)
      assertTrue(listener.isListenerInitiated)
    }

    // Check if the write succeeds and a ZK notification is received that causes the feature cache
    // to be populated.
    updateAndCheckCache(
      Features.finalizedFeatures(
        Map[String, FinalizedVersionRange](
        "feature_1" -> new FinalizedVersionRange(2, 4)).asJava))
    // Check if second write succeeds and a ZK notification is again received that causes the cache
    // to be populated. This check is needed to verify that the watch on the FeatureZNode was
    // re-established after the notification was received due to the first write above.
    updateAndCheckCache(
      Features.finalizedFeatures(
        Map[String, FinalizedVersionRange](
          "feature_1" -> new FinalizedVersionRange(2, 4),
          "feature_2" -> new FinalizedVersionRange(1, 3)).asJava))
  }

  /**
   * Tests that the listener can be initialized, and that it can process FeatureZNode deletion
   * successfully.
   */
  @Test
  def testFeatureZNodeDeleteNotificationProcessing(): Unit = {
    val brokerFeatures = createBrokerFeatures()
    val cache = new FinalizedFeatureCache(brokerFeatures)
    val initialFinalizedFeatures = createFinalizedFeatures()
    val listener = createListener(cache, Some(initialFinalizedFeatures))

    zkClient.deleteFeatureZNode()
    val (mayBeFeatureZNodeDeletedBytes, deletedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertEquals(deletedVersion, ZkVersion.UnknownVersion)
    assertTrue(mayBeFeatureZNodeDeletedBytes.isEmpty)
    TestUtils.waitUntilTrue(() => {
      cache.isEmpty
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
    val cache = new FinalizedFeatureCache(brokerFeatures)
    val initialFinalizedFeatures = createFinalizedFeatures()

    val updatedFinalizedFeaturesMap = Map[String, FinalizedVersionRange]()
    val updatedFinalizedFeatures = Features.finalizedFeatures(updatedFinalizedFeaturesMap.asJava)
    zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Disabled, updatedFinalizedFeatures))
    val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
    assertTrue(updatedVersion > initialFinalizedFeatures.epoch)
    assertTrue(cache.get.isEmpty)
  }

  /**
   * Tests that the wait operation on the cache fails (as expected) when an epoch can never be
   * reached. Also tests that the wait operation on the cache succeeds when an epoch is expected to
   * be reached.
   */
  @Test
  def testCacheUpdateWaitFailsForUnreachableVersion(): Unit = {
    val initialFinalizedFeatures = createFinalizedFeatures()
    val cache = new FinalizedFeatureCache(createBrokerFeatures())
    val listener = createListener(cache, Some(initialFinalizedFeatures))

    assertThrows(
      classOf[TimeoutException],
      () => cache.waitUntilEpochOrThrow(initialFinalizedFeatures.epoch + 1, JTestUtils.DEFAULT_MAX_WAIT_MS))

    val updatedFinalizedFeaturesMap = Map[String, FinalizedVersionRange]()
    val updatedFinalizedFeatures = Features.finalizedFeatures(updatedFinalizedFeaturesMap.asJava)
    zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Disabled, updatedFinalizedFeatures))
    val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
    assertTrue(updatedVersion > initialFinalizedFeatures.epoch)

    assertThrows(
      classOf[TimeoutException],
      () => cache.waitUntilEpochOrThrow(updatedVersion, JTestUtils.DEFAULT_MAX_WAIT_MS))
    assertTrue(cache.get.isEmpty)
    assertTrue(listener.isListenerInitiated)
  }

  /**
   * Tests that the listener initialization fails when it picks up a feature incompatibility from
   * ZK from an "Enabled" FeatureZNode.
   */
  @Test
  def testInitFailureDueToFeatureIncompatibility(): Unit = {
    val brokerFeatures = createBrokerFeatures()
    val cache = new FinalizedFeatureCache(brokerFeatures)

    val incompatibleFinalizedFeaturesMap = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 5))
    val incompatibleFinalizedFeatures = Features.finalizedFeatures(incompatibleFinalizedFeaturesMap.asJava)
    zkClient.createFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, incompatibleFinalizedFeatures))
    val (mayBeFeatureZNodeBytes, initialVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(initialVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeBytes.isEmpty)

    val exitLatch = new CountDownLatch(1)
    Exit.setExitProcedure((_, _) => exitLatch.countDown())
    try {
      val listener = new FinalizedFeatureChangeListener(cache, zkClient)
      assertFalse(listener.isListenerInitiated)
      assertTrue(cache.isEmpty)
      assertThrows(classOf[TimeoutException], () => listener.initOrThrow(5000))
      exitLatch.await()
      assertFalse(listener.isListenerInitiated)
      assertTrue(listener.isListenerDead)
      assertTrue(cache.isEmpty)
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
    val cache = new FinalizedFeatureCache(brokerFeatures)
    val listener = new FinalizedFeatureChangeListener(cache, zkClient)
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
    val cache = new FinalizedFeatureCache(brokerFeatures)
    val initialFinalizedFeatures = createFinalizedFeatures()
    val listener = createListener(cache, Some(initialFinalizedFeatures))

    val exitLatch = new CountDownLatch(1)
    Exit.setExitProcedure((_, _) => exitLatch.countDown())
    val incompatibleFinalizedFeaturesMap = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(
        brokerFeatures.supportedFeatures.get("feature_1").min(),
        (brokerFeatures.supportedFeatures.get("feature_1").max() + 1).asInstanceOf[Short]))
    val incompatibleFinalizedFeatures = Features.finalizedFeatures(incompatibleFinalizedFeaturesMap.asJava)
    zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, incompatibleFinalizedFeatures))
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
        cache.get.get.equals(initialFinalizedFeatures)
      }, "Timed out waiting for listener death and FinalizedFeatureCache to be updated")
    } finally {
      Exit.resetExitProcedure()
    }
  }
}
