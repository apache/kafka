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

import java.util.concurrent.CountDownLatch

import kafka.zk.{FeatureZNode, FeatureZNodeStatus, ZkVersion, ZooKeeperTestHarness}
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.feature.{Features, FinalizedVersionRange, SupportedVersionRange}
import org.junit.Assert.{assertEquals, assertFalse, assertNotEquals, assertThrows, assertTrue}
import org.junit.{Before, Test}

import scala.concurrent.TimeoutException
import scala.jdk.CollectionConverters._

class FinalizedFeatureChangeListenerTest extends ZooKeeperTestHarness {
  @Before
  override def setUp(): Unit = {
    super.setUp()
    FinalizedFeatureCache.clear()
    SupportedFeatures.clear()
  }

  private def createSupportedFeatures(): Features[SupportedVersionRange] = {
    val supportedFeaturesMap = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4),
      "feature_2" -> new SupportedVersionRange(1, 3))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeaturesMap.asJava))
    SupportedFeatures.get
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

  private def createListener(expectedCacheContent: Option[FinalizedFeaturesAndEpoch]): FinalizedFeatureChangeListener = {
    val listener = new FinalizedFeatureChangeListener(zkClient)
    assertFalse(listener.isListenerInitiated)
    assertTrue(FinalizedFeatureCache.isEmpty)
    listener.initOrThrow(15000)
    assertTrue(listener.isListenerInitiated)
    if (expectedCacheContent.isDefined) {
      val mayBeNewCacheContent = FinalizedFeatureCache.get
      assertFalse(mayBeNewCacheContent.isEmpty)
      val newCacheContent = mayBeNewCacheContent.get
      assertEquals(expectedCacheContent.get.features, newCacheContent.features)
      assertEquals(expectedCacheContent.get.epoch, newCacheContent.epoch)
    } else {
      val mayBeNewCacheContent = FinalizedFeatureCache.get
      assertTrue(mayBeNewCacheContent.isEmpty)
    }
    listener
  }

  /**
   * Tests that the listener can be initialized, and that it can listen to ZK notifications
   * successfully from an "Enabled" FeatureZNode (the ZK data has no feature incompatibilities).
   */
  @Test
  def testInitSuccessAndNotificationSuccess(): Unit = {
    createSupportedFeatures()
    val initialFinalizedFeatures = createFinalizedFeatures()
    val listener = createListener(Some(initialFinalizedFeatures))

    val updatedFinalizedFeaturesMap = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 4))
    val updatedFinalizedFeatures = Features.finalizedFeatures(updatedFinalizedFeaturesMap.asJava)
    zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, updatedFinalizedFeatures))
    val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
    assertTrue(updatedVersion > initialFinalizedFeatures.epoch)
    TestUtils.waitUntilTrue(() => {
      FinalizedFeatureCache.get.get.equals(FinalizedFeaturesAndEpoch(updatedFinalizedFeatures, updatedVersion))
    }, "Timed out waiting for FinalizedFeatureCache to be updated with new features")
    assertTrue(listener.isListenerInitiated)
  }

  /**
   * Tests that the listener can be initialized, and that it can process FeatureZNode deletion
   * successfully.
   */
  @Test
  def testFeatureZNodeDeleteNotificationProcessing(): Unit = {
    createSupportedFeatures()
    val initialFinalizedFeatures = createFinalizedFeatures()
    val listener = createListener(Some(initialFinalizedFeatures))

    zkClient.deleteFeatureZNode()
    val (mayBeFeatureZNodeDeletedBytes, deletedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertEquals(deletedVersion, ZkVersion.UnknownVersion)
    assertTrue(mayBeFeatureZNodeDeletedBytes.isEmpty)
    TestUtils.waitUntilTrue(() => {
      FinalizedFeatureCache.isEmpty
    }, "Timed out waiting for FinalizedFeatureCache to become empty")
    assertTrue(listener.isListenerInitiated)
  }

  /**
   * Tests that the listener can be initialized, and that it can process disabling of a FeatureZNode
   * successfully.
   */
  @Test
  def testFeatureZNodeDisablingNotificationProcessing(): Unit = {
    createSupportedFeatures()
    val initialFinalizedFeatures = createFinalizedFeatures()
    val listener = createListener(Some(initialFinalizedFeatures))

    val updatedFinalizedFeaturesMap = Map[String, FinalizedVersionRange]()
    val updatedFinalizedFeatures = Features.finalizedFeatures(updatedFinalizedFeaturesMap.asJava)
    zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Disabled, updatedFinalizedFeatures))
    val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
    assertTrue(updatedVersion > initialFinalizedFeatures.epoch)

    TestUtils.waitUntilTrue(() => {
      FinalizedFeatureCache.get.isEmpty
    }, "Timed out waiting for FinalizedFeatureCache to become empty")
    assertTrue(listener.isListenerInitiated)
  }

  /**
   * Tests that the listener initialization fails when it picks up a feature incompatibility from
   * ZK from an "Enabled" FeatureZNode.
   */
  @Test
  def testInitFailureDueToFeatureIncompatibility(): Unit = {
    createSupportedFeatures()

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
      val listener = new FinalizedFeatureChangeListener(zkClient)
      assertFalse(listener.isListenerInitiated)
      assertTrue(FinalizedFeatureCache.isEmpty)
      assertThrows(classOf[TimeoutException], () => listener.initOrThrow(5000))
      exitLatch.await()
      assertFalse(listener.isListenerInitiated)
      assertTrue(listener.isListenerDead)
      assertTrue(FinalizedFeatureCache.isEmpty)
    } finally {
      Exit.resetExitProcedure()
    }
  }

  /**
   * Tests that the listener initialization fails when invalid wait time (<= 0) is provided as input.
   */
  @Test
  def testInitFailureDueToInvalidWaitTime(): Unit = {
    val listener = new FinalizedFeatureChangeListener(zkClient)
    assertThrows(classOf[IllegalArgumentException], () => listener.initOrThrow(0))
    assertThrows(classOf[IllegalArgumentException], () => listener.initOrThrow(-1))
  }

  /**
   * Tests that after successful initialization, the listener fails when it picks up a feature
   * incompatibility from ZK.
   */
  @Test
  def testNotificationFailureDueToFeatureIncompatibility(): Unit = {
    createSupportedFeatures
    val initialFinalizedFeatures = createFinalizedFeatures()
    val listener = createListener(Some(initialFinalizedFeatures))

    val exitLatch = new CountDownLatch(1)
    Exit.setExitProcedure((_, _) => exitLatch.countDown())
    val incompatibleFinalizedFeaturesMap = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 5))
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
        FinalizedFeatureCache.get.get.equals(initialFinalizedFeatures)
      }, "Timed out waiting for listener death and FinalizedFeatureCache to be updated")
    } finally {
      Exit.resetExitProcedure()
    }
  }
}
