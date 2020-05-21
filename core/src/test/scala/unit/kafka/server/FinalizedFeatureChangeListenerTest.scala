package kafka.server

import kafka.zk.{FeatureZNode, FeatureZNodeStatus, ZkVersion, ZooKeeperTestHarness}
import kafka.utils.{Exit, TestUtils}
import org.apache.kafka.common.feature.{Features, FinalizedVersionRange, SupportedVersionRange}
import org.apache.kafka.common.internals.FatalExitError
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
    assertTrue(FinalizedFeatureCache.empty)
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
      FinalizedFeatureCache.empty
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
      FinalizedFeatureCache.empty
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

    Exit.setExitProcedure((status, _) => throw new FatalExitError(status))
    try {
      val listener = new FinalizedFeatureChangeListener(zkClient)
      assertFalse(listener.isListenerInitiated)
      assertTrue(FinalizedFeatureCache.empty)
      assertThrows(classOf[TimeoutException], () => listener.initOrThrow(5000))
      assertFalse(listener.isListenerInitiated)
      assertTrue(listener.isListenerDead)
      assertTrue(FinalizedFeatureCache.empty)
    } finally {
      Exit.resetExitProcedure()
    }
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

    Exit.setExitProcedure((status, _) => throw new FatalExitError(status))
    val incompatibleFinalizedFeaturesMap = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 5))
    val incompatibleFinalizedFeatures = Features.finalizedFeatures(incompatibleFinalizedFeaturesMap.asJava)
    zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, incompatibleFinalizedFeatures))
    val (mayBeFeatureZNodeIncompatibleBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeIncompatibleBytes.isEmpty)

    try {
      TestUtils.waitUntilTrue(() => {
        !listener.isListenerInitiated && listener.isListenerDead && FinalizedFeatureCache.get.get.equals(initialFinalizedFeatures)
      }, "Timed out waiting for listener death and FinalizedFeatureCache to be updated")
    } finally {
      Exit.resetExitProcedure()
    }
  }
}
