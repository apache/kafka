package kafka.server

import kafka.zk.{FeatureZNode, FeatureZNodeStatus, ZkVersion, ZooKeeperTestHarness}
import kafka.utils.{Exit, TestUtils}
import org.apache.kafka.common.feature.{Features, VersionLevelRange, VersionRange}
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

  /**
   * Tests that the listener can be initialized, and that it can listen to ZK notifications
   * successfully from an "Enabled" FeatureZNode (the ZK data has no feature incompatibilities).
   */
  @Test
  def testInitSuccessAndNotificationSuccess(): Unit = {
    val supportedFeatures = Map[String, VersionRange](
      "feature_1" -> new VersionRange(1, 4),
      "feature_2" -> new VersionRange(1, 3))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val initialFinalizedFeaturesMap = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(2, 3))
    val initialFinalizedFeatures = Features.finalizedFeatures(initialFinalizedFeaturesMap.asJava)
    zkClient.createFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, initialFinalizedFeatures))
    val (mayBeFeatureZNodeBytes, initialVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(initialVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeBytes.isEmpty)

    val listener = new FinalizedFeatureChangeListener(zkClient)
    assertFalse(listener.isListenerInitiated)
    assertTrue(FinalizedFeatureCache.empty)
    listener.initOrThrow(15000)
    assertTrue(listener.isListenerInitiated)
    val mayBeNewCacheContent = FinalizedFeatureCache.get
    assertFalse(mayBeNewCacheContent.isEmpty)
    val newCacheContent = mayBeNewCacheContent.get
    assertEquals(initialFinalizedFeatures, newCacheContent.features)
    assertEquals(initialVersion, newCacheContent.epoch)

    val updatedFinalizedFeaturesMap = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(2, 4))
    val updatedFinalizedFeatures = Features.finalizedFeatures(updatedFinalizedFeaturesMap.asJava)
    zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, updatedFinalizedFeatures))
    val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
    assertTrue(updatedVersion > initialVersion)
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
    val supportedFeatures = Map[String, VersionRange](
      "feature_1" -> new VersionRange(1, 4),
      "feature_2" -> new VersionRange(1, 3))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val initialFinalizedFeaturesMap = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(2, 3))
    val initialFinalizedFeatures = Features.finalizedFeatures(initialFinalizedFeaturesMap.asJava)
    zkClient.createFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, initialFinalizedFeatures))
    val (mayBeFeatureZNodeBytes, initialVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(initialVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeBytes.isEmpty)

    val listener = new FinalizedFeatureChangeListener(zkClient)
    assertFalse(listener.isListenerInitiated)
    assertTrue(FinalizedFeatureCache.empty)
    listener.initOrThrow(15000)
    assertTrue(listener.isListenerInitiated)
    val mayBeNewCacheContent = FinalizedFeatureCache.get
    assertFalse(mayBeNewCacheContent.isEmpty)
    val newCacheContent = mayBeNewCacheContent.get
    assertEquals(initialFinalizedFeatures, newCacheContent.features)
    assertEquals(initialVersion, newCacheContent.epoch)

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
    val supportedFeatures = Map[String, VersionRange](
      "feature_1" -> new VersionRange(1, 4),
      "feature_2" -> new VersionRange(1, 3))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val initialFinalizedFeaturesMap = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(2, 3))
    val initialFinalizedFeatures = Features.finalizedFeatures(initialFinalizedFeaturesMap.asJava)
    zkClient.createFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, initialFinalizedFeatures))
    val (mayBeFeatureZNodeBytes, initialVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(initialVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeBytes.isEmpty)

    val listener = new FinalizedFeatureChangeListener(zkClient)
    assertFalse(listener.isListenerInitiated)
    assertTrue(FinalizedFeatureCache.empty)
    listener.initOrThrow(15000)
    assertTrue(listener.isListenerInitiated)
    val mayBeNewCacheContent = FinalizedFeatureCache.get
    assertFalse(mayBeNewCacheContent.isEmpty)
    val newCacheContent = mayBeNewCacheContent.get
    assertEquals(initialFinalizedFeatures, newCacheContent.features)
    assertEquals(initialVersion, newCacheContent.epoch)

    val updatedFinalizedFeaturesMap = Map[String, VersionLevelRange]()
    val updatedFinalizedFeatures = Features.finalizedFeatures(updatedFinalizedFeaturesMap.asJava)
    zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Disabled, updatedFinalizedFeatures))
    val (mayBeFeatureZNodeNewBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeNewBytes.isEmpty)
    assertTrue(updatedVersion > initialVersion)
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
    val supportedFeatures = Map[String, VersionRange](
      "feature_1" -> new VersionRange(1, 4),
      "feature_2" -> new VersionRange(1, 3))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val incompatibleFinalizedFeaturesMap = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(2, 5))
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
    val supportedFeatures = Map[String, VersionRange](
      "feature_1" -> new VersionRange(1, 4),
      "feature_2" -> new VersionRange(1, 3))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val initialFinalizedFeaturesMap = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(2, 3))
    val initialFinalizedFeatures = Features.finalizedFeatures(initialFinalizedFeaturesMap.asJava)
    zkClient.createFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, initialFinalizedFeatures))
    val (mayBeFeatureZNodeBytes, initialVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(initialVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeBytes.isEmpty)

    val listener = new FinalizedFeatureChangeListener(zkClient)
    assertFalse(listener.isListenerInitiated)
    assertTrue(FinalizedFeatureCache.empty)
    listener.initOrThrow(15000)
    assertTrue(listener.isListenerInitiated)
    val mayBeNewCacheContent = FinalizedFeatureCache.get
    assertFalse(mayBeNewCacheContent.isEmpty)
    val newCacheContent = mayBeNewCacheContent.get
    assertEquals(initialFinalizedFeatures, newCacheContent.features)
    assertEquals(initialVersion, newCacheContent.epoch)

    Exit.setExitProcedure((status, _) => throw new FatalExitError(status))
    val incompatibleFinalizedFeaturesMap = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(2, 5))
    val incompatibleFinalizedFeatures = Features.finalizedFeatures(incompatibleFinalizedFeaturesMap.asJava)
    zkClient.updateFeatureZNode(FeatureZNode(FeatureZNodeStatus.Enabled, incompatibleFinalizedFeatures))
    val (mayBeFeatureZNodeIncompatibleBytes, updatedVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(updatedVersion, ZkVersion.UnknownVersion)
    assertFalse(mayBeFeatureZNodeIncompatibleBytes.isEmpty)

    try {
      TestUtils.waitUntilTrue(() => {
        !listener.isListenerInitiated && listener.isListenerDead && (FinalizedFeatureCache.get.get.equals(newCacheContent))
      }, "Timed out waiting for listener death and FinalizedFeatureCache to be updated")
    } finally {
      Exit.resetExitProcedure()
    }
  }
}
