package kafka.server

import org.apache.kafka.common.feature.{Features, VersionLevelRange, VersionRange}
import org.junit.Assert.{assertEquals, assertThrows, assertTrue}
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._

class FinalizedFeatureCacheTest {

  @Before
  def setUp(): Unit = {
    FinalizedFeatureCache.clear()
    SupportedFeatures.clear()
  }

  @Test
  def testEmpty(): Unit = {
    assertTrue(FinalizedFeatureCache.get.isEmpty)
  }

  @Test
  def testUpdateOrThrowFailedDueToInvalidEpoch(): Unit = {
    val supportedFeatures = Map[String, VersionRange](
      "feature_1" -> new VersionRange(1, 4))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(1, 4))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 10)
    assertEquals(finalizedFeatures, FinalizedFeatureCache.get.get.features)
    assertEquals(10, FinalizedFeatureCache.get.get.epoch)

    assertThrows(
      classOf[FeatureCacheUpdateException],
      () => FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 9))

    // Check that the failed updateOrThrow call did not make any mutations.
    assertEquals(finalizedFeatures, FinalizedFeatureCache.get.get.features)
    assertEquals(10, FinalizedFeatureCache.get.get.epoch)
  }

  @Test
  def testUpdateOrThrowFailedDueToInvalidFeatures(): Unit = {
    val supportedFeatures = Map[String, VersionRange](
      "feature_1" -> new VersionRange(1, 1))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(1, 2))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    assertThrows(
      classOf[FeatureCacheUpdateException],
      () => FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 12))

    // Check that the failed updateOrThrow call did not make any mutations.
    assertTrue(FinalizedFeatureCache.empty)
  }

  @Test
  def testUpdateOrThrowSuccess(): Unit = {
    val supportedFeatures = Map[String, VersionRange](
      "feature_1" -> new VersionRange(1, 4))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(2, 3))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 12)
    assertEquals(finalizedFeatures,  FinalizedFeatureCache.get.get.features)
    assertEquals(12, FinalizedFeatureCache.get.get.epoch)
  }

  @Test
  def testClear(): Unit = {
    val supportedFeatures = Map[String, VersionRange](
      "feature_1" -> new VersionRange(1, 4))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, VersionLevelRange](
      "feature_1" -> new VersionLevelRange(2, 3))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 12)
    assertEquals(finalizedFeatures, FinalizedFeatureCache.get.get.features)
    assertEquals(12, FinalizedFeatureCache.get.get.epoch)

    FinalizedFeatureCache.clear()
    assertTrue(FinalizedFeatureCache.empty)
  }
}
