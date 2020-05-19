package kafka.server

import org.apache.kafka.common.feature.{Features, FinalizedVersionRange, SupportedVersionRange}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._

class SupportedFeaturesTest {
  @Before
  def setUp(): Unit = {
    SupportedFeatures.clear()
  }

  @Test
  def testEmpty(): Unit = {
    assertTrue(SupportedFeatures.get.empty)
  }

  @Test
  def testIncompatibleFeatures(): Unit = {
    val supportedFeatures = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4),
      "feature_2" -> new SupportedVersionRange(1, 3))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 3),
      "feature_2" -> new FinalizedVersionRange(1, 4),
      "feature_3" -> new FinalizedVersionRange(3, 4))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    val incompatibleFeatures = SupportedFeatures.incompatibleFeatures(finalizedFeatures)
    assertEquals(Set("feature_2", "feature_3"), incompatibleFeatures)
  }
}
