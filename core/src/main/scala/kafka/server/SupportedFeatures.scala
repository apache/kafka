package kafka.server

import kafka.utils.Logging
import org.apache.kafka.common.feature.{Features, VersionRange, VersionLevelRange}
import org.apache.kafka.common.feature.Features._

import scala.jdk.CollectionConverters._

/**
 * A common object used in the Broker to define the latest features supported by the Broker.
 * Also provides API to check for incompatibilities between the latest features supported by the
 * Broker and cluster-wide finalized features.
 */
object SupportedFeatures extends Logging {
  /**
   * This is the latest features supported by the Broker.
   * This is currently empty, but in the future as we define supported features, this map should be
   * populated.
   */
  @volatile private var supportedFeatures = emptySupportedFeatures

  /**
   * Returns the latest features supported by the Broker.
   */
  def get: Features[VersionRange] = {
    supportedFeatures
  }

  // Should be used only for testing.
  def update(newFeatures: Features[VersionRange]): Unit = {
    supportedFeatures = newFeatures
  }

  // Should be used only for testing.
  def clear(): Unit = {
    supportedFeatures = emptySupportedFeatures
  }

  /**
   * Returns the set of feature names found to be incompatible between the latest features supported
   * by the Broker, and the provided cluster-wide finalized features.
   *
   * @param finalized   The finalized features against which incompatibilities need to be checked for.
   *
   * @return            The set of incompatible feature names. If the returned set is empty, it
   *                    means there were no feature incompatibilities found.
   */
  def incompatibleFeatures(finalized: Features[VersionLevelRange]): Set[String] = {
    val supported = get

    val incompatibilities = finalized.all.asScala.collect {
      case (feature, versionLevels) => {
        val supportedVersions = supported.get(feature);
        if (supportedVersions == null) {
          (feature, "{feature=%s, reason='Unsupported feature'}".format(feature))
        } else if (!versionLevels.isCompatibleWith(supportedVersions)) {
          (feature, "{feature=%s, reason='Finalized %s is incompatible with supported %s'}".format(
            feature, versionLevels, supportedVersions))
        } else {
          (feature, null)
        }
      }
    }.filter(entry => entry._2 != null)

    if (incompatibilities.nonEmpty) {
      warn("Feature incompatibilities seen: " + incompatibilities.values.toSet)
    }
    incompatibilities.keys.toSet
  }
}
