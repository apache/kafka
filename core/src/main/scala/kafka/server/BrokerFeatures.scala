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

import kafka.utils.Logging
import org.apache.kafka.common.feature.{Features, FinalizedVersionRange, SupportedVersionRange}
import org.apache.kafka.common.feature.Features._

import scala.jdk.CollectionConverters._

/**
 * A class that encapsulates the following:
 *
 * 1. The latest features supported by the Broker.
 *
 * 2. The default minimum version levels for specific features. This map enables feature
 *    version level deprecation. This is how it works: in order to deprecate feature version levels,
 *    in this map the default minimum version level of a feature can be set to a new value that's
 *    higher than 1 (let's call this new_min_version_level). In doing so, the feature version levels
 *    in the closed range: [1, latest_min_version_level - 1] get deprecated by the controller logic
 *    that applies this map to persistent finalized feature state in ZK (this mutation happens
 *    during controller election and during finalized feature updates via the
 *    ApiKeys.UPDATE_FINALIZED_FEATURES api). This will automatically mean clients have to stop
 *    using the finalized min version levels that have been deprecated.
 *
 * This class also provides APIs to check for incompatibilities between the features supported by
 * the Broker and finalized features. The class is generally immutable. It provides few APIs to
 * mutate state only for the purpose of testing.
 */
class BrokerFeatures private (@volatile var supportedFeatures: Features[SupportedVersionRange],
                              @volatile var defaultFeatureMinVersionLevels: Map[String, Short]) {
  require(BrokerFeatures.areFeatureMinVersionLevelsCompatible(
    supportedFeatures, defaultFeatureMinVersionLevels))

  // For testing only.
  def setSupportedFeatures(newFeatures: Features[SupportedVersionRange]): Unit = {
    require(
      BrokerFeatures.areFeatureMinVersionLevelsCompatible(newFeatures, defaultFeatureMinVersionLevels))
    supportedFeatures = newFeatures
  }

  /**
   * Returns the default minimum version level for a specific feature.
   *
   * @param feature   the name of the feature
   *
   * @return          the default minimum version level for the feature if its defined.
   *                  otherwise, returns 1.
   */
  def defaultMinVersionLevel(feature: String): Short = {
    defaultFeatureMinVersionLevels.getOrElse(feature, 1)
  }

  // For testing only.
  def setDefaultMinVersionLevels(newMinVersionLevels: Map[String, Short]): Unit = {
    require(
      BrokerFeatures.areFeatureMinVersionLevelsCompatible(supportedFeatures, newMinVersionLevels))
    defaultFeatureMinVersionLevels = newMinVersionLevels
  }

  /**
   * Returns the default finalized features that a new Kafka cluster with IBP config >= KAFKA_2_7_IV0
   * needs to be bootstrapped with.
   */
  def getDefaultFinalizedFeatures: Features[FinalizedVersionRange] = {
    Features.finalizedFeatures(
      supportedFeatures.features.asScala.map {
        case(name, versionRange) => (
          name, new FinalizedVersionRange(defaultMinVersionLevel(name), versionRange.max))
      }.asJava)
  }

  /**
   * Returns the set of feature names found to be 'incompatible'.
   * A feature incompatibility is a version mismatch between the latest feature supported by the
   * Broker, and the provided finalized feature. This can happen because a provided finalized
   * feature:
   *  1) Does not exist in the Broker (i.e. it is unknown to the Broker).
   *           [OR]
   *  2) Exists but the FinalizedVersionRange does not match with the
   *     supported feature's SupportedVersionRange.
   *
   * @param finalized   The finalized features against which incompatibilities need to be checked for.
   *
   * @return            The subset of input features which are incompatible. If the returned object
   *                    is empty, it means there were no feature incompatibilities found.
   */
  def incompatibleFeatures(finalized: Features[FinalizedVersionRange]): Features[FinalizedVersionRange] = {
    BrokerFeatures.incompatibleFeatures(supportedFeatures, finalized, logIncompatibilities = true)
  }
}

object BrokerFeatures extends Logging {

  def createDefault(): BrokerFeatures = {
    // The arguments are currently empty, but, in the future as we define features we should
    // populate the required values here.
    new BrokerFeatures(emptySupportedFeatures, Map[String, Short]())
  }

  /**
   * Returns true if any of the provided finalized features are incompatible with the provided
   * supported features.
   *
   * @param supportedFeatures   The supported features to be compared
   * @param finalizedFeatures   The finalized features to be compared
   *
   * @return                    - True if there are any feature incompatibilities found.
   *                            - False otherwise.
   */
  def hasIncompatibleFeatures(supportedFeatures: Features[SupportedVersionRange],
                              finalizedFeatures: Features[FinalizedVersionRange]): Boolean = {
    !incompatibleFeatures(supportedFeatures, finalizedFeatures, false).empty
  }

  private def incompatibleFeatures(supportedFeatures: Features[SupportedVersionRange],
                                   finalizedFeatures: Features[FinalizedVersionRange],
                                   logIncompatibilities: Boolean): Features[FinalizedVersionRange] = {
    val incompatibleFeaturesInfo = finalizedFeatures.features.asScala.map {
      case (feature, versionLevels) =>
        val supportedVersions = supportedFeatures.get(feature)
        if (supportedVersions == null) {
          (feature, versionLevels, "{feature=%s, reason='Unsupported feature'}".format(feature))
        } else if (versionLevels.isIncompatibleWith(supportedVersions)) {
          (feature, versionLevels, "{feature=%s, reason='%s is incompatible with %s'}".format(
            feature, versionLevels, supportedVersions))
        } else {
          (feature, versionLevels, null)
        }
    }.filter{ case(_, _, errorReason) => errorReason != null}.toList

    if (logIncompatibilities && incompatibleFeaturesInfo.nonEmpty) {
      warn(
        "Feature incompatibilities seen: " + incompatibleFeaturesInfo.map {
          case(_, _, errorReason) => errorReason })
    }
    Features.finalizedFeatures(incompatibleFeaturesInfo.map {
      case(feature, versionLevels, _) => (feature, versionLevels) }.toMap.asJava)
  }

  /**
   * A check that ensures each feature defined with min version level is a supported feature, and
   * the min version level value is valid (i.e. it is compatible with the supported version range).
   *
   * @param supportedFeatures         the supported features
   * @param featureMinVersionLevels   the feature minimum version levels
   *
   * @return                          - true, if the above described check passes.
   *                                  - false, otherwise.
   */
  private def areFeatureMinVersionLevelsCompatible(
    supportedFeatures: Features[SupportedVersionRange],
    featureMinVersionLevels: Map[String, Short]
  ): Boolean = {
    featureMinVersionLevels.forall {
      case(featureName, minVersionLevel) =>
        val supportedFeature = supportedFeatures.get(featureName)
        (supportedFeature != null) &&
          new FinalizedVersionRange(minVersionLevel, supportedFeature.max())
            .isCompatibleWith(supportedFeature)
    }
  }
}
