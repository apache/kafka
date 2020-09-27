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
 * A class that encapsulates the latest features supported by the Broker and also provides APIs to
 * check for incompatibilities between the features supported by the Broker and finalized features.
 * The class also enables feature version level deprecation, as explained below. This class is
 * immutable in production. It provides few APIs to mutate state only for the purpose of testing.
 *
 * Feature version level deprecation:
 * ==================================
 *
 * Deprecation of certain version levels of a feature is a process to stop supporting the
 * functionality offered by the feature at a those version levels, across the entire Kafka cluster.
 * Feature version deprecation is a simple 2-step process explained below. In each step below, an
 * example is provided to help understand the process better:
 *
 * STEP 1:
 * In the first step, a major Kafka release is made with a Broker code change (explained later
 * below) that establishes the intent to deprecate certain versions of one or more features
 * cluster-wide. When this new Kafka release is deployed to the cluster, the feature versioning
 * system (via the controller) will automatically persist the new minVersionLevel for the feature in
 * Zk to propagate the deprecation of certain versions. After this happens, any external client that
 * queries the Broker to learn the feature versions will at some point start to see the new value
 * for the finalized minVersionLevel for the feature. This makes the version deprecation permanent.
 *
 * Here is how the above code change needs to be done:
 * In order to deprecate feature version levels, in the supportedFeatures map you need to supply a
 * specific firstActiveVersion value that's higher than the minVersion for the feature. The
 * value for firstActiveVersion should be 1 beyond the highest version that you intend to deprecate
 * for that feature. When features are finalized via the ApiKeys.UPDATE_FEATURES api, the feature
 * version levels in the closed range: [minVersion, firstActiveVersion - 1] are automatically
 * deprecated in ZK by the controller logic.
 * Example:
 * - Let us assume the existing finalized feature in ZK:
 *   {
 *      "feature_1" -> FinalizedVersionRange(minVersionLevel=1, maxVersionLevel=5)
 *   }
 *   Now, supposing you would like to deprecate feature version levels: [1, 2].
 *   Then, in the supportedFeatures map you should supply the following:
 *   supportedFeatures = {
 *     "feature1" -> SupportedVersionRange(minVersion=1, firstActiveVersion=3, maxVersion=5)
 *   }
 * - If you do NOT want to deprecate a version level for a feature, then in the supportedFeatures
 *   map you should supply the firstActiveVersion to be the same as the minVersion supplied for that
 *   feature.
 *   Example:
 *   supportedFeatures = {
 *     "feature1" -> SupportedVersionRange(minVersion=1, firstActiveVersion=1, maxVersion=5)
 *   }
 *   This indicates no intent to deprecate any version levels for the feature.
 *
 * STEP 2:
 * After the first step is over, you may (at some point) want to permanently remove the code/logic
 * for the functionality offered by the deprecated feature. This is the second step. Here a
 * subsequent major Kafka release is made with another Broker code change that removes the code for
 * the functionality offered by the deprecated feature versions. This would completely drop support
 * for the deprecated versions. Such a code change needs to be supplemented by supplying a
 * suitable higher minVersion value for the feature in the supportedFeatures map.
 * Example:
 * - In the example above in step 1, we showed how to deprecate version levels [1, 2] for
 *   "feature_1". Now let us assume the following finalized feature in ZK (after the deprecation
 *   has been carried out):
 *   {
 *     "feature_1" -> FinalizedVersionRange(minVersionLevel=3, maxVersionLevel=5)
 *   }
 *   Now, supposing you would like to permanently remove support for feature versions: [1, 2].
 *   Then, in the supportedFeatures map you should now supply the following:
 *   supportedFeatures = {
 *     "feature1" -> SupportedVersionRange(minVersion=3, firstActiveVersion=3, maxVersion=5)
 *   }
 */
class BrokerFeatures private (@volatile var supportedFeatures: Features[SupportedVersionRange]) {
  // For testing only.
  def setSupportedFeatures(newFeatures: Features[SupportedVersionRange]): Unit = {
    supportedFeatures = newFeatures
  }

  /**
   * Returns the default finalized features that a new Kafka cluster with IBP config >= KAFKA_2_7_IV0
   * needs to be bootstrapped with.
   */
  def defaultFinalizedFeatures: Features[FinalizedVersionRange] = {
    Features.finalizedFeatures(
      supportedFeatures.features.asScala.map {
        case(name, versionRange) => (
          name, new FinalizedVersionRange(versionRange.firstActiveVersion, versionRange.max))
      }.asJava)
  }

  /**
   * Returns the set of feature names found to be incompatible.
   * A feature incompatibility is a version mismatch between the latest feature supported by the
   * Broker, and a provided finalized feature. This can happen because a provided finalized
   * feature:
   *  1) Does not exist in the Broker (i.e. it is unknown to the Broker).
   *           [OR]
   *  2) Exists but the FinalizedVersionRange does not match with the SupportedVersionRange
   *     of the supported feature.
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
    new BrokerFeatures(emptySupportedFeatures)
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
    !incompatibleFeatures(supportedFeatures, finalizedFeatures, logIncompatibilities = false).empty
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
      warn("Feature incompatibilities seen: " +
           incompatibleFeaturesInfo.map { case(_, _, errorReason) => errorReason }.mkString(", "))
    }
    Features.finalizedFeatures(
      incompatibleFeaturesInfo.map { case(feature, versionLevels, _) => (feature, versionLevels) }.toMap.asJava)
  }
}
