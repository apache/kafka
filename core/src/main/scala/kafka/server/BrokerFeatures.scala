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
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.server.common.MetadataVersion

import java.util
import scala.jdk.CollectionConverters._

/**
 * A class that encapsulates the latest features supported by the Broker and also provides APIs to
 * check for incompatibilities between the features supported by the Broker and finalized features.
 * This class is immutable in production. It provides few APIs to mutate state only for the purpose
 * of testing.
 */
class BrokerFeatures private (@volatile var supportedFeatures: Features[SupportedVersionRange]) {
  // For testing only.
  def setSupportedFeatures(newFeatures: Features[SupportedVersionRange]): Unit = {
    val combined = new util.HashMap[String, SupportedVersionRange](supportedFeatures.features())
    combined.putAll(newFeatures.features())
    supportedFeatures = Features.supportedFeatures(combined)
  }

  /**
   * Returns the default finalized features that a new Kafka cluster with IBP config >= IBP_2_7_IV0
   * needs to be bootstrapped with.
   */
  def defaultFinalizedFeatures: Map[String, Short] = {
    supportedFeatures.features.asScala.map {
      case(name, versionRange) => (name, versionRange.max)
    }.toMap
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
  def incompatibleFeatures(finalized: Map[String, Short]): Map[String, Short] = {
    BrokerFeatures.incompatibleFeatures(supportedFeatures, finalized, logIncompatibilities = true)
  }
}

object BrokerFeatures extends Logging {

  def createDefault(): BrokerFeatures = {
    new BrokerFeatures(defaultSupportedFeatures())
  }

  def defaultSupportedFeatures(): Features[SupportedVersionRange] = {
    Features.supportedFeatures(
      java.util.Collections.singletonMap(MetadataVersion.FEATURE_NAME,
        new SupportedVersionRange(MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel(), MetadataVersion.latest().featureLevel())))
  }

  def createEmpty(): BrokerFeatures = {
    new BrokerFeatures(Features.emptySupportedFeatures())
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
                              finalizedFeatures: Map[String, Short]): Boolean = {
    incompatibleFeatures(supportedFeatures, finalizedFeatures, logIncompatibilities = false).nonEmpty
  }

  private def incompatibleFeatures(supportedFeatures: Features[SupportedVersionRange],
                                   finalizedFeatures: Map[String, Short],
                                   logIncompatibilities: Boolean): Map[String, Short] = {
    val incompatibleFeaturesInfo = finalizedFeatures.map {
      case (feature, versionLevels) =>
        val supportedVersions = supportedFeatures.get(feature)
        if (supportedVersions == null) {
          (feature, versionLevels, "{feature=%s, reason='Unsupported feature'}".format(feature))
        } else if (supportedVersions.isIncompatibleWith(versionLevels)) {
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
    incompatibleFeaturesInfo.map { case(feature, versionLevels, _) => (feature, versionLevels) }.toMap
  }
}
