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
 * A common immutable object used in the Broker to define the latest features supported by the
 * Broker. Also provides API to check for incompatibilities between the latest features supported
 * by the Broker and cluster-wide finalized features.
 *
 * NOTE: the update() and clear() APIs of this class should be used only for testing purposes.
 */
object SupportedFeatures extends Logging {

  /**
   * This is the latest features supported by the Broker.
   * This is currently empty, but in the future as we define supported features, this map should be
   * populated.
   */
  @volatile private var supportedFeatures = emptySupportedFeatures

  /**
   * Returns a reference to the latest features supported by the Broker.
   */
  def get: Features[SupportedVersionRange] = {
    supportedFeatures
  }

  // For testing only.
  def update(newFeatures: Features[SupportedVersionRange]): Unit = {
    supportedFeatures = newFeatures
  }

  // For testing only.
  def clear(): Unit = {
    supportedFeatures = emptySupportedFeatures
  }

  /**
   * Returns the set of feature names found to be 'incompatible'.
   * A feature incompatibility is a version mismatch between the latest feature supported by the
   * Broker, and the provided finalized feature. This can happen because a provided finalized
   * feature:
   *  1) Does not exist in the Broker (i.e. it is unknown to the Broker).
   *           [OR]
   *  2) Exists but the FinalizedVersionRange does not match with the supported feature's SupportedVersionRange.
   *
   * @param finalized   The finalized features against which incompatibilities need to be checked for.
   *
   * @return            The subset of input features which are incompatible. If the returned object
   *                    is empty, it means there were no feature incompatibilities found.
   */
  def incompatibleFeatures(finalized: Features[FinalizedVersionRange]): Features[FinalizedVersionRange] = {
    val incompatibilities = finalized.features.asScala.map {
      case (feature, versionLevels) => {
        val supportedVersions = supportedFeatures.get(feature)
        if (supportedVersions == null) {
          (feature, versionLevels, "{feature=%s, reason='Unsupported feature'}".format(feature))
        } else if (versionLevels.isIncompatibleWith(supportedVersions)) {
          (feature, versionLevels, "{feature=%s, reason='%s is incompatible with %s'}".format(
            feature, versionLevels, supportedVersions))
        } else {
          (feature, versionLevels, null)
        }
      }
    }.filter{ case(_, _, errorReason) => errorReason != null}.toList

    if (incompatibilities.nonEmpty) {
      warn("Feature incompatibilities seen: " + incompatibilities.map{ case(_, _, errorReason) => errorReason })
    }
    Features.finalizedFeatures(incompatibilities.map{ case(feature, versionLevels, _) => (feature, versionLevels) }.toMap.asJava)
  }
}
