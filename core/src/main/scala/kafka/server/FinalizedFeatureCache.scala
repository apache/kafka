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
import org.apache.kafka.common.feature.{Features, FinalizedVersionRange}

// Raised whenever there was an error in updating the FinalizedFeatureCache with features.
class FeatureCacheUpdateException(message: String) extends RuntimeException(message) {
}

// Helper class that represents finalized features along with an epoch value.
case class FinalizedFeaturesAndEpoch(features: Features[FinalizedVersionRange], epoch: Int) {
  override def toString(): String = {
    "FinalizedFeaturesAndEpoch(features=%s, epoch=%s)".format(features, epoch)
  }
}

/**
 * A common mutable cache containing the latest finalized features and epoch. By default the contents of
 * the cache are empty. This cache needs to be populated at least once for its contents to become
 * non-empty. Currently the main reader of this cache is the read path that serves an ApiVersionsRequest,
 * returning the features information in the response.
 *
 * @see FinalizedFeatureChangeListener
 */
object FinalizedFeatureCache extends Logging {
  @volatile private var featuresAndEpoch: Option[FinalizedFeaturesAndEpoch] = Option.empty

  /**
   * @return   the latest known FinalizedFeaturesAndEpoch or empty if not defined in the cache.
   */
  def get: Option[FinalizedFeaturesAndEpoch] = {
    featuresAndEpoch
  }

  def isEmpty: Boolean = {
    featuresAndEpoch.isEmpty
  }

  /**
   * Clears all existing finalized features and epoch from the cache.
   */
  def clear(): Unit = {
    featuresAndEpoch = Option.empty
    info("Cleared cache")
  }

  /**
   * Updates the cache to the latestFeatures, and updates the existing epoch to latestEpoch.
   * Expects that the latestEpoch should be always greater than the existing epoch (when the
   * existing epoch is defined).
   *
   * @param latestFeatures   the latest finalized features to be set in the cache
   * @param latestEpoch      the latest epoch value to be set in the cache
   *
   * @throws                 FeatureCacheUpdateException if the cache update operation fails
   *                         due to invalid parameters or incompatibilities with the broker's
   *                         supported features. In such a case, the existing cache contents are
   *                         not modified.
   */
  def updateOrThrow(latestFeatures: Features[FinalizedVersionRange], latestEpoch: Int): Unit = {
    val latest = FinalizedFeaturesAndEpoch(latestFeatures, latestEpoch)
    val oldFeatureAndEpoch = featuresAndEpoch.map(item => item.toString()).getOrElse("<empty>")
    if (featuresAndEpoch.isDefined && featuresAndEpoch.get.epoch > latest.epoch) {
      val errorMsg = ("FinalizedFeatureCache update failed due to invalid epoch in new finalized %s." +
        " The existing cache contents are %s").format(latest, oldFeatureAndEpoch)
      throw new FeatureCacheUpdateException(errorMsg)
    } else {
      val incompatibleFeatures = SupportedFeatures.incompatibleFeatures(latest.features)
      if (!incompatibleFeatures.empty) {
        val errorMsg = ("FinalizedFeatureCache update failed since feature compatibility" +
          " checks failed! Supported %s has incompatibilities with the latest %s."
          ).format(SupportedFeatures.get, latest)
        throw new FeatureCacheUpdateException(errorMsg)
      } else {
        val logMsg = "Updated cache from existing finalized %s to latest finalized %s".format(
          oldFeatureAndEpoch, latest)
        featuresAndEpoch = Some(latest)
        info(logMsg)
      }
    }
  }
}
