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

import scala.concurrent.TimeoutException
import scala.math.max

// Raised whenever there was an error in updating the FinalizedFeatureCache with features.
class FeatureCacheUpdateException(message: String) extends RuntimeException(message) {
}

// Helper class that represents finalized features along with an epoch value.
case class FinalizedFeaturesAndEpoch(features: Features[FinalizedVersionRange], epoch: Long) {
  override def toString(): String = {
    s"FinalizedFeaturesAndEpoch(features=$features, epoch=$epoch)"
  }
}

/**
 * A common mutable cache containing the latest finalized features and epoch. By default the contents of
 * the cache are empty. This cache needs to be populated at least once for its contents to become
 * non-empty. Currently the main reader of this cache is the read path that serves an ApiVersionsRequest,
 * returning the features information in the response. This cache is typically updated asynchronously
 * whenever the finalized features and epoch values are modified in ZK by the KafkaController.
 * This cache is thread-safe for reads and writes.
 *
 * @see FinalizedFeatureChangeListener
 */
class FinalizedFeatureCache(private val brokerFeatures: BrokerFeatures) extends Logging {
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
   * Waits no more than timeoutMs for the cache's epoch to reach an epoch >= minExpectedEpoch.
   *
   * @param minExpectedEpoch   the minimum expected epoch to be reached by the cache
   *                           (should be >= 0)
   * @param timeoutMs          the timeout (in milli seconds)
   *
   * @throws                   TimeoutException if the cache's epoch has not reached at least
   *                           minExpectedEpoch within timeoutMs.
   */
  def waitUntilEpochOrThrow(minExpectedEpoch: Long, timeoutMs: Long): Unit = {
    if(minExpectedEpoch < 0L) {
      throw new IllegalArgumentException(
        s"Expected minExpectedEpoch >= 0, but $minExpectedEpoch was provided.")
    }
    waitUntilConditionOrThrow(
      () => featuresAndEpoch.isDefined && featuresAndEpoch.get.epoch >= minExpectedEpoch,
      timeoutMs)
  }

  /**
   * Clears all existing finalized features and epoch from the cache.
   */
  def clear(): Unit = {
    synchronized {
      featuresAndEpoch = Option.empty
      notifyAll()
    }
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
  def updateOrThrow(latestFeatures: Features[FinalizedVersionRange], latestEpoch: Long): Unit = {
    val latest = FinalizedFeaturesAndEpoch(latestFeatures, latestEpoch)
    val existing = featuresAndEpoch.map(item => item.toString()).getOrElse("<empty>")
    if (featuresAndEpoch.isDefined && featuresAndEpoch.get.epoch > latest.epoch) {
      val errorMsg = s"FinalizedFeatureCache update failed due to invalid epoch in new $latest." +
        s" The existing cache contents are $existing."
      throw new FeatureCacheUpdateException(errorMsg)
    } else {
      val incompatibleFeatures = brokerFeatures.incompatibleFeatures(latest.features)
      if (!incompatibleFeatures.empty) {
        val errorMsg = "FinalizedFeatureCache update failed since feature compatibility" +
          s" checks failed! Supported ${brokerFeatures.supportedFeatures} has incompatibilities" +
          s" with the latest $latest."
        throw new FeatureCacheUpdateException(errorMsg)
      } else {
        val logMsg = s"Updated cache from existing $existing to latest $latest."
        synchronized {
          featuresAndEpoch = Some(latest)
          notifyAll()
        }
        info(logMsg)
      }
    }
  }

  /**
   * Causes the current thread to wait no more than timeoutMs for the specified condition to be met.
   * It is guaranteed that the provided condition will always be invoked only from within a
   * synchronized block.
   *
   * @param waitCondition   the condition to be waited upon:
   *                         - if the condition returns true, then, the wait will stop.
   *                         - if the condition returns false, it means the wait must continue until
   *                           timeout.
   *
   * @param timeoutMs       the timeout (in milli seconds)
   *
   * @throws                TimeoutException if the condition is not met within timeoutMs.
   */
  private def waitUntilConditionOrThrow(waitCondition: () => Boolean, timeoutMs: Long): Unit = {
    if(timeoutMs < 0L) {
      throw new IllegalArgumentException(s"Expected timeoutMs >= 0, but $timeoutMs was provided.")
    }
    val waitEndTimeNanos = System.nanoTime() + (timeoutMs * 1000000)
    synchronized {
      while (!waitCondition()) {
        val nowNanos = System.nanoTime()
        if (nowNanos > waitEndTimeNanos) {
          throw new TimeoutException(
            s"Timed out after waiting for ${timeoutMs}ms for required condition to be met." +
              s" Current epoch: ${featuresAndEpoch.map(fe => fe.epoch).getOrElse("<none>")}.")
        }
        val sleepTimeMs = max(1L, (waitEndTimeNanos - nowNanos) / 1000000)
        wait(sleepTimeMs)
      }
    }
  }
}
