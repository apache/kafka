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

import org.apache.kafka.common.feature.{Features, FinalizedVersionRange, SupportedVersionRange}
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
    val supportedFeatures = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(1, 4))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 10)
    assertTrue(FinalizedFeatureCache.get.isDefined)
    assertEquals(finalizedFeatures, FinalizedFeatureCache.get.get.features)
    assertEquals(10, FinalizedFeatureCache.get.get.epoch)

    assertThrows(
      classOf[FeatureCacheUpdateException],
      () => FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 9))

    // Check that the failed updateOrThrow call did not make any mutations.
    assertTrue(FinalizedFeatureCache.get.isDefined)
    assertEquals(finalizedFeatures, FinalizedFeatureCache.get.get.features)
    assertEquals(10, FinalizedFeatureCache.get.get.epoch)
  }

  @Test
  def testUpdateOrThrowFailedDueToInvalidFeatures(): Unit = {
    val supportedFeatures = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 1))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(1, 2))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    assertThrows(
      classOf[FeatureCacheUpdateException],
      () => FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 12))

    // Check that the failed updateOrThrow call did not make any mutations.
    assertTrue(FinalizedFeatureCache.isEmpty)
  }

  @Test
  def testUpdateOrThrowSuccess(): Unit = {
    val supportedFeatures = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 3))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 12)
    assertTrue(FinalizedFeatureCache.get.isDefined)
    assertEquals(finalizedFeatures,  FinalizedFeatureCache.get.get.features)
    assertEquals(12, FinalizedFeatureCache.get.get.epoch)
  }

  @Test
  def testClear(): Unit = {
    val supportedFeatures = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 3))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    FinalizedFeatureCache.updateOrThrow(finalizedFeatures, 12)
    assertTrue(FinalizedFeatureCache.get.isDefined)
    assertEquals(finalizedFeatures, FinalizedFeatureCache.get.get.features)
    assertEquals(12, FinalizedFeatureCache.get.get.epoch)

    FinalizedFeatureCache.clear()
    assertTrue(FinalizedFeatureCache.isEmpty)
  }
}
