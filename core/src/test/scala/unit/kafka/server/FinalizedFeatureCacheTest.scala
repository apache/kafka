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
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class FinalizedFeatureCacheTest {

  @Test
  def testEmpty(): Unit = {
    assertTrue(new FinalizedFeatureCache(BrokerFeatures.createDefault()).get.isEmpty)
  }

  @Test
  def testUpdateOrThrowFailedDueToInvalidEpoch(): Unit = {
    val supportedFeatures = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4))
    val brokerFeatures = BrokerFeatures.createDefault()
    brokerFeatures.setSupportedFeatures(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(1, 4))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    val cache = new FinalizedFeatureCache(brokerFeatures)
    cache.updateOrThrow(finalizedFeatures, 10)
    assertTrue(cache.get.isDefined)
    assertEquals(finalizedFeatures, cache.get.get.features)
    assertEquals(10, cache.get.get.epoch)

    assertThrows(classOf[FeatureCacheUpdateException], () => cache.updateOrThrow(finalizedFeatures, 9))

    // Check that the failed updateOrThrow call did not make any mutations.
    assertTrue(cache.get.isDefined)
    assertEquals(finalizedFeatures, cache.get.get.features)
    assertEquals(10, cache.get.get.epoch)
  }

  @Test
  def testUpdateOrThrowFailedDueToInvalidFeatures(): Unit = {
    val supportedFeatures =
      Map[String, SupportedVersionRange]("feature_1" -> new SupportedVersionRange(1, 1))
    val brokerFeatures = BrokerFeatures.createDefault()
    brokerFeatures.setSupportedFeatures(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(1, 2))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    val cache = new FinalizedFeatureCache(brokerFeatures)
    assertThrows(classOf[FeatureCacheUpdateException], () => cache.updateOrThrow(finalizedFeatures, 12))

    // Check that the failed updateOrThrow call did not make any mutations.
    assertTrue(cache.isEmpty)
  }

  @Test
  def testUpdateOrThrowSuccess(): Unit = {
    val supportedFeatures =
      Map[String, SupportedVersionRange]("feature_1" -> new SupportedVersionRange(1, 4))
    val brokerFeatures = BrokerFeatures.createDefault()
    brokerFeatures.setSupportedFeatures(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 3))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    val cache = new FinalizedFeatureCache(brokerFeatures)
    cache.updateOrThrow(finalizedFeatures, 12)
    assertTrue(cache.get.isDefined)
    assertEquals(finalizedFeatures,  cache.get.get.features)
    assertEquals(12, cache.get.get.epoch)
  }

  @Test
  def testClear(): Unit = {
    val supportedFeatures =
      Map[String, SupportedVersionRange]("feature_1" -> new SupportedVersionRange(1, 4))
    val brokerFeatures = BrokerFeatures.createDefault()
    brokerFeatures.setSupportedFeatures(Features.supportedFeatures(supportedFeatures.asJava))

    val features = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 3))
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    val cache = new FinalizedFeatureCache(brokerFeatures)
    cache.updateOrThrow(finalizedFeatures, 12)
    assertTrue(cache.get.isDefined)
    assertEquals(finalizedFeatures, cache.get.get.features)
    assertEquals(12, cache.get.get.epoch)

    cache.clear()
    assertTrue(cache.isEmpty)
  }
}
