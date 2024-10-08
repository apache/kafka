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

import kafka.server.metadata.{FeatureCacheUpdateException, ZkMetadataCache}
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.server.BrokerFeatures
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class FinalizedFeatureCacheTest {

  @Test
  def testEmpty(): Unit = {
    assertTrue(new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, BrokerFeatures.createDefault(true)).getFeatureOption.isEmpty)
  }

  def asJava(input: Map[String, Short]): java.util.Map[String, java.lang.Short] = {
    input.map(kv => kv._1 -> kv._2.asInstanceOf[java.lang.Short]).asJava
  }

  @Test
  def testUpdateOrThrowFailedDueToInvalidEpoch(): Unit = {
    val supportedFeatures = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4))
    val brokerFeatures = BrokerFeatures.createDefault(true, Features.supportedFeatures(supportedFeatures.asJava))

    val finalizedFeatures = Map[String, Short]("feature_1" -> 4)

    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    cache.updateFeaturesOrThrow(finalizedFeatures, 10)
    assertTrue(cache.getFeatureOption.isDefined)
    assertEquals(asJava(finalizedFeatures), cache.getFeatureOption.get.finalizedFeatures())
    assertEquals(10, cache.getFeatureOption.get.finalizedFeaturesEpoch())

    assertThrows(classOf[FeatureCacheUpdateException], () => cache.updateFeaturesOrThrow(finalizedFeatures, 9))

    // Check that the failed updateOrThrow call did not make any mutations.
    assertTrue(cache.getFeatureOption.isDefined)
    assertEquals(asJava(finalizedFeatures), cache.getFeatureOption.get.finalizedFeatures())
    assertEquals(10, cache.getFeatureOption.get.finalizedFeaturesEpoch())
  }

  @Test
  def testUpdateOrThrowFailedDueToInvalidFeatures(): Unit = {
    val supportedFeatures =
      Map[String, SupportedVersionRange]("feature_1" -> new SupportedVersionRange(1, 1))
    val brokerFeatures = BrokerFeatures.createDefault(true, Features.supportedFeatures(supportedFeatures.asJava))

    val finalizedFeatures = Map[String, Short]("feature_1" -> 2)

    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    assertThrows(classOf[FeatureCacheUpdateException], () => cache.updateFeaturesOrThrow(finalizedFeatures, 12))

    // Check that the failed updateOrThrow call did not make any mutations.
    assertTrue(cache.getFeatureOption.isEmpty)
  }

  @Test
  def testUpdateOrThrowSuccess(): Unit = {
    val supportedFeatures =
      Map[String, SupportedVersionRange]("feature_1" -> new SupportedVersionRange(1, 4))
    val brokerFeatures = BrokerFeatures.createDefault(true, Features.supportedFeatures(supportedFeatures.asJava))

    val finalizedFeatures = Map[String, Short]("feature_1" -> 3)

    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    cache.updateFeaturesOrThrow(finalizedFeatures, 12)
    assertTrue(cache.getFeatureOption.isDefined)
    assertEquals(asJava(finalizedFeatures),  cache.getFeatureOption.get.finalizedFeatures())
    assertEquals(12, cache.getFeatureOption.get.finalizedFeaturesEpoch())
  }

  @Test
  def testClear(): Unit = {
    val supportedFeatures =
      Map[String, SupportedVersionRange]("feature_1" -> new SupportedVersionRange(1, 4))
    val brokerFeatures = BrokerFeatures.createDefault(true, Features.supportedFeatures(supportedFeatures.asJava))

    val finalizedFeatures = Map[String, Short]("feature_1" -> 3)

    val cache = new ZkMetadataCache(1, MetadataVersion.IBP_2_8_IV1, brokerFeatures)
    cache.updateFeaturesOrThrow(finalizedFeatures, 12)
    assertTrue(cache.getFeatureOption.isDefined)
    assertEquals(asJava(finalizedFeatures), cache.getFeatureOption.get.finalizedFeatures())
    assertEquals(12, cache.getFeatureOption.get.finalizedFeaturesEpoch())

    cache.clearFeatures()
    assertTrue(cache.getFeatureOption.isEmpty)
  }
}
