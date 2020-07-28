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
import org.junit.Test

import scala.jdk.CollectionConverters._

class BrokerFeaturesTest {

  @Test
  def testEmpty(): Unit = {
    assertTrue(BrokerFeatures.createDefault().supportedFeatures.empty)
  }

  @Test
  def testIncompatibleFeatures(): Unit = {
    val brokerFeatures = BrokerFeatures.createDefault()
    val supportedFeatures = Map[String, SupportedVersionRange](
      "test_feature_1" -> new SupportedVersionRange(1, 4),
      "test_feature_2" -> new SupportedVersionRange(1, 3))
    brokerFeatures.setSupportedFeatures(Features.supportedFeatures(supportedFeatures.asJava))

    val compatibleFeatures = Map[String, FinalizedVersionRange](
      "test_feature_1" -> new FinalizedVersionRange(2, 3))
    val inCompatibleFeatures = Map[String, FinalizedVersionRange](
      "test_feature_2" -> new FinalizedVersionRange(1, 4),
      "test_feature_3" -> new FinalizedVersionRange(3, 4))
    val features = compatibleFeatures++inCompatibleFeatures
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    assertEquals(
      Features.finalizedFeatures(inCompatibleFeatures.asJava),
      brokerFeatures.incompatibleFeatures(finalizedFeatures))
  }

  @Test
  def testFeatureVersionAssertions(): Unit = {
    val brokerFeatures = BrokerFeatures.createDefault()
    val supportedFeatures = Features.supportedFeatures(Map[String, SupportedVersionRange](
      "test_feature_1" -> new SupportedVersionRange(1, 4),
      "test_feature_2" -> new SupportedVersionRange(1, 3)).asJava)
    brokerFeatures.setSupportedFeatures(supportedFeatures)

    val defaultMinVersionLevelsWithNonExistingFeature = Map[String, Short](
      "test_feature_1" -> 2,
      "test_feature_2" -> 2,
      "test_feature_non_existing" -> 5)
    assertThrows(
      classOf[IllegalArgumentException],
      () => brokerFeatures.setDefaultMinVersionLevels(defaultMinVersionLevelsWithNonExistingFeature))

    val defaultMinVersionLevelsWithInvalidSmallValue = Map[String, Short](
      "test_feature_1" -> 2,
      "test_feature_2" -> (supportedFeatures.get("test_feature_2").min() - 1).asInstanceOf[Short])
    assertThrows(
      classOf[IllegalArgumentException],
      () => brokerFeatures.setDefaultMinVersionLevels(defaultMinVersionLevelsWithInvalidSmallValue))

    val defaultMinVersionLevelsWithInvalidLargeValue = Map[String, Short](
      "test_feature_1" -> 2,
      "test_feature_2" -> (supportedFeatures.get("test_feature_2").max() + 1).asInstanceOf[Short])
    assertThrows(
      classOf[IllegalArgumentException],
      () => brokerFeatures.setDefaultMinVersionLevels(defaultMinVersionLevelsWithInvalidLargeValue))
  }
}
