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

import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.server.common.{MetadataVersion, Features => ServerFeatures}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._

class BrokerFeaturesTest {

  @Test
  def testEmpty(): Unit = {
    assertTrue(BrokerFeatures.createEmpty().supportedFeatures.empty)
  }

  @Test
  def testIncompatibilitiesDueToAbsentFeature(): Unit = {
    val brokerFeatures = BrokerFeatures.createDefault(true)
    val supportedFeatures = Features.supportedFeatures(Map[String, SupportedVersionRange](
      "test_feature_1" -> new SupportedVersionRange(1, 4),
      "test_feature_2" -> new SupportedVersionRange(1, 3)).asJava)
    brokerFeatures.setSupportedFeatures(supportedFeatures)

    val compatibleFeatures = Map[String, Short]("test_feature_1" -> 4)
    val inCompatibleFeatures = Map[String, Short]("test_feature_3" -> 4)
    val features = compatibleFeatures++inCompatibleFeatures
    val finalizedFeatures = features

    assertEquals(inCompatibleFeatures,
      brokerFeatures.incompatibleFeatures(finalizedFeatures))
    assertTrue(BrokerFeatures.hasIncompatibleFeatures(supportedFeatures, finalizedFeatures))
  }

  @Test
  def testIncompatibilitiesDueToIncompatibleFeature(): Unit = {
    val brokerFeatures = BrokerFeatures.createDefault(true)
    val supportedFeatures = Features.supportedFeatures(Map[String, SupportedVersionRange](
      "test_feature_1" -> new SupportedVersionRange(1, 4),
      "test_feature_2" -> new SupportedVersionRange(1, 3)).asJava)
    brokerFeatures.setSupportedFeatures(supportedFeatures)

    val compatibleFeatures = Map[String, Short]("test_feature_1" -> 3)
    val inCompatibleFeatures = Map[String, Short]("test_feature_2" -> 4)
    val features = compatibleFeatures++inCompatibleFeatures
    val finalizedFeatures = features

    assertEquals(
      inCompatibleFeatures,
      brokerFeatures.incompatibleFeatures(finalizedFeatures))
    assertTrue(BrokerFeatures.hasIncompatibleFeatures(supportedFeatures, finalizedFeatures))
  }

  @Test
  def testCompatibleFeatures(): Unit = {
    val brokerFeatures = BrokerFeatures.createDefault(true)
    val supportedFeatures = Features.supportedFeatures(Map[String, SupportedVersionRange](
      "test_feature_1" -> new SupportedVersionRange(1, 4),
      "test_feature_2" -> new SupportedVersionRange(1, 3)).asJava)
    brokerFeatures.setSupportedFeatures(supportedFeatures)

    val compatibleFeatures = Map[String, Short](
      "test_feature_1" -> 3,
      "test_feature_2" -> 3)
    val finalizedFeatures = compatibleFeatures
    assertTrue(brokerFeatures.incompatibleFeatures(finalizedFeatures).isEmpty)
    assertFalse(BrokerFeatures.hasIncompatibleFeatures(supportedFeatures, finalizedFeatures))
  }

  @Test
  def testDefaultFinalizedFeatures(): Unit = {
    val brokerFeatures = BrokerFeatures.createDefault(true)
    val supportedFeatures = Features.supportedFeatures(Map[String, SupportedVersionRange](
      "test_feature_1" -> new SupportedVersionRange(1, 4),
      "test_feature_2" -> new SupportedVersionRange(1, 3),
      "test_feature_3" -> new SupportedVersionRange(3, 7)).asJava)
    brokerFeatures.setSupportedFeatures(supportedFeatures)

    val expectedFeatures = Map[String, Short](
      MetadataVersion.FEATURE_NAME -> MetadataVersion.latestTesting().featureLevel(),
      ServerFeatures.TRANSACTION_VERSION.featureName() -> ServerFeatures.TRANSACTION_VERSION.latestTesting(),
      ServerFeatures.GROUP_VERSION.featureName() -> ServerFeatures.GROUP_VERSION.latestTesting(),
      "kraft.version" -> 0,
      "test_feature_1" -> 4,
      "test_feature_2" -> 3,
      "test_feature_3" -> 7)
    assertEquals(expectedFeatures, brokerFeatures.defaultFinalizedFeatures)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def ensureDefaultSupportedFeaturesRangeMaxNotZero(unstableVersionsEnabled: Boolean): Unit = {
    val brokerFeatures = BrokerFeatures.createDefault(unstableVersionsEnabled)
    brokerFeatures.supportedFeatures.features().values().forEach { supportedVersionRange =>
      assertNotEquals(0, supportedVersionRange.max())
    }
  }
}
