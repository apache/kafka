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
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._

class SupportedFeaturesTest {
  @Before
  def setUp(): Unit = {
    SupportedFeatures.clear()
  }

  @Test
  def testEmpty(): Unit = {
    assertTrue(SupportedFeatures.get.empty)
  }

  @Test
  def testIncompatibleFeatures(): Unit = {
    val supportedFeatures = Map[String, SupportedVersionRange](
      "feature_1" -> new SupportedVersionRange(1, 4),
      "feature_2" -> new SupportedVersionRange(1, 3))
    SupportedFeatures.update(Features.supportedFeatures(supportedFeatures.asJava))

    val compatibleFeatures = Map[String, FinalizedVersionRange](
      "feature_1" -> new FinalizedVersionRange(2, 3))
    val inCompatibleFeatures = Map[String, FinalizedVersionRange](
      "feature_2" -> new FinalizedVersionRange(1, 4),
      "feature_3" -> new FinalizedVersionRange(3, 4))
    val features = compatibleFeatures++inCompatibleFeatures
    val finalizedFeatures = Features.finalizedFeatures(features.asJava)

    assertEquals(
      Features.finalizedFeatures(inCompatibleFeatures.asJava),
      SupportedFeatures.incompatibleFeatures(finalizedFeatures))
  }
}
