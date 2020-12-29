/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.zk

import java.nio.charset.StandardCharsets

import org.apache.kafka.common.feature.{Features, FinalizedVersionRange}
import org.apache.kafka.common.feature.Features._
import org.junit.Assert.{assertEquals, assertThrows}
import org.junit.Test

import scala.jdk.CollectionConverters._

class FeatureZNodeTest {

  @Test
  def testEncodeDecode(): Unit = {
    val featureZNode = FeatureZNode(
      FeatureZNodeStatus.Enabled,
      Features.finalizedFeatures(
        Map[String, FinalizedVersionRange](
          "feature1" -> new FinalizedVersionRange(1, 2),
          "feature2" -> new FinalizedVersionRange(2, 4)).asJava))
    val decoded = FeatureZNode.decode(FeatureZNode.encode(featureZNode))
    assertEquals(featureZNode, decoded)
  }

  @Test
  def testDecodeSuccess(): Unit = {
    val featureZNodeStrTemplate = """{
      "version":1,
      "status":1,
      "features":%s
    }"""

    val validFeatures = """{"feature1": {"min_version_level": 1, "max_version_level": 2}, "feature2": {"min_version_level": 2, "max_version_level": 4}}"""
    val node1 = FeatureZNode.decode(featureZNodeStrTemplate.format(validFeatures).getBytes(StandardCharsets.UTF_8))
    assertEquals(FeatureZNodeStatus.Enabled, node1.status)
    assertEquals(
      Features.finalizedFeatures(
        Map[String, FinalizedVersionRange](
          "feature1" -> new FinalizedVersionRange(1, 2),
          "feature2" -> new FinalizedVersionRange(2, 4)).asJava), node1.features)

    val emptyFeatures = "{}"
    val node2 = FeatureZNode.decode(featureZNodeStrTemplate.format(emptyFeatures).getBytes(StandardCharsets.UTF_8))
    assertEquals(FeatureZNodeStatus.Enabled, node2.status)
    assertEquals(emptyFinalizedFeatures, node2.features)
  }

  @Test
  def testDecodeFailOnInvalidVersionAndStatus(): Unit = {
    val featureZNodeStrTemplate =
      """{
      "version":%d,
      "status":%d,
      "features":{"feature1": {"min_version_level": 1, "max_version_level": 2}, "feature2": {"min_version_level": 2, "max_version_level": 4}}
    }"""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(FeatureZNode.V1 - 1, 1).getBytes(StandardCharsets.UTF_8)))
    val invalidStatus = FeatureZNodeStatus.Enabled.id + 1
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(FeatureZNode.CurrentVersion, invalidStatus).getBytes(StandardCharsets.UTF_8)))
  }

  @Test
  def testDecodeFailOnInvalidFeatures(): Unit = {
    val featureZNodeStrTemplate =
      """{
      "version":1,
      "status":1%s
    }"""

    val missingFeatures = ""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(missingFeatures).getBytes(StandardCharsets.UTF_8)))

    val malformedFeatures = ""","features":{"feature1": {"min_version_level": 1, "max_version_level": 2}, "partial"}"""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(malformedFeatures).getBytes(StandardCharsets.UTF_8)))

    val invalidFeaturesMinVersionLevel = ""","features":{"feature1": {"min_version_level": 0, "max_version_level": 2}}"""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(invalidFeaturesMinVersionLevel).getBytes(StandardCharsets.UTF_8)))

    val invalidFeaturesMaxVersionLevel = ""","features":{"feature1": {"min_version_level": 2, "max_version_level": 1}}"""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(invalidFeaturesMaxVersionLevel).getBytes(StandardCharsets.UTF_8)))

    val invalidFeaturesMissingMinVersionLevel = ""","features":{"feature1": {"max_version_level": 1}}"""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(invalidFeaturesMissingMinVersionLevel).getBytes(StandardCharsets.UTF_8)))
  }
}
