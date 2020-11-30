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
import java.util

import kafka.internals.generated.FeatureZNodeData
import org.junit.Assert.{assertEquals, assertThrows}
import org.junit.Test


class FeatureZNodeTest {

  @Test
  def testEncodeDecode(): Unit = {
    val features = util.Arrays.asList(
      new FeatureZNodeData.Feature()
        .setFeatureName("feature1")
        .setVersionRange(new FeatureZNodeData.FinalizedVersionRange().setMinValue(1).setMaxValue(2)),
      new FeatureZNodeData.Feature()
        .setFeatureName("feature2")
        .setVersionRange(new FeatureZNodeData.FinalizedVersionRange().setMinValue(2).setMaxValue(4)),
    )
    val featureZNodeData = new FeatureZNodeData()
      .setStatus( FeatureZNodeStatus.Enabled.id)
      .setFeatures(features)
    val decoded = FeatureZNode.decode(FeatureZNode.encode(featureZNodeData))
    assertEquals(featureZNodeData, decoded)
  }

  @Test
  def testDecodeSuccess(): Unit = {
    val featureZNodeStrTemplate = """{
      "version":1,
      "status":1,
      "features":%s
    }"""

    val validFeatures =
      """[
          {"featureName":"feature1","versionRange":{"minValue":1,"maxValue":2}},
          {"featureName":"feature2","versionRange":{"minValue":2,"maxValue":4}}
         ]"""
    val node1 = FeatureZNode.decode(featureZNodeStrTemplate.format(validFeatures).getBytes(StandardCharsets.UTF_8))
    assertEquals(FeatureZNodeStatus.Enabled.id, node1.status)
    assertEquals(
      util.Arrays.asList(
        new FeatureZNodeData.Feature()
          .setFeatureName("feature1")
          .setVersionRange(new FeatureZNodeData.FinalizedVersionRange().setMinValue(1).setMaxValue(2)),
        new FeatureZNodeData.Feature()
          .setFeatureName("feature2")
          .setVersionRange(new FeatureZNodeData.FinalizedVersionRange().setMinValue(2).setMaxValue(4)),
      ), node1.features)
    val emptyFeatures = "[]"
    val node2 = FeatureZNode.decode(featureZNodeStrTemplate.format(emptyFeatures).getBytes(StandardCharsets.UTF_8))
    assertEquals(FeatureZNodeStatus.Enabled.id, node2.status)
    assertEquals(0, node2.features.size())
  }

  @Test
  def testDecodeFailOnInvalidVersionAndStatus(): Unit = {
    val featureZNodeStrTemplate =
      """{
      "version":%d,
      "status":%d,
      "features":[{"featureName":"feature1","versionRange":{"minValue":1,"maxValue":2}},{"featureName":"feature2","versionRange":{"minValue":2,"maxValue":4}}]
    }"""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(FeatureZNodeData.LOWEST_SUPPORTED_VERSION - 1, 1).getBytes(StandardCharsets.UTF_8)))
    val invalidStatus = FeatureZNodeStatus.Enabled.id + 1
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(FeatureZNodeData.HIGHEST_SUPPORTED_VERSION, invalidStatus).getBytes(StandardCharsets.UTF_8)))
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

    val invalidFeaturesMinVersionLevel = ""","features":[{"featureName":"feature1","versionRange":{"minValue":0,"maxValue":2}}]"""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.getFeatures(
        FeatureZNode.decode(
          featureZNodeStrTemplate.format(invalidFeaturesMinVersionLevel).getBytes(StandardCharsets.UTF_8))))

    val invalidFeaturesMaxVersionLevel = ""","features":[{"featureName":"feature1","versionRange":{"minValue":2,"maxValue":1}}]"""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.getFeatures(
        FeatureZNode.decode(
          featureZNodeStrTemplate.format(invalidFeaturesMaxVersionLevel).getBytes(StandardCharsets.UTF_8))))

    val invalidFeaturesMissingMinVersionLevel = ""","features":[{"featureName":"feature1","versionRange":{"maxValue":1}}]"""
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureZNode.decode(
        featureZNodeStrTemplate.format(invalidFeaturesMissingMinVersionLevel).getBytes(StandardCharsets.UTF_8)))
  }
}
