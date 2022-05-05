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

package integration.kafka.server

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType
import org.apache.kafka.clients.admin.{FeatureUpdate, UpdateFeaturesOptions}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class MetadataVersionIntegrationTest {
  @ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_0_IV0)
  def testBasicMetadataVersionUpgrade(clusterInstance: ClusterInstance): Unit = {
    val admin = clusterInstance.createAdminClient()
    val describeResult = admin.describeFeatures()
    val ff = describeResult.featureMetadata().get().finalizedFeatures().get(MetadataVersion.FEATURE_NAME)
    assertEquals(ff.minVersionLevel(), 1)
    assertEquals(ff.maxVersionLevel(), 1)

    // Update to V2
    val updateVersion = MetadataVersion.IBP_3_3_IV0.featureLevel.shortValue
    val updateResult = admin.updateFeatures(
      Map("metadata.version" -> new FeatureUpdate(updateVersion, UpgradeType.UPGRADE)).asJava, new UpdateFeaturesOptions())
    updateResult.all().get()

    // Verify that V2 is visible on broker
    TestUtils.waitUntilTrue(() => {
      val describeResult2 = admin.describeFeatures()
      val ff2 = describeResult2.featureMetadata().get().finalizedFeatures().get(MetadataVersion.FEATURE_NAME)
      ff2.minVersionLevel() == updateVersion && ff2.maxVersionLevel() == updateVersion
    }, "Never saw metadata.version increase on broker")
  }

  @ClusterTest(clusterType = Type.KRAFT)
  def testBootstrapLatestVersion(clusterInstance: ClusterInstance): Unit = {
    val admin = clusterInstance.createAdminClient()
    val describeResult = admin.describeFeatures()
    val ff = describeResult.featureMetadata().get().finalizedFeatures().get(MetadataVersion.FEATURE_NAME)
    assertEquals(ff.minVersionLevel(), MetadataVersion.latest().featureLevel())
    assertEquals(ff.maxVersionLevel(), MetadataVersion.latest().featureLevel())
  }
}
