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

package kafka.server

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterTest, ClusterTests, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType
import org.apache.kafka.clients.admin.{Admin, FeatureUpdate, UpdateFeaturesOptions}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.extension.ExtendWith

import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.util.Using

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class MetadataVersionIntegrationTest {
  @ClusterTests(value = Array(
      new ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_3_IV0),
      new ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_3_IV1),
      new ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_3_IV2),
      new ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_3_IV3),
      new ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_4_IV0),
      new ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_4_IV0)
  ))
  def testBasicMetadataVersionUpgrade(clusterInstance: ClusterInstance): Unit = {
    Using(Admin.create(clusterInstance.adminConfigs(Collections.emptyMap()))) { admin =>
      val describeResult = admin.describeFeatures()
      val ff = describeResult.featureMetadata().get().finalizedFeatures().get(MetadataVersion.FEATURE_NAME)
      assertEquals(ff.minVersionLevel(), clusterInstance.config().metadataVersion().featureLevel())
      assertEquals(ff.maxVersionLevel(), clusterInstance.config().metadataVersion().featureLevel())

      // Update to new version
      val updateVersion = MetadataVersion.IBP_3_5_IV1.featureLevel.shortValue
      val updateResult = admin.updateFeatures(
        Map("metadata.version" -> new FeatureUpdate(updateVersion, UpgradeType.UPGRADE)).asJava, new UpdateFeaturesOptions())
      updateResult.all().get()

      // Verify that new version is visible on broker
      TestUtils.waitUntilTrue(() => {
        val describeResult2 = admin.describeFeatures()
        val ff2 = describeResult2.featureMetadata().get().finalizedFeatures().get(MetadataVersion.FEATURE_NAME)
        ff2.minVersionLevel() == updateVersion && ff2.maxVersionLevel() == updateVersion
      }, "Never saw metadata.version increase on broker")
    }
  }

  @ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_3_IV0)
  def testUpgradeSameVersion(clusterInstance: ClusterInstance): Unit = {
    Using(Admin.create(clusterInstance.adminConfigs(Collections.emptyMap()))) { admin =>
      val updateVersion = MetadataVersion.IBP_3_3_IV0.featureLevel.shortValue
      val updateResult = admin.updateFeatures(
        Map("metadata.version" -> new FeatureUpdate(updateVersion, UpgradeType.UPGRADE)).asJava, new UpdateFeaturesOptions())
      updateResult.all().get()
    }
  }

  @ClusterTest(clusterType = Type.KRAFT)
  def testDefaultIsLatestVersion(clusterInstance: ClusterInstance): Unit = {
    Using(Admin.create(clusterInstance.adminConfigs(Collections.emptyMap()))) { admin =>
      val describeResult = admin.describeFeatures()
      val ff = describeResult.featureMetadata().get().finalizedFeatures().get(MetadataVersion.FEATURE_NAME)
      assertEquals(ff.minVersionLevel(), MetadataVersion.latestTesting().featureLevel(),
        "If this test fails, check the default MetadataVersion in the @ClusterTest annotation")
      assertEquals(ff.maxVersionLevel(), MetadataVersion.latestTesting().featureLevel())
    }
  }
}
