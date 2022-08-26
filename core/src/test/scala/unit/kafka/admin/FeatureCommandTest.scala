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

package kafka.admin

import kafka.server.{BaseRequestTest, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.utils.TestUtils.waitUntilTrue
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.utils.Utils
import java.util.Properties

import org.apache.kafka.server.common.MetadataVersion.IBP_2_7_IV0
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class FeatureCommandTest extends BaseRequestTest {
  override def brokerCount: Int = 3

  override def brokerPropertyOverrides(props: Properties): Unit = {
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, IBP_2_7_IV0.toString)
  }

  private val defaultSupportedFeatures: Features[SupportedVersionRange] =
    Features.supportedFeatures(Utils.mkMap(Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
                                           Utils.mkEntry("feature_2", new SupportedVersionRange(1, 5))))

  private def updateSupportedFeatures(features: Features[SupportedVersionRange],
                                      targetServers: Set[KafkaServer]): Unit = {
    targetServers.foreach(s => {
      s.brokerFeatures.setSupportedFeatures(features)
      s.zkClient.updateBrokerInfo(s.createBrokerInfo)
    })

    // Wait until updates to all BrokerZNode supported features propagate to the controller.
    val brokerIds = targetServers.map(s => s.config.brokerId)
    waitUntilTrue(
      () => servers.exists(s => {
        if (s.kafkaController.isActive) {
          s.kafkaController.controllerContext.liveOrShuttingDownBrokers
            .filter(b => brokerIds.contains(b.id))
            .forall(b => {
              b.features.equals(features)
            })
        } else {
          false
        }
      }),
      "Controller did not get broker updates")
  }

  private def updateSupportedFeaturesInAllBrokers(features: Features[SupportedVersionRange]): Unit = {
    updateSupportedFeatures(features, Set[KafkaServer]() ++ servers)
  }

  /**
   * Tests if the FeatureApis#describeFeatures API works as expected when describing features before and
   * after upgrading features.
   */
  @Test
  def testDescribeFeaturesSuccess(): Unit = {
    updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures)

    val initialDescribeOutput = TestUtils.grabConsoleOutput(FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(), "describe")))
    val expectedInitialDescribeOutputs = Seq(
      "Feature: feature_1\tSupportedMinVersion: 1\tSupportedMaxVersion: 3\tFinalizedVersionLevel: -",
      "Feature: feature_2\tSupportedMinVersion: 1\tSupportedMaxVersion: 5\tFinalizedVersionLevel: -"
    )

    expectedInitialDescribeOutputs.foreach { expectedOutput =>
      assertTrue(initialDescribeOutput.contains(expectedOutput))
    }

    FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(), "upgrade",
      "--feature", "feature_1", "--version", "3", "--feature", "feature_2", "--version", "5"))
    val upgradeDescribeOutput = TestUtils.grabConsoleOutput(FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(), "describe")))
    val expectedUpgradeDescribeOutput = Seq(
      "Feature: feature_1\tSupportedMinVersion: 1\tSupportedMaxVersion: 3\tFinalizedVersionLevel: 3",
      "Feature: feature_2\tSupportedMinVersion: 1\tSupportedMaxVersion: 5\tFinalizedVersionLevel: 5"
    )
    expectedUpgradeDescribeOutput.foreach { expectedOutput =>
      assertTrue(upgradeDescribeOutput.contains(expectedOutput))
    }

    FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(), "downgrade",
      "--feature", "feature_1", "--version", "2", "--feature", "feature_2", "--version", "2"))
    val downgradeDescribeOutput = TestUtils.grabConsoleOutput(FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(), "describe")))
    val expectedFinalDescribeOutput = Seq(
      "Feature: feature_1\tSupportedMinVersion: 1\tSupportedMaxVersion: 3\tFinalizedVersionLevel: 2",
      "Feature: feature_2\tSupportedMinVersion: 1\tSupportedMaxVersion: 5\tFinalizedVersionLevel: 2"
    )
    expectedFinalDescribeOutput.foreach { expectedOutput =>
      assertTrue(downgradeDescribeOutput.contains(expectedOutput))
    }
  }
}
