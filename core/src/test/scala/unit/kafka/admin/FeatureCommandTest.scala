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

import kafka.api.KAFKA_2_7_IV0
import kafka.server.{BaseRequestTest, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.utils.TestUtils.waitUntilTrue
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.utils.Utils

import java.util.Properties

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.scalatest.Assertions.intercept

class FeatureCommandTest extends BaseRequestTest {
  override def brokerCount: Int = 3

  override def brokerPropertyOverrides(props: Properties): Unit = {
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, KAFKA_2_7_IV0.toString)
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
    val featureApis = new FeatureApis(new FeatureCommandOptions(Array("--bootstrap-server", brokerList, "--describe")))
    featureApis.setSupportedFeatures(defaultSupportedFeatures)
    try {
      val initialDescribeOutput = TestUtils.grabConsoleOutput(featureApis.describeFeatures())
      val expectedInitialDescribeOutput =
        "Feature: feature_1\tSupportedMinVersion: 1\tSupportedMaxVersion: 3\tFinalizedMinVersionLevel: -\tFinalizedMaxVersionLevel: -\tEpoch: 0\n" +
        "Feature: feature_2\tSupportedMinVersion: 1\tSupportedMaxVersion: 5\tFinalizedMinVersionLevel: -\tFinalizedMaxVersionLevel: -\tEpoch: 0\n"
      assertEquals(expectedInitialDescribeOutput, initialDescribeOutput)
      featureApis.upgradeAllFeatures()
      val finalDescribeOutput = TestUtils.grabConsoleOutput(featureApis.describeFeatures())
      val expectedFinalDescribeOutput =
        "Feature: feature_1\tSupportedMinVersion: 1\tSupportedMaxVersion: 3\tFinalizedMinVersionLevel: 1\tFinalizedMaxVersionLevel: 3\tEpoch: 1\n" +
        "Feature: feature_2\tSupportedMinVersion: 1\tSupportedMaxVersion: 5\tFinalizedMinVersionLevel: 1\tFinalizedMaxVersionLevel: 5\tEpoch: 1\n"
      assertEquals(expectedFinalDescribeOutput, finalDescribeOutput)
    } finally {
      featureApis.close()
    }
  }

  /**
   * Tests if the FeatureApis#upgradeAllFeatures API works as expected during a success case.
   */
  @Test
  def testUpgradeAllFeaturesSuccess(): Unit = {
    val upgradeOpts = new FeatureCommandOptions(Array("--bootstrap-server", brokerList, "--upgrade-all"))
    val featureApis = new FeatureApis(upgradeOpts)
    try {
      // Step (1):
      // - Update the supported features across all brokers.
      // - Upgrade non-existing feature_1 to maxVersionLevel: 2.
      // - Verify results.
      val initialSupportedFeatures = Features.supportedFeatures(Utils.mkMap(Utils.mkEntry("feature_1", new SupportedVersionRange(1, 2))))
      updateSupportedFeaturesInAllBrokers(initialSupportedFeatures)
      featureApis.setSupportedFeatures(initialSupportedFeatures)
      var output = TestUtils.grabConsoleOutput(featureApis.upgradeAllFeatures())
      var expected =
        "      [Add]\tFeature: feature_1\tExistingFinalizedMaxVersion: -\tNewFinalizedMaxVersion: 2\tResult: OK\n"
      assertEquals(expected, output)

      // Step (2):
      // - Update the supported features across all brokers.
      // - Upgrade existing feature_1 to maxVersionLevel: 3.
      // - Upgrade non-existing feature_2 to maxVersionLevel: 5.
      // - Verify results.
      updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures)
      featureApis.setSupportedFeatures(defaultSupportedFeatures)
      output = TestUtils.grabConsoleOutput(featureApis.upgradeAllFeatures())
      expected =
        "  [Upgrade]\tFeature: feature_1\tExistingFinalizedMaxVersion: 2\tNewFinalizedMaxVersion: 3\tResult: OK\n" +
        "      [Add]\tFeature: feature_2\tExistingFinalizedMaxVersion: -\tNewFinalizedMaxVersion: 5\tResult: OK\n"
      assertEquals(expected, output)

      // Step (3):
      // - Perform an upgrade of all features again.
      // - Since supported features have not changed, expect that the above action does not yield
      //   any results.
      output = TestUtils.grabConsoleOutput(featureApis.upgradeAllFeatures())
      assertTrue(output.isEmpty)
      featureApis.setOptions(upgradeOpts)
      output = TestUtils.grabConsoleOutput(featureApis.upgradeAllFeatures())
      assertTrue(output.isEmpty)
    } finally {
      featureApis.close()
    }
  }

  /**
   * Tests if the FeatureApis#downgradeAllFeatures API works as expected during a success case.
   */
  @Test
  def testDowngradeFeaturesSuccess(): Unit = {
    val downgradeOpts = new FeatureCommandOptions(Array("--bootstrap-server", brokerList, "--downgrade-all"))
    val upgradeOpts = new FeatureCommandOptions(Array("--bootstrap-server", brokerList, "--upgrade-all"))
    val featureApis = new FeatureApis(upgradeOpts)
    try {
      // Step (1):
      // - Update the supported features across all brokers.
      // - Upgrade non-existing feature_1 to maxVersionLevel: 3.
      // - Upgrade non-existing feature_2 to maxVersionLevel: 5.
      updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures)
      featureApis.setSupportedFeatures(defaultSupportedFeatures)
      featureApis.upgradeAllFeatures()

      // Step (2):
      // - Downgrade existing feature_1 to maxVersionLevel: 2.
      // - Delete feature_2 since it is no longer supported by the FeatureApis object.
      // - Verify results.
      val downgradedFeatures = Features.supportedFeatures(Utils.mkMap(Utils.mkEntry("feature_1", new SupportedVersionRange(1, 2))))
      featureApis.setSupportedFeatures(downgradedFeatures)
      featureApis.setOptions(downgradeOpts)
      var output = TestUtils.grabConsoleOutput(featureApis.downgradeAllFeatures())
      var expected =
        "[Downgrade]\tFeature: feature_1\tExistingFinalizedMaxVersion: 3\tNewFinalizedMaxVersion: 2\tResult: OK\n" +
        "   [Delete]\tFeature: feature_2\tExistingFinalizedMaxVersion: 5\tNewFinalizedMaxVersion: -\tResult: OK\n"
      assertEquals(expected, output)

      // Step (3):
      // - Perform a downgrade of all features again.
      // - Since supported features have not changed, expect that the above action does not yield
      //   any results.
      updateSupportedFeaturesInAllBrokers(downgradedFeatures)
      output = TestUtils.grabConsoleOutput(featureApis.downgradeAllFeatures())
      assertTrue(output.isEmpty)

      // Step (4):
      // - Delete feature_1 since it is no longer supported by the FeatureApis object.
      // - Verify results.
      featureApis.setSupportedFeatures(Features.emptySupportedFeatures())
      output = TestUtils.grabConsoleOutput(featureApis.downgradeAllFeatures())
      expected =
        "   [Delete]\tFeature: feature_1\tExistingFinalizedMaxVersion: 2\tNewFinalizedMaxVersion: -\tResult: OK\n"
      assertEquals(expected, output)
    } finally {
      featureApis.close()
    }
  }

  /**
   * Tests if the FeatureApis#upgradeAllFeatures API works as expected during a partial failure case.
   */
  @Test
  def testUpgradeFeaturesFailure(): Unit = {
    val upgradeOpts = new FeatureCommandOptions(Array("--bootstrap-server", brokerList, "--upgrade-all"))
    val featureApis = new FeatureApis(upgradeOpts)
    try {
      // Step (1): Update the supported features across all brokers.
      updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures)

      // Step (2):
      // - Intentionally setup the FeatureApis object such that it contains incompatible target
      //   features (viz. feature_2 and feature_3).
      // - Upgrade non-existing feature_1 to maxVersionLevel: 4. Expect the operation to fail with
      //   an incompatibility failure.
      // - Upgrade non-existing feature_2 to maxVersionLevel: 5. Expect the operation to succeed.
      // - Upgrade non-existing feature_3 to maxVersionLevel: 3. Expect the operation to fail
      //   since the feature is not supported.
      val targetFeaturesWithIncompatibilities =
        Features.supportedFeatures(
          Utils.mkMap(Utils.mkEntry("feature_1", new SupportedVersionRange(1, 4)),
                      Utils.mkEntry("feature_2", new SupportedVersionRange(1, 5)),
                      Utils.mkEntry("feature_3", new SupportedVersionRange(1, 3))))
      featureApis.setSupportedFeatures(targetFeaturesWithIncompatibilities)
      val output = TestUtils.grabConsoleOutput({
        val exception = intercept[UpdateFeaturesException] {
          featureApis.upgradeAllFeatures()
        }
        assertEquals("2 feature updates failed!", exception.getMessage)
      })
      val expected =
        "      [Add]\tFeature: feature_1\tExistingFinalizedMaxVersion: -" +
        "\tNewFinalizedMaxVersion: 4\tResult: FAILED due to" +
        " org.apache.kafka.common.errors.InvalidRequestException: Could not apply finalized" +
        " feature update because brokers were found to have incompatible versions for the" +
        " feature.\n" +
        "      [Add]\tFeature: feature_2\tExistingFinalizedMaxVersion: -" +
        "\tNewFinalizedMaxVersion: 5\tResult: OK\n" +
        "      [Add]\tFeature: feature_3\tExistingFinalizedMaxVersion: -" +
        "\tNewFinalizedMaxVersion: 3\tResult: FAILED due to" +
        " org.apache.kafka.common.errors.InvalidRequestException: Could not apply finalized" +
        " feature update because the provided feature is not supported.\n"
      assertEquals(expected, output)
    } finally {
      featureApis.close()
    }
  }
}
