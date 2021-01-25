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

import java.util.{Optional, Properties}
import java.util.concurrent.ExecutionException

import kafka.api.KAFKA_2_7_IV0
import kafka.utils.TestUtils
import kafka.zk.{FeatureZNode, FeatureZNodeStatus, ZkVersion}
import kafka.utils.TestUtils.waitUntilTrue
import org.apache.kafka.clients.admin.{Admin, FeatureUpdate, UpdateFeaturesOptions, UpdateFeaturesResult}
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.feature.FinalizedVersionRange
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.message.UpdateFeaturesRequestData
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKeyCollection
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{UpdateFeaturesRequest, UpdateFeaturesResponse}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertNotNull, assertTrue, assertThrows}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.matching.Regex

class UpdateFeaturesTest extends BaseRequestTest {

  override def brokerCount = 3

  override def brokerPropertyOverrides(props: Properties): Unit = {
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, KAFKA_2_7_IV0.toString)
  }

  private def defaultSupportedFeatures(): Features[SupportedVersionRange] = {
    Features.supportedFeatures(Utils.mkMap(Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3))))
  }

  private def defaultFinalizedFeatures(): Features[FinalizedVersionRange] = {
    Features.finalizedFeatures(Utils.mkMap(Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2))))
  }

  private def updateSupportedFeatures(
    features: Features[SupportedVersionRange], targetServers: Set[KafkaServer]): Unit = {
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

  private def updateFeatureZNode(features: Features[FinalizedVersionRange]): Int = {
    val server = serverForId(0).get
    val newNode = new FeatureZNode(FeatureZNodeStatus.Enabled, features)
    val newVersion = server.zkClient.updateFeatureZNode(newNode)
    servers.foreach(s => {
      s.featureCache.waitUntilEpochOrThrow(newVersion, s.config.zkConnectionTimeoutMs)
    })
    newVersion
  }

  private def getFeatureZNode(): FeatureZNode = {
    val (mayBeFeatureZNodeBytes, version) = serverForId(0).get.zkClient.getDataAndVersion(FeatureZNode.path)
    assertNotEquals(version, ZkVersion.UnknownVersion)
    FeatureZNode.decode(mayBeFeatureZNodeBytes.get)
  }

  private def finalizedFeatures(features: java.util.Map[String, org.apache.kafka.clients.admin.FinalizedVersionRange]): Features[FinalizedVersionRange] = {
    Features.finalizedFeatures(features.asScala.map {
      case(name, versionRange) =>
        (name, new FinalizedVersionRange(versionRange.minVersionLevel(), versionRange.maxVersionLevel()))
    }.asJava)
  }

  private def supportedFeatures(features: java.util.Map[String, org.apache.kafka.clients.admin.SupportedVersionRange]): Features[SupportedVersionRange] = {
    Features.supportedFeatures(features.asScala.map {
      case(name, versionRange) =>
        (name, new SupportedVersionRange(versionRange.minVersion(), versionRange.maxVersion()))
    }.asJava)
  }

  private def checkFeatures(client: Admin,
                            expectedNode: FeatureZNode,
                            expectedFinalizedFeatures: Features[FinalizedVersionRange],
                            expectedFinalizedFeaturesEpoch: Long,
                            expectedSupportedFeatures: Features[SupportedVersionRange]): Unit = {
    assertEquals(expectedNode, getFeatureZNode())
    val featureMetadata = client.describeFeatures.featureMetadata.get
    assertEquals(expectedFinalizedFeatures, finalizedFeatures(featureMetadata.finalizedFeatures))
    assertEquals(expectedSupportedFeatures, supportedFeatures(featureMetadata.supportedFeatures))
    assertEquals(Optional.of(expectedFinalizedFeaturesEpoch), featureMetadata.finalizedFeaturesEpoch)
  }

  private def checkException[ExceptionType <: Throwable](result: UpdateFeaturesResult,
                                                         featureExceptionMsgPatterns: Map[String, Regex])
                                                        (implicit tag: ClassTag[ExceptionType]): Unit = {
    featureExceptionMsgPatterns.foreach {
      case (feature, exceptionMsgPattern) =>
        val exception = assertThrows(classOf[ExecutionException], () => result.values().get(feature).get())
        val cause = exception.getCause
        assertNotNull(cause)
        assertEquals(cause.getClass, tag.runtimeClass)
        assertTrue(exceptionMsgPattern.findFirstIn(cause.getMessage).isDefined,
                   s"Received unexpected error message: ${cause.getMessage}")
    }
  }

  /**
   * Tests whether an invalid feature update does not get processed on the server as expected,
   * and raises the ExceptionType on the client side as expected.
   *
   * @param feature               the feature to be updated
   * @param invalidUpdate         the invalid feature update to be sent in the
   *                              updateFeatures request to the server
   * @param exceptionMsgPattern   a pattern for the expected exception message
   */
  private def testWithInvalidFeatureUpdate[ExceptionType <: Throwable](feature: String,
                                                                       invalidUpdate: FeatureUpdate,
                                                                       exceptionMsgPattern: Regex)
                                                                      (implicit tag: ClassTag[ExceptionType]): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures())
    val versionBefore = updateFeatureZNode(defaultFinalizedFeatures())
    val adminClient = createAdminClient()
    val nodeBefore = getFeatureZNode()

    val result = adminClient.updateFeatures(Utils.mkMap(Utils.mkEntry(feature, invalidUpdate)), new UpdateFeaturesOptions())

    checkException[ExceptionType](result, Map(feature -> exceptionMsgPattern))
    checkFeatures(
      adminClient,
      nodeBefore,
      defaultFinalizedFeatures(),
      versionBefore,
      defaultSupportedFeatures())
  }

  /**
   * Tests that an UpdateFeatures request sent to a non-Controller node fails as expected.
   */
  @Test
  def testShouldFailRequestIfNotController(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures())
    val versionBefore = updateFeatureZNode(defaultFinalizedFeatures())

    val nodeBefore = getFeatureZNode()
    val validUpdates = new FeatureUpdateKeyCollection()
    val validUpdate = new UpdateFeaturesRequestData.FeatureUpdateKey();
    validUpdate.setFeature("feature_1");
    validUpdate.setMaxVersionLevel(defaultSupportedFeatures().get("feature_1").max())
    validUpdate.setAllowDowngrade(false)
    validUpdates.add(validUpdate)

    val response = connectAndReceive[UpdateFeaturesResponse](
      new UpdateFeaturesRequest.Builder(new UpdateFeaturesRequestData().setFeatureUpdates(validUpdates)).build(),
      notControllerSocketServer)

    assertEquals(Errors.NOT_CONTROLLER, Errors.forCode(response.data.errorCode()))
    assertNotNull(response.data.errorMessage())
    assertEquals(0, response.data.results.size)
    checkFeatures(
      createAdminClient(),
      nodeBefore,
      defaultFinalizedFeatures(),
      versionBefore,
      defaultSupportedFeatures())
  }

  /**
   * Tests that an UpdateFeatures request fails in the Controller, when, for a feature the
   * allowDowngrade flag is not set during a downgrade request.
   */
  @Test
  def testShouldFailRequestWhenDowngradeFlagIsNotSetDuringDowngrade(): Unit = {
    val targetMaxVersionLevel = (defaultFinalizedFeatures().get("feature_1").max() - 1).asInstanceOf[Short]
    testWithInvalidFeatureUpdate[InvalidRequestException](
      "feature_1",
      new FeatureUpdate(targetMaxVersionLevel,false),
      ".*Can not downgrade finalized feature.*allowDowngrade.*".r)
  }

  /**
   * Tests that an UpdateFeatures request fails in the Controller, when, for a feature the downgrade
   * is attempted to a max version level higher than the existing max version level.
   */
  @Test
  def testShouldFailRequestWhenDowngradeToHigherVersionLevelIsAttempted(): Unit = {
    val targetMaxVersionLevel = (defaultFinalizedFeatures().get("feature_1").max() + 1).asInstanceOf[Short]
    testWithInvalidFeatureUpdate[InvalidRequestException](
      "feature_1",
      new FeatureUpdate(targetMaxVersionLevel, true),
      ".*When the allowDowngrade flag set in the request, the provided maxVersionLevel:3.*existing maxVersionLevel:2.*".r)
  }

  /**
   * Tests that an UpdateFeatures request fails in the Controller, when, a feature deletion is
   * attempted without setting the allowDowngrade flag.
   */
  @Test
  def testShouldFailRequestInServerWhenDowngradeFlagIsNotSetDuringDeletion(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures())
    val versionBefore = updateFeatureZNode(defaultFinalizedFeatures())

    val adminClient = createAdminClient()
    val nodeBefore = getFeatureZNode()

    val invalidUpdates
      = new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();
    val invalidUpdate = new UpdateFeaturesRequestData.FeatureUpdateKey();
    invalidUpdate.setFeature("feature_1")
    invalidUpdate.setMaxVersionLevel(0)
    invalidUpdate.setAllowDowngrade(false)
    invalidUpdates.add(invalidUpdate);
    val requestData = new UpdateFeaturesRequestData()
    requestData.setFeatureUpdates(invalidUpdates);

    val response = connectAndReceive[UpdateFeaturesResponse](
      new UpdateFeaturesRequest.Builder(new UpdateFeaturesRequestData().setFeatureUpdates(invalidUpdates)).build(),
      controllerSocketServer)

    assertEquals(1, response.data().results().size())
    val result = response.data.results.asScala.head
    assertEquals("feature_1", result.feature)
    assertEquals(Errors.INVALID_REQUEST, Errors.forCode(result.errorCode))
    assertNotNull(result.errorMessage)
    assertFalse(result.errorMessage.isEmpty)
    val exceptionMsgPattern = ".*Can not provide maxVersionLevel: 0 less than 1.*allowDowngrade.*".r
    assertTrue(exceptionMsgPattern.findFirstIn(result.errorMessage).isDefined, result.errorMessage)
    checkFeatures(
      adminClient,
      nodeBefore,
      defaultFinalizedFeatures(),
      versionBefore,
      defaultSupportedFeatures())
  }

  /**
   * Tests that an UpdateFeatures request fails in the Controller, when, a feature version level
   * upgrade is attempted for a non-existing feature.
   */
  @Test
  def testShouldFailRequestDuringDeletionOfNonExistingFeature(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](
      "feature_non_existing",
      new FeatureUpdate(3, true),
      ".*Could not apply finalized feature update because the provided feature is not supported.*".r)
  }

  /**
   * Tests that an UpdateFeatures request fails in the Controller, when, a feature version level
   * upgrade is attempted to a version level same as the existing max version level.
   */
  @Test
  def testShouldFailRequestWhenUpgradingToSameVersionLevel(): Unit = {
    val targetMaxVersionLevel = defaultFinalizedFeatures().get("feature_1").max()
    testWithInvalidFeatureUpdate[InvalidRequestException](
      "feature_1",
      new FeatureUpdate(targetMaxVersionLevel, false),
      ".*Can not upgrade a finalized feature.*to the same value.*".r)
  }

  private def testShouldFailRequestDuringBrokerMaxVersionLevelIncompatibility(
    featureName: String,
    supportedVersionRange: SupportedVersionRange,
    initialFinalizedVersionRange: Option[FinalizedVersionRange]
  ): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val controller = servers.filter { server => server.kafkaController.isActive}.head
    val nonControllerServers = servers.filter { server => !server.kafkaController.isActive}
    // We setup the supported features on the broker such that 1/3 of the brokers does not
    // support an expected feature version, while 2/3 brokers support the expected feature
    // version.
    val brokersWithVersionIncompatibility = Set[KafkaServer](nonControllerServers.head)
    val versionCompatibleBrokers = Set[KafkaServer](nonControllerServers(1), controller)

    val supportedFeatures = Features.supportedFeatures(Utils.mkMap(Utils.mkEntry(featureName, supportedVersionRange)))
    updateSupportedFeatures(supportedFeatures, versionCompatibleBrokers)

    val unsupportedMaxVersion = (supportedVersionRange.max() - 1).asInstanceOf[Short]
    val supportedFeaturesWithVersionIncompatibility = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1",
          new SupportedVersionRange(
            supportedVersionRange.min(),
            unsupportedMaxVersion))))
    updateSupportedFeatures(supportedFeaturesWithVersionIncompatibility, brokersWithVersionIncompatibility)

    val initialFinalizedFeatures = initialFinalizedVersionRange.map(
      versionRange => Features.finalizedFeatures(Utils.mkMap(Utils.mkEntry(featureName, versionRange)))
    ).getOrElse(Features.emptyFinalizedFeatures())
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

    val invalidUpdate = new FeatureUpdate(supportedVersionRange.max(), false)
    val nodeBefore = getFeatureZNode()
    val adminClient = createAdminClient()
    val result = adminClient.updateFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", invalidUpdate)),
      new UpdateFeaturesOptions())

    checkException[InvalidRequestException](result, Map("feature_1" -> ".*brokers.*incompatible.*".r))
    checkFeatures(
      adminClient,
      nodeBefore,
      initialFinalizedFeatures,
      versionBefore,
      supportedFeatures)
  }

  /**
   * Tests that an UpdateFeatures request fails in the Controller, when for an existing finalized
   * feature, a version level upgrade introduces a version incompatibility with existing supported
   * features.
   */
  @Test
  def testShouldFailRequestDuringBrokerMaxVersionLevelIncompatibilityForExistingFinalizedFeature(): Unit = {
    val feature = "feature_1"
    testShouldFailRequestDuringBrokerMaxVersionLevelIncompatibility(
      feature,
      defaultSupportedFeatures().get(feature),
      Some(defaultFinalizedFeatures().get(feature)))
  }

  /**
   * Tests that an UpdateFeatures request fails in the Controller, when for a non-existing finalized
   * feature, a version level upgrade introduces a version incompatibility with existing supported
   * features.
   */
  @Test
  def testShouldFailRequestDuringBrokerMaxVersionLevelIncompatibilityWithNoExistingFinalizedFeature(): Unit = {
    val feature = "feature_1"
    testShouldFailRequestDuringBrokerMaxVersionLevelIncompatibility(
      feature,
      defaultSupportedFeatures().get(feature),
      Option.empty)
  }

  /**
   * Tests that an UpdateFeatures request succeeds in the Controller, when, there are no existing
   * finalized features in FeatureZNode when the test starts.
   */
  @Test
  def testSuccessfulFeatureUpgradeAndWithNoExistingFinalizedFeatures(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val supportedFeatures =
      Features.supportedFeatures(
        Utils.mkMap(
          Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
          Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5))))
    updateSupportedFeaturesInAllBrokers(supportedFeatures)
    val versionBefore = updateFeatureZNode(Features.emptyFinalizedFeatures())

    val targetFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 3))))
    val update1 = new FeatureUpdate(targetFinalizedFeatures.get("feature_1").max(), false)
    val update2 = new FeatureUpdate(targetFinalizedFeatures.get("feature_2").max(), false)

    val adminClient = createAdminClient()
    adminClient.updateFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", update1), Utils.mkEntry("feature_2", update2)),
      new UpdateFeaturesOptions()
    ).all().get()

    checkFeatures(
      adminClient,
      new FeatureZNode(FeatureZNodeStatus.Enabled, targetFinalizedFeatures),
      targetFinalizedFeatures,
      versionBefore + 1,
      supportedFeatures)
  }

  /**
   * Tests that an UpdateFeatures request succeeds in the Controller, when, the request contains
   * both a valid feature version level upgrade as well as a downgrade request.
   */
  @Test
  def testSuccessfulFeatureUpgradeAndDowngrade(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val supportedFeatures = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5))))
    updateSupportedFeaturesInAllBrokers(supportedFeatures)
    val initialFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 4))))
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

    // Below we aim to do the following:
    // - Valid upgrade of feature_1 maxVersionLevel from 2 to 3
    // - Valid downgrade of feature_2 maxVersionLevel from 4 to 3
    val targetFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 3))))
    val update1 = new FeatureUpdate(targetFinalizedFeatures.get("feature_1").max(), false)
    val update2 = new FeatureUpdate(targetFinalizedFeatures.get("feature_2").max(), true)

    val adminClient = createAdminClient()
    adminClient.updateFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", update1), Utils.mkEntry("feature_2", update2)),
      new UpdateFeaturesOptions()
    ).all().get()

    checkFeatures(
      adminClient,
      new FeatureZNode(FeatureZNodeStatus.Enabled, targetFinalizedFeatures),
      targetFinalizedFeatures,
      versionBefore + 1,
      supportedFeatures)
  }

  /**
   * Tests that an UpdateFeatures request succeeds partially in the Controller, when, the request
   * contains a valid feature version level upgrade and an invalid version level downgrade.
   * i.e. expect the upgrade operation to succeed, and the downgrade operation to fail.
   */
  @Test
  def testPartialSuccessDuringValidFeatureUpgradeAndInvalidDowngrade(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val supportedFeatures = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5))))
    updateSupportedFeaturesInAllBrokers(supportedFeatures)
    val initialFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 4))))
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

    // Below we aim to do the following:
    // - Valid upgrade of feature_1 maxVersionLevel from 2 to 3
    // - Invalid downgrade of feature_2 maxVersionLevel from 4 to 3
    //   (because we intentionally do not set the allowDowngrade flag)
    val targetFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 3))))
    val validUpdate = new FeatureUpdate(targetFinalizedFeatures.get("feature_1").max(), false)
    val invalidUpdate = new FeatureUpdate(targetFinalizedFeatures.get("feature_2").max(), false)

    val adminClient = createAdminClient()
    val result = adminClient.updateFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", validUpdate), Utils.mkEntry("feature_2", invalidUpdate)),
      new UpdateFeaturesOptions())

    // Expect update for "feature_1" to have succeeded.
    result.values().get("feature_1").get()
    // Expect update for "feature_2" to have failed.
    checkException[InvalidRequestException](
      result, Map("feature_2" -> ".*Can not downgrade finalized feature.*allowDowngrade.*".r))
    val expectedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", targetFinalizedFeatures.get("feature_1")),
        Utils.mkEntry("feature_2", initialFinalizedFeatures.get("feature_2"))))
    checkFeatures(
      adminClient,
      FeatureZNode(FeatureZNodeStatus.Enabled, expectedFeatures),
      expectedFeatures,
      versionBefore + 1,
      supportedFeatures)
  }

  /**
   * Tests that an UpdateFeatures request succeeds partially in the Controller, when, the request
   * contains an invalid feature version level upgrade and a valid version level downgrade.
   * i.e. expect the downgrade operation to succeed, and the upgrade operation to fail.
   */
  @Test
  def testPartialSuccessDuringInvalidFeatureUpgradeAndValidDowngrade(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val controller = servers.filter { server => server.kafkaController.isActive}.head
    val nonControllerServers = servers.filter { server => !server.kafkaController.isActive}
    // We setup the supported features on the broker such that 1/3 of the brokers does not
    // support an expected feature version, while 2/3 brokers support the expected feature
    // version.
    val brokersWithVersionIncompatibility = Set[KafkaServer](nonControllerServers.head)
    val versionCompatibleBrokers = Set[KafkaServer](nonControllerServers(1), controller)

    val supportedFeatures = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5))))
    updateSupportedFeatures(supportedFeatures, versionCompatibleBrokers)

    val supportedFeaturesWithVersionIncompatibility = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new SupportedVersionRange(1, 2)),
        Utils.mkEntry("feature_2", supportedFeatures.get("feature_2"))))
    updateSupportedFeatures(supportedFeaturesWithVersionIncompatibility, brokersWithVersionIncompatibility)

    val initialFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 4))))
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

    // Below we aim to do the following:
    // - Invalid upgrade of feature_1 maxVersionLevel from 2 to 3
    //   (because one of the brokers does not support the max version: 3)
    // - Valid downgrade of feature_2 maxVersionLevel from 4 to 3
    val targetFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 3))))
    val invalidUpdate = new FeatureUpdate(targetFinalizedFeatures.get("feature_1").max(), false)
    val validUpdate = new FeatureUpdate(targetFinalizedFeatures.get("feature_2").max(), true)

    val adminClient = createAdminClient()
    val result = adminClient.updateFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", invalidUpdate), Utils.mkEntry("feature_2", validUpdate)),
      new UpdateFeaturesOptions())

    // Expect update for "feature_2" to have succeeded.
    result.values().get("feature_2").get()
    // Expect update for "feature_1" to have failed.
    checkException[InvalidRequestException](result, Map("feature_1" -> ".*brokers.*incompatible.*".r))
    val expectedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", initialFinalizedFeatures.get("feature_1")),
        Utils.mkEntry("feature_2", targetFinalizedFeatures.get("feature_2"))))
    checkFeatures(
      adminClient,
      FeatureZNode(FeatureZNodeStatus.Enabled, expectedFeatures),
      expectedFeatures,
      versionBefore + 1,
      supportedFeatures)
  }
}
