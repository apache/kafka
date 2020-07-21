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

import java.util
import java.util.Arrays
import java.util.Collections
import java.util.HashSet
import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.api.KAFKA_2_7_IV0
import kafka.utils.TestUtils
import kafka.zk.{FeatureZNode, FeatureZNodeStatus, ZkVersion}
import kafka.utils.TestUtils.waitUntilTrue
import org.apache.kafka.clients.admin.{Admin, DescribeFeaturesOptions, FeatureMetadata, FeatureUpdate, UpdateFeaturesOptions, UpdateFeaturesResult}
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.feature.FinalizedVersionRange
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.message.UpdateFeaturesRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{UpdateFeaturesRequest, UpdateFeaturesResponse}
import org.apache.kafka.common.utils.Utils
import org.junit.Test
import org.junit.Assert.{assertEquals, assertNotEquals, assertNotNull, assertTrue}
import org.scalatest.Assertions.{assertThrows, intercept}

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

  private def updateSupportedFeatures(features: Features[SupportedVersionRange]): Unit = {
    updateSupportedFeatures(features, Set[KafkaServer]() ++ servers)
  }

  private def updateDefaultMinVersionLevels(newMinVersionLevels: Map[String, Short]): Unit = {
    servers.foreach(s => {
      s.brokerFeatures.setDefaultMinVersionLevels(newMinVersionLevels)
    })
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

  private def checkFeatures(client: Admin, expectedNode: FeatureZNode, expectedMetadata: FeatureMetadata): Unit = {
    assertEquals(expectedNode, getFeatureZNode())
    val featureMetadata = client.describeFeatures(
      new DescribeFeaturesOptions().sendRequestToController(true)).featureMetadata().get()
    assertEquals(expectedMetadata, featureMetadata)
  }

  private def checkException[ExceptionType <: Throwable](
                                                          result: UpdateFeaturesResult,
                                                          exceptionMsgPattern: Regex
  )(implicit tag: ClassTag[ExceptionType]): Unit = {
    val exception = intercept[ExecutionException] {
      result.result().get()
    }
    assertNotNull(exception.getCause)
    assertEquals(exception.getCause.getClass, tag.runtimeClass)
    assertTrue(exceptionMsgPattern.findFirstIn(exception.getCause.getMessage).isDefined)
  }

  /**
   * Tests whether an invalid feature update does not get processed on the server as expected,
   * and raises the ExceptionType on the client side as expected.
   *
   * @param invalidUpdate         the invalid feature update to be sent in the
   *                              updateFeatures request to the server
   * @param exceptionMsgPattern   a pattern for the expected exception message
   */
  private def testWithInvalidFeatureUpdate[ExceptionType <: Throwable](
                                                                        invalidUpdate: FeatureUpdate,
                                                                        exceptionMsgPattern: Regex
  )(implicit tag: ClassTag[ExceptionType]): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeatures(defaultSupportedFeatures())
    val versionBefore = updateFeatureZNode(defaultFinalizedFeatures())

    val adminClient = createAdminClient()
    val nodeBefore = getFeatureZNode()

    val result = adminClient.updateFeatures(
      new HashSet[FeatureUpdate](Collections.singletonList(invalidUpdate)), new UpdateFeaturesOptions())

    checkException[ExceptionType](result, exceptionMsgPattern)
    checkFeatures(
      adminClient,
      nodeBefore,
      new FeatureMetadata(defaultFinalizedFeatures(), versionBefore, defaultSupportedFeatures()))
  }

  @Test
  def testShouldFailRequestIfNotController(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeatures(defaultSupportedFeatures())
    val versionBefore = updateFeatureZNode(defaultFinalizedFeatures())

    val nodeBefore = getFeatureZNode()
    val requestData = FeatureUpdate.createRequest(
      new util.HashSet[FeatureUpdate](
        Collections.singletonList(new FeatureUpdate("feature_1",
          defaultSupportedFeatures().get("feature_1").max(),
        false))))

    val response = connectAndReceive[UpdateFeaturesResponse](
      new UpdateFeaturesRequest.Builder(requestData).build(), notControllerSocketServer)

    assertEquals(Errors.NOT_CONTROLLER, response.error())
    checkFeatures(
      createAdminClient(),
      nodeBefore,
      new FeatureMetadata(defaultFinalizedFeatures(), versionBefore, defaultSupportedFeatures()))
  }

  @Test
  def testShouldFailRequestForInvalidFeatureName(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](new FeatureUpdate(
      "",
      defaultSupportedFeatures().get("feature_1").max(),
      false),
      ".*empty feature name.*".r)
  }

  @Test
  def testShouldFailRequestWhenDowngradeFlagIsNotSetDuringDowngrade(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](new FeatureUpdate(
      "feature_1",
      (defaultFinalizedFeatures().get("feature_1").max() - 1).asInstanceOf[Short],
      false),
      ".*Can not downgrade finalized feature: 'feature_1'.*allowDowngrade.*".r)
  }

  @Test
  def testShouldFailRequestWhenDowngradeToHigherVersionLevelIsAttempted(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](
      new FeatureUpdate(
        "feature_1",
        defaultSupportedFeatures().get("feature_1").max(),
        true),
      ".*finalized feature: 'feature_1'.*allowDowngrade.* provided maxVersionLevel:3.*existing maxVersionLevel:2.*".r)
  }

  @Test
  def testShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion(): Unit = {
    assertThrows[IllegalArgumentException] {
      new FeatureUpdate("feature_1", 0, false)
    }
  }

  @Test
  def testShouldFailRequestInServerWhenDowngradeFlagIsNotSetDuringDeletion(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeatures(defaultSupportedFeatures())
    val versionBefore = updateFeatureZNode(defaultFinalizedFeatures())

    val adminClient = createAdminClient()
    val nodeBefore = getFeatureZNode()

    val featureUpdates
      = new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();
    val featureUpdate = new UpdateFeaturesRequestData.FeatureUpdateKey();
    featureUpdate.setName("feature_1")
    featureUpdate.setMaxVersionLevel(0)
    featureUpdate.setAllowDowngrade(false)
    featureUpdates.add(featureUpdate);
    val requestData = new UpdateFeaturesRequestData()
    requestData.setFeatureUpdates(featureUpdates);

    val response = connectAndReceive[UpdateFeaturesResponse](
      new UpdateFeaturesRequest.Builder(requestData).build(), controllerSocketServer)

    assertEquals(Errors.INVALID_REQUEST, response.error)
    val exceptionMsgPattern = ".*Can not delete feature: 'feature_1'.*allowDowngrade.*".r
    assertTrue(exceptionMsgPattern.findFirstIn(response.data.errorMessage).isDefined)
    checkFeatures(
      adminClient,
      nodeBefore,
      new FeatureMetadata(defaultFinalizedFeatures(), versionBefore, defaultSupportedFeatures()))
  }

  @Test
  def testShouldFailRequestDuringDeletionOfNonExistingFeature(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](
      new FeatureUpdate("feature_non_existing", 0, true),
      ".*Can not delete non-existing finalized feature: 'feature_non_existing'.*".r)
  }

  @Test
  def testShouldFailRequestWhenUpgradingToSameVersionLevel(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](
      new FeatureUpdate(
        "feature_1", defaultFinalizedFeatures().get("feature_1").max(), false),
      ".*Can not upgrade a finalized feature: 'feature_1'.*to the same value.*".r)
  }

  @Test
  def testShouldFailRequestWhenDowngradingBelowMinVersionLevel(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeatures(defaultSupportedFeatures())
    val minVersionLevel = 2.asInstanceOf[Short]
    updateDefaultMinVersionLevels(Map[String, Short]("feature_1" -> minVersionLevel))
    val initialFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", new FinalizedVersionRange(minVersionLevel, 2))))
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

    val update = new FeatureUpdate(
      "feature_1", (minVersionLevel - 1).asInstanceOf[Short], true)
    val adminClient = createAdminClient()
    val nodeBefore = getFeatureZNode()

    val result = adminClient.updateFeatures(
      new HashSet[FeatureUpdate](Collections.singletonList(update)), new UpdateFeaturesOptions())

    checkException[InvalidRequestException](
      result, ".*Can not downgrade finalized feature: 'feature_1' to maxVersionLevel:1.*existing minVersionLevel:2.*".r)
    checkFeatures(
      adminClient,
      nodeBefore,
      new FeatureMetadata(initialFinalizedFeatures, versionBefore, defaultSupportedFeatures()))
  }

  @Test
  def testShouldFailRequestDuringBrokerMaxVersionLevelIncompatibility(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val controller = servers.filter { server => server.kafkaController.isActive}.head
    val nonControllerServers = servers.filter { server => !server.kafkaController.isActive}
    val unsupportedBrokers = Set[KafkaServer](nonControllerServers(0))
    val supportedBrokers = Set[KafkaServer](nonControllerServers(1), controller)

    updateSupportedFeatures(defaultSupportedFeatures(), supportedBrokers)

    val validMinVersion = defaultSupportedFeatures().get("feature_1").min()
    val unsupportedMaxVersion =
      (defaultSupportedFeatures().get("feature_1").max() - 1).asInstanceOf[Short]
    val badSupportedFeatures = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1",
          new SupportedVersionRange(
            validMinVersion,
            unsupportedMaxVersion))))
    updateSupportedFeatures(badSupportedFeatures, unsupportedBrokers)

    val versionBefore = updateFeatureZNode(defaultFinalizedFeatures())

    val invalidUpdate = new FeatureUpdate(
      "feature_1", defaultSupportedFeatures().get("feature_1").max(), false)
    val nodeBefore = getFeatureZNode()
    val adminClient = createAdminClient()
    val result = adminClient.updateFeatures(
      new HashSet[FeatureUpdate](Collections.singletonList(invalidUpdate)),
      new UpdateFeaturesOptions())

    checkException[InvalidRequestException](
      result, ".*1 broker.*incompatible.*".r)
    checkFeatures(
      adminClient,
      nodeBefore,
      new FeatureMetadata(defaultFinalizedFeatures(), versionBefore, defaultSupportedFeatures()))
  }

  @Test
  def testSuccessFeatureUpgradeAndDowngrade(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeatures(
      Features.supportedFeatures(
        Utils.mkMap(
          Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
          Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5)))))
    updateDefaultMinVersionLevels(Map[String, Short]("feature_1" -> 1, "feature_2" -> 2))
    val versionBefore = updateFeatureZNode(
      Features.finalizedFeatures(
        Utils.mkMap(
          Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2)),
          Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 4)))))

    val targetFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 3))))
    val update1 = new FeatureUpdate(
      "feature_1", targetFinalizedFeatures.get("feature_1").max(), false)
    val update2 = new FeatureUpdate(
      "feature_2", targetFinalizedFeatures.get("feature_2").max(), true)

    val expected = new FeatureMetadata(
      targetFinalizedFeatures,
      versionBefore + 1,
      Features.supportedFeatures(
        Utils.mkMap(
          Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
          Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5)))))

    val adminClient = createAdminClient()
    adminClient.updateFeatures(
      new HashSet[FeatureUpdate](Arrays.asList(update1, update2)),
      new UpdateFeaturesOptions()).result().get()

    checkFeatures(
      adminClient,
      new FeatureZNode(FeatureZNodeStatus.Enabled, targetFinalizedFeatures),
      expected)
  }

  @Test
  def testShouldFailRequestDuringValidFeatureUpgradeAndInvalidDowngrade(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val initialSupportedFeatures = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5))))
    updateSupportedFeatures(initialSupportedFeatures)
    updateDefaultMinVersionLevels(Map[String, Short]("feature_1" -> 1, "feature_2" -> 2))
    val initialFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 4))))
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

    val targetFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 3))))
    val validUpdate = new FeatureUpdate(
      "feature_1", targetFinalizedFeatures.get("feature_1").max(), false)
    val invalidUpdate = new FeatureUpdate(
      "feature_2", targetFinalizedFeatures.get("feature_2").max(), false)

    val nodeBefore = getFeatureZNode()
    val adminClient = createAdminClient()
    val result = adminClient.updateFeatures(
      new HashSet[FeatureUpdate](Arrays.asList(validUpdate, invalidUpdate)),
      new UpdateFeaturesOptions())

    checkException[InvalidRequestException](
      result, ".*Can not downgrade finalized feature: 'feature_2'.*allowDowngrade.*".r)
    checkFeatures(
      adminClient,
      nodeBefore,
      new FeatureMetadata(initialFinalizedFeatures, versionBefore, initialSupportedFeatures))
  }

  @Test
  def testShouldFailRequestDuringInvalidFeatureUpgradeAndValidDowngrade(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val controller = servers.filter { server => server.kafkaController.isActive}.head
    val nonControllerServers = servers.filter { server => !server.kafkaController.isActive}
    val unsupportedBrokers = Set[KafkaServer](nonControllerServers(0))
    val supportedBrokers = Set[KafkaServer](nonControllerServers(1), controller)

    val initialSupportedFeatures = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5))))
    updateSupportedFeatures(initialSupportedFeatures, supportedBrokers)

    val badSupportedFeatures = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new SupportedVersionRange(1, 2)),
        Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5))))
    updateSupportedFeatures(badSupportedFeatures, unsupportedBrokers)

    updateDefaultMinVersionLevels(Map[String, Short]("feature_1" -> 1, "feature_2" -> 2))

    val initialFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 4))))
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

    val targetFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 3))))
    val invalidUpdate = new FeatureUpdate(
      "feature_1", targetFinalizedFeatures.get("feature_1").max(), false)
    val validUpdate = new FeatureUpdate(
      "feature_2", targetFinalizedFeatures.get("feature_2").max(), true)

    val nodeBefore = getFeatureZNode()
    val adminClient = createAdminClient()
    val result = adminClient.updateFeatures(
      new HashSet[FeatureUpdate](Arrays.asList(invalidUpdate, validUpdate)),
      new UpdateFeaturesOptions())

    checkException[InvalidRequestException](result, ".*1 broker.*incompatible.*".r)
    checkFeatures(
      adminClient,
      nodeBefore,
      new FeatureMetadata(initialFinalizedFeatures, versionBefore, initialSupportedFeatures))
  }
}
