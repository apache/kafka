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
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKeyCollection
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{UpdateFeaturesRequest, UpdateFeaturesResponse}
import org.apache.kafka.common.utils.Utils
import org.junit.Test
import org.junit.Assert.{assertEquals, assertFalse, assertNotEquals, assertNotNull, assertTrue}
import org.scalatest.Assertions.{assertThrows, intercept}

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

  private def updateDefaultMinVersionLevelsInAllBrokers(newMinVersionLevels: Map[String, Short]): Unit = {
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

  private def checkException[ExceptionType <: Throwable](result: UpdateFeaturesResult,
                                                         featureExceptionMsgPatterns: Map[String, Regex])
                                                        (implicit tag: ClassTag[ExceptionType]): Unit = {
    featureExceptionMsgPatterns.foreach {
      case (feature, exceptionMsgPattern) =>
        val exception = intercept[ExecutionException] {
          result.values().get(feature).get()
        }
        val cause = exception.getCause
        assertNotNull(cause)
        assertEquals(cause.getClass, tag.runtimeClass)
        assertTrue(cause.getMessage, exceptionMsgPattern.findFirstIn(cause.getMessage).isDefined)
    }

  }

  /**
   * Tests whether an invalid feature update does not get processed on the server as expected,
   * and raises the ExceptionType on the client side as expected.
   *
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
      new FeatureMetadata(defaultFinalizedFeatures(), versionBefore, defaultSupportedFeatures()))
  }

  @Test
  def testShouldFailRequestIfNotController(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures())
    val versionBefore = updateFeatureZNode(defaultFinalizedFeatures())

    val nodeBefore = getFeatureZNode()
    val updates = new FeatureUpdateKeyCollection()
    val update = new UpdateFeaturesRequestData.FeatureUpdateKey();
    update.setFeature("feature_1");
    update.setMaxVersionLevel(defaultSupportedFeatures().get("feature_1").max())
    update.setAllowDowngrade(false)
    updates.add(update)

    val response = connectAndReceive[UpdateFeaturesResponse](
      new UpdateFeaturesRequest.Builder(new UpdateFeaturesRequestData().setFeatureUpdates(updates)).build(),
      notControllerSocketServer)

    assertEquals(1, response.data.results.size)
    val result = response.data.results.asScala.head
    assertEquals("feature_1", result.feature)
    assertEquals(Errors.NOT_CONTROLLER, Errors.forCode(result.errorCode))
    assertNotNull(result.errorMessage)
    assertFalse(result.errorMessage.isEmpty)
    checkFeatures(
      createAdminClient(),
      nodeBefore,
      new FeatureMetadata(defaultFinalizedFeatures(), versionBefore, defaultSupportedFeatures()))
  }

  @Test
  def testShouldFailRequestForEmptyUpdates(): Unit = {
    val nullMap: util.Map[String, FeatureUpdate] = null
    val emptyMap: util.Map[String, FeatureUpdate] = Utils.mkMap()
    Set(nullMap, emptyMap).foreach { updates =>
      val client = createAdminClient()
      val exception = intercept[IllegalArgumentException] {
        client.updateFeatures(updates, new UpdateFeaturesOptions())
      }
      assertNotNull(exception)
      assertEquals("Feature updates can not be null or empty.", exception.getMessage)
    }
  }

  @Test
  def testShouldFailRequestForNullUpdateFeaturesOptions(): Unit = {
    val client = createAdminClient()
    val update = new FeatureUpdate(defaultSupportedFeatures().get("feature_1").max(), false)
    val exception = intercept[NullPointerException] {
      client.updateFeatures(Utils.mkMap(Utils.mkEntry("feature_1", update)), null)
    }
    assertNotNull(exception)
    assertEquals("UpdateFeaturesOptions can not be null", exception.getMessage)
  }

  @Test
  def testShouldFailRequestForInvalidFeatureName(): Unit = {
    val client = createAdminClient()
    val update = new FeatureUpdate(defaultSupportedFeatures().get("feature_1").max(), false)
    val exception = intercept[IllegalArgumentException] {
      client.updateFeatures(Utils.mkMap(Utils.mkEntry("", update)), new UpdateFeaturesOptions())
    }
    assertNotNull(exception)
    assertTrue((".*Provided feature can not be null or empty.*"r).findFirstIn(exception.getMessage).isDefined)
  }

  @Test
  def testShouldFailRequestWhenDowngradeFlagIsNotSetDuringDowngrade(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](
      "feature_1",
      new FeatureUpdate((defaultFinalizedFeatures().get("feature_1").max() - 1).asInstanceOf[Short],false),
      ".*Can not downgrade finalized feature: 'feature_1'.*allowDowngrade.*".r)
  }

  @Test
  def testShouldFailRequestWhenDowngradeToHigherVersionLevelIsAttempted(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](
      "feature_1",
      new FeatureUpdate(defaultSupportedFeatures().get("feature_1").max(), true),
      ".*finalized feature: 'feature_1'.*allowDowngrade.* provided maxVersionLevel:3.*existing maxVersionLevel:2.*".r)
  }

  @Test
  def testShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion(): Unit = {
    assertThrows[IllegalArgumentException] {
      new FeatureUpdate(0, false)
    }
  }

  @Test
  def testShouldFailRequestInServerWhenDowngradeFlagIsNotSetDuringDeletion(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures())
    val versionBefore = updateFeatureZNode(defaultFinalizedFeatures())

    val adminClient = createAdminClient()
    val nodeBefore = getFeatureZNode()

    val updates
      = new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();
    val update = new UpdateFeaturesRequestData.FeatureUpdateKey();
    update.setFeature("feature_1")
    update.setMaxVersionLevel(0)
    update.setAllowDowngrade(false)
    updates.add(update);
    val requestData = new UpdateFeaturesRequestData()
    requestData.setFeatureUpdates(updates);

    val response = connectAndReceive[UpdateFeaturesResponse](
      new UpdateFeaturesRequest.Builder(new UpdateFeaturesRequestData().setFeatureUpdates(updates)).build(),
      controllerSocketServer)

    assertEquals(1, response.data().results().size())
    val result = response.data.results.asScala.head
    assertEquals("feature_1", result.feature)
    assertEquals(Errors.INVALID_REQUEST, Errors.forCode(result.errorCode))
    assertNotNull(result.errorMessage)
    assertFalse(result.errorMessage.isEmpty)
    val exceptionMsgPattern = ".*Can not provide maxVersionLevel: 0 less than 1 for feature: 'feature_1'.*allowDowngrade.*".r
    assertTrue(exceptionMsgPattern.findFirstIn(result.errorMessage).isDefined)
    checkFeatures(
      adminClient,
      nodeBefore,
      new FeatureMetadata(defaultFinalizedFeatures(), versionBefore, defaultSupportedFeatures()))
  }

  @Test
  def testShouldFailRequestDuringDeletionOfNonExistingFeature(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](
      "feature_non_existing",
      new FeatureUpdate(0, true),
      ".*Can not delete non-existing finalized feature: 'feature_non_existing'.*".r)
  }

  @Test
  def testShouldFailRequestWhenUpgradingToSameVersionLevel(): Unit = {
    testWithInvalidFeatureUpdate[InvalidRequestException](
      "feature_1",
      new FeatureUpdate(defaultFinalizedFeatures().get("feature_1").max(), false),
      ".*Can not upgrade a finalized feature: 'feature_1'.*to the same value.*".r)
  }

  @Test
  def testShouldFailRequestWhenDowngradingBelowMinVersionLevel(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeaturesInAllBrokers(defaultSupportedFeatures())
    val minVersionLevel = 2.asInstanceOf[Short]
    updateDefaultMinVersionLevelsInAllBrokers(Map[String, Short]("feature_1" -> minVersionLevel))
    val initialFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", new FinalizedVersionRange(minVersionLevel, 2))))
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

    val update = new FeatureUpdate((minVersionLevel - 1).asInstanceOf[Short], true)
    val adminClient = createAdminClient()
    val nodeBefore = getFeatureZNode()

    val result = adminClient.updateFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", update)), new UpdateFeaturesOptions())

    checkException[InvalidRequestException](
      result,
      Map("feature_1" -> ".*Can not downgrade finalized feature: 'feature_1' to maxVersionLevel:1.*existing minVersionLevel:2.*".r))
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
    val unsupportedBrokers = Set[KafkaServer](nonControllerServers.head)
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

    val invalidUpdate = new FeatureUpdate(defaultSupportedFeatures().get("feature_1").max(), false)
    val nodeBefore = getFeatureZNode()
    val adminClient = createAdminClient()
    val result = adminClient.updateFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", invalidUpdate)),
      new UpdateFeaturesOptions())

    checkException[InvalidRequestException](result, Map("feature_1" -> ".*1 broker.*incompatible.*".r))
    checkFeatures(
      adminClient,
      nodeBefore,
      new FeatureMetadata(defaultFinalizedFeatures(), versionBefore, defaultSupportedFeatures()))
  }

  @Test
  def testSuccessfulFeatureUpgradeAndWithNoExistingFinalizedFeatures(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeaturesInAllBrokers(
      Features.supportedFeatures(
        Utils.mkMap(
          Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
          Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5)))))
    updateDefaultMinVersionLevelsInAllBrokers(Map[String, Short]("feature_1" -> 1, "feature_2" -> 2))
    val versionBefore = updateFeatureZNode(Features.emptyFinalizedFeatures())

    val targetFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 3))))
    val update1 = new FeatureUpdate(targetFinalizedFeatures.get("feature_1").max(), false)
    val update2 = new FeatureUpdate(targetFinalizedFeatures.get("feature_2").max(), false)

    val expected = new FeatureMetadata(
      targetFinalizedFeatures,
      versionBefore + 1,
      Features.supportedFeatures(
        Utils.mkMap(
          Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
          Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5)))))

    val adminClient = createAdminClient()
    adminClient.updateFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", update1), Utils.mkEntry("feature_2", update2)),
      new UpdateFeaturesOptions()
    ).all().get()

    checkFeatures(
      adminClient,
      new FeatureZNode(FeatureZNodeStatus.Enabled, targetFinalizedFeatures),
      expected)
  }

  @Test
  def testSuccessfulFeatureUpgradeAndDowngrade(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    updateSupportedFeaturesInAllBrokers(
      Features.supportedFeatures(
        Utils.mkMap(
          Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
          Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5)))))
    updateDefaultMinVersionLevelsInAllBrokers(Map[String, Short]("feature_1" -> 1, "feature_2" -> 2))
    val versionBefore = updateFeatureZNode(
      Features.finalizedFeatures(
        Utils.mkMap(
          Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2)),
          Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 4)))))

    val targetFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 3))))
    val update1 = new FeatureUpdate(targetFinalizedFeatures.get("feature_1").max(), false)
    val update2 = new FeatureUpdate(targetFinalizedFeatures.get("feature_2").max(), true)

    val expected = new FeatureMetadata(
      targetFinalizedFeatures,
      versionBefore + 1,
      Features.supportedFeatures(
        Utils.mkMap(
          Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
          Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5)))))

    val adminClient = createAdminClient()
    adminClient.updateFeatures(
      Utils.mkMap(Utils.mkEntry("feature_1", update1), Utils.mkEntry("feature_2", update2)),
      new UpdateFeaturesOptions()
    ).all().get()

    checkFeatures(
      adminClient,
      new FeatureZNode(FeatureZNodeStatus.Enabled, targetFinalizedFeatures),
      expected)
  }

  @Test
  def testPartialSuccessDuringValidFeatureUpgradeAndInvalidDowngrade(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val initialSupportedFeatures = Features.supportedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new SupportedVersionRange(1, 3)),
        Utils.mkEntry("feature_2", new SupportedVersionRange(2, 5))))
    updateSupportedFeaturesInAllBrokers(initialSupportedFeatures)
    updateDefaultMinVersionLevelsInAllBrokers(Map[String, Short]("feature_1" -> 1, "feature_2" -> 2))
    val initialFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 4))))
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

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
      result, Map("feature_2" -> ".*Can not downgrade finalized feature: 'feature_2'.*allowDowngrade.*".r))
    val expectedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", targetFinalizedFeatures.get("feature_1")),
        Utils.mkEntry("feature_2", initialFinalizedFeatures.get("feature_2"))))
    checkFeatures(
      adminClient,
      FeatureZNode(FeatureZNodeStatus.Enabled, expectedFeatures),
      new FeatureMetadata(expectedFeatures, versionBefore + 1, initialSupportedFeatures))
  }

  @Test
  def testPartialSuccessDuringInvalidFeatureUpgradeAndValidDowngrade(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)

    val controller = servers.filter { server => server.kafkaController.isActive}.head
    val nonControllerServers = servers.filter { server => !server.kafkaController.isActive}
    val unsupportedBrokers = Set[KafkaServer](nonControllerServers.head)
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

    updateDefaultMinVersionLevelsInAllBrokers(Map[String, Short]("feature_1" -> 1, "feature_2" -> 2))

    val initialFinalizedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", new FinalizedVersionRange(1, 2)),
        Utils.mkEntry("feature_2", new FinalizedVersionRange(2, 4))))
    val versionBefore = updateFeatureZNode(initialFinalizedFeatures)

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
    checkException[InvalidRequestException](result, Map("feature_1" -> ".*1 broker.*incompatible.*".r))
    val expectedFeatures = Features.finalizedFeatures(
      Utils.mkMap(
        Utils.mkEntry("feature_1", initialFinalizedFeatures.get("feature_1")),
        Utils.mkEntry("feature_2", targetFinalizedFeatures.get("feature_2"))))
    checkFeatures(
      adminClient,
      FeatureZNode(FeatureZNodeStatus.Enabled, expectedFeatures),
      new FeatureMetadata(expectedFeatures, versionBefore + 1, initialSupportedFeatures))
  }
}
