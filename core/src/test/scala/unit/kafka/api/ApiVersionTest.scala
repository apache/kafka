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

package kafka.api

import java.util

import org.apache.kafka.common.feature.{Features, FinalizedVersionRange, SupportedVersionRange}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.{RecordBatch, RecordVersion}
import org.apache.kafka.common.requests.{AbstractResponse, ApiVersionsResponse}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class ApiVersionTest {

  @Test
  def testApply(): Unit = {
    assertEquals(KAFKA_0_8_0, ApiVersion("0.8.0"))
    assertEquals(KAFKA_0_8_0, ApiVersion("0.8.0.0"))
    assertEquals(KAFKA_0_8_0, ApiVersion("0.8.0.1"))

    assertEquals(KAFKA_0_8_1, ApiVersion("0.8.1"))
    assertEquals(KAFKA_0_8_1, ApiVersion("0.8.1.0"))
    assertEquals(KAFKA_0_8_1, ApiVersion("0.8.1.1"))

    assertEquals(KAFKA_0_8_2, ApiVersion("0.8.2"))
    assertEquals(KAFKA_0_8_2, ApiVersion("0.8.2.0"))
    assertEquals(KAFKA_0_8_2, ApiVersion("0.8.2.1"))

    assertEquals(KAFKA_0_9_0, ApiVersion("0.9.0"))
    assertEquals(KAFKA_0_9_0, ApiVersion("0.9.0.0"))
    assertEquals(KAFKA_0_9_0, ApiVersion("0.9.0.1"))

    assertEquals(KAFKA_0_10_0_IV0, ApiVersion("0.10.0-IV0"))

    assertEquals(KAFKA_0_10_0_IV1, ApiVersion("0.10.0"))
    assertEquals(KAFKA_0_10_0_IV1, ApiVersion("0.10.0.0"))
    assertEquals(KAFKA_0_10_0_IV1, ApiVersion("0.10.0.0-IV0"))
    assertEquals(KAFKA_0_10_0_IV1, ApiVersion("0.10.0.1"))

    assertEquals(KAFKA_0_10_1_IV0, ApiVersion("0.10.1-IV0"))
    assertEquals(KAFKA_0_10_1_IV1, ApiVersion("0.10.1-IV1"))

    assertEquals(KAFKA_0_10_1_IV2, ApiVersion("0.10.1"))
    assertEquals(KAFKA_0_10_1_IV2, ApiVersion("0.10.1.0"))
    assertEquals(KAFKA_0_10_1_IV2, ApiVersion("0.10.1-IV2"))
    assertEquals(KAFKA_0_10_1_IV2, ApiVersion("0.10.1.1"))

    assertEquals(KAFKA_0_10_2_IV0, ApiVersion("0.10.2"))
    assertEquals(KAFKA_0_10_2_IV0, ApiVersion("0.10.2.0"))
    assertEquals(KAFKA_0_10_2_IV0, ApiVersion("0.10.2-IV0"))
    assertEquals(KAFKA_0_10_2_IV0, ApiVersion("0.10.2.1"))

    assertEquals(KAFKA_0_11_0_IV0, ApiVersion("0.11.0-IV0"))
    assertEquals(KAFKA_0_11_0_IV1, ApiVersion("0.11.0-IV1"))

    assertEquals(KAFKA_0_11_0_IV2, ApiVersion("0.11.0"))
    assertEquals(KAFKA_0_11_0_IV2, ApiVersion("0.11.0.0"))
    assertEquals(KAFKA_0_11_0_IV2, ApiVersion("0.11.0-IV2"))
    assertEquals(KAFKA_0_11_0_IV2, ApiVersion("0.11.0.1"))

    assertEquals(KAFKA_1_0_IV0, ApiVersion("1.0"))
    assertEquals(KAFKA_1_0_IV0, ApiVersion("1.0.0"))
    assertEquals(KAFKA_1_0_IV0, ApiVersion("1.0.0-IV0"))
    assertEquals(KAFKA_1_0_IV0, ApiVersion("1.0.1"))

    assertEquals(KAFKA_1_1_IV0, ApiVersion("1.1-IV0"))

    assertEquals(KAFKA_2_0_IV1, ApiVersion("2.0"))
    assertEquals(KAFKA_2_0_IV0, ApiVersion("2.0-IV0"))
    assertEquals(KAFKA_2_0_IV1, ApiVersion("2.0-IV1"))

    assertEquals(KAFKA_2_1_IV2, ApiVersion("2.1"))
    assertEquals(KAFKA_2_1_IV0, ApiVersion("2.1-IV0"))
    assertEquals(KAFKA_2_1_IV1, ApiVersion("2.1-IV1"))
    assertEquals(KAFKA_2_1_IV2, ApiVersion("2.1-IV2"))

    assertEquals(KAFKA_2_2_IV1, ApiVersion("2.2"))
    assertEquals(KAFKA_2_2_IV0, ApiVersion("2.2-IV0"))
    assertEquals(KAFKA_2_2_IV1, ApiVersion("2.2-IV1"))

    assertEquals(KAFKA_2_3_IV1, ApiVersion("2.3"))
    assertEquals(KAFKA_2_3_IV0, ApiVersion("2.3-IV0"))
    assertEquals(KAFKA_2_3_IV1, ApiVersion("2.3-IV1"))

    assertEquals(KAFKA_2_4_IV1, ApiVersion("2.4"))
    assertEquals(KAFKA_2_4_IV0, ApiVersion("2.4-IV0"))
    assertEquals(KAFKA_2_4_IV1, ApiVersion("2.4-IV1"))

    assertEquals(KAFKA_2_5_IV0, ApiVersion("2.5"))
    assertEquals(KAFKA_2_5_IV0, ApiVersion("2.5-IV0"))

    assertEquals(KAFKA_2_6_IV0, ApiVersion("2.6"))
    assertEquals(KAFKA_2_6_IV0, ApiVersion("2.6-IV0"))

    assertEquals(KAFKA_2_7_IV0, ApiVersion("2.7-IV0"))
    assertEquals(KAFKA_2_7_IV1, ApiVersion("2.7-IV1"))
    assertEquals(KAFKA_2_7_IV2, ApiVersion("2.7-IV2"))

    assertEquals(KAFKA_2_8_IV1, ApiVersion("2.8"))
    assertEquals(KAFKA_2_8_IV0, ApiVersion("2.8-IV0"))
    assertEquals(KAFKA_2_8_IV1, ApiVersion("2.8-IV1"))
  }

  @Test
  def testApiVersionUniqueIds(): Unit = {
    val allIds: Seq[Int] = ApiVersion.allVersions.map(apiVersion => {
      apiVersion.id
    })

    val uniqueIds: Set[Int] = allIds.toSet

    assertEquals(allIds.size, uniqueIds.size)
  }

  @Test
  def testMinSupportedVersionFor(): Unit = {
    assertEquals(KAFKA_0_8_0, ApiVersion.minSupportedFor(RecordVersion.V0))
    assertEquals(KAFKA_0_10_0_IV0, ApiVersion.minSupportedFor(RecordVersion.V1))
    assertEquals(KAFKA_0_11_0_IV0, ApiVersion.minSupportedFor(RecordVersion.V2))

    // Ensure that all record versions have a defined min version so that we remember to update the method
    for (recordVersion <- RecordVersion.values)
      assertNotNull(ApiVersion.minSupportedFor(recordVersion))
  }

  @Test
  def testShortVersion(): Unit = {
    assertEquals("0.8.0", KAFKA_0_8_0.shortVersion)
    assertEquals("0.10.0", KAFKA_0_10_0_IV0.shortVersion)
    assertEquals("0.10.0", KAFKA_0_10_0_IV1.shortVersion)
    assertEquals("0.11.0", KAFKA_0_11_0_IV0.shortVersion)
    assertEquals("0.11.0", KAFKA_0_11_0_IV1.shortVersion)
    assertEquals("0.11.0", KAFKA_0_11_0_IV2.shortVersion)
    assertEquals("1.0", KAFKA_1_0_IV0.shortVersion)
    assertEquals("1.1", KAFKA_1_1_IV0.shortVersion)
    assertEquals("2.0", KAFKA_2_0_IV0.shortVersion)
    assertEquals("2.0", KAFKA_2_0_IV1.shortVersion)
    assertEquals("2.1", KAFKA_2_1_IV0.shortVersion)
    assertEquals("2.1", KAFKA_2_1_IV1.shortVersion)
    assertEquals("2.1", KAFKA_2_1_IV2.shortVersion)
    assertEquals("2.2", KAFKA_2_2_IV0.shortVersion)
    assertEquals("2.2", KAFKA_2_2_IV1.shortVersion)
    assertEquals("2.3", KAFKA_2_3_IV0.shortVersion)
    assertEquals("2.3", KAFKA_2_3_IV1.shortVersion)
    assertEquals("2.4", KAFKA_2_4_IV0.shortVersion)
    assertEquals("2.5", KAFKA_2_5_IV0.shortVersion)
    assertEquals("2.6", KAFKA_2_6_IV0.shortVersion)
    assertEquals("2.7", KAFKA_2_7_IV2.shortVersion)
    assertEquals("2.8", KAFKA_2_8_IV0.shortVersion)
  }

  @Test
  def testApiVersionValidator(): Unit = {
    val str = ApiVersionValidator.toString
    val apiVersions = str.slice(1, str.length).split(",")
    assertEquals(ApiVersion.allVersions.size, apiVersions.length)
  }

  @Test
  def shouldCreateApiResponseOnlyWithKeysSupportedByMagicValue(): Unit = {
    val response = ApiVersion.apiVersionsResponse(
      10,
      RecordBatch.MAGIC_VALUE_V1,
      Features.emptySupportedFeatures,
      None
    )
    verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1)
    assertEquals(10, response.throttleTimeMs)
    assertTrue(response.data.supportedFeatures.isEmpty)
    assertTrue(response.data.finalizedFeatures.isEmpty)
    assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data.finalizedFeaturesEpoch)
  }

  @Test
  def shouldReturnFeatureKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle(): Unit = {
    val response = ApiVersion.apiVersionsResponse(
      10,
      RecordBatch.MAGIC_VALUE_V1,
      Features.supportedFeatures(
        Utils.mkMap(Utils.mkEntry("feature", new SupportedVersionRange(1.toShort, 4.toShort)))),
      Features.finalizedFeatures(
        Utils.mkMap(Utils.mkEntry("feature", new FinalizedVersionRange(2.toShort, 3.toShort)))),
      10,
      None
    )

    verifyApiKeysForMagic(response, RecordBatch.MAGIC_VALUE_V1)
    assertEquals(10, response.throttleTimeMs)
    assertEquals(1, response.data.supportedFeatures.size)
    val sKey = response.data.supportedFeatures.find("feature")
    assertNotNull(sKey)
    assertEquals(1, sKey.minVersion)
    assertEquals(4, sKey.maxVersion)
    assertEquals(1, response.data.finalizedFeatures.size)
    val fKey = response.data.finalizedFeatures.find("feature")
    assertNotNull(fKey)
    assertEquals(2, fKey.minVersionLevel)
    assertEquals(3, fKey.maxVersionLevel)
    assertEquals(10, response.data.finalizedFeaturesEpoch)
  }

  private def verifyApiKeysForMagic(response: ApiVersionsResponse, maxMagic: Byte): Unit = {
    for (version <- response.data.apiKeys.asScala) {
      assertTrue(ApiKeys.forId(version.apiKey).minRequiredInterBrokerMagic <= maxMagic)
    }
  }

  @Test
  def shouldReturnAllKeysWhenMagicIsCurrentValueAndThrottleMsIsDefaultThrottle(): Unit = {
    val response = ApiVersion.apiVersionsResponse(
      AbstractResponse.DEFAULT_THROTTLE_TIME,
      RecordBatch.CURRENT_MAGIC_VALUE,
      Features.emptySupportedFeatures,
      None
    )
    assertEquals(new util.HashSet[ApiKeys](ApiKeys.brokerApis), apiKeysInResponse(response))
    assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs)
    assertTrue(response.data.supportedFeatures.isEmpty)
    assertTrue(response.data.finalizedFeatures.isEmpty)
    assertEquals(ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH, response.data.finalizedFeaturesEpoch)
  }

  @Test
  def testMetadataQuorumApisAreDisabled(): Unit = {
    val response = ApiVersion.apiVersionsResponse(
      AbstractResponse.DEFAULT_THROTTLE_TIME,
      RecordBatch.CURRENT_MAGIC_VALUE,
      Features.emptySupportedFeatures,
      None
    )

    // Ensure that APIs needed for the internal metadata quorum (KIP-500)
    // are not exposed through ApiVersions until we are ready for them
    val exposedApis = apiKeysInResponse(response)
    assertFalse(exposedApis.contains(ApiKeys.ENVELOPE))
    assertFalse(exposedApis.contains(ApiKeys.VOTE))
    assertFalse(exposedApis.contains(ApiKeys.BEGIN_QUORUM_EPOCH))
    assertFalse(exposedApis.contains(ApiKeys.END_QUORUM_EPOCH))
    assertFalse(exposedApis.contains(ApiKeys.DESCRIBE_QUORUM))
  }

  private def apiKeysInResponse(apiVersions: ApiVersionsResponse) = {
    val apiKeys = new util.HashSet[ApiKeys]
    for (version <- apiVersions.data.apiKeys.asScala) {
      apiKeys.add(ApiKeys.forId(version.apiKey))
    }
    apiKeys
  }
}
