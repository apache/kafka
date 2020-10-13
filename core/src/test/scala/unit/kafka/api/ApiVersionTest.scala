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

import org.apache.kafka.common.feature.Features
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.{RecordBatch, RecordVersion}
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.junit.Test
import org.junit.Assert._

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

    assertEquals(KAFKA_2_7_IV2, ApiVersion("2.7"))
    assertEquals(KAFKA_2_7_IV0, ApiVersion("2.7-IV0"))
    assertEquals(KAFKA_2_7_IV1, ApiVersion("2.7-IV1"))
    assertEquals(KAFKA_2_7_IV2, ApiVersion("2.7-IV2"))

    assertEquals(KAFKA_2_8_IV0, ApiVersion("2.8"))
    assertEquals(KAFKA_2_8_IV0, ApiVersion("2.8-IV0"))
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
  def testInterBrokerProtocolVersionConstraint(): Unit = {
    val response = ApiVersionsResponse.apiVersionsResponse(
      10,
      ApiVersion.latestVersion.id,
      RecordBatch.MAGIC_VALUE_V2,
      Features.emptySupportedFeatures())
    response.data.apiKeys().forEach(
      version => {
        val apiKeys = ApiKeys.forId(version.apiKey())
        apiKeys match {
          case ApiKeys.ALTER_CONFIGS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 1)
            }

          case ApiKeys.INCREMENTAL_ALTER_CONFIGS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 1)
            }

          case ApiKeys.ALTER_CLIENT_QUOTAS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 0)
            }

          case ApiKeys.CREATE_ACLS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 2)
            }

          case ApiKeys.DELETE_ACLS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 2)
            }

          case ApiKeys.CREATE_DELEGATION_TOKEN =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 2)
            }

          case ApiKeys.RENEW_DELEGATION_TOKEN =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 2)
            }

          case ApiKeys.EXPIRE_DELEGATION_TOKEN =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 2)
            }

          case ApiKeys.ALTER_PARTITION_REASSIGNMENTS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 0)
            }

          case ApiKeys.CREATE_PARTITIONS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 3)
            }

          case ApiKeys.CREATE_TOPICS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 6)
            }

          case ApiKeys.DELETE_TOPICS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 5)
            }

          case ApiKeys.UPDATE_FEATURES =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 0)
            }

          case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS =>
            if (ApiVersion.latestVersion >= KAFKA_2_8_IV0) {
              verifyIBPVersionConstraint(apiKeys, 0)
            }
          case _ =>
        }
      }
    )
  }

  private def verifyIBPVersionConstraint(apiKeys: ApiKeys, expectedVersion: Short): Unit = {
    assertEquals(s"The latest version of RPC $apiKeys does not match " +
      s"expected version $expectedVersion. If you recently " +
      s"bumped this RPC version, you should also bump IBP and update this test correspondingly.",
      expectedVersion, apiKeys.latestVersion())
  }
}

