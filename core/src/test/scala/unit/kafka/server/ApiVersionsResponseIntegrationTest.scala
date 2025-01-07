/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package kafka.server

import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKeyCollection
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties

class ApiVersionsResponseIntegrationTest extends BaseRequestTest {
  override def brokerCount: Int = 1

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false")
    properties.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
    properties.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1")
  }

  private def sendApiVersionsRequest(version: Short): ApiVersionsResponse = {
    val request = new ApiVersionsRequest.Builder().build(version)
    connectAndReceive[ApiVersionsResponse](request)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testSendV3ApiVersionsRequest(quorum: String): Unit = {
    val response = sendApiVersionsRequest(3)
    if (quorum.equals("kraft")) {
      assertFeatureHasMinVersion("metadata.version", response.data().supportedFeatures(), 1)
      assertFeatureMissing("kraft.version", response.data().supportedFeatures())
    } else {
      assertEquals(0, response.data().supportedFeatures().size())
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testSendV4ApiVersionsRequest(quorum: String): Unit = {
    val response = sendApiVersionsRequest(4)
    if (quorum.equals("kraft")) {
      assertFeatureHasMinVersion("metadata.version", response.data().supportedFeatures(), 1)
      assertFeatureHasMinVersion("kraft.version", response.data().supportedFeatures(), 0)
    } else {
      assertEquals(0, response.data().supportedFeatures().size())
    }
  }

  def assertFeatureHasMinVersion(
    name: String,
    coll: SupportedFeatureKeyCollection,
    expectedMinVersion: Short
  ): Unit = {
    val key = coll.find(name)
    assertNotNull(key)
    assertEquals(name, key.name())
    assertEquals(expectedMinVersion, key.minVersion())
  }

  private def assertFeatureMissing(
    name: String,
    coll: SupportedFeatureKeyCollection,
  ): Unit = {
    val key = coll.find(name)
    assertNull(key)
  }
}
