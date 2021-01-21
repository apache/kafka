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

import java.util.Properties
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

abstract class AbstractApiVersionsRequestTest extends BaseRequestTest {

  def controlPlaneListenerName = new ListenerName("CONTROLLER")

  // Configure control plane listener to make sure we have separate listeners for testing.
  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.ControlPlaneListenerNameProp, controlPlaneListenerName.value())
    properties.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, s"${controlPlaneListenerName.value()}:$securityProtocol,$securityProtocol:$securityProtocol")
    properties.setProperty("listeners", s"$securityProtocol://localhost:0,${controlPlaneListenerName.value()}://localhost:0")
    properties.setProperty(KafkaConfig.AdvertisedListenersProp, s"$securityProtocol://localhost:0,${controlPlaneListenerName.value()}://localhost:0")
  }

  def sendUnsupportedApiVersionRequest(request: ApiVersionsRequest): ApiVersionsResponse = {
    val overrideHeader = nextRequestHeader(ApiKeys.API_VERSIONS, Short.MaxValue)
    val socket = connect(anySocketServer)
    try {
      sendWithHeader(request, overrideHeader, socket)
      receive[ApiVersionsResponse](socket, ApiKeys.API_VERSIONS, 0.toShort)
    } finally socket.close()
  }

  def validateApiVersionsResponse(apiVersionsResponse: ApiVersionsResponse, listenerName: ListenerName = interBrokerListenerName): Unit = {
    val expectedApis = ApiKeys.brokerApis()
    if (listenerName == controlPlaneListenerName) {
      expectedApis.add(ApiKeys.ENVELOPE)
    }
    assertEquals(expectedApis.size(), apiVersionsResponse.data.apiKeys().size(),
      "API keys in ApiVersionsResponse must match API keys supported by broker.")
    for (expectedApiVersion: ApiVersion <- ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.apiKeys().asScala) {
      val actualApiVersion = apiVersionsResponse.apiVersion(expectedApiVersion.apiKey)
      assertNotNull(actualApiVersion, s"API key ${actualApiVersion.apiKey} is supported by broker, but not received in ApiVersionsResponse.")
      assertEquals(expectedApiVersion.apiKey, actualApiVersion.apiKey, "API key must be supported by the broker.")
      assertEquals(expectedApiVersion.minVersion, actualApiVersion.minVersion, s"Received unexpected min version for API key ${actualApiVersion.apiKey}.")
      assertEquals(expectedApiVersion.maxVersion, actualApiVersion.maxVersion, s"Received unexpected max version for API key ${actualApiVersion.apiKey}.")
    }
  }
}
