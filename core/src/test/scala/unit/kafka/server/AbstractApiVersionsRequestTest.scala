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

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

abstract class AbstractApiVersionsRequestTest extends BaseRequestTest {

  def sendUnsupportedApiVersionRequest(request: ApiVersionsRequest): ApiVersionsResponse = {
    val overrideHeader = nextRequestHeader(ApiKeys.API_VERSIONS, Short.MaxValue)
    val socket = connect(anySocketServer)
    try {
      sendWithHeader(request, overrideHeader, socket)
      receive[ApiVersionsResponse](socket, ApiKeys.API_VERSIONS, 0.toShort)
    } finally socket.close()
  }

  def validateApiVersionsResponse(apiVersionsResponse: ApiVersionsResponse): Unit = {
    val enabledPublicApis = ApiKeys.enabledApis()
    assertEquals(enabledPublicApis.size(),
      apiVersionsResponse.data.apiKeys().size(), "API keys in ApiVersionsResponse must match API keys supported by broker.")
    for (expectedApiVersion: ApiVersionsResponseKey <- ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.apiKeys().asScala) {
      val actualApiVersion = apiVersionsResponse.apiVersion(expectedApiVersion.apiKey)
      assertNotNull(actualApiVersion, s"API key ${actualApiVersion.apiKey} is supported by broker, but not received in ApiVersionsResponse.")
      assertEquals(expectedApiVersion.apiKey, actualApiVersion.apiKey, "API key must be supported by the broker.")
      assertEquals(expectedApiVersion.minVersion, actualApiVersion.minVersion, s"Received unexpected min version for API key ${actualApiVersion.apiKey}.")
      assertEquals(expectedApiVersion.maxVersion, actualApiVersion.maxVersion, s"Received unexpected max version for API key ${actualApiVersion.apiKey}.")
    }
  }
}
