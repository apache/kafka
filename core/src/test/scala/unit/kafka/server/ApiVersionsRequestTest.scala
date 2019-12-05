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

import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse}
import org.junit.Assert._
import org.junit.Test

class ApiVersionsRequestTest extends AbstractApiVersionsRequestTest {

  override def brokerCount: Int = 1

  @Test
  def testApiVersionsRequest(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request)
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @Test
  def testApiVersionsRequestWithUnsupportedVersion(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendUnsupportedApiVersionRequest(apiVersionsRequest)
    assertEquals(Errors.UNSUPPORTED_VERSION.code(), apiVersionsResponse.data.errorCode())
    assertFalse(apiVersionsResponse.data.apiKeys().isEmpty)
    val apiVersion = apiVersionsResponse.data.apiKeys().find(ApiKeys.API_VERSIONS.id)
    assertEquals(ApiKeys.API_VERSIONS.id, apiVersion.apiKey())
    assertEquals(ApiKeys.API_VERSIONS.oldestVersion(), apiVersion.minVersion())
    assertEquals(ApiKeys.API_VERSIONS.latestVersion(), apiVersion.maxVersion())
  }

  @Test
  def testApiVersionsRequestValidationV0(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest)
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @Test
  def testApiVersionsRequestValidationV3(): Unit = {
    // Invalid request because Name and Version are empty by default
    val apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData(), 3.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest)
    assertEquals(Errors.INVALID_REQUEST.code(), apiVersionsResponse.data.errorCode())
  }

  private def sendApiVersionsRequest(request: ApiVersionsRequest): ApiVersionsResponse = {
    connectAndReceive[ApiVersionsResponse](request)
  }

}
