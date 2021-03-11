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

import kafka.test.{ClusterConfig, ClusterInstance}
import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.ApiVersionsRequest
import kafka.test.annotation.ClusterTest
import kafka.test.junit.ClusterTestExtensions
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith


@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ApiVersionsRequestTest(cluster: ClusterInstance) extends AbstractApiVersionsRequestTest(cluster) {

  @BeforeEach
  def setup(config: ClusterConfig): Unit = {
    super.brokerPropertyOverrides(config.serverProperties())
  }

  @ClusterTest
  def testApiVersionsRequest(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, cluster.clientListener())
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTest
  def testApiVersionsRequestThroughControlPlaneListener(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, super.controlPlaneListenerName)
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTest
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

  @ClusterTest
  def testApiVersionsRequestValidationV0(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.clientListener())
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTest
  def testApiVersionsRequestValidationV0ThroughControlPlaneListener(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, super.controlPlaneListenerName)
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTest
  def testApiVersionsRequestValidationV3(): Unit = {
    // Invalid request because Name and Version are empty by default
    val apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData(), 3.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.clientListener())
    assertEquals(Errors.INVALID_REQUEST.code(), apiVersionsResponse.data.errorCode())
  }
}
