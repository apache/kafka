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
import kafka.test.annotation.{ClusterConfigProperty, ClusterTemplate, ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import scala.jdk.CollectionConverters._

object ApiVersionsRequestTest {

  def controlPlaneListenerProperties(): java.util.HashMap[String, String] = {
    // Configure control plane listener to make sure we have separate listeners for testing.
    val serverProperties = new java.util.HashMap[String, String]()
    serverProperties.put("control.plane.listener.name", "CONTROL_PLANE")
    serverProperties.put("listener.security.protocol.map", "CONTROL_PLANE:PLAINTEXT,PLAINTEXT:PLAINTEXT")
    serverProperties.put("listeners", "PLAINTEXT://localhost:0,CONTROL_PLANE://localhost:0")
    serverProperties.put("advertised.listeners", "PLAINTEXT://localhost:0,CONTROL_PLANE://localhost:0")
    serverProperties
  }

  def testApiVersionsRequestTemplate(): java.util.List[ClusterConfig] = {
    val serverProperties: java.util.HashMap[String, String] = controlPlaneListenerProperties()
    serverProperties.put("unstable.api.versions.enable", "false")
    serverProperties.put("unstable.feature.versions.enable", "true")
    List(ClusterConfig.defaultBuilder()
      .setTypes(java.util.Collections.singleton(Type.ZK))
      .setServerProperties(serverProperties)
      .setMetadataVersion(MetadataVersion.latestTesting())
      .build()).asJava
  }

  def testApiVersionsRequestIncludesUnreleasedApisTemplate(): java.util.List[ClusterConfig] = {
    val serverProperties: java.util.HashMap[String, String] = controlPlaneListenerProperties()
    serverProperties.put("unstable.api.versions.enable", "true")
    serverProperties.put("unstable.feature.versions.enable", "true")
    List(ClusterConfig.defaultBuilder()
      .setTypes(java.util.Collections.singleton(Type.ZK))
      .setServerProperties(serverProperties)
      .build()).asJava
  }

  def testApiVersionsRequestValidationV0Template(): java.util.List[ClusterConfig] = {
    val serverProperties: java.util.HashMap[String, String] = controlPlaneListenerProperties()
    serverProperties.put("unstable.api.versions.enable", "false")
    serverProperties.put("unstable.feature.versions.enable", "false")
    List(ClusterConfig.defaultBuilder()
      .setTypes(java.util.Collections.singleton(Type.ZK))
      .setMetadataVersion(MetadataVersion.latestProduction())
      .build()).asJava
  }

  def zkApiVersionsRequest(): java.util.List[ClusterConfig] = {
    List(ClusterConfig.defaultBuilder()
      .setTypes(java.util.Collections.singleton(Type.ZK))
      .setServerProperties(controlPlaneListenerProperties())
      .build()).asJava
  }
}

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ApiVersionsRequestTest(cluster: ClusterInstance) extends AbstractApiVersionsRequestTest(cluster) {

  @ClusterTemplate("testApiVersionsRequestTemplate")
  @ClusterTest(types = Array(Type.KRAFT, Type.CO_KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
    new ClusterConfigProperty(key = "unstable.feature.versions.enable", value = "true")
  ))
  def testApiVersionsRequest(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, cluster.clientListener())
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTemplate("testApiVersionsRequestIncludesUnreleasedApisTemplate")
  @ClusterTest(types = Array(Type.KRAFT, Type.CO_KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    new ClusterConfigProperty(key = "unstable.feature.versions.enable", value = "true"),
  ))
  def testApiVersionsRequestIncludesUnreleasedApis(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, cluster.clientListener())
    validateApiVersionsResponse(apiVersionsResponse, enableUnstableLastVersion = true)
  }

  @ClusterTemplate("zkApiVersionsRequest")
  def testApiVersionsRequestThroughControlPlaneListener(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, cluster.controlPlaneListenerName().get())
    validateApiVersionsResponse(apiVersionsResponse, cluster.controlPlaneListenerName().get(), true)
  }

  @ClusterTest(types = Array(Type.KRAFT))
  def testApiVersionsRequestThroughControllerListener(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, cluster.controllerListenerName.get())
    validateApiVersionsResponse(apiVersionsResponse, cluster.controllerListenerName.get(), enableUnstableLastVersion = true)
  }

  @ClusterTemplate("zkApiVersionsRequest")
  @ClusterTest(types = Array(Type.KRAFT, Type.CO_KRAFT))
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

  // Use the latest production MV for this test
  @ClusterTemplate("testApiVersionsRequestValidationV0Template")
  @ClusterTest(types = Array(Type.KRAFT, Type.CO_KRAFT), metadataVersion = MetadataVersion.IBP_3_8_IV0, serverProperties = Array(
      new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
      new ClusterConfigProperty(key = "unstable.feature.versions.enable", value = "false"),
  ))
  def testApiVersionsRequestValidationV0(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.clientListener())
    validateApiVersionsResponse(apiVersionsResponse, apiVersion = 0,
      enableUnstableLastVersion = !"false".equals(
        cluster.config().serverProperties().get("unstable.api.versions.enable")))
  }

  @ClusterTemplate("zkApiVersionsRequest")
  def testApiVersionsRequestValidationV0ThroughControlPlaneListener(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.controlPlaneListenerName().get())
    validateApiVersionsResponse(apiVersionsResponse, cluster.controlPlaneListenerName().get(), true)
  }

  @ClusterTest(types = Array(Type.KRAFT))
  def testApiVersionsRequestValidationV0ThroughControllerListener(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.controllerListenerName.get())
    validateApiVersionsResponse(apiVersionsResponse, cluster.controllerListenerName.get(), apiVersion = 0, enableUnstableLastVersion = true)
  }

  @ClusterTemplate("zkApiVersionsRequest")
  @ClusterTest(types = Array(Type.KRAFT, Type.CO_KRAFT))
  def testApiVersionsRequestValidationV3(): Unit = {
    // Invalid request because Name and Version are empty by default
    val apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData(), 3.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.clientListener())
    assertEquals(Errors.INVALID_REQUEST.code(), apiVersionsResponse.data.errorCode())
  }
}
