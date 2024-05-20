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

import kafka.test.{ClusterConfig, ClusterGenerator, ClusterInstance}
import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.ApiVersionsRequest
import kafka.test.annotation.{ClusterConfigProperty, ClusterTemplate, ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith

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

  def testApiVersionsRequestTemplate(clusterGenerator: ClusterGenerator): Unit = {
    val serverProperties: java.util.HashMap[String, String] = controlPlaneListenerProperties()
    serverProperties.put("unstable.api.versions.enable", "false")
    serverProperties.put("unstable.metadata.versions.enable", "true")
    clusterGenerator.accept(ClusterConfig.defaultBuilder()
      .setTypes(java.util.Collections.singleton(Type.ZK))
      .setServerProperties(serverProperties)
      .setMetadataVersion(MetadataVersion.IBP_3_8_IV0)
      .build())
  }

  def testApiVersionsRequestIncludesUnreleasedApisTemplate(clusterGenerator: ClusterGenerator): Unit = {
    val serverProperties: java.util.HashMap[String, String] = controlPlaneListenerProperties()
    serverProperties.put("unstable.api.versions.enable", "true")
    serverProperties.put("unstable.metadata.versions.enable", "true")
    clusterGenerator.accept(ClusterConfig.defaultBuilder()
      .setTypes(java.util.Collections.singleton(Type.ZK))
      .setServerProperties(serverProperties)
      .build())
  }

  def testApiVersionsRequestValidationV0Template(clusterGenerator: ClusterGenerator): Unit = {
    val serverProperties: java.util.HashMap[String, String] = controlPlaneListenerProperties()
    serverProperties.put("unstable.api.versions.enable", "false")
    serverProperties.put("unstable.metadata.versions.enable", "false")
    clusterGenerator.accept(ClusterConfig.defaultBuilder()
      .setTypes(java.util.Collections.singleton(Type.ZK))
      .setMetadataVersion(MetadataVersion.IBP_3_7_IV4)
      .build())
  }

  def zkApiVersionsRequest(clusterGenerator: ClusterGenerator): Unit = {
    clusterGenerator.accept(ClusterConfig.defaultBuilder()
      .setTypes(java.util.Collections.singleton(Type.ZK))
      .setServerProperties(controlPlaneListenerProperties())
      .build())
  }
}

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ApiVersionsRequestTest(cluster: ClusterInstance) extends AbstractApiVersionsRequestTest(cluster) {

  @ClusterTemplate("testApiVersionsRequestTemplate")
  @ClusterTest(types = Array(Type.KRAFT, Type.CO_KRAFT), metadataVersion = MetadataVersion.IBP_3_8_IV0, serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
    new ClusterConfigProperty(key = "unstable.metadata.versions.enable", value = "true")
  ))
  def testApiVersionsRequest(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, cluster.clientListener())
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTemplate("testApiVersionsRequestIncludesUnreleasedApisTemplate")
  @ClusterTest(types = Array(Type.KRAFT, Type.CO_KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
    new ClusterConfigProperty(key = "unstable.metadata.versions.enable", value = "true"),
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
    validateApiVersionsResponse(apiVersionsResponse, cluster.controlPlaneListenerName().get())
  }

  @ClusterTest(types = Array(Type.KRAFT))
  def testApiVersionsRequestThroughControllerListener(): Unit = {
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request, cluster.controllerListenerName.get())
    validateApiVersionsResponse(apiVersionsResponse, cluster.controllerListenerName.get())
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

  @ClusterTemplate("testApiVersionsRequestValidationV0Template")
  @ClusterTest(types = Array(Type.KRAFT, Type.CO_KRAFT), metadataVersion = MetadataVersion.IBP_3_7_IV4, serverProperties = Array(
      new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "false"),
      new ClusterConfigProperty(key = "unstable.metadata.versions.enable", value = "false"),
  ))
  def testApiVersionsRequestValidationV0(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.clientListener())
    validateApiVersionsResponse(apiVersionsResponse, apiVersion = 0)
  }

  @ClusterTemplate("zkApiVersionsRequest")
  def testApiVersionsRequestValidationV0ThroughControlPlaneListener(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.controlPlaneListenerName().get())
    validateApiVersionsResponse(apiVersionsResponse, cluster.controlPlaneListenerName().get())
  }

  @ClusterTest(types = Array(Type.KRAFT))
  def testApiVersionsRequestValidationV0ThroughControllerListener(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest, cluster.controllerListenerName.get())
    validateApiVersionsResponse(apiVersionsResponse, cluster.controllerListenerName.get(), apiVersion = 0)
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
