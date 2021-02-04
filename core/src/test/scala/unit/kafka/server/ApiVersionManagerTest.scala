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

import kafka.api.ApiVersion
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.common.message.ApiMessageType.ApiScope
import org.apache.kafka.common.protocol.ApiKeys
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.mockito.Mockito

class ApiVersionManagerTest {

  @Test
  def testControllerApiIntersection(): Unit = {
    val controllerMinVersion: Short = 1
    val controllerMaxVersion: Short = 5

    val forwardingManager = Mockito.mock(classOf[ForwardingManager])

    Mockito.when(forwardingManager.controllerApiVersions).thenReturn(Some(NodeApiVersions.create(
      ApiKeys.CREATE_TOPICS.id,
      controllerMinVersion,
      controllerMaxVersion
    )))

    val brokerFeatures = BrokerFeatures.createDefault()
    val featureCache = new FinalizedFeatureCache(brokerFeatures)

    val versionManager = new DefaultApiVersionManager(
      apiScope = ApiScope.ZK_BROKER,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      forwardingManager = Some(forwardingManager),
      features = brokerFeatures,
      featureCache = featureCache
    )

    val apiVersionsResponse = versionManager.apiVersionResponse(throttleTimeMs = 0)
    val alterConfigVersion = apiVersionsResponse.data.apiKeys.find(ApiKeys.CREATE_TOPICS.id)
    assertNotNull(alterConfigVersion)
    assertEquals(controllerMinVersion, alterConfigVersion.minVersion)
    assertEquals(controllerMaxVersion, alterConfigVersion.maxVersion)
  }

  @Test
  def testEnvelopeEnabledWhenForwardingManagerPresent(): Unit = {
    val forwardingManager = Mockito.mock(classOf[ForwardingManager])
    Mockito.when(forwardingManager.controllerApiVersions).thenReturn(None)

    val brokerFeatures = BrokerFeatures.createDefault()
    val featureCache = new FinalizedFeatureCache(brokerFeatures)

    val versionManager = new DefaultApiVersionManager(
      apiScope = ApiScope.ZK_BROKER,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      forwardingManager = Some(forwardingManager),
      features = brokerFeatures,
      featureCache = featureCache
    )
    assertTrue(versionManager.isApiEnabled(ApiKeys.ENVELOPE))
    assertTrue(versionManager.enabledApis.contains(ApiKeys.ENVELOPE))

    val apiVersionsResponse = versionManager.apiVersionResponse(throttleTimeMs = 0)
    val envelopeVersion = apiVersionsResponse.data.apiKeys.find(ApiKeys.ENVELOPE.id)
    assertNotNull(envelopeVersion)
    assertEquals(ApiKeys.ENVELOPE.oldestVersion, envelopeVersion.minVersion)
    assertEquals(ApiKeys.ENVELOPE.latestVersion, envelopeVersion.maxVersion)
  }

  @Test
  def testEnvelopeDisabledWhenForwardingManagerEmpty(): Unit = {
    val brokerFeatures = BrokerFeatures.createDefault()
    val featureCache = new FinalizedFeatureCache(brokerFeatures)

    val versionManager = new DefaultApiVersionManager(
      apiScope = ApiScope.ZK_BROKER,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      forwardingManager = None,
      features = brokerFeatures,
      featureCache = featureCache
    )
    assertFalse(versionManager.isApiEnabled(ApiKeys.ENVELOPE))
    assertFalse(versionManager.enabledApis.contains(ApiKeys.ENVELOPE))

    val apiVersionsResponse = versionManager.apiVersionResponse(throttleTimeMs = 0)
    assertNull(apiVersionsResponse.data.apiKeys.find(ApiKeys.ENVELOPE.id))
  }

}
