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

import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.server.BrokerFeatures
import org.apache.kafka.server.common.KRaftVersion
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mockito

import scala.jdk.CollectionConverters._

class ApiVersionManagerTest {
  private val brokerFeatures = BrokerFeatures.createDefault(true)
  private val metadataCache = MetadataCache.kRaftMetadataCache(1, () => KRaftVersion.LATEST_PRODUCTION)

  @ParameterizedTest
  @EnumSource(classOf[ListenerType])
  def testApiScope(apiScope: ListenerType): Unit = {
    val forwardingManager = Mockito.mock(classOf[ForwardingManager])
    val versionManager = new DefaultApiVersionManager(
      listenerType = apiScope,
      forwardingManager = forwardingManager,
      brokerFeatures = brokerFeatures,
      metadataCache = metadataCache,
      enableUnstableLastVersion = true
    )
    assertEquals(ApiKeys.apisForListener(apiScope).asScala, versionManager.enabledApis)
    assertTrue(ApiKeys.apisForListener(apiScope).asScala.forall { apiKey =>
      apiKey.allVersions.asScala.forall { version =>
        versionManager.isApiEnabled(apiKey, version)
      }
    })
  }

  @ParameterizedTest
  @EnumSource(classOf[ListenerType])
  def testDisabledApis(apiScope: ListenerType): Unit = {
    val forwardingManager = Mockito.mock(classOf[ForwardingManager])
    val versionManager = new DefaultApiVersionManager(
      listenerType = apiScope,
      forwardingManager = forwardingManager,
      brokerFeatures = brokerFeatures,
      metadataCache = metadataCache,
      enableUnstableLastVersion = false
    )

    ApiKeys.apisForListener(apiScope).forEach { apiKey =>
      if (apiKey.messageType.latestVersionUnstable()) {
        assertFalse(versionManager.isApiEnabled(apiKey, apiKey.latestVersion),
          s"$apiKey version ${apiKey.latestVersion} should be disabled.")
      }
    }
  }

  @Test
  def testControllerApiIntersection(): Unit = {
    val controllerMinVersion: Short = 3
    val controllerMaxVersion: Short = 5

    val forwardingManager = Mockito.mock(classOf[ForwardingManager])

    Mockito.when(forwardingManager.controllerApiVersions).thenReturn(Some(NodeApiVersions.create(
      ApiKeys.CREATE_TOPICS.id,
      controllerMinVersion,
      controllerMaxVersion
    )))

    val versionManager = new DefaultApiVersionManager(
      listenerType = ListenerType.BROKER,
      forwardingManager = forwardingManager,
      brokerFeatures = brokerFeatures,
      metadataCache = metadataCache,
      enableUnstableLastVersion = true
    )

    val apiVersionsResponse = versionManager.apiVersionResponse(throttleTimeMs = 0, false)
    val alterConfigVersion = apiVersionsResponse.data.apiKeys.find(ApiKeys.CREATE_TOPICS.id)
    assertNotNull(alterConfigVersion)
    assertEquals(controllerMinVersion, alterConfigVersion.minVersion)
    assertEquals(controllerMaxVersion, alterConfigVersion.maxVersion)
  }

  @Test
  def testEnvelopeDisabledForKRaftBroker(): Unit = {
    val forwardingManager = Mockito.mock(classOf[ForwardingManager])
    Mockito.when(forwardingManager.controllerApiVersions).thenReturn(None)

    val versionManager = new DefaultApiVersionManager(
      listenerType = ListenerType.BROKER,
      forwardingManager = forwardingManager,
      brokerFeatures = brokerFeatures,
      metadataCache = metadataCache,
      enableUnstableLastVersion = true
    )
    assertFalse(versionManager.isApiEnabled(ApiKeys.ENVELOPE, ApiKeys.ENVELOPE.latestVersion))
    assertFalse(versionManager.enabledApis.contains(ApiKeys.ENVELOPE))

    val apiVersionsResponse = versionManager.apiVersionResponse(throttleTimeMs = 0, false)
    val envelopeVersion = apiVersionsResponse.data.apiKeys.find(ApiKeys.ENVELOPE.id)
    assertNull(envelopeVersion)
  }
}
