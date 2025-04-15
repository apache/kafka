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
import org.apache.kafka.common.metadata.FeatureLevelRecord
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.{BrokerFeatures, DefaultApiVersionManager}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mockito

import java.util.Optional
import java.util.function.Supplier
import scala.jdk.CollectionConverters._

class DefaultApiVersionManagerTest {
  private val brokerFeatures = BrokerFeatures.createDefault(true)
  private val metadataCache = {
    val cache = new KRaftMetadataCache(1, () => KRaftVersion.LATEST_PRODUCTION)
    val delta = new MetadataDelta(MetadataImage.EMPTY)
    delta.replay(new FeatureLevelRecord()
      .setName(MetadataVersion.FEATURE_NAME)
      .setFeatureLevel(MetadataVersion.latestProduction().featureLevel())
    )
    cache.setImage(delta.apply(MetadataProvenance.EMPTY))
    cache
  }

  @ParameterizedTest
  @EnumSource(classOf[ListenerType])
  def testApiScope(apiScope: ListenerType): Unit = {
    val nodeApiVersionsSupplier = Mockito.mock(classOf[Supplier[Optional[NodeApiVersions]]])
    val versionManager = new DefaultApiVersionManager(
      apiScope,
      nodeApiVersionsSupplier,
      brokerFeatures,
      metadataCache,
      true,
      Optional.empty
    )
    assertTrue(ApiKeys.apisForListener(apiScope).asScala.forall { apiKey =>
      apiKey.allVersions.asScala.forall { version =>
        versionManager.isApiEnabled(apiKey, version)
      }
    })
  }

  @ParameterizedTest
  @EnumSource(classOf[ListenerType])
  def testDisabledApis(apiScope: ListenerType): Unit = {
    val nodeApiVersionsSupplier = Mockito.mock(classOf[Supplier[Optional[NodeApiVersions]]])
    val versionManager = new DefaultApiVersionManager(
      apiScope,
      nodeApiVersionsSupplier,
      brokerFeatures,
      metadataCache,
      false,
      Optional.empty
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

    val nodeApiVersionsSupplier = Mockito.mock(classOf[Supplier[Optional[NodeApiVersions]]])

    Mockito.when(nodeApiVersionsSupplier.get).thenReturn(Optional.of(NodeApiVersions.create(
      ApiKeys.CREATE_TOPICS.id,
      controllerMinVersion,
      controllerMaxVersion
    )))

    val versionManager = new DefaultApiVersionManager(
      ListenerType.BROKER,
      nodeApiVersionsSupplier,
      brokerFeatures,
      metadataCache,
      true,
      Optional.empty
    )

    val apiVersionsResponse = versionManager.apiVersionResponse(0, false)
    val alterConfigVersion = apiVersionsResponse.data.apiKeys.find(ApiKeys.CREATE_TOPICS.id)
    assertNotNull(alterConfigVersion)
    assertEquals(controllerMinVersion, alterConfigVersion.minVersion)
    assertEquals(controllerMaxVersion, alterConfigVersion.maxVersion)
  }

  @Test
  def testEnvelopeDisabledForKRaftBroker(): Unit = {
    val nodeApiVersionsSupplier = Mockito.mock(classOf[Supplier[Optional[NodeApiVersions]]])
    Mockito.when(nodeApiVersionsSupplier.get).thenReturn(Optional.empty())

    val versionManager = new DefaultApiVersionManager(
      ListenerType.BROKER,
      nodeApiVersionsSupplier,
      brokerFeatures,
      metadataCache,
      true,
      Optional.empty
    )
    assertFalse(versionManager.isApiEnabled(ApiKeys.ENVELOPE, ApiKeys.ENVELOPE.latestVersion))
    assertFalse(ApiKeys.apisForListener(versionManager.listenerType()).contains(ApiKeys.ENVELOPE))

    val apiVersionsResponse = versionManager.apiVersionResponse(0, false)
    val envelopeVersion = apiVersionsResponse.data.apiKeys.find(ApiKeys.ENVELOPE.id)
    assertNull(envelopeVersion)
  }
}
