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

import org.apache.kafka.common.feature.SupportedVersionRange
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.apache.kafka.server.{BrokerFeatures, ClientMetricsManager}
import org.apache.kafka.server.common.FinalizedFeatures

import scala.collection.mutable
import scala.jdk.CollectionConverters._

trait ApiVersionManager {
  def enableUnstableLastVersion: Boolean
  def listenerType: ListenerType
  def enabledApis: collection.Set[ApiKeys]

  def apiVersionResponse(throttleTimeMs: Int, alterFeatureLevel0: Boolean): ApiVersionsResponse

  def isApiEnabled(apiKey: ApiKeys, apiVersion: Short): Boolean = {
    apiKey != null && apiKey.inScope(listenerType) && apiKey.isVersionEnabled(apiVersion, enableUnstableLastVersion)
  }
  def newRequestMetrics: RequestChannelMetrics = new RequestChannelMetrics(enabledApis.asJava)

  def features: FinalizedFeatures
}

object ApiVersionManager {
  def apply(
    listenerType: ListenerType,
    config: KafkaConfig,
    forwardingManager: Option[ForwardingManager],
    supportedFeatures: BrokerFeatures,
    metadataCache: MetadataCache,
    clientMetricsManager: Option[ClientMetricsManager]
  ): ApiVersionManager = {
    new DefaultApiVersionManager(
      listenerType,
      forwardingManager,
      supportedFeatures,
      metadataCache,
      config.unstableApiVersionsEnabled,
      clientMetricsManager
    )
  }
}

/**
 * A simple ApiVersionManager that does not support forwarding and does not have metadata cache, used in kraft controller.
 * its enabled apis are determined by the listener type, its finalized features are dynamically determined by the controller.
 *
 * @param listenerType the listener type
 * @param enabledApis the enabled apis, which are computed by the listener type
 * @param brokerFeatures the broker features
 * @param enableUnstableLastVersion whether to enable unstable last version, see [[KafkaConfig.unstableApiVersionsEnabled]]
 * @param featuresProvider a provider to the finalized features supported
 */
class SimpleApiVersionManager(
  val listenerType: ListenerType,
  val enabledApis: collection.Set[ApiKeys],
  brokerFeatures: org.apache.kafka.common.feature.Features[SupportedVersionRange],
  val enableUnstableLastVersion: Boolean,
  val featuresProvider: () => FinalizedFeatures
) extends ApiVersionManager {

  def this(
    listenerType: ListenerType,
    enableUnstableLastVersion: Boolean,
    featuresProvider: () => FinalizedFeatures
  ) = {
    this(
      listenerType,
      ApiKeys.apisForListener(listenerType).asScala,
      BrokerFeatures.defaultSupportedFeatures(enableUnstableLastVersion),
      enableUnstableLastVersion,
      featuresProvider
    )
  }

  private val apiVersions = ApiVersionsResponse.collectApis(enabledApis.asJava, enableUnstableLastVersion)

  override def apiVersionResponse(
    throttleTimeMs: Int,
    alterFeatureLevel0: Boolean
  ): ApiVersionsResponse = {
    val currentFeatures = features
    new ApiVersionsResponse.Builder().
      setThrottleTimeMs(throttleTimeMs).
      setApiVersions(apiVersions).
      setSupportedFeatures(brokerFeatures).
      setFinalizedFeatures(currentFeatures.finalizedFeatures()).
      setFinalizedFeaturesEpoch(currentFeatures.finalizedFeaturesEpoch()).
      setZkMigrationEnabled(false).
      setAlterFeatureLevel0(alterFeatureLevel0).
      build()
  }

  override def features: FinalizedFeatures = featuresProvider.apply()
}

/**
 * The default ApiVersionManager that supports forwarding and has metadata cache, used in broker and zk controller.
 * When forwarding is enabled, the enabled apis are determined by the broker listener type and the controller apis,
 * otherwise the enabled apis are determined by the broker listener type, which is the same with SimpleApiVersionManager.
 *
 * @param listenerType the listener type
 * @param forwardingManager the forwarding manager,
 * @param brokerFeatures the broker features
 * @param metadataCache the metadata cache, used to get the finalized features and the metadata version
 * @param enableUnstableLastVersion whether to enable unstable last version, see [[KafkaConfig.unstableApiVersionsEnabled]]
 * @param clientMetricsManager the client metrics manager, helps to determine whether client telemetry is enabled
 */
class DefaultApiVersionManager(
  val listenerType: ListenerType,
  forwardingManager: Option[ForwardingManager],
  brokerFeatures: BrokerFeatures,
  metadataCache: MetadataCache,
  val enableUnstableLastVersion: Boolean,
  val clientMetricsManager: Option[ClientMetricsManager] = None
) extends ApiVersionManager {

  val enabledApis: mutable.Set[ApiKeys] = ApiKeys.apisForListener(listenerType).asScala

  override def apiVersionResponse(
    throttleTimeMs: Int,
    alterFeatureLevel0: Boolean
  ): ApiVersionsResponse = {
    val finalizedFeatures = metadataCache.features()
    val controllerApiVersions = forwardingManager.flatMap(_.controllerApiVersions)
    val clientTelemetryEnabled = clientMetricsManager match {
      case Some(manager) => manager.isTelemetryReceiverConfigured
      case None => false
    }
    val apiVersions = if (controllerApiVersions.isDefined) {
      ApiVersionsResponse.controllerApiVersions(
        finalizedFeatures.metadataVersion().highestSupportedRecordVersion,
        controllerApiVersions.get,
        listenerType,
        enableUnstableLastVersion,
        clientTelemetryEnabled)
    } else {
      ApiVersionsResponse.brokerApiVersions(
        finalizedFeatures.metadataVersion().highestSupportedRecordVersion,
        listenerType,
        enableUnstableLastVersion,
        clientTelemetryEnabled)
    }
    new ApiVersionsResponse.Builder().
      setThrottleTimeMs(throttleTimeMs).
      setApiVersions(apiVersions).
      setSupportedFeatures(brokerFeatures.supportedFeatures).
      setFinalizedFeatures(finalizedFeatures.finalizedFeatures()).
      setFinalizedFeaturesEpoch(finalizedFeatures.finalizedFeaturesEpoch()).
      setZkMigrationEnabled(false).
      setAlterFeatureLevel0(alterFeatureLevel0).
      build()
  }

  override def features: FinalizedFeatures = metadataCache.features()
}
