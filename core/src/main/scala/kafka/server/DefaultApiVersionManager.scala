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

import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.{ApiVersionManager, BrokerFeatures, ClientMetricsManager}
import org.apache.kafka.server.common.FinalizedFeatures

import java.util

/**
 * The default ApiVersionManager that supports forwarding and has metadata cache, used in brokers.
 * The enabled APIs are determined by the broker listener type and the controller APIs.
 *
 * @param listenerType the listener type
 * @param forwardingManager the forwarding manager
 * @param brokerFeatures the broker features
 * @param metadataCache the metadata cache, used to get the finalized features and the metadata version
 * @param enableUnstableLastVersion whether to enable unstable last version, see [[KafkaConfig.unstableApiVersionsEnabled]]
 * @param clientMetricsManager the client metrics manager, helps to determine whether client telemetry is enabled
 */
class DefaultApiVersionManager(
  val listenerType: ListenerType,
  forwardingManager: ForwardingManager,
  brokerFeatures: BrokerFeatures,
  metadataCache: MetadataCache,
  val enableUnstableLastVersion: Boolean,
  val clientMetricsManager: Option[ClientMetricsManager] = None
) extends ApiVersionManager {

  val enabledApis: util.Set[ApiKeys] = ApiKeys.apisForListener(listenerType)

  override def apiVersionResponse(
    throttleTimeMs: Int,
    alterFeatureLevel0: Boolean
  ): ApiVersionsResponse = {
    val finalizedFeatures = metadataCache.features()
    val controllerApiVersions = forwardingManager.controllerApiVersions
    val clientTelemetryEnabled = clientMetricsManager match {
      case Some(manager) => manager.isTelemetryReceiverConfigured
      case None => false
    }
    val apiVersions = if (controllerApiVersions.isDefined) {
      ApiVersionsResponse.controllerApiVersions(
        controllerApiVersions.get,
        listenerType,
        enableUnstableLastVersion,
        clientTelemetryEnabled)
    } else {
      ApiVersionsResponse.brokerApiVersions(
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
      setAlterFeatureLevel0(alterFeatureLevel0).
      build()
  }

  override def features: FinalizedFeatures = metadataCache.features()
}
