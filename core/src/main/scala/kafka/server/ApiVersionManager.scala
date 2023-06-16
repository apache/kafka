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

import kafka.network
import kafka.network.RequestChannel
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsResponse

import scala.jdk.CollectionConverters._

trait ApiVersionManager {
  def enableUnstableLastVersion: Boolean
  def listenerType: ListenerType
  def enabledApis: collection.Set[ApiKeys]

  /**
   * @see [[DefaultApiVersionManager.apiVersionResponse]]
   * @see [[kafka.server.KafkaApis.handleApiVersionsRequest]]
   */
  def apiVersionResponse(throttleTimeMs: Int): ApiVersionsResponse

  /**
   * @see [[SimpleApiVersionManager.apiVersionResponse]]
   * @see [[kafka.server.ControllerApis.handleApiVersionsRequest]]
   */
  def apiVersionResponse(throttleTimeMs: Int, finalizedFeatures: Map[String, java.lang.Short], finalizedFeaturesEpoch: Long): ApiVersionsResponse

  def isApiEnabled(apiKey: ApiKeys, apiVersion: Short): Boolean = {
    apiKey != null && apiKey.inScope(listenerType) && apiKey.isVersionEnabled(apiVersion, enableUnstableLastVersion)
  }
  def newRequestMetrics: RequestChannel.Metrics = new network.RequestChannel.Metrics(enabledApis)
}

object ApiVersionManager {
  def apply(
    listenerType: ListenerType,
    config: KafkaConfig,
    forwardingManager: Option[ForwardingManager],
    supportedFeatures: BrokerFeatures,
    metadataCache: MetadataCache
  ): ApiVersionManager = {
    new DefaultApiVersionManager(
      listenerType,
      forwardingManager,
      supportedFeatures,
      metadataCache,
      config.unstableApiVersionsEnabled,
      config.migrationEnabled
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
 * @param zkMigrationEnabled whether to enable zk migration, see [[KafkaConfig.migrationEnabled]]
 */
class SimpleApiVersionManager(
  val listenerType: ListenerType,
  val enabledApis: collection.Set[ApiKeys],
  brokerFeatures: Features[SupportedVersionRange],
  val enableUnstableLastVersion: Boolean,
  val zkMigrationEnabled: Boolean
) extends ApiVersionManager {

  def this(
    listenerType: ListenerType,
    enableUnstableLastVersion: Boolean,
    zkMigrationEnabled: Boolean
  ) = {
    this(
      listenerType,
      ApiKeys.apisForListener(listenerType).asScala,
      BrokerFeatures.defaultSupportedFeatures(),
      enableUnstableLastVersion,
      zkMigrationEnabled
    )
  }

  private val apiVersions = ApiVersionsResponse.collectApis(enabledApis.asJava, enableUnstableLastVersion)

  override def apiVersionResponse(requestThrottleMs: Int): ApiVersionsResponse = {
    throw new UnsupportedOperationException("This method is not supported in SimpleApiVersionManager, use apiVersionResponse(throttleTimeMs, finalizedFeatures, epoch) instead")
  }

  override def apiVersionResponse(throttleTimeMs: Int, finalizedFeatures: Map[String, java.lang.Short], finalizedFeaturesEpoch: Long): ApiVersionsResponse = {
    ApiVersionsResponse.createApiVersionsResponse(
      throttleTimeMs,
      apiVersions,
      brokerFeatures,
      finalizedFeatures.asJava,
      finalizedFeaturesEpoch,
      zkMigrationEnabled
    )
  }
}

/**
 * The default ApiVersionManager that supports forwarding and has metadata cache, used in broker and zk controller.
 * When forwarding is enabled, the enabled apis are determined by the broker listener type and the controller apis,
 * otherwise the enabled apis are determined by the broker listener type, which is the same with SimpleApiVersionManager.
 *
 * @param listenerType the listener type
 * @param forwardingManager the forwarding manager,
 * @param features the broker features
 * @param metadataCache the metadata cache, used to get the finalized features and the metadata version
 * @param enableUnstableLastVersion whether to enable unstable last version, see [[KafkaConfig.unstableApiVersionsEnabled]]
 * @param zkMigrationEnabled whether to enable zk migration, see [[KafkaConfig.migrationEnabled]]
 */
class DefaultApiVersionManager(
  val listenerType: ListenerType,
  forwardingManager: Option[ForwardingManager],
  features: BrokerFeatures,
  metadataCache: MetadataCache,
  val enableUnstableLastVersion: Boolean,
  val zkMigrationEnabled: Boolean = false
) extends ApiVersionManager {

  val enabledApis = ApiKeys.apisForListener(listenerType).asScala

  override def apiVersionResponse(throttleTimeMs: Int): ApiVersionsResponse = {
    val supportedFeatures = features.supportedFeatures
    val finalizedFeatures = metadataCache.features()
    val controllerApiVersions = forwardingManager.flatMap(_.controllerApiVersions)

    ApiVersionsResponse.createApiVersionsResponse(
      throttleTimeMs,
      metadataCache.metadataVersion().highestSupportedRecordVersion,
      supportedFeatures,
      finalizedFeatures.features.map(kv => (kv._1, kv._2.asInstanceOf[java.lang.Short])).asJava,
      finalizedFeatures.epoch,
      controllerApiVersions.orNull,
      listenerType,
      enableUnstableLastVersion,
      zkMigrationEnabled
    )
  }

  override def apiVersionResponse(throttleTimeMs: Int, finalizedFeatures: Map[String, java.lang.Short], finalizedFeatureEpoch: Long): ApiVersionsResponse = {
    throw new UnsupportedOperationException("This method is not supported in DefaultApiVersionManager, use apiVersionResponse(throttleTimeMs) instead")
  }
}
