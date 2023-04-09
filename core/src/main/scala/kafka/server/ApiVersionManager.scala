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
  def listenerType: ListenerType
  def enabledApis: collection.Set[ApiKeys]
  def apiVersionResponse(throttleTimeMs: Int): ApiVersionsResponse
  def isApiEnabled(apiKey: ApiKeys): Boolean = enabledApis.contains(apiKey)
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
      metadataCache
    )
  }
}

class SimpleApiVersionManager(
  val listenerType: ListenerType,
  val enabledApis: collection.Set[ApiKeys],
  brokerFeatures: Features[SupportedVersionRange]
) extends ApiVersionManager {

  def this(listenerType: ListenerType) = {
    this(listenerType, ApiKeys.apisForListener(listenerType).asScala, BrokerFeatures.defaultSupportedFeatures())
  }

  private val apiVersions = ApiVersionsResponse.collectApis(enabledApis.asJava)

  override def apiVersionResponse(requestThrottleMs: Int): ApiVersionsResponse = {
    ApiVersionsResponse.createApiVersionsResponse(requestThrottleMs, apiVersions, brokerFeatures)
  }
}

class DefaultApiVersionManager(
  val listenerType: ListenerType,
  forwardingManager: Option[ForwardingManager],
  features: BrokerFeatures,
  metadataCache: MetadataCache
) extends ApiVersionManager {

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
        listenerType)
  }

  override def enabledApis: collection.Set[ApiKeys] = {
    ApiKeys.apisForListener(listenerType).asScala
  }

  override def isApiEnabled(apiKey: ApiKeys): Boolean = {
    apiKey.inScope(listenerType)
  }
}
