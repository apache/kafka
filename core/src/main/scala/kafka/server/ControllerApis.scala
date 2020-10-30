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

import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation.{CLUSTER_ACTION, DESCRIBE}
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.{ApiVersionsResponseData, MetadataResponseData}
import org.apache.kafka.common.message.ApiVersionsResponseData.{ApiVersionsResponseKey, FinalizedFeatureKey, SupportedFeatureKey}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AlterIsrRequest, ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse}
import org.apache.kafka.common.resource.Resource
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.CLUSTER
import org.apache.kafka.common.utils.Time
import org.apache.kafka.controller.{Controller, LeaderAndIsr}
import org.apache.kafka.metadata.{FeatureManager, VersionRange}
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.mutable
import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/**
 * Request handler for Controller APIs
 */
class ControllerApis(val requestChannel: RequestChannel,
                     val authorizer: Option[Authorizer],
                     val quotas: QuotaManagers,
                     val time: Time,
                     val supportedFeatures: Map[String, VersionRange],
                     val controller: Controller,
                     val config: KafkaConfig,
                     val metaProperties: MetaProperties) extends ApiRequestHandler with Logging {

  val apisUtils = new ApisUtils(requestChannel, authorizer, quotas, time)

  val supportedApiKeys = Set(
    ApiKeys.METADATA,
    //ApiKeys.SASL_HANDSHAKE
    ApiKeys.API_VERSIONS,
    //ApiKeys.CREATE_TOPICS,
    //ApiKeys.DELETE_TOPICS,
    //ApiKeys.DESCRIBE_ACLS,
    //ApiKeys.CREATE_ACLS,
    //ApiKeys.DELETE_ACLS,
    //ApiKeys.DESCRIBE_CONFIGS,
    //ApiKeys.ALTER_CONFIGS,
    //ApiKeys.SASL_AUTHENTICATE,
    //ApiKeys.CREATE_PARTITIONS,
    //ApiKeys.CREATE_DELEGATION_TOKEN
    //ApiKeys.RENEW_DELEGATION_TOKEN
    //ApiKeys.EXPIRE_DELEGATION_TOKEN
    //ApiKeys.DESCRIBE_DELEGATION_TOKEN
    //ApiKeys.ELECT_LEADERS
    //ApiKeys.INCREMENTAL_ALTER_CONFIGS
    //ApiKeys.ALTER_PARTITION_REASSIGNMENTS
    //ApiKeys.LIST_PARTITION_REASSIGNMENTS
    //ApiKeys.DESCRIBE_CLIENT_QUOTAS
    //ApiKeys.ALTER_CLIENT_QUOTAS
    //ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS
    //ApiKeys.ALTER_USER_SCRAM_CREDENTIALS
    //ApiKeys.VOTE
    //ApiKeys.BEGIN_QUORUM_EPOCH
    //ApiKeys.END_QUORUM_EPOCH
    //ApiKeys.DESCRIBE_QUORUM
    ApiKeys.ALTER_ISR,
    //ApiKeys.UPDATE_FEATURES
    //ApiKeys.BROKER_REGISTRATION
    //ApiKeys.BROKER_HEARTBEAT
  )

  override def handle(request: RequestChannel.Request): Unit = {
    try {
      request.header.apiKey match {
        case ApiKeys.METADATA => handleMetadataRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.ALTER_ISR => handleAlterIsrRequest(request)
          // TODO other APIs
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey()}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => apisUtils.handleError(request, e)
    } finally {

    }
  }

  def handleApiVersionsRequest(request: RequestChannel.Request): Unit = {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on an SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    def createResponseCallback(features: FeatureManager.FinalizedFeaturesAndEpoch,
                               requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion)
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      else if (!apiVersionRequest.isValid)
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
      else {
        val data = new ApiVersionsResponseData().
          setErrorCode(0.toShort).
          setThrottleTimeMs(requestThrottleMs).
          setFinalizedFeaturesEpoch(features.epoch())
        supportedFeatures.foreach {
          case (k, v) => data.supportedFeatures().add(new SupportedFeatureKey().
            setName(k).setMaxVersion(v.max()).setMinVersion(v.min()))
        }
        features.finalizedFeatures().asScala.foreach {
          case (k, v) => data.finalizedFeatures().add(new FinalizedFeatureKey().
            setName(k).setMaxVersionLevel(v.max()).setMinVersionLevel(v.min()))
        }
        ApiKeys.enabledApis().asScala.foreach {
          case key => if (supportedApiKeys.contains(key)) {
            data.apiKeys().add(new ApiVersionsResponseKey().
              setApiKey(key.id).
              setMaxVersion(key.latestVersion()).
              setMinVersion(key.oldestVersion()))
          }
        }
        new ApiVersionsResponse(data)
      }
    }
    FutureConverters.toScala(controller.finalizedFeatures()).onComplete(
      result => result match {
        case Success(features) =>
          apisUtils.sendResponseMaybeThrottle(request,
            requestThrottleMs => createResponseCallback(features, requestThrottleMs))
        case Failure(e) => apisUtils.handleError(request, e)
      }
    )
  }

  def handleMetadataRequest(request: RequestChannel.Request): Unit = {
    val metadataRequest = request.body[MetadataRequest]
    def createResponseCallback(requestThrottleMs: Int): MetadataResponse = {
      val metadataResponseData = new MetadataResponseData()
      metadataResponseData.setThrottleTimeMs(requestThrottleMs)
      config.controllerConnectNodes.foreach {
        case node => metadataResponseData.brokers().add(
          new MetadataResponseBroker().setHost(node.host()).
            setNodeId(node.id()).setPort(node.port()).setRack(node.rack()))
      }
      metadataResponseData.setClusterId(metaProperties.clusterId.toString)
      // TODO: get a better estimate of the current active controller from Raft or somewhere.
      if (controller.curClaimEpoch() > 0) {
        metadataResponseData.setControllerId(config.controllerId)
      } else {
        metadataResponseData.setControllerId(-1)
      }
      val clusterAuthorizedOperations = if (metadataRequest.data.includeClusterAuthorizedOperations) {
        if (apisUtils.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
          apisUtils.authorizedOperations(request, Resource.CLUSTER)
        } else {
          0
        }
      } else {
        Int.MinValue
      }
      // TODO: fill in information about the metadata topic
      metadataResponseData.setClusterAuthorizedOperations(clusterAuthorizedOperations)
      new MetadataResponse(metadataResponseData)
    }
    apisUtils.sendResponseMaybeThrottle(request,
      requestThrottleMs => createResponseCallback(requestThrottleMs))
  }

  def handleAlterIsrRequest(request: RequestChannel.Request): Unit = {
    val alterIsrRequest = request.body[AlterIsrRequest]
    if (!apisUtils.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      val isrsToAlter = mutable.Map[TopicPartition, LeaderAndIsr]()
      alterIsrRequest.data.topics.forEach { topicReq =>
        topicReq.partitions.forEach { partitionReq =>
          val tp = new TopicPartition(topicReq.name, partitionReq.partitionIndex)
          val newIsr = partitionReq.newIsr()
          isrsToAlter.put(tp, new LeaderAndIsr(
            alterIsrRequest.data.brokerId,
            partitionReq.leaderEpoch,
            newIsr,
            partitionReq.currentIsrVersion))
        }
      }

      controller.alterIsr(
        alterIsrRequest.data().brokerId(),
        alterIsrRequest.data().brokerEpoch(),
        isrsToAlter.asJava)
    }
  }
}
