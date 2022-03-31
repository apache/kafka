/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.Kafka.{info, warn}
import kafka.common.KafkaException
import kafka.metrics.clientmetrics.{ClientMetricsCache, ClientMetricsConfig, ClientMetricsReceiverPlugin, CmClientInformation, CmClientInstanceState}
import kafka.network.RequestChannel
import kafka.server.ClientMetricsManager.{getCurrentTime, getSupportedCompressionTypes}
import org.apache.kafka.common.errors.ClientMetricsReceiverPluginNotFoundException
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.requests.{GetTelemetrySubscriptionRequest, GetTelemetrySubscriptionResponse, PushTelemetryRequest, PushTelemetryResponse, RequestContext}

import java.util.{Calendar, Properties}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object ClientMetricsManager {
  private val _instance = new ClientMetricsManager
  def getInstance = _instance
  def getCurrentTime = Calendar.getInstance.getTime.getTime

  def getSupportedCompressionTypes: List[java.lang.Byte] = {
    val compressionTypes = new ListBuffer[java.lang.Byte]
    CompressionType.values.filter(x => x != CompressionType.NONE).foreach(x => compressionTypes.append(x.id.toByte))
    compressionTypes.toList
  }

  // TODO: Needs to integrate with external plugin changes..
  // if plugin is not configured, getMetricsSubscriptions and pushMetricSubscription needs to return errors
  // to the client.
  @Evolving
  def checkCmReceiverPluginConfigured()  = {
    if (ClientMetricsReceiverPlugin.getCmReceiver().isEmpty) {
      throw new ClientMetricsReceiverPluginNotFoundException("Broker does not have any configured client metrics receiver plugin")
    }
  }

  def processGetTelemetrySubscriptionRequest(request: RequestChannel.Request,
                                             throttleMs: Int): GetTelemetrySubscriptionResponse = {
    checkCmReceiverPluginConfigured()
    val subscriptionRequest = request.body[GetTelemetrySubscriptionRequest]
    val clientInfo = CmClientInformation(request, subscriptionRequest.getClientInstanceId.toString)
    _instance.processGetSubscriptionRequest(subscriptionRequest, clientInfo, throttleMs)
  }

  def processPushTelemetryRequest(request: RequestChannel.Request, throttleMs: Int): PushTelemetryResponse = {
    checkCmReceiverPluginConfigured()
    val pushTelemetryRequest = request.body[PushTelemetryRequest]
    val clientInfo = CmClientInformation(request, pushTelemetryRequest.getClientInstanceId.toString)
    _instance.processPushTelemetryRequest(pushTelemetryRequest, request.context, clientInfo, throttleMs)
  }
}

class ClientMetricsManager {

  class ClientMetricsException(val s: String, var errorCode: Errors) extends KafkaException(s) {
    def getErrorCode: Errors = this.errorCode
  }

  def getClientInstance(id: Uuid) : Option[CmClientInstanceState] =  {
    Option(id) match {
      case None | Some(Uuid.ZERO_UUID) => None
      case _ => ClientMetricsCache.getInstance.get(id)
    }
  }

  // Generates a new random client id and makes sure that it was not assigned to any of the previous clients
  def generateNewClientId(): Uuid = {
    var id = Uuid.randomUuid()
    while (!ClientMetricsCache.getInstance.get(id).isEmpty) {
      id = Uuid.randomUuid()
    }
    id
  }

  def processGetSubscriptionRequest(subscriptionRequest: GetTelemetrySubscriptionRequest,
                                    clientInfo: CmClientInformation,
                                    throttleMs: Int): GetTelemetrySubscriptionResponse = {

    val clientInstanceId = Option(subscriptionRequest.getClientInstanceId) match {
      case None | Some(Uuid.ZERO_UUID) => generateNewClientId()

      case _ =>  subscriptionRequest.getClientInstanceId
    }

    val clientInstance = getClientInstance(clientInstanceId).getOrElse(createClientInstance(clientInstanceId, clientInfo))
    val data =  new GetTelemetrySubscriptionsResponseData()
        .setThrottleTimeMs(throttleMs)
        .setClientInstanceId(clientInstanceId)
        .setSubscriptionId(clientInstance.getSubscriptionId) // TODO: should we use LONG instead of int?
        .setAcceptedCompressionTypes(getSupportedCompressionTypes.asJava)
        .setPushIntervalMs(clientInstance.getPushIntervalMs)
        .setDeltaTemporality(true)
        .setErrorCode(Errors.NONE.code())
        .setRequestedMetrics(clientInstance.getMetrics.asJava)

    if (clientInstance.isDisabledForMetricsCollection) {
      info(s"Metrics collection is disabled for the client: ${clientInstance.getId.toString}")
    }

    clientInstance.updateLastAccessTs(getCurrentTime)

    new GetTelemetrySubscriptionResponse(data)
  }

  def validatePushRequest(pushTelemetryRequest: PushTelemetryRequest,
                          clientInfo: CmClientInformation): Unit = {

    def isSupportedCompressionType(id: Int) : Boolean = {
      try {
        CompressionType.forId(id)
        true
      } catch {
        case e: IllegalArgumentException => false
      }
    }

    val clientInstanceId = Option(pushTelemetryRequest.getClientInstanceId) match {
      case None | Some(Uuid.ZERO_UUID) =>
        val msg = String.format("Invalid request from the client [%s], missing client instance id", clientInfo.getClientId)
        throw new ClientMetricsException(msg, Errors.INVALID_REQUEST)
      case _ => pushTelemetryRequest.getClientInstanceId
    }

    val clientInstance = getClientInstance(clientInstanceId).getOrElse(createClientInstance(clientInstanceId, clientInfo))

    // Once client set the state to Terminating do not accept any further requests.
    if (clientInstance.isClientTerminating) {
      val msg = String.format(
        "Client [%s] sent the previous request with state terminating to TRUE, can not accept any requests after that",
        pushTelemetryRequest.getClientInstanceId.toString)
      throw new ClientMetricsException(msg, Errors.INVALID_REQUEST)
    }

    // Make sure that this request is arrived after the next push interval, but this check would be bypassed if the
    // client has set the Terminating flag.
    if (!clientInstance.canAcceptPushRequest() && !pushTelemetryRequest.isClientTerminating) {
      val msg = String.format("Request from the client [%s] arrived before the next push interval time",
          pushTelemetryRequest.getClientInstanceId.toString)
      throw new ClientMetricsException(msg, Errors.CLIENT_METRICS_RATE_LIMITED)
    }

    if (pushTelemetryRequest.getSubscriptionId != clientInstance.getSubscriptionId) {
      val msg = String.format("Client's subscription id [%s] != Broker's cached client's subscription id [%s]",
        pushTelemetryRequest.getSubscriptionId.toString, clientInstance.getSubscriptionId.toString)
      throw new ClientMetricsException(msg, Errors.UNKNOWN_CLIENT_METRICS_SUBSCRIPTION_ID)
    }

    if (!isSupportedCompressionType(pushTelemetryRequest.data().compressionType)) {
      val msg = String.format("Unknown compression type [%s] is received in PushTelemetryRequest from %s",
        pushTelemetryRequest.data().compressionType().toString, pushTelemetryRequest.getClientInstanceId.toString)
      throw new ClientMetricsException(msg, Errors.UNSUPPORTED_COMPRESSION_TYPE)
    }

  }

  def processPushTelemetryRequest(pushTelemetryRequest: PushTelemetryRequest,
                                  requestContext: RequestContext,
                                  clientInfo: CmClientInformation,
                                  throttleMs: Int): PushTelemetryResponse = {

    def createResponse(errors: Option[Errors]) : PushTelemetryResponse = {
      var adjustedThrottleMs = throttleMs
      val clientInstance = getClientInstance(pushTelemetryRequest.getClientInstanceId)

      // Before sending the response make sure to update the book keeping markers like
      // lastAccessTime, isTerminating flag etc..
      if (!clientInstance.isEmpty) {
        adjustedThrottleMs = Math.max(clientInstance.get.getAdjustedPushInterval(), throttleMs)
        clientInstance.get.updateLastAccessTs(getCurrentTime)

        // update the client terminating flag only once
        if (!pushTelemetryRequest.isClientTerminating) {
          clientInstance.get.setTerminatingFlag(pushTelemetryRequest.isClientTerminating)
        }
      }

      pushTelemetryRequest.createResponse(adjustedThrottleMs, errors.getOrElse(Errors.NONE))
    }

    var errorCode = Errors.NONE
    try {
      // Validate the push request parameters
      validatePushRequest(pushTelemetryRequest, clientInfo)

      // Push the metrics to the external client receiver plugin.
      val metrics = Option(pushTelemetryRequest.data().metrics())
      if (!metrics.isEmpty && !metrics.get.isEmpty) {
        val payload = ClientMetricsReceiverPlugin.createPayload(pushTelemetryRequest)
        ClientMetricsReceiverPlugin.getCmReceiver().get.exportMetrics(requestContext, payload)
      }
    } catch {
      case e: ClientMetricsException => {
        warn("PushTelemetry request raised an exception: " + e.getMessage)
        errorCode = e.getErrorCode
      }
    }

    // Finally, send the response back to the client
    createResponse(Option(errorCode))
  }

  def updateSubscription(groupId :String, properties :Properties) = {
    ClientMetricsConfig.updateClientSubscription(groupId, properties)
  }

  def createClientInstance(clientInstanceId: Uuid, clientInfo: CmClientInformation): CmClientInstanceState = {
    val clientInstance = CmClientInstanceState(clientInstanceId, clientInfo,
                                               ClientMetricsConfig.getClientSubscriptions)
    // Add to the cache and if cache size > max entries then time to make some room by running
    // GC to clean up all the expired entries in the cache.
    ClientMetricsCache.getInstance.add(clientInstance)
    ClientMetricsCache.deleteExpiredEntries()
    clientInstance
  }
}

