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

import kafka.cluster.Partition
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse}
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._


object RequestHandlerHelper {

  def onLeadershipChange(groupCoordinator: GroupCoordinator,
                         txnCoordinator: TransactionCoordinator,
                         updatedLeaders: Iterable[Partition],
                         updatedFollowers: Iterable[Partition]): Unit = {
    // for each new leader or follower, call coordinator to handle consumer group migration.
    // this callback is invoked under the replica state change lock to ensure proper order of
    // leadership changes
    updatedLeaders.foreach { partition =>
      if (partition.topic == Topic.GROUP_METADATA_TOPIC_NAME)
        groupCoordinator.onElection(partition.partitionId)
      else if (partition.topic == Topic.TRANSACTION_STATE_TOPIC_NAME)
        txnCoordinator.onElection(partition.partitionId, partition.getLeaderEpoch)
    }

    updatedFollowers.foreach { partition =>
      if (partition.topic == Topic.GROUP_METADATA_TOPIC_NAME)
        groupCoordinator.onResignation(partition.partitionId)
      else if (partition.topic == Topic.TRANSACTION_STATE_TOPIC_NAME)
        txnCoordinator.onResignation(partition.partitionId, Some(partition.getLeaderEpoch))
    }
  }
}



class RequestHandlerHelper(requestChannel: RequestChannel,
                           quotas: QuotaManagers,
                           time: Time,
                           logPrefix: String) extends Logging {

  this.logIdent = logPrefix

  def handleError(request: RequestChannel.Request, e: Throwable): Unit = {
    val mayThrottle = e.isInstanceOf[ClusterAuthorizationException] || !request.header.apiKey.clusterAction
    error("Error when handling request: " +
      s"clientId=${request.header.clientId}, " +
      s"correlationId=${request.header.correlationId}, " +
      s"api=${request.header.apiKey}, " +
      s"version=${request.header.apiVersion}, " +
      s"body=${request.body[AbstractRequest]}", e)
    if (mayThrottle)
      sendErrorResponseMaybeThrottle(request, e)
    else
      sendErrorResponseExemptThrottle(request, e)
  }

  def sendForwardedResponse(request: RequestChannel.Request,
                            response: AbstractResponse): Unit = {
    // For forwarded requests, we take the throttle time from the broker that
    // the request was forwarded to
    val throttleTimeMs = response.throttleTimeMs()
    quotas.request.throttle(request, throttleTimeMs, requestChannel.sendResponse)
    sendResponse(request, Some(response), None)
  }

  // Throttle the channel if the request quota is enabled but has been violated. Regardless of throttling, send the
  // response immediately.
  def sendResponseMaybeThrottle(request: RequestChannel.Request,
                                createResponse: Int => AbstractResponse): Unit = {
    val throttleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    // Only throttle non-forwarded requests
    if (!request.isForwarded)
      quotas.request.throttle(request, throttleTimeMs, requestChannel.sendResponse)
    sendResponse(request, Some(createResponse(throttleTimeMs)), None)
  }

  def sendErrorResponseMaybeThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    val throttleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    // Only throttle non-forwarded requests or cluster authorization failures
    if (error.isInstanceOf[ClusterAuthorizationException] || !request.isForwarded)
      quotas.request.throttle(request, throttleTimeMs, requestChannel.sendResponse)
    sendErrorOrCloseConnection(request, error, throttleTimeMs)
  }

  def maybeRecordAndGetThrottleTimeMs(request: RequestChannel.Request): Int = {
    val throttleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds())
    request.apiThrottleTimeMs = throttleTimeMs
    throttleTimeMs
  }

  /**
   * Throttle the channel if the controller mutations quota or the request quota have been violated.
   * Regardless of throttling, send the response immediately.
   */
  def sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota: ControllerMutationQuota,
                                                   request: RequestChannel.Request,
                                                   createResponse: Int => AbstractResponse): Unit = {
    val timeMs = time.milliseconds
    val controllerThrottleTimeMs = controllerMutationQuota.throttleTime
    val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
    val maxThrottleTimeMs = Math.max(controllerThrottleTimeMs, requestThrottleTimeMs)
    // Only throttle non-forwarded requests
    if (maxThrottleTimeMs > 0 && !request.isForwarded) {
      request.apiThrottleTimeMs = maxThrottleTimeMs
      if (controllerThrottleTimeMs > requestThrottleTimeMs) {
        quotas.controllerMutation.throttle(request, controllerThrottleTimeMs, requestChannel.sendResponse)
      } else {
        quotas.request.throttle(request, requestThrottleTimeMs, requestChannel.sendResponse)
      }
    }

    sendResponse(request, Some(createResponse(maxThrottleTimeMs)), None)
  }

  def sendResponseExemptThrottle(request: RequestChannel.Request,
                                 response: AbstractResponse,
                                 onComplete: Option[Send => Unit] = None): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendResponse(request, Some(response), onComplete)
  }

  def sendErrorOrCloseConnection(request: RequestChannel.Request, error: Throwable, throttleMs: Int): Unit = {
    val requestBody = request.body[AbstractRequest]
    val response = requestBody.getErrorResponse(throttleMs, error)
    if (response == null)
      closeConnection(request, requestBody.errorCounts(error))
    else
      sendResponse(request, Some(response), None)
  }

  def sendErrorResponseExemptThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendErrorOrCloseConnection(request, error, 0)
  }

  def sendNoOpResponseExemptThrottle(request: RequestChannel.Request): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendResponse(request, None, None)
  }

  def closeConnection(request: RequestChannel.Request, errorCounts: java.util.Map[Errors, Integer]): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    requestChannel.updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    requestChannel.sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  def sendResponse(request: RequestChannel.Request,
                   responseOpt: Option[AbstractResponse],
                   onComplete: Option[Send => Unit]): Unit = {
    // Update error metrics for each error code in the response including Errors.NONE
    responseOpt.foreach(response => requestChannel.updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala))

    val response = responseOpt match {
      case Some(response) =>
        new RequestChannel.SendResponse(
          request,
          request.buildResponseSend(response),
          request.responseNode(response),
          onComplete
        )
      case None =>
        new RequestChannel.NoOpResponse(request)
    }

    requestChannel.sendResponse(response)
  }
}
