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
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinator

import java.util.OptionalInt

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
        groupCoordinator.onElection(partition.partitionId, partition.getLeaderEpoch)
      else if (partition.topic == Topic.TRANSACTION_STATE_TOPIC_NAME)
        txnCoordinator.onElection(partition.partitionId, partition.getLeaderEpoch)
    }

    updatedFollowers.foreach { partition =>
      if (partition.topic == Topic.GROUP_METADATA_TOPIC_NAME)
        groupCoordinator.onResignation(partition.partitionId, OptionalInt.of(partition.getLeaderEpoch))
      else if (partition.topic == Topic.TRANSACTION_STATE_TOPIC_NAME)
        txnCoordinator.onResignation(partition.partitionId, Some(partition.getLeaderEpoch))
    }
  }

}

class RequestHandlerHelper(
  requestChannel: RequestChannel,
  quotas: QuotaManagers,
  time: Time
) {

  def throttle(
    quotaManager: ClientQuotaManager,
    request: RequestChannel.Request,
    throttleTimeMs: Int
  ): Unit = {
    val callback = new ThrottleCallback {
      override def startThrottling(): Unit = requestChannel.startThrottling(request)
      override def endThrottling(): Unit = requestChannel.endThrottling(request)
    }
    quotaManager.throttle(request, callback, throttleTimeMs)
  }

  def handleError(request: RequestChannel.Request, e: Throwable): Unit = {
    val mayThrottle = e.isInstanceOf[ClusterAuthorizationException] || !request.header.apiKey.clusterAction
    if (mayThrottle)
      sendErrorResponseMaybeThrottle(request, e)
    else
      sendErrorResponseExemptThrottle(request, e)
  }

  def sendErrorOrCloseConnection(
    request: RequestChannel.Request,
    error: Throwable,
    throttleMs: Int
  ): Unit = {
    val requestBody = request.body[AbstractRequest]
    val response = requestBody.getErrorResponse(throttleMs, error)
    if (response == null)
      requestChannel.closeConnection(request, requestBody.errorCounts(error))
    else
      requestChannel.sendResponse(request, response, None)
  }

  def sendForwardedResponse(request: RequestChannel.Request,
                            response: AbstractResponse): Unit = {
    // For requests forwarded to the controller, we take the maximum of the local
    // request throttle and the throttle sent by the controller in the response.
    val controllerThrottleTimeMs = response.throttleTimeMs()
    val requestThrottleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    val appliedThrottleTimeMs = math.max(controllerThrottleTimeMs, requestThrottleTimeMs)
    throttle(quotas.request, request, appliedThrottleTimeMs)
    response.maybeSetThrottleTimeMs(appliedThrottleTimeMs)
    requestChannel.sendResponse(request, response, None)
  }

  // Throttle the channel if the request quota is enabled but has been violated. Regardless of throttling, send the
  // response immediately.
  def sendMaybeThrottle(
    request: RequestChannel.Request,
    response: AbstractResponse
  ): Unit = {
    val throttleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    // Only throttle non-forwarded requests
    if (!request.isForwarded)
      throttle(quotas.request, request, throttleTimeMs)
    response.maybeSetThrottleTimeMs(throttleTimeMs)
    requestChannel.sendResponse(request, response, None)
  }

  def sendResponseMaybeThrottle(request: RequestChannel.Request,
                                createResponse: Int => AbstractResponse): Unit = {
    val throttleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    // Only throttle non-forwarded requests
    if (!request.isForwarded)
      throttle(quotas.request, request, throttleTimeMs)
    requestChannel.sendResponse(request, createResponse(throttleTimeMs), None)
  }

  def sendErrorResponseMaybeThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    val throttleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    // Only throttle non-forwarded requests or cluster authorization failures
    if (error.isInstanceOf[ClusterAuthorizationException] || !request.isForwarded)
      throttle(quotas.request, request, throttleTimeMs)
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
  def sendResponseMaybeThrottleWithControllerQuota(
    controllerMutationQuota: ControllerMutationQuota,
    request: RequestChannel.Request,
    response: AbstractResponse
  ): Unit = {
    val timeMs = time.milliseconds
    val controllerThrottleTimeMs = controllerMutationQuota.throttleTime
    val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
    val maxThrottleTimeMs = Math.max(controllerThrottleTimeMs, requestThrottleTimeMs)
    // Only throttle non-forwarded requests
    if (maxThrottleTimeMs > 0 && !request.isForwarded) {
      request.apiThrottleTimeMs = maxThrottleTimeMs
      if (controllerThrottleTimeMs > requestThrottleTimeMs) {
        throttle(quotas.controllerMutation, request, controllerThrottleTimeMs)
      } else {
        throttle(quotas.request, request, requestThrottleTimeMs)
      }
    }

    response.maybeSetThrottleTimeMs(maxThrottleTimeMs)
    requestChannel.sendResponse(request, response, None)
  }

  def sendResponseExemptThrottle(request: RequestChannel.Request,
                                 response: AbstractResponse,
                                 onComplete: Option[Send => Unit] = None): Unit = {
    quotas.request.maybeRecordExempt(request)
    requestChannel.sendResponse(request, response, onComplete)
  }

  def sendErrorResponseExemptThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendErrorOrCloseConnection(request, error, 0)
  }

  def sendNoOpResponseExemptThrottle(request: RequestChannel.Request): Unit = {
    quotas.request.maybeRecordExempt(request)
    requestChannel.sendNoOpResponse(request)
  }

}
