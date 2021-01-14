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

import java.lang.{Byte => JByte}
import java.util.Collections
import kafka.network.RequestChannel
import kafka.security.authorizer.AclEntry
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, RequestContext}
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.CLUSTER
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}

import scala.jdk.CollectionConverters._

/**
 * Helper methods for request handlers
 */
object RequestHandlerUtils {
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

class AuthHelper(val requestChannel: RequestChannel,
                 val authorizer: Option[Authorizer]) {
  def authorize(requestContext: RequestContext,
                operation: AclOperation,
                resourceType: ResourceType,
                resourceName: String,
                logIfAllowed: Boolean = true,
                logIfDenied: Boolean = true,
                refCount: Int = 1): Boolean = {
    authorizer.forall { authZ =>
      val resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL)
      val actions = Collections.singletonList(new Action(operation, resource, refCount, logIfAllowed, logIfDenied))
      authZ.authorize(requestContext, actions).get(0) == AuthorizationResult.ALLOWED
    }
  }

  def authorizeClusterOperation(request: RequestChannel.Request, operation: AclOperation): Unit = {
    if (!authorize(request.context, operation, CLUSTER, CLUSTER_NAME))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }

  def authorizedOperations(request: RequestChannel.Request, resource: Resource): Int = {
    val supportedOps = AclEntry.supportedOperations(resource.resourceType).toList
    val authorizedOps = authorizer match {
      case Some(authZ) =>
        val resourcePattern = new ResourcePattern(resource.resourceType, resource.name, PatternType.LITERAL)
        val actions = supportedOps.map { op => new Action(op, resourcePattern, 1, false, false) }
        authZ.authorize(request.context, actions.asJava).asScala
          .zip(supportedOps)
          .filter(_._1 == AuthorizationResult.ALLOWED)
          .map(_._2).toSet
      case None =>
        supportedOps.toSet
    }
    Utils.to32BitField(authorizedOps.map(operation => operation.code.asInstanceOf[JByte]).asJava)
  }
}

class ChannelHelper(val requestChannel: RequestChannel,
                    val quotas: QuotaManagers,
                    val time: Time,
                    val logPrefix: String) extends Logging {

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
    requestChannel.sendResponse(request, Some(response), None)
  }

  // Throttle the channel if the request quota is enabled but has been violated. Regardless of throttling, send the
  // response immediately.
  def sendResponseMaybeThrottle(request: RequestChannel.Request,
                                createResponse: Int => AbstractResponse): Unit = {
    val throttleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    // Only throttle non-forwarded requests
    if (!request.isForwarded)
      quotas.request.throttle(request, throttleTimeMs, requestChannel.sendResponse)
    requestChannel.sendResponse(request, Some(createResponse(throttleTimeMs)), None)
  }

  def sendErrorResponseMaybeThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    val throttleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    // Only throttle non-forwarded requests or cluster authorization failures
    if (error.isInstanceOf[ClusterAuthorizationException] || !request.isForwarded)
      quotas.request.throttle(request, throttleTimeMs, requestChannel.sendResponse)
    requestChannel.sendErrorOrCloseConnection(request, error, throttleTimeMs)
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

    requestChannel.sendResponse(request, Some(createResponse(maxThrottleTimeMs)), None)
  }

  def sendResponseExemptThrottle(request: RequestChannel.Request,
                                 response: AbstractResponse,
                                 onComplete: Option[Send => Unit] = None): Unit = {
    quotas.request.maybeRecordExempt(request)
    requestChannel.sendResponse(request, Some(response), onComplete)
  }

  def sendErrorResponseExemptThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    quotas.request.maybeRecordExempt(request)
    requestChannel.sendErrorOrCloseConnection(request, error, 0)
  }

  def sendNoOpResponseExemptThrottle(request: RequestChannel.Request): Unit = {
    quotas.request.maybeRecordExempt(request)
    requestChannel.sendResponse(request, None, None)
  }
}
