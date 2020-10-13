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

import java.lang.{Byte => JByte}
import java.util.Collections

import kafka.network.RequestChannel
import kafka.security.authorizer.AclEntry
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, RequestContext}
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.CLUSTER
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}

import scala.jdk.CollectionConverters._

/**
 * Helper class for request handlers. Provides common functionality around throttling, authorizations, and error handling
 */
class ApisUtils(val requestChannel: RequestChannel,
                val authorizer: Option[Authorizer],
                val quotas: QuotaManagers,
                val time: Time) extends Logging {

  // private package for testing
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

  // Throttle the channel if the request quota is enabled but has been violated. Regardless of throttling, send the
  // response immediately.
  def sendResponseMaybeThrottle(request: RequestChannel.Request,
                                createResponse: Int => AbstractResponse,
                                onComplete: Option[Send => Unit] = None): Unit = {
    val throttleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    quotas.request.throttle(request, throttleTimeMs, requestChannel.sendResponse)
    requestChannel.sendResponse(request, Some(createResponse(throttleTimeMs)), onComplete)
  }

  def sendErrorResponseMaybeThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    val throttleTimeMs = maybeRecordAndGetThrottleTimeMs(request)
    quotas.request.throttle(request, throttleTimeMs, requestChannel.sendResponse)
    sendErrorOrCloseConnection(request, error, throttleTimeMs)
  }

  private def maybeRecordAndGetThrottleTimeMs(request: RequestChannel.Request): Int = {
    val throttleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds())
    request.apiThrottleTimeMs = throttleTimeMs
    throttleTimeMs
  }

  /**
   * Throttle the channel if the controller mutations quota or the request quota have been violated.
   * Regardless of throttling, send the response immediately.
   */
  def sendResponseMaybeThrottle(controllerMutationQuota: ControllerMutationQuota,
                                request: RequestChannel.Request,
                                createResponse: Int => AbstractResponse,
                                onComplete: Option[Send => Unit]): Unit = {
    val timeMs = time.milliseconds
    val controllerThrottleTimeMs = controllerMutationQuota.throttleTime
    val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
    val maxThrottleTimeMs = Math.max(controllerThrottleTimeMs, requestThrottleTimeMs)
    if (maxThrottleTimeMs > 0) {
      request.apiThrottleTimeMs = maxThrottleTimeMs
      if (controllerThrottleTimeMs > requestThrottleTimeMs) {
        quotas.controllerMutation.throttle(request, controllerThrottleTimeMs, requestChannel.sendResponse)
      } else {
        quotas.request.throttle(request, requestThrottleTimeMs, requestChannel.sendResponse)
      }
    }

    requestChannel.sendResponse(request, Some(createResponse(maxThrottleTimeMs)), onComplete)
  }

  def sendResponseExemptThrottle(request: RequestChannel.Request,
                                 response: AbstractResponse,
                                 onComplete: Option[Send => Unit] = None): Unit = {
    quotas.request.maybeRecordExempt(request)
    requestChannel.sendResponse(request, Some(response), onComplete)
  }

  def sendErrorResponseExemptThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendErrorOrCloseConnection(request, error, 0)
  }

  def sendErrorOrCloseConnection(request: RequestChannel.Request, error: Throwable, throttleMs: Int): Unit = {
    val requestBody = request.body[AbstractRequest]
    val response = requestBody.getErrorResponse(throttleMs, error)
    if (response == null)
      requestChannel.closeConnection(request, requestBody.errorCounts(error))
    else
      requestChannel.sendResponse(request, Some(response), None)
  }

  def sendNoOpResponseExemptThrottle(request: RequestChannel.Request): Unit = {
    quotas.request.maybeRecordExempt(request)
    requestChannel.sendResponse(request, None, None)
  }
}
