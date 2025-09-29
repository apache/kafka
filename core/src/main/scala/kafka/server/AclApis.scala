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
import kafka.utils.Logging
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult
import org.apache.kafka.common.message._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.security.authorizer.AuthorizerUtils
import org.apache.kafka.server.authorizer._

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional

/**
 * Logic to handle ACL requests.
 */
class AclApis(authHelper: AuthHelper,
              authorizer: Option[Authorizer],
              requestHelper: RequestHandlerHelper,
              name: String,
              config: KafkaConfig) extends Logging {
  this.logIdent = "[AclApis-%s-%s] ".format(name, config.nodeId)
  private val alterAclsPurgatory =
      new DelayedFuturePurgatory(purgatoryName = "AlterAcls", brokerId = config.nodeId)

  def isClosed: Boolean = alterAclsPurgatory.isShutdown

  def close(): Unit = alterAclsPurgatory.shutdown()

  def handleDescribeAcls(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    val describeAclsRequest = request.body[DescribeAclsRequest]
    authorizer match {
      case None =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(new DescribeAclsResponseData()
            .setErrorCode(Errors.SECURITY_DISABLED.code)
            .setErrorMessage("No Authorizer is configured on the broker")
            .setThrottleTimeMs(requestThrottleMs),
          describeAclsRequest.version))
      case Some(auth) =>
        val filter = describeAclsRequest.filter
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(new DescribeAclsResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setResources(DescribeAclsResponse.aclsResources(auth.acls(filter))),
          describeAclsRequest.version))
    }
    CompletableFuture.completedFuture[Unit](())
  }

  def handleCreateAcls(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, ALTER)
    val createAclsRequest = request.body[CreateAclsRequest]

    authorizer match {
      case None => requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        createAclsRequest.getErrorResponse(requestThrottleMs,
          new SecurityDisabledException("No Authorizer is configured.")))
        CompletableFuture.completedFuture[Unit](())
      case Some(auth) =>
        val allBindings = createAclsRequest.aclCreations.asScala.map(CreateAclsRequest.aclBinding)
        val errorResults = mutable.Map[AclBinding, AclCreateResult]()
        val validBindings = new ArrayBuffer[AclBinding]
        allBindings.foreach { acl =>
          val resource = acl.pattern
          val throwable = if (resource.resourceType == ResourceType.CLUSTER && !AuthorizerUtils.isClusterResource(resource.name))
              new InvalidRequestException("The only valid name for the CLUSTER resource is " + CLUSTER_NAME)
          else if (resource.name.isEmpty)
            new InvalidRequestException("Invalid empty resource name")
          else
            null
          if (throwable != null) {
            debug(s"Failed to add acl $acl to $resource", throwable)
            errorResults(acl) = new AclCreateResult(throwable)
          } else
            validBindings += acl
        }

        val future = new CompletableFuture[util.List[AclCreationResult]]()
        val createResults = auth.createAcls(request.context, validBindings.asJava).asScala.map(_.toCompletableFuture)

        def sendResponseCallback(): Unit = {
          val aclCreationResults = allBindings.map { acl =>
            val result = errorResults.getOrElse(acl, createResults(validBindings.indexOf(acl)).get)
            val creationResult = new AclCreationResult()
            result.exception.toScala.foreach { throwable =>
              val apiError = ApiError.fromThrowable(throwable)
              creationResult
                .setErrorCode(apiError.error.code)
                .setErrorMessage(apiError.message)
            }
            creationResult
          }
          future.complete(aclCreationResults.asJava)
        }
        alterAclsPurgatory.tryCompleteElseWatch(config.connectionsMaxIdleMs, createResults, sendResponseCallback)

        future.thenApply[Unit] { aclCreationResults =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new CreateAclsResponse(new CreateAclsResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setResults(aclCreationResults)))
        }
    }
  }

  def handleDeleteAcls(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, ALTER)
    val deleteAclsRequest = request.body[DeleteAclsRequest]
    authorizer match {
      case None =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          deleteAclsRequest.getErrorResponse(requestThrottleMs,
            new SecurityDisabledException("No Authorizer is configured.")))
        CompletableFuture.completedFuture[Unit](())
      case Some(auth) =>

        val future = new CompletableFuture[util.List[DeleteAclsFilterResult]]()
        val deleteResults = auth.deleteAcls(request.context, deleteAclsRequest.filters)
          .asScala.map(_.toCompletableFuture).toList

        def sendResponseCallback(): Unit = {
          val filterResults = deleteResults.map(_.get).map(DeleteAclsResponse.filterResult).asJava
          future.complete(filterResults)
        }

        alterAclsPurgatory.tryCompleteElseWatch(config.connectionsMaxIdleMs, deleteResults, sendResponseCallback)
        future.thenApply[Unit] { filterResults =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new DeleteAclsResponse(
              new DeleteAclsResponseData()
                .setThrottleTimeMs(requestThrottleMs)
                .setFilterResults(filterResults),
              deleteAclsRequest.version))
        }
    }
  }
 }
