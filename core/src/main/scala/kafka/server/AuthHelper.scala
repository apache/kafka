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
import kafka.utils.CoreUtils
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.CLUSTER
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class AuthHelper(authorizer: Option[Authorizer]) {

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

  def authorizeByResourceType(requestContext: RequestContext, operation: AclOperation,
                              resourceType: ResourceType): Boolean = {
    authorizer.forall { authZ =>
      authZ.authorizeByResourceType(requestContext, operation, resourceType) == AuthorizationResult.ALLOWED
    }
  }

  def partitionSeqByAuthorized[T](requestContext: RequestContext,
                                  operation: AclOperation,
                                  resourceType: ResourceType,
                                  resources: Seq[T],
                                  logIfAllowed: Boolean = true,
                                  logIfDenied: Boolean = true)(resourceName: T => String): (Seq[T], Seq[T]) = {
    authorizer match {
      case Some(_) =>
        val authorizedResourceNames = filterByAuthorized(requestContext, operation, resourceType,
          resources, logIfAllowed, logIfDenied)(resourceName)
        resources.partition(resource => authorizedResourceNames.contains(resourceName(resource)))
      case None => (resources, Seq.empty)
    }
  }

  def partitionMapByAuthorized[K, V](requestContext: RequestContext,
                                     operation: AclOperation,
                                     resourceType: ResourceType,
                                     resources: Map[K, V],
                                     logIfAllowed: Boolean = true,
                                     logIfDenied: Boolean = true)(resourceName: K => String): (Map[K, V], Map[K, V]) = {
    authorizer match {
      case Some(_) =>
        val authorizedResourceNames = filterByAuthorized(requestContext, operation, resourceType,
          resources.keySet, logIfAllowed, logIfDenied)(resourceName)
        resources.partition { case (k, _) => authorizedResourceNames.contains(resourceName(k)) }
      case None => (resources, Map.empty)
    }
  }

  def filterByAuthorized[T](requestContext: RequestContext,
                            operation: AclOperation,
                            resourceType: ResourceType,
                            resources: Iterable[T],
                            logIfAllowed: Boolean = true,
                            logIfDenied: Boolean = true)(resourceName: T => String): Set[String] = {
    authorizer match {
      case Some(authZ) =>
        val resourceNameToCount = CoreUtils.groupMapReduce(resources)(resourceName)(_ => 1)(_ + _)
        val actions = resourceNameToCount.iterator.map { case (resourceName, count) =>
          val resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL)
          new Action(operation, resource, count, logIfAllowed, logIfDenied)
        }.toBuffer
        authZ.authorize(requestContext, actions.asJava).asScala
          .zip(resourceNameToCount.keySet)
          .collect { case (authzResult, resourceName) if authzResult == AuthorizationResult.ALLOWED =>
            resourceName
          }.toSet
      case None => resources.iterator.map(resourceName).toSet
    }
  }

}
