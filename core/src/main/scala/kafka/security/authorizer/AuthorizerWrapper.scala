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

package kafka.security.authorizer

import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.{lang, util}

import kafka.network.RequestChannel.Session
import kafka.security.auth.{Acl, Operation, Resource}
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclBindingFilter}
import org.apache.kafka.common.errors.{ApiException, InvalidRequestException}
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult
import org.apache.kafka.server.authorizer.{AuthorizableRequestContext, AuthorizerServerInfo, _}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, immutable, mutable}

class AuthorizerWrapper(private[kafka] val baseAuthorizer: kafka.security.auth.Authorizer) extends Authorizer {

  override def configure(configs: util.Map[String, _]): Unit = {
    baseAuthorizer.configure(configs)
  }

  override def start(serverInfo: AuthorizerServerInfo): util.Map[Endpoint, _ <: CompletionStage[Void]] = {
    serverInfo.endpoints.asScala.map { endpoint =>
      endpoint -> CompletableFuture.completedFuture[Void](null) }.toMap.asJava
  }

  override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
    val session = Session(requestContext.principal, requestContext.clientAddress)
    actions.asScala.map { action =>
      val operation = Operation.fromJava(action.operation)
      if (baseAuthorizer.authorize(session, operation, AuthorizerUtils.convertToResource(action.resourcePattern)))
        AuthorizationResult.ALLOWED
      else
        AuthorizationResult.DENIED
    }.asJava
  }

  override def createAcls(requestContext: AuthorizableRequestContext,
                          aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = {
    aclBindings.asScala
      .map { aclBinding =>
        AuthorizerUtils.convertToResourceAndAcl(aclBinding.toFilter) match {
          case Left(apiError) => new AclCreateResult(apiError.exception)
          case Right((resource, acl)) =>
            try {
              baseAuthorizer.addAcls(Set(acl), resource)
              AclCreateResult.SUCCESS
            } catch {
              case e: ApiException => new AclCreateResult(e)
              case e: Throwable => new AclCreateResult(new InvalidRequestException("Failed to create ACL", e))
            }
        }
      }.toList.map(CompletableFuture.completedFuture[AclCreateResult]).asJava
  }

  override def deleteAcls(requestContext: AuthorizableRequestContext,
                          aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = {
    val filters = aclBindingFilters.asScala
    val results = mutable.Map[Int, AclDeleteResult]()
    val toDelete = mutable.Map[Int, ArrayBuffer[(Resource, Acl)]]()

    if (filters.forall(_.matchesAtMostOne)) {
      // Delete based on a list of ACL fixtures.
      for ((filter, i) <- filters.zipWithIndex) {
        AuthorizerUtils.convertToResourceAndAcl(filter) match {
          case Left(apiError) => results.put(i, new AclDeleteResult(apiError.exception))
          case Right(binding) => toDelete.put(i, ArrayBuffer(binding))
        }
      }
    } else {
      // Delete based on filters that may match more than one ACL.
      val aclMap = baseAuthorizer.getAcls()
      val filtersWithIndex = filters.zipWithIndex
      for ((resource, acls) <- aclMap; acl <- acls) {
        val binding = new AclBinding(
          new ResourcePattern(resource.resourceType.toJava, resource.name, resource.patternType),
          new AccessControlEntry(acl.principal.toString, acl.host.toString, acl.operation.toJava,
            acl.permissionType.toJava))

        for ((filter, i) <- filtersWithIndex if filter.matches(binding))
          toDelete.getOrElseUpdate(i, ArrayBuffer.empty) += ((resource, acl))
      }
    }

    for ((i, acls) <- toDelete) {
      val deletionResults = acls.flatMap { case (resource, acl) =>
        val aclBinding = AuthorizerUtils.convertToAclBinding(resource, acl)
        try {
          if (baseAuthorizer.removeAcls(immutable.Set(acl), resource))
            Some(new AclBindingDeleteResult(aclBinding))
          else None
        } catch {
          case throwable: Throwable =>
            Some(new AclBindingDeleteResult(aclBinding, ApiError.fromThrowable(throwable).exception))
        }
      }.asJava

      results.put(i, new AclDeleteResult(deletionResults))
    }

    filters.indices.map { i =>
      results.getOrElse(i, new AclDeleteResult(Seq.empty[AclBindingDeleteResult].asJava))
    }.map(CompletableFuture.completedFuture[AclDeleteResult]).asJava
  }

  override def acls(filter: AclBindingFilter): lang.Iterable[AclBinding] = {
    baseAuthorizer.getAcls().flatMap { case (resource, acls) =>
      acls.map(acl => AuthorizerUtils.convertToAclBinding(resource, acl)).filter(filter.matches)
    }.asJava
  }

  override def close(): Unit = {
    baseAuthorizer.close()
  }
}