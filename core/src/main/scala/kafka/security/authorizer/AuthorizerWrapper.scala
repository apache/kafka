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
import kafka.security.auth.{Acl, Operation, PermissionType, Resource, ResourceType => ResourceTypeLegacy}
import kafka.security.authorizer.AuthorizerWrapper._
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.errors.{ApiException, InvalidRequestException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.utils.SecurityUtils.parseKafkaPrincipal
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult
import org.apache.kafka.server.authorizer.{AuthorizableRequestContext, AuthorizerServerInfo, _}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, immutable, mutable}
import scala.util.{Failure, Success, Try}

@deprecated("Use kafka.security.authorizer.AclAuthorizer", "Since 2.5")
object AuthorizerWrapper {

  def convertToResourceAndAcl(filter: AclBindingFilter): Either[ApiError, (Resource, Acl)] = {
    (for {
      resourceType <- Try(ResourceTypeLegacy.fromJava(filter.patternFilter.resourceType))
      principal <- Try(parseKafkaPrincipal(filter.entryFilter.principal))
      operation <- Try(Operation.fromJava(filter.entryFilter.operation))
      permissionType <- Try(PermissionType.fromJava(filter.entryFilter.permissionType))
      resource = Resource(resourceType, filter.patternFilter.name, filter.patternFilter.patternType)
      acl = Acl(principal, permissionType, filter.entryFilter.host, operation)
    } yield (resource, acl)) match {
      case Failure(throwable) => Left(new ApiError(Errors.INVALID_REQUEST, throwable.getMessage))
      case Success(s) => Right(s)
    }
  }

  def convertToAclBinding(resource: Resource, acl: Acl): AclBinding = {
    val resourcePattern = new ResourcePattern(resource.resourceType.toJava, resource.name, resource.patternType)
    new AclBinding(resourcePattern, convertToAccessControlEntry(acl))
  }

  def convertToAccessControlEntry(acl: Acl): AccessControlEntry = {
    new AccessControlEntry(acl.principal.toString, acl.host.toString,
      acl.operation.toJava, acl.permissionType.toJava)
  }

  def convertToAcl(ace: AccessControlEntry): Acl = {
    new Acl(parseKafkaPrincipal(ace.principal), PermissionType.fromJava(ace.permissionType), ace.host,
      Operation.fromJava(ace.operation))
  }

  def convertToResource(resourcePattern: ResourcePattern): Resource = {
    Resource(ResourceTypeLegacy.fromJava(resourcePattern.resourceType), resourcePattern.name, resourcePattern.patternType)
  }
}

@deprecated("Use kafka.security.authorizer.AclAuthorizer", "Since 2.5")
class AuthorizerWrapper(private[kafka] val baseAuthorizer: kafka.security.auth.Authorizer) extends Authorizer {

  var shouldAllowEveryoneIfNoAclIsFound = false

  override def configure(configs: util.Map[String, _]): Unit = {
    baseAuthorizer.configure(configs)
    shouldAllowEveryoneIfNoAclIsFound = configs.asScala.get(
      AclAuthorizer.AllowEveryoneIfNoAclIsFoundProp).exists(_.toString.toBoolean)
  }

  override def start(serverInfo: AuthorizerServerInfo): util.Map[Endpoint, _ <: CompletionStage[Void]] = {
    serverInfo.endpoints.asScala.map { endpoint =>
      endpoint -> CompletableFuture.completedFuture[Void](null) }.toMap.asJava
  }

  override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
    val session = Session(requestContext.principal, requestContext.clientAddress)
    actions.asScala.map { action =>
      val operation = Operation.fromJava(action.operation)
      if (baseAuthorizer.authorize(session, operation, convertToResource(action.resourcePattern)))
        AuthorizationResult.ALLOWED
      else
        AuthorizationResult.DENIED
    }.asJava
  }

  override def createAcls(requestContext: AuthorizableRequestContext,
                          aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = {
    aclBindings.asScala
      .map { aclBinding =>
        convertToResourceAndAcl(aclBinding.toFilter) match {
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
        convertToResourceAndAcl(filter) match {
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
        val aclBinding = convertToAclBinding(resource, acl)
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
      acls.map(acl => convertToAclBinding(resource, acl)).filter(filter.matches)
    }.asJava
  }

  override def close(): Unit = {
    baseAuthorizer.close()
  }

  override def authorizeAny(requestContext: AuthorizableRequestContext,
                            op: AclOperation,
                            resourceType: ResourceType): AuthorizationResult = {
    if (resourceType == ResourceType.ANY)
      throw new IllegalArgumentException("Must specify a non-filter resource type for authorizeAny")

    if (resourceType == ResourceType.UNKNOWN)
      throw new IllegalArgumentException("Unknown resource type")

    if (op == AclOperation.ANY)
      throw new IllegalArgumentException("Must specify a non-filter operation type for authorizeAny")

    if (op == AclOperation.UNKNOWN)
      throw new IllegalArgumentException("Unknown operation type")

    if (shouldAllowEveryoneIfNoAclIsFound && !denyAllResource(requestContext, op, resourceType)) {
      AuthorizationResult.ALLOWED
    } else {
      super.authorizeAny(requestContext, op, resourceType)
    }
  }

  def denyAllResource(requestContext: AuthorizableRequestContext,
                      op: AclOperation,
                      resourceType: ResourceType): Boolean = {
    val resourceTypeFilter = new ResourcePatternFilter(
      resourceType, null, PatternType.ANY)
    val accessControlEntry = new AccessControlEntryFilter(
      null, null, null, AclPermissionType.DENY)
    val aclFilter = new AclBindingFilter(resourceTypeFilter, accessControlEntry)

    for (binding <- acls(aclFilter).asScala) {
      if (aceMatched(requestContext, op, binding) && canDenyAll(binding.pattern()))
        return true
    }
    false
  }

  def aceMatched(requestContext: AuthorizableRequestContext,
                 op: AclOperation,
                 binding: AclBinding): Boolean = {
    (hostMatched(requestContext, binding) && principleMatched(requestContext, binding)
      && operationMatched(op, binding))
  }

  def hostMatched(requestContext: AuthorizableRequestContext,
                  binding: AclBinding): Boolean =
    (binding.entry().host().equals(requestContext.clientAddress().getHostAddress)
      || binding.entry().host().equals(AclEntry.WildcardHost))

  def principleMatched(requestContext: AuthorizableRequestContext,
                  binding: AclBinding): Boolean =
    (binding.entry().principal().equals(requestContext.principal().toString)
      || binding.entry().principal().equals(AclEntry.WildcardPrincipal.toString))


  def operationMatched(op: AclOperation,
                       binding: AclBinding): Boolean =
    (binding.entry().operation() == op
      || binding.entry().operation() == AclOperation.ALL)

  def canDenyAll(pattern: ResourcePattern): Boolean = {
    pattern.patternType() match {
      case PatternType.LITERAL => pattern.name().equals(ResourcePattern.WILDCARD_RESOURCE)
      case PatternType.PREFIXED => pattern.name().isEmpty
      case _ => false
    }
  }

}
