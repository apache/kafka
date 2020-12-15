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
package kafka.security.auth

import java.util

import kafka.network.RequestChannel.Session
import kafka.security.auth.SimpleAclAuthorizer.BaseAuthorizer
import kafka.security.authorizer.{AclAuthorizer, AuthorizerUtils, AuthorizerWrapper}
import kafka.utils._
import kafka.zk.ZkVersion
import org.apache.kafka.common.acl.{AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.resource.{PatternType, ResourcePatternFilter}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

@deprecated("Use kafka.security.authorizer.AclAuthorizer", "Since 2.4")
object SimpleAclAuthorizer {
  //optional override zookeeper cluster configuration where acls will be stored, if not specified acls will be stored in
  //same zookeeper where all other kafka broker info is stored.
  val ZkUrlProp = AclAuthorizer.ZkUrlProp
  val ZkConnectionTimeOutProp = AclAuthorizer.ZkConnectionTimeOutProp
  val ZkSessionTimeOutProp = AclAuthorizer.ZkSessionTimeOutProp
  val ZkMaxInFlightRequests = AclAuthorizer.ZkMaxInFlightRequests

  //List of users that will be treated as super users and will have access to all the resources for all actions from all hosts, defaults to no super users.
  val SuperUsersProp = AclAuthorizer.SuperUsersProp
  //If set to true when no acls are found for a resource , authorizer allows access to everyone. Defaults to false.
  val AllowEveryoneIfNoAclIsFoundProp = AclAuthorizer.AllowEveryoneIfNoAclIsFoundProp

  case class VersionedAcls(acls: Set[Acl], zkVersion: Int) {
    def exists: Boolean = zkVersion != ZkVersion.UnknownVersion
  }
  val NoAcls = VersionedAcls(Set.empty, ZkVersion.UnknownVersion)

  private[auth] class BaseAuthorizer extends AclAuthorizer {
    override def logAuditMessage(requestContext: AuthorizableRequestContext, action: Action, authorized: Boolean): Unit = {
      val principal = requestContext.principal
      val host = requestContext.clientAddress.getHostAddress
      val operation = Operation.fromJava(action.operation)
      val resource = AuthorizerWrapper.convertToResource(action.resourcePattern)
      def logMessage: String = {
        val authResult = if (authorized) "Allowed" else "Denied"
        s"Principal = $principal is $authResult Operation = $operation from host = $host on resource = $resource"
      }

      if (authorized) authorizerLogger.debug(logMessage)
      else authorizerLogger.info(logMessage)
    }
  }
}

@deprecated("Use kafka.security.authorizer.AclAuthorizer", "Since 2.4")
class SimpleAclAuthorizer extends Authorizer with Logging {

  private val aclAuthorizer = new BaseAuthorizer

  // The maximum number of times we should try to update the resource acls in zookeeper before failing;
  // This should never occur, but is a safeguard just in case.
  protected[auth] var maxUpdateRetries = 10


  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def configure(javaConfigs: util.Map[String, _]): Unit = {
    aclAuthorizer.configure(javaConfigs)
  }

  override def authorize(session: Session, operation: Operation, resource: Resource): Boolean = {
    val requestContext = AuthorizerUtils.sessionToRequestContext(session)
    val action = new Action(operation.toJava, resource.toPattern, 1, true, true)
    aclAuthorizer.authorize(requestContext, List(action).asJava).asScala.head == AuthorizationResult.ALLOWED
  }

  def isSuperUser(operation: Operation, resource: Resource, principal: KafkaPrincipal, host: String): Boolean = {
    aclAuthorizer.isSuperUser(principal)
  }

  override def addAcls(acls: Set[Acl], resource: Resource): Unit = {
    aclAuthorizer.maxUpdateRetries = maxUpdateRetries
    if (acls != null && acls.nonEmpty) {
      val bindings = acls.map { acl => AuthorizerWrapper.convertToAclBinding(resource, acl) }
      createAcls(bindings)
    }
  }

  override def removeAcls(aclsTobeRemoved: Set[Acl], resource: Resource): Boolean = {
    val filters = aclsTobeRemoved.map { acl =>
      new AclBindingFilter(resource.toPattern.toFilter, AuthorizerWrapper.convertToAccessControlEntry(acl).toFilter)
    }
    deleteAcls(filters)
  }

  override def removeAcls(resource: Resource): Boolean = {
    val filter = new AclBindingFilter(resource.toPattern.toFilter, AccessControlEntryFilter.ANY)
    deleteAcls(Set(filter))
  }

  override def getAcls(resource: Resource): Set[Acl] = {
    val filter = new AclBindingFilter(resource.toPattern.toFilter, AccessControlEntryFilter.ANY)
    acls(filter).getOrElse(resource, Set.empty)
  }

  override def getAcls(principal: KafkaPrincipal): Map[Resource, Set[Acl]] = {
    val filter = new AclBindingFilter(ResourcePatternFilter.ANY,
      new AccessControlEntryFilter(principal.toString, null, AclOperation.ANY, AclPermissionType.ANY))
    acls(filter)
  }

  def getMatchingAcls(resourceType: ResourceType, resourceName: String): Set[Acl] = {
    val filter = new AclBindingFilter(new ResourcePatternFilter(resourceType.toJava, resourceName, PatternType.MATCH),
      AccessControlEntryFilter.ANY)
    acls(filter).flatMap(_._2).toSet
  }

  override def getAcls(): Map[Resource, Set[Acl]] = {
    acls(AclBindingFilter.ANY)
  }

  def close(): Unit = {
    aclAuthorizer.close()
  }

  private def createAcls(bindings: Set[AclBinding]): Unit = {
    aclAuthorizer.maxUpdateRetries = maxUpdateRetries
    val results = aclAuthorizer.createAcls(null, bindings.toList.asJava).asScala.map(_.toCompletableFuture.get)
    results.foreach { result => result.exception.ifPresent(throwException) }
  }

  private def deleteAcls(filters: Set[AclBindingFilter]): Boolean = {
    aclAuthorizer.maxUpdateRetries = maxUpdateRetries
    val results = aclAuthorizer.deleteAcls(null, filters.toList.asJava).asScala.map(_.toCompletableFuture.get)
    results.foreach { result => result.exception.ifPresent(throwException) }
    results.flatMap(_.aclBindingDeleteResults.asScala).foreach { result => result.exception.ifPresent(e => throw e) }
    results.exists(r => r.aclBindingDeleteResults.asScala.exists(d => !d.exception.isPresent))
  }

  private def acls(filter: AclBindingFilter): Map[Resource, Set[Acl]] = {
    val result = mutable.Map[Resource, mutable.Set[Acl]]()
    aclAuthorizer.acls(filter).forEach { binding =>
      val resource = AuthorizerWrapper.convertToResource(binding.pattern)
      val acl = AuthorizerWrapper.convertToAcl(binding.entry)
      result.getOrElseUpdate(resource, mutable.Set()).add(acl)
    }
    result.mapValues(_.toSet).toMap
  }

  // To retain the same exceptions as in previous versions, throw the underlying exception when the exception
  // was wrapped by AclAuthorizer in an ApiException
  private def throwException(e: ApiException): Unit = {
    if (e.getCause != null)
      throw e.getCause
    else
      throw e
  }
}
