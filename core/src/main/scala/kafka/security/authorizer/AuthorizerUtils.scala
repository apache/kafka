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

import java.net.InetAddress

import kafka.network.RequestChannel.Session
import kafka.security.auth._
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclBindingFilter, AclOperation}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.resource.{ResourcePattern, ResourceType => JResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.SecurityUtils._
import org.apache.kafka.server.authorizer.AuthorizableRequestContext

import scala.util.{Failure, Success, Try}


object AuthorizerUtils {
  val WildcardPrincipal = "User:*"
  val WildcardHost = "*"

  def convertToResourceAndAcl(filter: AclBindingFilter): Either[ApiError, (Resource, Acl)] = {
    (for {
      resourceType <- Try(ResourceType.fromJava(filter.patternFilter.resourceType))
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
    Resource(ResourceType.fromJava(resourcePattern.resourceType), resourcePattern.name, resourcePattern.patternType)
  }

  def validateAclBinding(aclBinding: AclBinding): Unit = {
    if (aclBinding.isUnknown)
      throw new IllegalArgumentException("ACL binding contains unknown elements")
  }

  def supportedOperations(resourceType: JResourceType): Set[AclOperation] = {
    ResourceType.fromJava(resourceType).supportedOperations.map(_.toJava)
  }

  def isClusterResource(name: String): Boolean = name.equals(Resource.ClusterResourceName)

  def sessionToRequestContext(session: Session): AuthorizableRequestContext = {
    new AuthorizableRequestContext {
      override def clientId(): String = ""
      override def requestType(): Int = -1
      override def listenerName(): String = ""
      override def clientAddress(): InetAddress = session.clientAddress
      override def principal(): KafkaPrincipal = session.principal
      override def securityProtocol(): SecurityProtocol = null
      override def correlationId(): Int = -1
      override def requestVersion(): Int = -1
    }
  }
}
