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

package kafka.security

import kafka.security.auth.{Acl, Operation, PermissionType, Resource, ResourceNameType, ResourceType}
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclBindingFilter}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.resource.{Resource => AdminResource}
import org.apache.kafka.common.security.auth.KafkaPrincipal

import scala.util.{Failure, Success, Try}


object SecurityUtils {

  def convertToResourceAndAcl(filter: AclBindingFilter): Either[ApiError, (Resource, Acl)] = {
    (for {
      resourceType <- Try(ResourceType.fromJava(filter.resourceFilter.resourceType))
      resourceNameType <- Try(ResourceNameType.fromJava(filter.resourceFilter.nameType))
      principal <- Try(KafkaPrincipal.fromString(filter.entryFilter.principal))
      operation <- Try(Operation.fromJava(filter.entryFilter.operation))
      permissionType <- Try(PermissionType.fromJava(filter.entryFilter.permissionType))
      resource = Resource(resourceType, filter.resourceFilter.name, resourceNameType)
      acl = Acl(principal, permissionType, filter.entryFilter.host, operation)
    } yield (resource, acl)) match {
      case Failure(throwable) => Left(new ApiError(Errors.INVALID_REQUEST, throwable.getMessage))
      case Success(s) => Right(s)
    }
  }

  def convertToAclBinding(resource: Resource, acl: Acl): AclBinding = {
    val adminResource = new AdminResource(resource.resourceType.toJava, resource.name, resource.resourceNameType.toJava)
    val entry = new AccessControlEntry(acl.principal.toString, acl.host.toString,
      acl.operation.toJava, acl.permissionType.toJava)
    new AclBinding(adminResource, entry)
  }

}
