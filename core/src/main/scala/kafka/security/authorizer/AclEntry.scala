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

import kafka.utils.Json
import org.apache.kafka.common.acl.{AccessControlEntry, AclOperation, AclPermissionType}
import org.apache.kafka.common.acl.AclOperation.{READ, WRITE, CREATE, DESCRIBE, DELETE, ALTER, DESCRIBE_CONFIGS, ALTER_CONFIGS, CLUSTER_ACTION, IDEMPOTENT_WRITE, CREATE_TOKENS, DESCRIBE_TOKENS}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.resource.{ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.SecurityUtils

import scala.jdk.CollectionConverters._

object AclEntry {
  val WildcardPrincipal: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*")
  val WildcardPrincipalString: String = WildcardPrincipal.toString
  val WildcardHost: String = "*"
  val WildcardResource: String = ResourcePattern.WILDCARD_RESOURCE

  val ResourceSeparator: String = ":"
  val ResourceTypes: Set[ResourceType] = ResourceType.values.toSet
    .filterNot(t => t == ResourceType.UNKNOWN || t == ResourceType.ANY)
  val AclOperations: Set[AclOperation] = AclOperation.values.toSet
    .filterNot(t => t == AclOperation.UNKNOWN || t == AclOperation.ANY)

  private val PrincipalKey = "principal"
  private val PermissionTypeKey = "permissionType"
  private val OperationKey = "operation"
  private val HostsKey = "host"
  val VersionKey: String = "version"
  val CurrentVersion: Int = 1
  private val AclsKey = "acls"

  def apply(principal: KafkaPrincipal,
            permissionType: AclPermissionType,
            host: String,
            operation: AclOperation): AclEntry = {
    new AclEntry(new AccessControlEntry(if (principal == null) null else principal.toString,
      host, operation, permissionType))
  }

  /**
   * Parse JSON representation of ACLs
   * @param bytes of acls json string
   *
   * <p>
      {
        "version": 1,
        "acls": [
          {
            "host":"host1",
            "permissionType": "Deny",
            "operation": "Read",
            "principal": "User:alice"
          }
        ]
      }
   * </p>
   *
   * @return set of AclEntry objects from the JSON string
   */
  def fromBytes(bytes: Array[Byte]): Set[AclEntry] = {
    if (bytes == null || bytes.isEmpty)
      return collection.immutable.Set.empty[AclEntry]

    Json.parseBytes(bytes).map(_.asJsonObject).map { js =>
      //the acl json version.
      require(js(VersionKey).to[Int] == CurrentVersion)
      js(AclsKey).asJsonArray.iterator.map(_.asJsonObject).map { itemJs =>
        val principal = SecurityUtils.parseKafkaPrincipal(itemJs(PrincipalKey).to[String])
        val permissionType = SecurityUtils.permissionType(itemJs(PermissionTypeKey).to[String])
        val host = itemJs(HostsKey).to[String]
        val operation = SecurityUtils.operation(itemJs(OperationKey).to[String])
        AclEntry(principal, permissionType, host, operation)
      }.toSet
    }.getOrElse(Set.empty)
  }

  def toJsonCompatibleMap(acls: Set[AclEntry]): Map[String, Any] = {
    Map(AclEntry.VersionKey -> AclEntry.CurrentVersion, AclEntry.AclsKey -> acls.map(acl => acl.toMap.asJava).toList.asJava)
  }

  def supportedOperations(resourceType: ResourceType): Set[AclOperation] = {
    resourceType match {
      case ResourceType.TOPIC => Set(READ, WRITE, CREATE, DESCRIBE, DELETE, ALTER, DESCRIBE_CONFIGS, ALTER_CONFIGS)
      case ResourceType.GROUP => Set(READ, DESCRIBE, DELETE)
      case ResourceType.CLUSTER => Set(CREATE, CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, ALTER, DESCRIBE)
      case ResourceType.TRANSACTIONAL_ID => Set(DESCRIBE, WRITE)
      case ResourceType.DELEGATION_TOKEN => Set(DESCRIBE)
      case ResourceType.USER => Set(CREATE_TOKENS, DESCRIBE_TOKENS)
      case _ => throw new IllegalArgumentException("Not a concrete resource type")
    }
  }

  def authorizationError(resourceType: ResourceType): Errors = {
    resourceType match {
      case ResourceType.TOPIC => Errors.TOPIC_AUTHORIZATION_FAILED
      case ResourceType.GROUP => Errors.GROUP_AUTHORIZATION_FAILED
      case ResourceType.CLUSTER => Errors.CLUSTER_AUTHORIZATION_FAILED
      case ResourceType.TRANSACTIONAL_ID => Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
      case ResourceType.DELEGATION_TOKEN => Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED
      case _ => throw new IllegalArgumentException("Authorization error type not known")
    }
  }
}

class AclEntry(val ace: AccessControlEntry)
  extends AccessControlEntry(ace.principal, ace.host, ace.operation, ace.permissionType) {

  val kafkaPrincipal: KafkaPrincipal = if (principal == null)
    null
  else
    SecurityUtils.parseKafkaPrincipal(principal)

  def toMap: Map[String, Any] = {
    Map(AclEntry.PrincipalKey -> principal,
      AclEntry.PermissionTypeKey -> SecurityUtils.permissionTypeName(permissionType),
      AclEntry.OperationKey -> SecurityUtils.operationName(operation),
      AclEntry.HostsKey -> host)
  }

  override def hashCode(): Int = ace.hashCode()

  override def equals(o: scala.Any): Boolean = super.equals(o) // to keep spotbugs happy

  override def toString: String = {
    "%s has %s permission for operations: %s from hosts: %s".format(principal, permissionType.name, operation, host)
  }

}

