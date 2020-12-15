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

package kafka.security.auth

import kafka.security.authorizer.AclEntry
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.security.auth.KafkaPrincipal

@deprecated("Use org.apache.kafka.common.acl.AclBinding", "Since 2.5")
object Acl {
  val WildCardPrincipal: KafkaPrincipal = AclEntry.WildcardPrincipal
  val WildCardHost: String = AclEntry.WildcardHost
  val WildCardResource: String = ResourcePattern.WILDCARD_RESOURCE
  val AllowAllAcl = new Acl(WildCardPrincipal, Allow, WildCardHost, All)
  val PrincipalKey = AclEntry.PrincipalKey
  val PermissionTypeKey = AclEntry.PermissionTypeKey
  val OperationKey = AclEntry.OperationKey
  val HostsKey = AclEntry.HostsKey
  val VersionKey = AclEntry.VersionKey
  val CurrentVersion = AclEntry.CurrentVersion
  val AclsKey = AclEntry.AclsKey

  /**
   *
   * @see AclEntry
   */
  def fromBytes(bytes: Array[Byte]): Set[Acl] = {
    AclEntry.fromBytes(bytes)
      .map(ace => Acl(ace.kafkaPrincipal,
        PermissionType.fromJava(ace.permissionType()),
        ace.host(),
        Operation.fromJava(ace.operation())))
  }

  def toJsonCompatibleMap(acls: Set[Acl]): Map[String, Any] = {
    AclEntry.toJsonCompatibleMap(acls.map(acl =>
      AclEntry(acl.principal, acl.permissionType.toJava, acl.host, acl.operation.toJava)
    ))
  }
}

/**
 * An instance of this class will represent an acl that can express following statement.
 * <pre>
 * Principal P has permissionType PT on Operation O1 from hosts H1.
 * </pre>
 * @param principal A value of *:* indicates all users.
 * @param permissionType
 * @param host A value of * indicates all hosts.
 * @param operation A value of ALL indicates all operations.
 */
@deprecated("Use org.apache.kafka.common.acl.AclBinding", "Since 2.5")
case class Acl(principal: KafkaPrincipal, permissionType: PermissionType, host: String, operation: Operation) {

  /**
   * TODO: Ideally we would have a symmetric toJson method but our current json library can not jsonify/dejsonify complex objects.
   * @return Map representation of the Acl.
   */
  def toMap(): Map[String, Any] = {
    Map(Acl.PrincipalKey -> principal.toString,
      Acl.PermissionTypeKey -> permissionType.name,
      Acl.OperationKey -> operation.name,
      Acl.HostsKey -> host)
  }

  override def toString: String = {
    "%s has %s permission for operations: %s from hosts: %s".format(principal, permissionType.name, operation, host)
  }

}

