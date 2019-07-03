/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.auth

import kafka.common.{BaseEnum, KafkaException}
import org.apache.kafka.common.acl.AclPermissionType

sealed trait PermissionType extends BaseEnum {
  val toJava: AclPermissionType
}

case object Allow extends PermissionType {
  val name = "Allow"
  val toJava = AclPermissionType.ALLOW
}

case object Deny extends PermissionType {
  val name = "Deny"
  val toJava = AclPermissionType.DENY
}

object PermissionType {
  def fromString(permissionType: String): PermissionType = {
    val pType = values.find(pType => pType.name.equalsIgnoreCase(permissionType))
    pType.getOrElse(throw new KafkaException(permissionType + " not a valid permissionType name. The valid names are " + values.mkString(",")))
  }

  def fromJava(permissionType: AclPermissionType): PermissionType = fromString(permissionType.toString)

  def values: Seq[PermissionType] = List(Allow, Deny)
}

