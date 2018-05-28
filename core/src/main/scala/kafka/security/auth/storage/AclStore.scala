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

package kafka.security.auth.storage

import java.nio.charset.StandardCharsets.UTF_8

import kafka.security.auth.SimpleAclAuthorizer.VersionedAcls
import kafka.security.auth.{Acl, Resource, ResourceNameType, ResourceType}
import kafka.utils.Json
import org.apache.zookeeper.data.Stat

import scala.collection.JavaConverters._

/**
  *
  */
trait AclStore {

  def aclZNode: AclZNode

  def aclChangesZNode: AclChangesZNode

  def resourceNameType: ResourceNameType

  def resourceTypeZNode: ResourceTypeZNode = new ResourceTypeZNode {
    override def aclZNode: AclZNode = aclZNode
  }

  def resourceZNode: ResourceZNode = new ResourceZNode {
    override def aclZNode: AclZNode = aclZNode
  }

  def aclChangeNotificationSequenceZNode: AclChangeNotificationSequenceZNode = new AclChangeNotificationSequenceZNode {
    override def aclChangesZNode: AclChangesZNode = aclChangesZNode
  }

  trait AclZNode {
    def path: String
  }

  trait AclChangesZNode {
    def path: String
  }

  trait ResourceTypeZNode {
    def aclZNode: AclZNode
    def path(resourceType: ResourceType) = s"${aclZNode.path}/${resourceType.name}"
  }

  trait ResourceZNode {
    def aclZNode: AclZNode
    def path(resource: Resource) = s"${aclZNode.path}/${resource.resourceType.name}/${resource.name}"
    def encode(acls: Set[Acl]): Array[Byte] = Json.encodeAsBytes(Acl.toJsonCompatibleMap(acls).asJava)
    def decode(bytes: Array[Byte], stat: Stat): VersionedAcls = VersionedAcls(Acl.fromBytes(bytes), stat.getVersion)
  }

  trait AclChangeNotificationSequenceZNode {
    def aclChangesZNode: AclChangesZNode
    def SequenceNumberPrefix = "acl_changes_"
    def createPath = s"${aclChangesZNode.path}/$SequenceNumberPrefix"
    def deletePath(sequenceNode: String) = s"${aclChangesZNode.path}/$sequenceNode"
    def encode(resourceName : String): Array[Byte] = resourceName.getBytes(UTF_8)
    def decode(bytes: Array[Byte]): String = new String(bytes, UTF_8)
  }

}
