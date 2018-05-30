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
import kafka.security.auth._
import kafka.utils.{Json, ZkUtils}
import org.apache.zookeeper.data.Stat

import scala.collection.JavaConverters._
import scala.collection.Seq

/**
  * Acl Store.
  */
class AclStore(aclZNod: AclZNode, aclChangesZNod: AclChangesZNode, resourceNameTyp: ResourceNameType) {
  def aclZNode: AclZNode = aclZNod
  def aclChangesZNode: AclChangesZNode = aclChangesZNod
  def resourceNameType: ResourceNameType = resourceNameTyp
  def resourceTypeZNode = ResourceTypeZNode(aclZNode)
  def resourceZNode = ResourceZNode(aclZNode)
  def aclChangeNotificationSequenceZNode = AclChangeNotificationSequenceZNode(aclChangesZNode)
}

case class AclZNode(nodePath: String) {
  def path: String = nodePath
}

case class AclChangesZNode(nodePath: String) {
  def path: String = nodePath
}

case class ResourceTypeZNode(aclZNode: AclZNode) {
  def path(resourceType: ResourceType) = s"${aclZNode.path}/${resourceType.name}"
}

case class ResourceZNode(aclZNode: AclZNode) {
  def path(resource: Resource) = s"${aclZNode.path}/${resource.resourceType.name}/${resource.name}"
  def encode(acls: Set[Acl]): Array[Byte] = Json.encodeAsBytes(Acl.toJsonCompatibleMap(acls).asJava)
  def decode(bytes: Array[Byte], stat: Stat): VersionedAcls = VersionedAcls(Acl.fromBytes(bytes), stat.getVersion)
}

case class AclChangeNotificationSequenceZNode(aclChangesZNode: AclChangesZNode) {
  def SequenceNumberPrefix = "acl_changes_"
  def createPath = s"${aclChangesZNode.path}/$SequenceNumberPrefix"
  def deletePath(sequenceNode: String) = s"${aclChangesZNode.path}/$sequenceNode"
  def encode(resourceName: String): Array[Byte] = resourceName.getBytes(UTF_8)
  def decode(bytes: Array[Byte]): String = new String(bytes, UTF_8)
}

object AclStore {
  val literalAclStore: AclStore = new AclStore(
    AclZNode(
      /**
        * The root acl storage node. Under this node there will be one child node per resource type (Topic, Cluster, Group).
        * under each resourceType there will be a unique child for each resource instance and the data for that child will contain
        * list of its acls as a json object. Following gives an example:
        *
        * <pre>
        * /kafka-acl/Topic/topic-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
        * /kafka-acl/Cluster/kafka-cluster => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
        * /kafka-acl/Group/group-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
        * </pre>
        */
      ZkUtils.KafkaAclPath),
    AclChangesZNode(
      /**
        * Notification node which gets updated with the resource name when acl on a resource is changed.
        */
      ZkUtils.KafkaAclChangesPath),
    Literal
  )

  val wildcardSuffixedAclStore: AclStore = new AclStore(
    AclZNode(ZkUtils.KafkaWildcardSuffixedAclPath),
    AclChangesZNode(ZkUtils.KafkaWildcardSuffixedAclChangesPath),
    WildcardSuffixed
  )

  val AclStores = Seq(literalAclStore, wildcardSuffixedAclStore)

  def fromResource(resource: Resource): AclStore = {
    AclStores.find(_.resourceNameType.equals(resource.resourceNameType)).getOrElse(
      throw new IllegalArgumentException("Unsupported resource name type: " + resource.resourceNameType)
    )
  }
}
