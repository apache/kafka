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

import kafka.security.auth.{Literal, ResourceNameType}
import kafka.utils.ZkUtils

class LiteralAclStore extends AclStore {

  override val aclZNode: AclZNode = new AclZNode {
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
    override val path: String = ZkUtils.KafkaAclPath
  }

  override val aclChangesZNode: AclChangesZNode = new AclChangesZNode {
    /**
      * Notification node which gets updated with the resource name when acl on a resource is changed.
      */
    override val path: String = ZkUtils.KafkaAclChangesPath
  }

  override val resourceNameType: ResourceNameType = Literal

}
