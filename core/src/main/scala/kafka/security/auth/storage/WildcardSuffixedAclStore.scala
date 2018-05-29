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

import kafka.security.auth.{ResourceNameType, WildcardSuffixed}
import kafka.utils.ZkUtils

class WildcardSuffixedAclStore extends AclStore {

  override val aclZNode: AclZNode = new AclZNode {
    override val path: String = ZkUtils.KafkaWildcardSuffixedAclPath
  }

  override val aclChangesZNode: AclChangesZNode = new AclChangesZNode {
    override val path: String = ZkUtils.KafkaWildcardSuffixedAclChangesPath
  }

  override val resourceNameType: ResourceNameType = WildcardSuffixed


//
//  /**
//    * Similar to AclZkPath but stores wildcard suffixed Acls.
//    */
//
//  /**
//    * Notification node which gets updated with the resource name when wildcard suffixed acls on a resource is changed.
//    *
//    */

}
