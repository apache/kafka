/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.security.auth.storage

import kafka.security.auth.storage.AclStore
import kafka.security.auth.{Literal, Resource, Topic, WildcardSuffixed}
import kafka.utils.ZkUtils
import org.junit.Assert.assertEquals
import org.junit.Test

class AclStoreTest {

  @Test
  def testAclStore(): Unit = {

    assertEquals(ZkUtils.KafkaAclPath, AclStore.literalAclStore.aclZNode.path)
    assertEquals(WildcardSuffixed, AclStore.wildcardSuffixedAclStore.resourceNameType)
    assertEquals("/kafka-wildcard-acl/Topic/topicName", AclStore.wildcardSuffixedAclStore.resourceZNode.path(new Resource(Topic, "topicName")))

  }

  @Test
  def testFromResource(): Unit = {
    val literalResource = new Resource(Topic, "name", Literal)
    assertEquals(AclStore.literalAclStore, AclStore.fromResource(literalResource))
    val wildcardResource = new Resource(Topic, "name", WildcardSuffixed)
    assertEquals(AclStore.wildcardSuffixedAclStore, AclStore.fromResource(wildcardResource))
  }
}
