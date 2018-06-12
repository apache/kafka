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

package kafka.zk

import kafka.security.auth.{Resource, Topic}
import org.apache.kafka.common.resource.ResourceNameType.{LITERAL, PREFIXED}
import org.junit.Assert.assertEquals
import org.junit.Test

class ExtendedZkAclStoreTest {
  private val literalResource = Resource(Topic, "some-topic", LITERAL)
  private val prefixedResource = Resource(Topic, "some-topic", PREFIXED)
  private val store = new ExtendedZkAclStore(PREFIXED)

  @Test
  def shouldHaveCorrectPaths(): Unit = {
    assertEquals("/kafka-acl-extended/prefixed", store.aclPath)
    assertEquals("/kafka-acl-extended/prefixed/Topic", store.path(Topic))
    assertEquals("/kafka-acl-extended-changes/prefixed", store.aclChangePath)
  }

  @Test
  def shouldHaveCorrectPatternType(): Unit = {
    assertEquals(PREFIXED, store.patternType)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldThrowIfConstructedWithLiteral(): Unit = {
    new ExtendedZkAclStore(LITERAL)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldThrowFromEncodeOnLiteral(): Unit = {
    store.changeNode.createChangeNode(literalResource)
  }

  @Test
  def shouldWriteChangesToTheWritePath(): Unit = {
    val changeNode = store.changeNode.createChangeNode(prefixedResource)

    assertEquals("/kafka-acl-extended-changes/prefixed/acl_changes_", changeNode.path)
  }

  @Test
  def shouldRoundTripChangeNode(): Unit = {
    val changeNode = store.changeNode.createChangeNode(prefixedResource)

    val actual = store.changeNode.decode(changeNode.bytes)

    assertEquals(prefixedResource, actual)
  }
}