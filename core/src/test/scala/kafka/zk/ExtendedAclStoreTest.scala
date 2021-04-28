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

import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType.TOPIC
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

class ExtendedAclStoreTest {
  private val literalResource = new ResourcePattern(TOPIC, "some-topic", LITERAL)
  private val prefixedResource = new ResourcePattern(TOPIC, "some-topic", PREFIXED)
  private val store = new ExtendedAclStore(PREFIXED)

  @Test
  def shouldHaveCorrectPaths(): Unit = {
    assertEquals("/kafka-acl-extended/prefixed", store.aclPath)
    assertEquals("/kafka-acl-extended/prefixed/Topic", store.path(TOPIC))
    assertEquals("/kafka-acl-extended-changes", store.changeStore.aclChangePath)
  }

  @Test
  def shouldHaveCorrectPatternType(): Unit = {
    assertEquals(PREFIXED, store.patternType)
  }

  @Test
  def shouldThrowIfConstructedWithLiteral(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => new ExtendedAclStore(LITERAL))
  }

  @Test
  def shouldThrowFromEncodeOnLiteral(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => store.changeStore.createChangeNode(literalResource))
  }

  @Test
  def shouldWriteChangesToTheWritePath(): Unit = {
    val changeNode = store.changeStore.createChangeNode(prefixedResource)

    assertEquals("/kafka-acl-extended-changes/acl_changes_", changeNode.path)
  }

  @Test
  def shouldRoundTripChangeNode(): Unit = {
    val changeNode = store.changeStore.createChangeNode(prefixedResource)

    val actual = store.changeStore.decode(changeNode.bytes)

    assertEquals(prefixedResource, actual)
  }
}