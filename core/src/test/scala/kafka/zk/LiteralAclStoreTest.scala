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

import java.nio.charset.StandardCharsets.UTF_8

import kafka.security.auth.{Group, Resource, Topic}
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.junit.Assert.assertEquals
import org.junit.Test

class LiteralAclStoreTest {
  private val literalResource = Resource(Topic, "some-topic", LITERAL)
  private val prefixedResource = Resource(Topic, "some-topic", PREFIXED)
  private val store = LiteralAclStore

  @Test
  def shouldHaveCorrectPaths(): Unit = {
    assertEquals("/kafka-acl", store.aclPath)
    assertEquals("/kafka-acl/Topic", store.path(Topic))
    assertEquals("/kafka-acl-changes", store.changeStore.aclChangePath)
  }

  @Test
  def shouldHaveCorrectPatternType(): Unit = {
    assertEquals(LITERAL, store.patternType)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldThrowFromEncodeOnNoneLiteral(): Unit = {
    store.changeStore.createChangeNode(prefixedResource)
  }

  @Test
  def shouldWriteChangesToTheWritePath(): Unit = {
    val changeNode = store.changeStore.createChangeNode(literalResource)

    assertEquals("/kafka-acl-changes/acl_changes_", changeNode.path)
  }

  @Test
  def shouldRoundTripChangeNode(): Unit = {
    val changeNode = store.changeStore.createChangeNode(literalResource)

    val actual = store.changeStore.decode(changeNode.bytes)

    assertEquals(literalResource, actual)
  }

  @Test
  def shouldDecodeResourceUsingTwoPartLogic(): Unit = {
    val resource = Resource(Group, "PREFIXED:this, including the PREFIXED part, is a valid two part group name", LITERAL)
    val encoded = (resource.resourceType +  Resource.Separator + resource.name).getBytes(UTF_8)

    val actual = store.changeStore.decode(encoded)

    assertEquals(resource, actual)
  }
}