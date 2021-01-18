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

package kafka.security.auth

import kafka.common.KafkaException
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

@deprecated("Use org.apache.kafka.common.resource.ResourcePattern", "Since 2.5")
class ResourceTest {
  @Test
  def shouldThrowOnTwoPartStringWithUnknownResourceType(): Unit = {
    assertThrows(classOf[KafkaException], () => Resource.fromString("Unknown:fred"))
  }

  @Test
  def shouldThrowOnBadResourceTypeSeparator(): Unit = {
    assertThrows(classOf[KafkaException], () => Resource.fromString("Topic-fred"))
  }

  @Test
  def shouldParseOldTwoPartString(): Unit = {
    assertEquals(Resource(Group, "fred", LITERAL), Resource.fromString("Group:fred"))
    assertEquals(Resource(Topic, "t", LITERAL), Resource.fromString("Topic:t"))
  }

  @Test
  def shouldParseOldTwoPartWithEmbeddedSeparators(): Unit = {
    assertEquals(Resource(Group, ":This:is:a:weird:group:name:", LITERAL), Resource.fromString("Group::This:is:a:weird:group:name:"))
  }

  @Test
  def shouldParseThreePartString(): Unit = {
    assertEquals(Resource(Group, "fred", PREFIXED), Resource.fromString("Group:PREFIXED:fred"))
    assertEquals(Resource(Topic, "t", LITERAL), Resource.fromString("Topic:LITERAL:t"))
  }

  @Test
  def shouldParseThreePartWithEmbeddedSeparators(): Unit = {
    assertEquals(Resource(Group, ":This:is:a:weird:group:name:", PREFIXED), Resource.fromString("Group:PREFIXED::This:is:a:weird:group:name:"))
    assertEquals(Resource(Group, ":This:is:a:weird:group:name:", LITERAL), Resource.fromString("Group:LITERAL::This:is:a:weird:group:name:"))
  }

  @Test
  def shouldRoundTripViaString(): Unit = {
    val expected = Resource(Group, "fred", PREFIXED)

    val actual = Resource.fromString(expected.toString)

    assertEquals(expected, actual)
  }
}