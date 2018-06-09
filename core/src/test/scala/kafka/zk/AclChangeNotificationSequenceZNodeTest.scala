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
import org.apache.kafka.common.resource.ResourceNameType
import org.junit.Test
import org.junit.Assert.assertEquals

class AclChangeNotificationSequenceZNodeTest {
  private val literalResource = Resource(Topic, "some-topic", ResourceNameType.LITERAL)
  private val prefixedResource = Resource(Topic, "some-topic", ResourceNameType.PREFIXED)

  @Test(expected = classOf[IllegalArgumentException])
  def shouldThrowFromLegacyEncodeOnNoneLiteral(): Unit = {
    AclChangeNotificationSequenceZNode.encodeLegacy(prefixedResource)
  }

  @Test
  def shouldRoundTripLegacyString(): Unit = {
    val bytes = AclChangeNotificationSequenceZNode.encodeLegacy(literalResource)
    val actual = AclChangeNotificationSequenceZNode.decode(bytes)

    assertEquals(literalResource, actual)
  }

  @Test
  def shouldRoundTripJSON(): Unit = {
    val bytes = AclChangeNotificationSequenceZNode.encode(prefixedResource)
    val actual = AclChangeNotificationSequenceZNode.decode(bytes)

    assertEquals(prefixedResource, actual)
  }
}