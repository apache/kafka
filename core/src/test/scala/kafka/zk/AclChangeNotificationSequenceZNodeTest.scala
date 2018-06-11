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

import kafka.security.auth.Resource.Separator
import kafka.security.auth.{Resource, ResourceType, Topic}
import org.apache.kafka.common.resource.ResourceNameType.{LITERAL, PREFIXED}
import org.junit.Test
import org.junit.Assert.assertEquals

class AclChangeNotificationSequenceZNodeTest {
  private val literalResource = Resource(Topic, "some-topic", LITERAL)
  private val prefixedResource = Resource(Topic, "some-topic", PREFIXED)

  @Test(expected = classOf[IllegalArgumentException])
  def shouldThrowFromLegacyEncodeOnNoneLiteral(): Unit = {
    AclChangeNotificationSequenceZNode.encodeLegacy(prefixedResource)
  }

  @Test
  def shouldRoundTripLegacyTwoPartString(): Unit = {
    val bytes = AclChangeNotificationSequenceZNode.encodeLegacy(literalResource)
    val actual = AclChangeNotificationSequenceZNode.decode(bytes)

    assertEquals(literalResource, actual)
  }

  @Test
  def shouldRoundTripThreePartString(): Unit = {
    val bytes = AclChangeNotificationSequenceZNode.encode(prefixedResource)
    val actual = AclChangeNotificationSequenceZNode.decode(bytes)

    assertEquals(prefixedResource, actual)
  }

  @Test
  def shouldNotThrowIfOldBrokerParsingNewFormatWithLiteralAcl(): Unit = {
    val bytes = AclChangeNotificationSequenceZNode.encode(literalResource)
    val actual = legacyDecode(bytes)

    assertEquals(Resource(Topic, "LITERAL:some-topic", LITERAL), actual)
  }

  @Test
  def shouldNotThrowIfOldBrokerParsingNewFormatWithPrefixedAcl(): Unit = {
    val bytes = AclChangeNotificationSequenceZNode.encode(prefixedResource)
    val actual = legacyDecode(bytes)

    assertEquals(Resource(Topic, "PREFIXED:some-topic", LITERAL), actual)
  }

  private def legacyDecode(bytes: Array[Byte]): Resource =
    legacyFromString(new String(bytes, UTF_8))

  //noinspection ScalaDeprecation
  // Old version of kafka.auth.Resource.fromString used in pre-2.0 Kafka:
  private def legacyFromString(str: String): Resource = {
    str.split(Separator, 2) match {
      case Array(resourceType, name, _*) => new Resource(ResourceType.fromString(resourceType), name)
      case _ => throw new IllegalArgumentException("expected a string in format ResourceType:ResourceName but got " + str)
    }
  }
}