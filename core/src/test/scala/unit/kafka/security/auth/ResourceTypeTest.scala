/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.auth

import kafka.common.KafkaException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.apache.kafka.common.resource.{ResourceType => JResourceType}

@deprecated("Scala Authorizer API classes gave been deprecated", "Since 2.5")
class ResourceTypeTest {

  @Test
  def testFromString(): Unit = {
    val resourceType = ResourceType.fromString("Topic")
    assertEquals(Topic, resourceType)
    assertThrows(classOf[KafkaException], () => ResourceType.fromString("badName"))
  }

  /**
    * Test round trip conversions between org.apache.kafka.common.acl.ResourceType and
    * kafka.security.auth.ResourceType.
    */
  @Test
  def testJavaConversions(): Unit = {
    JResourceType.values.foreach {
      case JResourceType.UNKNOWN | JResourceType.ANY =>
      case jResourceType =>
        val resourceType = ResourceType.fromJava(jResourceType)
        val jResourceType2 = resourceType.toJava
        assertEquals(jResourceType, jResourceType2)
    }
  }
}
