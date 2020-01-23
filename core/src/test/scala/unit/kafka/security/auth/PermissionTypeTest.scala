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
import org.apache.kafka.common.acl.AclPermissionType
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.Assertions.fail

@deprecated("Scala Authorizer API classes gave been deprecated", "Since 2.5")
class PermissionTypeTest {

  @Test
  def testFromString(): Unit = {
    val permissionType = PermissionType.fromString("Allow")
    assertEquals(Allow, permissionType)

    try {
      PermissionType.fromString("badName")
      fail("Expected exception on invalid PermissionType name.")
    } catch {
      case _: KafkaException => // expected
    }
  }

  /**
    * Test round trip conversions between org.apache.kafka.common.acl.AclPermissionType and
    * kafka.security.auth.PermissionType.
    */
  @Test
  def testJavaConversions(): Unit = {
    AclPermissionType.values().foreach {
      case AclPermissionType.UNKNOWN | AclPermissionType.ANY =>
      case aclPerm =>
        val perm = PermissionType.fromJava(aclPerm)
        val aclPerm2 = perm.toJava
        assertEquals(aclPerm, aclPerm2)
    }
  }
}
