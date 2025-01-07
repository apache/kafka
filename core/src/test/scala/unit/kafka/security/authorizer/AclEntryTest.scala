/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.authorizer

import java.nio.charset.StandardCharsets.UTF_8
import kafka.utils.Json
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclOperation.READ
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.security.authorizer.AclEntry
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util

class AclEntryTest {

  val AclJson = """{"version": 1, "acls": [{"host": "host1","permissionType": "Deny","operation": "READ", "principal": "User:alice"  },
    {  "host":  "*" ,  "permissionType": "Allow",  "operation":  "Read", "principal": "User:bob"  },
    {  "host": "host1",  "permissionType": "Deny",  "operation":   "Read" ,  "principal": "User:bob"}]}"""

  @Test
  def testAclJsonConversion(): Unit = {
    val acl1 = new AclEntry(new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice").toString, "host1", READ, DENY))
    val acl2 = new AclEntry(new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob").toString, "*", READ, ALLOW))
    val acl3 = new AclEntry(new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob").toString, "host1", READ, DENY))

    val acls = new util.HashSet[AclEntry](util.Arrays.asList(acl1, acl2, acl3))

    assertEquals(acls, AclEntry.fromBytes(Json.encodeAsBytes(AclEntry.toJsonCompatibleMap(acls))))
    assertEquals(acls, AclEntry.fromBytes(AclJson.getBytes(UTF_8)))
  }
}
