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
package kafka.security.auth

import kafka.utils.Json
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.{Test, Assert}
import org.scalatest.junit.JUnitSuite

class AclTest extends JUnitSuite {

  val AclJson = "{\"version\": 1, \"acls\": [{\"host\": \"host1\",\"permissionType\": \"Deny\",\"operation\": \"READ\", \"principal\": \"User:alice\"  },  " +
    "{  \"host\":  \"*\" ,  \"permissionType\": \"Allow\",  \"operation\":  \"Read\", \"principal\": \"User:bob\"  },  " +
    "{  \"host\": \"host1\",  \"permissionType\": \"Deny\",  \"operation\":   \"Read\" ,  \"principal\": \"User:bob\"}  ]}"

  @Test
  def testAclJsonConversion(): Unit = {
    val acl1 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"), Deny, "host1" , Read)
    val acl2 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), Allow, "*", Read)
    val acl3 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), Deny, "host1", Read)

    val acls = Set[Acl](acl1, acl2, acl3)
    val jsonAcls = Json.encode(Acl.toJsonCompatibleMap(acls))

    Assert.assertEquals(acls, Acl.fromJson(jsonAcls))
    Assert.assertEquals(acls, Acl.fromJson(AclJson))
  }

}
