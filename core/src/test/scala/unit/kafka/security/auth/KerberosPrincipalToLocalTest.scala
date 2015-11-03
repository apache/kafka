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
package unit.kafka.security.auth

import kafka.security.auth.KerberosPrincipalToLocal
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.{Test, Assert}
import org.scalatest.junit.{JUnitSuite}
import java.security.Principal

class KerberosPrincipalToLocalTest extends JUnitSuite   {

  @Test
  def testToLocal(): Unit = {
    val user: String = "test"
    val principalToLocal: KerberosPrincipalToLocal = new KerberosPrincipalToLocal()
    val principalWithHostAndRealm: Principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test/testhost@testRealm.com")
    val principalWithHostNoRealm: Principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test/testhost")
    val principalWithRealmNoHost: Principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test@testRealm.com")
    val principalWithNoRealmNoHost: Principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test")

    Assert.assertEquals(user, principalToLocal.toLocal(principalWithHostAndRealm))
    Assert.assertEquals(user, principalToLocal.toLocal(principalWithHostNoRealm))
    Assert.assertEquals(user, principalToLocal.toLocal(principalWithRealmNoHost))
    Assert.assertEquals(user, principalToLocal.toLocal(principalWithNoRealmNoHost))
  }
}
