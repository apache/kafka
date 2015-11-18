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

import kafka.security.auth.PrincipalToUnixGroup
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

class PrincipalToUnixGroupTest extends JUnitSuite   {

  @Test
  def testToGroups(): Unit = {
    val user: String = "test"
    val principalToGroup = new PrincipalToUnixGroup
    principalToGroup.configure(null)

    val currentUser: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, System.getProperty("user.name"))
    Assert.assertTrue("Should have returned > 0 groups for the current user", principalToGroup.toGroups(currentUser).size > 0)

    val unknownUser: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unknown-user")
    Assert.assertTrue("Should have returned 0 groups for the current user", principalToGroup.toGroups(unknownUser).size == 0)

    Assert.assertTrue("Should have returned 0 groups for the current user", principalToGroup.toGroups(null).size == 0)
  }
}
