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

package unit.kafka.log

import org.apache.kafka.storage.internals.log.VerificationGuard
import org.apache.kafka.storage.internals.log.VerificationGuard.SENTINEL
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertTrue}
import org.junit.jupiter.api.Test

class VerificationGuardTest {

  @Test
  def testEqualsAndHashCode(): Unit = {
    val verificationGuard1 = new VerificationGuard
    val verificationGuard2 = new VerificationGuard

    assertNotEquals(verificationGuard1, verificationGuard2)
    assertNotEquals(SENTINEL, verificationGuard1)
    assertEquals(SENTINEL, SENTINEL)

    assertNotEquals(verificationGuard1.hashCode, verificationGuard2.hashCode)
    assertNotEquals(SENTINEL.hashCode, verificationGuard1.hashCode)
    assertEquals(SENTINEL.hashCode, SENTINEL.hashCode)
  }

  @Test
  def testVerify(): Unit = {
    val verificationGuard1 = new VerificationGuard
    val verificationGuard2 = new VerificationGuard

    assertFalse(verificationGuard1.verify(verificationGuard2))
    assertFalse(verificationGuard1.verify(SENTINEL))
    assertFalse(SENTINEL.verify(verificationGuard1))
    assertFalse(SENTINEL.verify(SENTINEL))
    assertTrue(verificationGuard1.verify(verificationGuard1))
  }

}
