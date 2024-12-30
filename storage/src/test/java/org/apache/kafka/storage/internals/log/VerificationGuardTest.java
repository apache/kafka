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
package org.apache.kafka.storage.internals.log;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VerificationGuardTest {

    @Test
    public void testEqualsAndHashCode() {
        VerificationGuard verificationGuard1 = new VerificationGuard();
        VerificationGuard verificationGuard2 = new VerificationGuard();

        assertNotEquals(verificationGuard1, verificationGuard2);
        assertNotEquals(VerificationGuard.SENTINEL, verificationGuard1);
        assertEquals(VerificationGuard.SENTINEL, VerificationGuard.SENTINEL);

        assertNotEquals(verificationGuard1.hashCode(), verificationGuard2.hashCode());
        assertNotEquals(VerificationGuard.SENTINEL.hashCode(), verificationGuard1.hashCode());
        assertEquals(VerificationGuard.SENTINEL.hashCode(), VerificationGuard.SENTINEL.hashCode());
    }

    @Test
    public void testVerify() {
        VerificationGuard verificationGuard1 = new VerificationGuard();
        VerificationGuard verificationGuard2 = new VerificationGuard();

        assertFalse(verificationGuard1.verify(verificationGuard2));
        assertFalse(verificationGuard1.verify(VerificationGuard.SENTINEL));
        assertFalse(VerificationGuard.SENTINEL.verify(verificationGuard1));
        assertFalse(VerificationGuard.SENTINEL.verify(VerificationGuard.SENTINEL));
        assertTrue(verificationGuard1.verify(verificationGuard1));
    }
}
