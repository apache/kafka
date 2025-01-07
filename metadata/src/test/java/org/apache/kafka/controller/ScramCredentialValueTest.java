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

package org.apache.kafka.controller;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.controller.ScramControlManager.ScramCredentialValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ScramCredentialValueTest {

    @Test
    public void testEqualsAndHashCode() {
        byte[] salt1 = {1, 2, 3};
        byte[] storedKey1 = {4, 5, 6};
        byte[] serverKey1 = {7, 8, 9};
        int iterations1 = 1000;

        byte[] salt2 = {1, 2, 3};
        byte[] storedKey2 = {4, 5, 6};
        byte[] serverKey2 = {7, 8, 9};
        int iterations2 = 1000;

        ScramCredentialValue credential1 = new ScramCredentialValue(salt1, storedKey1, serverKey1, iterations1);
        ScramCredentialValue credential2 = new ScramCredentialValue(salt2, storedKey2, serverKey2, iterations2);

        assertEquals(credential1, credential2);
        assertEquals(credential1.hashCode(), credential2.hashCode());
    }

    @Test
    public void testNotEqualsDifferentContent() {
        byte[] salt1 = {1, 2, 3};
        byte[] storedKey1 = {4, 5, 6};
        byte[] serverKey1 = {7, 8, 9};
        int iterations1 = 1000;

        byte[] salt2 = {9, 8, 7};
        byte[] storedKey2 = {6, 5, 4};
        byte[] serverKey2 = {3, 2, 1};
        int iterations2 = 2000;

        ScramCredentialValue credential1 = new ScramCredentialValue(salt1, storedKey1, serverKey1, iterations1);
        ScramCredentialValue credential2 = new ScramCredentialValue(salt2, storedKey2, serverKey2, iterations2);

        assertNotEquals(credential1, credential2);
        assertNotEquals(credential1.hashCode(), credential2.hashCode());
    }

    @Test
    public void testEqualsSameInstance() {
        byte[] salt = {1, 2, 3};
        byte[] storedKey = {4, 5, 6};
        byte[] serverKey = {7, 8, 9};
        int iterations = 1000;

        ScramCredentialValue credential = new ScramCredentialValue(salt, storedKey, serverKey, iterations);

        // Test equals method for same instance
        assertEquals(credential, credential);
        assertEquals(credential.hashCode(), credential.hashCode());
    }
}
