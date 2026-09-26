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
package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import org.junit.jupiter.api.Test;

import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssertionUtilsTest {

    @Test
    public void testKeyAlgorithmRs256() {
        assertEquals("RSA", AssertionUtils.keyAlgorithm("RS256"));
    }

    @Test
    public void testKeyAlgorithmEs256() {
        assertEquals("EC", AssertionUtils.keyAlgorithm("ES256"));
    }

    @Test
    public void testKeyAlgorithmCaseInsensitive() {
        assertEquals("RSA", AssertionUtils.keyAlgorithm("rs256"));
        assertEquals("EC", AssertionUtils.keyAlgorithm("es256"));
    }

    @Test
    public void testKeyAlgorithmUnsupported() {
        assertThrows(IllegalArgumentException.class, () -> AssertionUtils.keyAlgorithm("PS256"));
    }

    @Test
    public void testGetSignatureRs256() throws GeneralSecurityException {
        Signature signature = AssertionUtils.getSignature("RS256");
        assertNotNull(signature);
    }

    @Test
    public void testGetSignatureEs256() throws GeneralSecurityException {
        Signature signature = AssertionUtils.getSignature("ES256");
        assertNotNull(signature);
    }

    @Test
    public void testGetSignatureUnsupported() {
        assertThrows(NoSuchAlgorithmException.class, () -> AssertionUtils.getSignature("PS256"));
    }
}
