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

package org.apache.kafka.common.security.oauthbearer;

import org.apache.kafka.common.security.oauthbearer.internals.secured.ClaimValidationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClaimValidationUtilsTest extends OAuthBearerTest {

    @Test
    public void testValidateScopes() {
        Set<String> scopes = ClaimValidationUtils.validateScopes("scope", Arrays.asList("  a  ", "    b    "));

        assertEquals(2, scopes.size());
        assertTrue(scopes.contains("a"));
        assertTrue(scopes.contains("b"));
    }

    @Test
    public void testValidateScopesDisallowsDuplicates() {
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateScopes("scope", Arrays.asList("a", "b", "a")));
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateScopes("scope", Arrays.asList("a", "b", "  a  ")));
    }

    @Test
    public void testValidateScopesDisallowsEmptyNullAndWhitespace() {
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateScopes("scope", Arrays.asList("a", "")));
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateScopes("scope", Arrays.asList("a", null)));
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateScopes("scope", Arrays.asList("a", "  ")));
    }

    @Test
    public void testValidateScopesResultIsImmutable() {
        SortedSet<String> callerSet = new TreeSet<>(Arrays.asList("a", "b", "c"));
        Set<String> scopes = ClaimValidationUtils.validateScopes("scope", callerSet);

        assertEquals(3, scopes.size());

        callerSet.add("d");
        assertEquals(4, callerSet.size());
        assertTrue(callerSet.contains("d"));
        assertEquals(3, scopes.size());
        assertFalse(scopes.contains("d"));

        callerSet.remove("c");
        assertEquals(3, callerSet.size());
        assertFalse(callerSet.contains("c"));
        assertEquals(3, scopes.size());
        assertTrue(scopes.contains("c"));

        callerSet.clear();
        assertEquals(0, callerSet.size());
        assertEquals(3, scopes.size());
    }

    @Test
    public void testValidateScopesResultThrowsExceptionOnMutation() {
        SortedSet<String> callerSet = new TreeSet<>(Arrays.asList("a", "b", "c"));
        Set<String> scopes = ClaimValidationUtils.validateScopes("scope", callerSet);
        assertThrows(UnsupportedOperationException.class, scopes::clear);
    }

    @Test
    public void testValidateExpiration() {
        Long expected = 1L;
        Long actual = ClaimValidationUtils.validateExpiration("exp", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateExpirationAllowsZero() {
        Long expected = 0L;
        Long actual = ClaimValidationUtils.validateExpiration("exp", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateExpirationDisallowsNull() {
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateExpiration("exp", null));
    }

    @Test
    public void testValidateExpirationDisallowsNegatives() {
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateExpiration("exp", -1L));
    }

    @Test
    public void testValidateSubject() {
        String expected = "jdoe";
        String actual = ClaimValidationUtils.validateSubject("sub", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateSubjectDisallowsEmptyNullAndWhitespace() {
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateSubject("sub", ""));
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateSubject("sub", null));
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateSubject("sub", "  "));
    }

    @Test
    public void testValidateClaimNameOverride() {
        String expected = "email";
        String actual = ClaimValidationUtils.validateClaimNameOverride("sub", String.format("  %s  ", expected));
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateClaimNameOverrideDisallowsEmptyNullAndWhitespace() {
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateSubject("sub", ""));
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateSubject("sub", null));
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateSubject("sub", "  "));
    }

    @Test
    public void testValidateIssuedAt() {
        Long expected = 1L;
        Long actual = ClaimValidationUtils.validateIssuedAt("iat", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateIssuedAtAllowsZero() {
        Long expected = 0L;
        Long actual = ClaimValidationUtils.validateIssuedAt("iat", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateIssuedAtAllowsNull() {
        Long expected = null;
        Long actual = ClaimValidationUtils.validateIssuedAt("iat", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateIssuedAtDisallowsNegatives() {
        assertThrows(JwtValidatorException.class, () -> ClaimValidationUtils.validateIssuedAt("iat", -1L));
    }

}
