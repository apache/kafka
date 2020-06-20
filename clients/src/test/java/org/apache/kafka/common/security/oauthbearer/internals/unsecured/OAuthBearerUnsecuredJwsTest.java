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
package org.apache.kafka.common.security.oauthbearer.internals.unsecured;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

public class OAuthBearerUnsecuredJwsTest {
    private static final String QUOTE = "\"";
    private static final String HEADER_COMPACT_SERIALIZATION = Base64.getUrlEncoder().withoutPadding()
            .encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8)) + ".";

    @Test
    public void validClaims() throws OAuthBearerIllegalTokenException {
        double issuedAtSeconds = 100.1;
        double expirationTimeSeconds = 300.3;
        StringBuilder sb = new StringBuilder("{");
        appendJsonText(sb, "sub", "SUBJECT");
        appendCommaJsonText(sb, "iat", issuedAtSeconds);
        appendCommaJsonText(sb, "exp", expirationTimeSeconds);
        sb.append("}");
        String compactSerialization = HEADER_COMPACT_SERIALIZATION
                + Base64.getUrlEncoder().withoutPadding().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8))
                + ".";
        OAuthBearerUnsecuredJws testJwt = new OAuthBearerUnsecuredJws(compactSerialization, "sub", "scope");
        assertEquals(compactSerialization, testJwt.value());
        assertEquals("sub", testJwt.principalClaimName());
        assertEquals(1, testJwt.header().size());
        assertEquals("none", testJwt.header().get("alg"));
        assertEquals("scope", testJwt.scopeClaimName());
        assertEquals(expirationTimeSeconds, testJwt.expirationTime());
        assertTrue(testJwt.isClaimType("exp", Number.class));
        assertEquals(issuedAtSeconds, testJwt.issuedAt());
        assertEquals("SUBJECT", testJwt.subject());
    }

    @Test
    public void validCompactSerialization() {
        String subject = "foo";
        long issuedAt = 100;
        long expirationTime = issuedAt + 60 * 60;
        List<String> scope = Arrays.asList("scopeValue1", "scopeValue2");
        String validCompactSerialization = compactSerialization(subject, issuedAt, expirationTime, scope);
        OAuthBearerUnsecuredJws jws = new OAuthBearerUnsecuredJws(validCompactSerialization, "sub", "scope");
        assertEquals(1, jws.header().size());
        assertEquals("none", jws.header().get("alg"));
        assertEquals(4, jws.claims().size());
        assertEquals(subject, jws.claims().get("sub"));
        assertEquals(subject, jws.principalName());
        assertEquals(issuedAt, Number.class.cast(jws.claims().get("iat")).longValue());
        assertEquals(expirationTime, Number.class.cast(jws.claims().get("exp")).longValue());
        assertEquals(expirationTime * 1000, jws.lifetimeMs());
        assertEquals(scope, jws.claims().get("scope"));
        assertEquals(new HashSet<>(scope), jws.scope());
        assertEquals(3, jws.splits().size());
        assertEquals(validCompactSerialization.split("\\.")[0], jws.splits().get(0));
        assertEquals(validCompactSerialization.split("\\.")[1], jws.splits().get(1));
        assertEquals("", jws.splits().get(2));
    }

    @Test(expected = OAuthBearerIllegalTokenException.class)
    public void missingPrincipal() {
        String subject = null;
        long issuedAt = 100;
        Long expirationTime = null;
        List<String> scope = Arrays.asList("scopeValue1", "scopeValue2");
        String validCompactSerialization = compactSerialization(subject, issuedAt, expirationTime, scope);
        new OAuthBearerUnsecuredJws(validCompactSerialization, "sub", "scope");
    }

    @Test(expected = OAuthBearerIllegalTokenException.class)
    public void blankPrincipalName() {
        String subject = "   ";
        long issuedAt = 100;
        long expirationTime = issuedAt + 60 * 60;
        List<String> scope = Arrays.asList("scopeValue1", "scopeValue2");
        String validCompactSerialization = compactSerialization(subject, issuedAt, expirationTime, scope);
        new OAuthBearerUnsecuredJws(validCompactSerialization, "sub", "scope");
    }

    private static String compactSerialization(String subject, Long issuedAt, Long expirationTime, List<String> scope) {
        Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String algorithm = "none";
        String headerJson = "{\"alg\":\"" + algorithm + "\"}";
        String encodedHeader = encoder.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));
        String subjectJson = subject != null ? "\"sub\":\"" + subject + "\"" : null;
        String issuedAtJson = issuedAt != null ? "\"iat\":" + issuedAt.longValue() : null;
        String expirationTimeJson = expirationTime != null ? "\"exp\":" + expirationTime.longValue() : null;
        String scopeJson = scope != null ? scopeJson(scope) : null;
        String claimsJson = claimsJson(subjectJson, issuedAtJson, expirationTimeJson, scopeJson);
        String encodedClaims = encoder.encodeToString(claimsJson.getBytes(StandardCharsets.UTF_8));
        return encodedHeader + "." + encodedClaims + ".";
    }

    private static String claimsJson(String... jsonValues) {
        StringBuilder claimsJsonBuilder = new StringBuilder("{");
        int initialLength = claimsJsonBuilder.length();
        for (String jsonValue : jsonValues) {
            if (jsonValue != null) {
                if (claimsJsonBuilder.length() > initialLength)
                    claimsJsonBuilder.append(',');
                claimsJsonBuilder.append(jsonValue);
            }
        }
        claimsJsonBuilder.append('}');
        return claimsJsonBuilder.toString();
    }

    private static String scopeJson(List<String> scope) {
        StringBuilder scopeJsonBuilder = new StringBuilder("\"scope\":[");
        int initialLength = scopeJsonBuilder.length();
        for (String scopeValue : scope) {
            if (scopeJsonBuilder.length() > initialLength)
                scopeJsonBuilder.append(',');
            scopeJsonBuilder.append('"').append(scopeValue).append('"');
        }
        scopeJsonBuilder.append(']');
        return scopeJsonBuilder.toString();
    }

    private static void appendCommaJsonText(StringBuilder sb, String claimName, Number claimValue) {
        sb.append(',').append(QUOTE).append(escape(claimName)).append(QUOTE).append(":").append(claimValue);
    }

    private static void appendJsonText(StringBuilder sb, String claimName, String claimValue) {
        sb.append(QUOTE).append(escape(claimName)).append(QUOTE).append(":").append(QUOTE).append(escape(claimValue))
                .append(QUOTE);
    }

    private static String escape(String jsonStringValue) {
        return jsonStringValue.replace("\"", "\\\"").replace("\\", "\\\\");
    }
}
