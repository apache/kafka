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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

public class OAuthBearerValidationUtilsTest {
    private static final String QUOTE = "\"";
    private static final String HEADER_COMPACT_SERIALIZATION = Base64.getUrlEncoder().withoutPadding()
            .encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8)) + ".";
    private static final Time TIME = Time.SYSTEM;

    @Test
    public void validateClaimForExistenceAndType() throws OAuthBearerIllegalTokenException {
        String claimName = "foo";
        for (Boolean exists : new Boolean[] {null, Boolean.TRUE, Boolean.FALSE}) {
            boolean useErrorValue = exists == null;
            for (Boolean required : new boolean[] {true, false}) {
                StringBuilder sb = new StringBuilder("{");
                appendJsonText(sb, "exp", 100);
                appendCommaJsonText(sb, "sub", "principalName");
                if (useErrorValue)
                    appendCommaJsonText(sb, claimName, 1);
                else if (exists)
                    appendCommaJsonText(sb, claimName, claimName);
                sb.append("}");
                String compactSerialization = HEADER_COMPACT_SERIALIZATION + Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8)) + ".";
                OAuthBearerUnsecuredJws testJwt = new OAuthBearerUnsecuredJws(compactSerialization, "sub", "scope");
                OAuthBearerValidationResult result = OAuthBearerValidationUtils
                        .validateClaimForExistenceAndType(testJwt, required, claimName, String.class);
                if (useErrorValue || required && !exists)
                    assertTrue(isFailureWithMessageAndNoFailureScope(result));
                else
                    assertTrue(isSuccess(result));
            }
        }
    }

    @Test
    public void validateIssuedAt() {
        long nowMs = TIME.milliseconds();
        double nowClaimValue = ((double) nowMs) / 1000;
        for (boolean exists : new boolean[] {true, false}) {
            StringBuilder sb = new StringBuilder("{");
            appendJsonText(sb, "exp", nowClaimValue);
            appendCommaJsonText(sb, "sub", "principalName");
            if (exists)
                appendCommaJsonText(sb, "iat", nowClaimValue);
            sb.append("}");
            String compactSerialization = HEADER_COMPACT_SERIALIZATION + Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8)) + ".";
            OAuthBearerUnsecuredJws testJwt = new OAuthBearerUnsecuredJws(compactSerialization, "sub", "scope");
            for (boolean required : new boolean[] {true, false}) {
                for (int allowableClockSkewMs : new int[] {0, 5, 10, 20}) {
                    for (long whenCheckOffsetMs : new long[] {-10, 0, 10}) {
                        long whenCheckMs = nowMs + whenCheckOffsetMs;
                        OAuthBearerValidationResult result = OAuthBearerValidationUtils.validateIssuedAt(testJwt,
                                required, whenCheckMs, allowableClockSkewMs);
                        if (required && !exists)
                            assertTrue(isFailureWithMessageAndNoFailureScope(result), "useErrorValue || required && !exists");
                        else if (!required && !exists)
                            assertTrue(isSuccess(result), "!required && !exists");
                        else if (nowClaimValue * 1000 > whenCheckMs + allowableClockSkewMs) // issued in future
                            assertTrue(isFailureWithMessageAndNoFailureScope(result),
                                assertionFailureMessage(nowClaimValue, allowableClockSkewMs, whenCheckMs));
                        else
                            assertTrue(isSuccess(result),
                                assertionFailureMessage(nowClaimValue, allowableClockSkewMs, whenCheckMs));
                    }
                }
            }
        }
    }

    @Test
    public void validateExpirationTime() {
        long nowMs = TIME.milliseconds();
        double nowClaimValue = ((double) nowMs) / 1000;
        StringBuilder sb = new StringBuilder("{");
        appendJsonText(sb, "exp", nowClaimValue);
        appendCommaJsonText(sb, "sub", "principalName");
        sb.append("}");
        String compactSerialization = HEADER_COMPACT_SERIALIZATION
                + Base64.getUrlEncoder().withoutPadding().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8))
                + ".";
        OAuthBearerUnsecuredJws testJwt = new OAuthBearerUnsecuredJws(compactSerialization, "sub", "scope");
        for (int allowableClockSkewMs : new int[] {0, 5, 10, 20}) {
            for (long whenCheckOffsetMs : new long[] {-10, 0, 10}) {
                long whenCheckMs = nowMs + whenCheckOffsetMs;
                OAuthBearerValidationResult result = OAuthBearerValidationUtils.validateExpirationTime(testJwt,
                        whenCheckMs, allowableClockSkewMs);
                if (whenCheckMs - allowableClockSkewMs >= nowClaimValue * 1000) // expired
                    assertTrue(isFailureWithMessageAndNoFailureScope(result),
                        assertionFailureMessage(nowClaimValue, allowableClockSkewMs, whenCheckMs));
                else
                    assertTrue(isSuccess(result), assertionFailureMessage(nowClaimValue, allowableClockSkewMs, whenCheckMs));
            }
        }
    }

    @Test
    public void validateExpirationTimeAndIssuedAtConsistency() throws OAuthBearerIllegalTokenException {
        long nowMs = TIME.milliseconds();
        double nowClaimValue = ((double) nowMs) / 1000;
        for (boolean issuedAtExists : new boolean[] {true, false}) {
            if (!issuedAtExists) {
                StringBuilder sb = new StringBuilder("{");
                appendJsonText(sb, "exp", nowClaimValue);
                appendCommaJsonText(sb, "sub", "principalName");
                sb.append("}");
                String compactSerialization = HEADER_COMPACT_SERIALIZATION + Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8)) + ".";
                OAuthBearerUnsecuredJws testJwt = new OAuthBearerUnsecuredJws(compactSerialization, "sub", "scope");
                assertTrue(isSuccess(OAuthBearerValidationUtils.validateTimeConsistency(testJwt)));
            } else
                for (int expirationTimeOffset = -1; expirationTimeOffset <= 1; ++expirationTimeOffset) {
                    StringBuilder sb = new StringBuilder("{");
                    appendJsonText(sb, "iat", nowClaimValue);
                    appendCommaJsonText(sb, "exp", nowClaimValue + expirationTimeOffset);
                    appendCommaJsonText(sb, "sub", "principalName");
                    sb.append("}");
                    String compactSerialization = HEADER_COMPACT_SERIALIZATION + Base64.getUrlEncoder().withoutPadding()
                            .encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8)) + ".";
                    OAuthBearerUnsecuredJws testJwt = new OAuthBearerUnsecuredJws(compactSerialization, "sub", "scope");
                    OAuthBearerValidationResult result = OAuthBearerValidationUtils.validateTimeConsistency(testJwt);
                    if (expirationTimeOffset <= 0)
                        assertTrue(isFailureWithMessageAndNoFailureScope(result));
                    else
                        assertTrue(isSuccess(result));
                }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void validateScope() {
        long nowMs = TIME.milliseconds();
        double nowClaimValue = ((double) nowMs) / 1000;
        final List<String> noScope = Collections.emptyList();
        final List<String> scope1 = Arrays.asList("scope1");
        final List<String> scope1And2 = Arrays.asList("scope1", "scope2");
        for (boolean actualScopeExists : new boolean[] {true, false}) {
            List<? extends List> scopes = !actualScopeExists ? Arrays.asList((List) null)
                    : Arrays.asList(noScope, scope1, scope1And2);
            for (List<String> actualScope : scopes) {
                for (boolean requiredScopeExists : new boolean[] {true, false}) {
                    List<? extends List> requiredScopes = !requiredScopeExists ? Arrays.asList((List) null)
                            : Arrays.asList(noScope, scope1, scope1And2);
                    for (List<String> requiredScope : requiredScopes) {
                        StringBuilder sb = new StringBuilder("{");
                        appendJsonText(sb, "exp", nowClaimValue);
                        appendCommaJsonText(sb, "sub", "principalName");
                        if (actualScope != null)
                            sb.append(',').append(scopeJson(actualScope));
                        sb.append("}");
                        String compactSerialization = HEADER_COMPACT_SERIALIZATION + Base64.getUrlEncoder()
                                .withoutPadding().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8)) + ".";
                        OAuthBearerUnsecuredJws testJwt = new OAuthBearerUnsecuredJws(compactSerialization, "sub",
                                "scope");
                        OAuthBearerValidationResult result = OAuthBearerValidationUtils.validateScope(testJwt,
                                requiredScope);
                        if (!requiredScopeExists || requiredScope.isEmpty())
                            assertTrue(isSuccess(result));
                        else if (!actualScopeExists || actualScope.size() < requiredScope.size())
                            assertTrue(isFailureWithMessageAndFailureScope(result));
                        else
                            assertTrue(isSuccess(result));
                    }
                }
            }
        }
    }

    private static String assertionFailureMessage(double claimValue, int allowableClockSkewMs, long whenCheckMs) {
        return String.format("time=%f seconds, whenCheck = %d ms, allowableClockSkew=%d ms", claimValue, whenCheckMs,
                allowableClockSkewMs);
    }

    private static boolean isSuccess(OAuthBearerValidationResult result) {
        return result.success();
    }

    private static boolean isFailureWithMessageAndNoFailureScope(OAuthBearerValidationResult result) {
        return !result.success() && !result.failureDescription().isEmpty() && result.failureScope() == null
                && result.failureOpenIdConfig() == null;
    }

    private static boolean isFailureWithMessageAndFailureScope(OAuthBearerValidationResult result) {
        return !result.success() && !result.failureDescription().isEmpty() && !result.failureScope().isEmpty()
                && result.failureOpenIdConfig() == null;
    }

    private static void appendCommaJsonText(StringBuilder sb, String claimName, Number claimValue) {
        sb.append(',').append(QUOTE).append(escape(claimName)).append(QUOTE).append(":").append(claimValue);
    }

    private static void appendCommaJsonText(StringBuilder sb, String claimName, String claimValue) {
        sb.append(',').append(QUOTE).append(escape(claimName)).append(QUOTE).append(":").append(QUOTE)
                .append(escape(claimValue)).append(QUOTE);
    }

    private static void appendJsonText(StringBuilder sb, String claimName, Number claimValue) {
        sb.append(QUOTE).append(escape(claimName)).append(QUOTE).append(":").append(claimValue);
    }

    private static String escape(String jsonStringValue) {
        return jsonStringValue.replace("\"", "\\\"").replace("\\", "\\\\");
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
}
