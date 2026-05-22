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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientJwtValidatorTest extends JwtValidatorTest {

    @Override
    protected JwtValidator createJwtValidator(AccessTokenBuilder builder) {
        return new ClientJwtValidator();
    }

    @Test
    void testJwtRequiresBase64UrlDecoding() throws Exception {
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\": \"jdoe\", \"exp\": 0, \"iat\": 0, \"data\":\">>>___<<<---\"}";
        String signature = "dummysignature";
        String jwt = createJwt(header, payload, signature);

        // Verify that decoding the payload fails for "plain" base 64, but works with URL-safe base 64.
        String urlEncodedPayload = Base64.getUrlEncoder().encodeToString(Utils.utf8(payload));
        assertThrows(IllegalArgumentException.class, () -> Base64.getDecoder().decode(urlEncodedPayload));
        assertDoesNotThrow(() -> Base64.getUrlDecoder().decode(urlEncodedPayload));

        try (JwtValidator validator = createJwtValidator()) {
            validator.configure(getSaslConfigs(), OAUTHBEARER_MECHANISM, getJaasConfigEntries());
            assertDoesNotThrow(
                () -> validator.validate(jwt),
                "Valid, URL-safe base 64-encoded JWT should be decodable"
            );
        }
    }

    @Test
    void testSpaceDelimitedStringScopesProcessedAccordingToRfc6749() throws Exception {
        OAuthBearerToken token = validateTokenWithScope("email profile phone address");

        assertEquals(Set.of("email", "profile", "phone", "address"), token.scope());
    }

    @Test
    void testSpaceDelimitedStringScopesTrimmedAndCollapsed() throws Exception {
        OAuthBearerToken token = validateTokenWithScope("   email   profile   phone   ");

        assertEquals(Set.of("email", "profile", "phone"), token.scope());
    }

    @Test
    void testSpaceDelimitedStringScopesProcessedWithConfiguredScopeClaimName() throws Exception {
        OAuthBearerToken token = validateTokenWithCustomScopeClaimName("scp", "email profile phone");

        assertEquals(Set.of("email", "profile", "phone"), token.scope());
    }

    @Test
    void testDuplicateSpaceDelimitedStringScopesRejected() {
        JwtValidatorException exception = assertThrows(JwtValidatorException.class,
            () -> validateTokenWithScope("email profile email"));

        assertErrorMessageContains(exception.getMessage(), "scope value must not contain duplicates");
    }

    @Test
    void testBlankSpaceDelimitedStringScopesRejected() {
        JwtValidatorException exception = assertThrows(JwtValidatorException.class,
            () -> validateTokenWithScope("   "));

        assertErrorMessageContains(exception.getMessage(), "scope value must not contain only whitespace");
    }

    @Test
    void testCollectionScopesStillProcessed() throws Exception {
        OAuthBearerToken token = validateTokenWithScope(List.of("email", "profile", "phone"));

        assertEquals(Set.of("email", "profile", "phone"), token.scope());
    }

    private OAuthBearerToken validateTokenWithScope(Object scope) throws Exception {
        JwtValidator validator = createJwtValidator();
        validator.configure(getSaslConfigs(), OAUTHBEARER_MECHANISM, getJaasConfigEntries());

        return validator.validate(createJwtWithScope(scope));
    }

    private OAuthBearerToken validateTokenWithCustomScopeClaimName(String scopeClaimName, Object scope) throws Exception {
        JwtValidator validator = createJwtValidator();
        validator.configure(
            getSaslConfigs(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, scopeClaimName),
            OAUTHBEARER_MECHANISM,
            getJaasConfigEntries()
        );

        return validator.validate(createJwtWithScopeClaimName(scopeClaimName, scope));
    }

    private String createJwtWithScope(Object scope) {
        return createJwtWithScopeClaimName("scope", scope);
    }

    private String createJwtWithScopeClaimName(String scopeClaimName, Object scope) {
        String defaultScopeJson = "scope".equals(scopeClaimName) ? "" : "\"scope\":\"engineering\",";
        return createJwt(
            "{\"alg\":\"HS256\",\"typ\":\"JWT\"}",
            String.format(
                "{\"sub\":\"jdoe\",\"exp\":60,\"iat\":0,%s\"%s\":%s}",
                defaultScopeJson,
                scopeClaimName,
                scopeJson(scope)
            ),
            "dummysignature"
        );
    }

    private String scopeJson(Object scope) {
        if (scope instanceof String)
            return "\"" + escapeJson((String) scope) + "\"";

        @SuppressWarnings("unchecked")
        List<String> scopes = (List<String>) scope;
        return scopes.stream()
            .map(scopeValue -> "\"" + escapeJson(scopeValue) + "\"")
            .reduce((left, right) -> left + "," + right)
            .map(scopeValues -> "[" + scopeValues + "]")
            .orElse("[]");
    }

    private String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
