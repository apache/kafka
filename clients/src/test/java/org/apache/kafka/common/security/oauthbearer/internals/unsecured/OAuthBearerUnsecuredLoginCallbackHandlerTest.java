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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

public class OAuthBearerUnsecuredLoginCallbackHandlerTest {

    @Test
    public void addsExtensions() throws IOException, UnsupportedCallbackException {
        Map<String, String> options = new HashMap<>();
        options.put("unsecuredLoginExtension_testId", "1");
        OAuthBearerUnsecuredLoginCallbackHandler callbackHandler = createCallbackHandler(options, new MockTime());
        SaslExtensionsCallback callback = new SaslExtensionsCallback();

        callbackHandler.handle(new Callback[] {callback});

        assertEquals("1", callback.extensions().map().get("testId"));
    }

    @Test
    public void throwsErrorOnInvalidExtensionName() {
        Map<String, String> options = new HashMap<>();
        options.put("unsecuredLoginExtension_test.Id", "1");
        OAuthBearerUnsecuredLoginCallbackHandler callbackHandler = createCallbackHandler(options, new MockTime());
        SaslExtensionsCallback callback = new SaslExtensionsCallback();

        assertThrows(IOException.class, () -> callbackHandler.handle(new Callback[] {callback}));
    }

    @Test
    public void throwsErrorOnInvalidExtensionValue() {
        Map<String, String> options = new HashMap<>();
        options.put("unsecuredLoginExtension_testId", "Çalifornia");
        OAuthBearerUnsecuredLoginCallbackHandler callbackHandler = createCallbackHandler(options, new MockTime());
        SaslExtensionsCallback callback = new SaslExtensionsCallback();

        assertThrows(IOException.class, () -> callbackHandler.handle(new Callback[] {callback}));
    }

    @Test
    public void minimalToken() throws IOException, UnsupportedCallbackException {
        Map<String, String> options = new HashMap<>();
        String user = "user";
        options.put("unsecuredLoginStringClaim_sub", user);
        MockTime mockTime = new MockTime();
        OAuthBearerUnsecuredLoginCallbackHandler callbackHandler = createCallbackHandler(options, mockTime);
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        callbackHandler.handle(new Callback[] {callback});
        OAuthBearerUnsecuredJws jws = (OAuthBearerUnsecuredJws) callback.token();
        assertNotNull(jws, "create token failed");
        long startMs = mockTime.milliseconds();
        confirmCorrectValues(jws, user, startMs, 1000 * 60 * 60);
        assertEquals(new HashSet<>(Arrays.asList("sub", "iat", "exp")), jws.claims().keySet());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validOptionsWithExplicitOptionValues()
            throws IOException, UnsupportedCallbackException {
        String explicitScope1 = "scope1";
        String explicitScope2 = "scope2";
        String explicitScopeClaimName = "putScopeInHere";
        String principalClaimName = "principal";
        final String[] scopeClaimNameOptionValues = {null, explicitScopeClaimName};
        for (String scopeClaimNameOptionValue : scopeClaimNameOptionValues) {
            Map<String, String> options = new HashMap<>();
            String user = "user";
            options.put("unsecuredLoginStringClaim_" + principalClaimName, user);
            options.put("unsecuredLoginListClaim_" + "list", ",1,2,");
            options.put("unsecuredLoginListClaim_" + "emptyList1", "");
            options.put("unsecuredLoginListClaim_" + "emptyList2", ",");
            options.put("unsecuredLoginNumberClaim_" + "number", "1");
            long lifetimeSeconds = 10000;
            options.put("unsecuredLoginLifetimeSeconds", String.valueOf(lifetimeSeconds));
            options.put("unsecuredLoginPrincipalClaimName", principalClaimName);
            if (scopeClaimNameOptionValue != null)
                options.put("unsecuredLoginScopeClaimName", scopeClaimNameOptionValue);
            String actualScopeClaimName = scopeClaimNameOptionValue == null ? "scope" : explicitScopeClaimName;
            options.put("unsecuredLoginListClaim_" + actualScopeClaimName,
                    String.format("|%s|%s", explicitScope1, explicitScope2));
            MockTime mockTime = new MockTime();
            OAuthBearerUnsecuredLoginCallbackHandler callbackHandler = createCallbackHandler(options, mockTime);
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            callbackHandler.handle(new Callback[] {callback});
            OAuthBearerUnsecuredJws jws = (OAuthBearerUnsecuredJws) callback.token();
            assertNotNull(jws, "create token failed");
            long startMs = mockTime.milliseconds();
            confirmCorrectValues(jws, user, startMs, lifetimeSeconds * 1000);
            Map<String, Object> claims = jws.claims();
            assertEquals(new HashSet<>(Arrays.asList(actualScopeClaimName, principalClaimName, "iat", "exp", "number",
                    "list", "emptyList1", "emptyList2")), claims.keySet());
            assertEquals(new HashSet<>(Arrays.asList(explicitScope1, explicitScope2)),
                    new HashSet<>((List<String>) claims.get(actualScopeClaimName)));
            assertEquals(new HashSet<>(Arrays.asList(explicitScope1, explicitScope2)), jws.scope());
            assertEquals(1.0, jws.claim("number", Number.class));
            assertEquals(Arrays.asList("1", "2", ""), jws.claim("list", List.class));
            assertEquals(Collections.emptyList(), jws.claim("emptyList1", List.class));
            assertEquals(Collections.emptyList(), jws.claim("emptyList2", List.class));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static OAuthBearerUnsecuredLoginCallbackHandler createCallbackHandler(Map<String, String> options,
            MockTime mockTime) {
        TestJaasConfig config = new TestJaasConfig();
        config.createOrUpdateEntry("KafkaClient", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                (Map) options);
        OAuthBearerUnsecuredLoginCallbackHandler callbackHandler = new OAuthBearerUnsecuredLoginCallbackHandler();
        callbackHandler.time(mockTime);
        callbackHandler.configure(Collections.emptyMap(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                Arrays.asList(config.getAppConfigurationEntry("KafkaClient")[0]));
        return callbackHandler;
    }

    private static void confirmCorrectValues(OAuthBearerUnsecuredJws jws, String user, long startMs,
            long lifetimeSeconds) throws OAuthBearerIllegalTokenException {
        Map<String, Object> header = jws.header();
        assertEquals(header.size(), 1);
        assertEquals("none", header.get("alg"));
        assertEquals(user != null ? user : "<unknown>", jws.principalName());
        assertEquals(Long.valueOf(startMs), jws.startTimeMs());
        assertEquals(startMs, Math.round(jws.issuedAt().doubleValue() * 1000));
        assertEquals(startMs + lifetimeSeconds, jws.lifetimeMs());
        assertEquals(jws.lifetimeMs(), Math.round(jws.expirationTime().doubleValue() * 1000));
    }
}
