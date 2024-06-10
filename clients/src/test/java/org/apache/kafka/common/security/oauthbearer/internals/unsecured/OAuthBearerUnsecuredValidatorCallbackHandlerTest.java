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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.Callback;

import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

public class OAuthBearerUnsecuredValidatorCallbackHandlerTest {
    private static final String UNSECURED_JWT_HEADER_JSON = "{" + claimOrHeaderText("alg", "none") + "}";
    private static final Time MOCK_TIME = new MockTime();
    private static final String QUOTE = "\"";
    private static final String PRINCIPAL_CLAIM_VALUE = "username";
    private static final String PRINCIPAL_CLAIM_TEXT = claimOrHeaderText("principal", PRINCIPAL_CLAIM_VALUE);
    private static final String SUB_CLAIM_TEXT = claimOrHeaderText("sub", PRINCIPAL_CLAIM_VALUE);
    private static final String BAD_PRINCIPAL_CLAIM_TEXT = claimOrHeaderText("principal", 1);
    private static final long LIFETIME_SECONDS_TO_USE = 1000 * 60 * 60;
    private static final String EXPIRATION_TIME_CLAIM_TEXT = expClaimText(LIFETIME_SECONDS_TO_USE);
    private static final String TOO_EARLY_EXPIRATION_TIME_CLAIM_TEXT = expClaimText(0);
    private static final String ISSUED_AT_CLAIM_TEXT = claimOrHeaderText("iat", MOCK_TIME.milliseconds() / 1000.0);
    private static final String SCOPE_CLAIM_TEXT = claimOrHeaderText("scope", "scope1");
    private static final Map<String, String> MODULE_OPTIONS_MAP_NO_SCOPE_REQUIRED;
    static {
        Map<String, String> tmp = new HashMap<>();
        tmp.put("unsecuredValidatorPrincipalClaimName", "principal");
        tmp.put("unsecuredValidatorAllowableClockSkewMs", "1");
        MODULE_OPTIONS_MAP_NO_SCOPE_REQUIRED = Collections.unmodifiableMap(tmp);
    }
    private static final Map<String, String> MODULE_OPTIONS_MAP_REQUIRE_EXISTING_SCOPE;
    static {
        Map<String, String> tmp = new HashMap<>();
        tmp.put("unsecuredValidatorRequiredScope", "scope1");
        MODULE_OPTIONS_MAP_REQUIRE_EXISTING_SCOPE = Collections.unmodifiableMap(tmp);
    }
    private static final Map<String, String> MODULE_OPTIONS_MAP_REQUIRE_ADDITIONAL_SCOPE;
    static {
        Map<String, String> tmp = new HashMap<>();
        tmp.put("unsecuredValidatorRequiredScope", "scope1 scope2");
        MODULE_OPTIONS_MAP_REQUIRE_ADDITIONAL_SCOPE = Collections.unmodifiableMap(tmp);
    }

    @Test
    public void validToken() {
        for (final boolean includeOptionalIssuedAtClaim : new boolean[] {true, false}) {
            String claimsJson = "{" + PRINCIPAL_CLAIM_TEXT + comma(EXPIRATION_TIME_CLAIM_TEXT)
                    + (includeOptionalIssuedAtClaim ? comma(ISSUED_AT_CLAIM_TEXT) : "") + "}";
            Object validationResult = validationResult(UNSECURED_JWT_HEADER_JSON, claimsJson,
                    MODULE_OPTIONS_MAP_NO_SCOPE_REQUIRED);
            assertInstanceOf(OAuthBearerValidatorCallback.class, validationResult);
            assertInstanceOf(OAuthBearerUnsecuredJws.class, ((OAuthBearerValidatorCallback) validationResult).token());
        }
    }

    @Test
    public void badOrMissingPrincipal() {
        for (boolean exists : new boolean[] {true, false}) {
            String claimsJson = "{" + EXPIRATION_TIME_CLAIM_TEXT + (exists ? comma(BAD_PRINCIPAL_CLAIM_TEXT) : "")
                    + "}";
            confirmFailsValidation(UNSECURED_JWT_HEADER_JSON, claimsJson, MODULE_OPTIONS_MAP_NO_SCOPE_REQUIRED);
        }
    }

    @Test
    public void tooEarlyExpirationTime() {
        String claimsJson = "{" + PRINCIPAL_CLAIM_TEXT + comma(ISSUED_AT_CLAIM_TEXT)
                + comma(TOO_EARLY_EXPIRATION_TIME_CLAIM_TEXT) + "}";
        confirmFailsValidation(UNSECURED_JWT_HEADER_JSON, claimsJson, MODULE_OPTIONS_MAP_NO_SCOPE_REQUIRED);
    }

    @Test
    public void includesRequiredScope() {
        String claimsJson = "{" + SUB_CLAIM_TEXT + comma(EXPIRATION_TIME_CLAIM_TEXT) + comma(SCOPE_CLAIM_TEXT) + "}";
        Object validationResult = validationResult(UNSECURED_JWT_HEADER_JSON, claimsJson,
                MODULE_OPTIONS_MAP_REQUIRE_EXISTING_SCOPE);
        assertInstanceOf(OAuthBearerValidatorCallback.class, validationResult);
        assertInstanceOf(OAuthBearerUnsecuredJws.class, ((OAuthBearerValidatorCallback) validationResult).token());
    }

    @Test
    public void missingRequiredScope() {
        String claimsJson = "{" + SUB_CLAIM_TEXT + comma(EXPIRATION_TIME_CLAIM_TEXT) + comma(SCOPE_CLAIM_TEXT) + "}";
        confirmFailsValidation(UNSECURED_JWT_HEADER_JSON, claimsJson, MODULE_OPTIONS_MAP_REQUIRE_ADDITIONAL_SCOPE,
                "[scope1, scope2]");
    }

    private static void confirmFailsValidation(String headerJson, String claimsJson,
            Map<String, String> moduleOptionsMap) throws OAuthBearerConfigException, OAuthBearerIllegalTokenException {
        confirmFailsValidation(headerJson, claimsJson, moduleOptionsMap, null);
    }

    private static void confirmFailsValidation(String headerJson, String claimsJson,
            Map<String, String> moduleOptionsMap, String optionalFailureScope) throws OAuthBearerConfigException,
            OAuthBearerIllegalTokenException {
        Object validationResultObj = validationResult(headerJson, claimsJson, moduleOptionsMap);
        assertInstanceOf(OAuthBearerValidatorCallback.class, validationResultObj);
        OAuthBearerValidatorCallback callback = (OAuthBearerValidatorCallback) validationResultObj;
        assertNull(callback.token());
        assertNull(callback.errorOpenIDConfiguration());
        if (optionalFailureScope == null) {
            assertEquals("invalid_token", callback.errorStatus());
            assertNull(callback.errorScope());
        } else {
            assertEquals("insufficient_scope", callback.errorStatus());
            assertEquals(optionalFailureScope, callback.errorScope());
        }
    }

    private static Object validationResult(String headerJson, String claimsJson, Map<String, String> moduleOptionsMap) {
        Encoder urlEncoderNoPadding = Base64.getUrlEncoder().withoutPadding();
        try {
            String tokenValue = String.format("%s.%s.",
                    urlEncoderNoPadding.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8)),
                    urlEncoderNoPadding.encodeToString(claimsJson.getBytes(StandardCharsets.UTF_8)));
            OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(tokenValue);
            createCallbackHandler(moduleOptionsMap).handle(new Callback[] {callback});
            return callback;
        } catch (Exception e) {
            return e;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static OAuthBearerUnsecuredValidatorCallbackHandler createCallbackHandler(Map<String, String> options) {
        TestJaasConfig config = new TestJaasConfig();
        config.createOrUpdateEntry("KafkaClient", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                (Map) options);
        OAuthBearerUnsecuredValidatorCallbackHandler callbackHandler = new OAuthBearerUnsecuredValidatorCallbackHandler();
        callbackHandler.configure(Collections.emptyMap(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                Arrays.asList(config.getAppConfigurationEntry("KafkaClient")[0]));
        return callbackHandler;
    }

    private static String comma(String value) {
        return "," + value;
    }

    private static String claimOrHeaderText(String claimName, Number claimValue) {
        return QUOTE + claimName + QUOTE + ":" + claimValue;
    }

    private static String claimOrHeaderText(String claimName, String claimValue) {
        return QUOTE + claimName + QUOTE + ":" + QUOTE + claimValue + QUOTE;
    }

    private static String expClaimText(long lifetimeSeconds) {
        return claimOrHeaderText("exp", MOCK_TIME.milliseconds() / 1000.0 + lifetimeSeconds);
    }
}
