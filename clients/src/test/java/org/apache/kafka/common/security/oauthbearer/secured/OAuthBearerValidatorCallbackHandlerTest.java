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

package org.apache.kafka.common.security.oauthbearer.secured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Key;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.utils.Utils;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.junit.jupiter.api.Test;

public class OAuthBearerValidatorCallbackHandlerTest extends OAuthBearerTest {

    @Test
    public void testBasic() throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder();
        String accessToken = builder.build();

        Map<String, Object> options = new HashMap<>();
        options.put(ValidatorCallbackHandlerConfiguration.EXPECTED_AUDIENCE_CONFIG, "test");
        OAuthBearerValidatorCallbackHandler handler = createHandler(options, builder);

        OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(accessToken);
        handler.handle(new Callback[] {callback});

        assertNotNull(callback.token());
        OAuthBearerToken token = callback.token();
        assertEquals(accessToken, token.value());
        assertEquals(builder.subject(), token.principalName());
        assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
        assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
    }

    @Test
    public void testInvalidAccessToken() throws Exception {
        // There aren't different error messages for the validation step, so these are all the
        // same :(
        String substring = "invalid_token";
        assertInvalidAccessTokenFails("this isn't valid", substring);
        assertInvalidAccessTokenFails("this.isn't.valid", substring);
        assertInvalidAccessTokenFails(createAccessKey("this", "isn't", "valid"), substring);
        assertInvalidAccessTokenFails(createAccessKey("{}", "{}", "{}"), substring);
    }

    private void assertInvalidAccessTokenFails(String accessToken, String expectedMessageSubstring) throws Exception {
        Map<String, Object> options = new HashMap<>();
        OAuthBearerValidatorCallbackHandler handler = createHandler(options, new AccessTokenBuilder());

        OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(accessToken);
        handler.handle(new Callback[] {callback});

        assertNull(callback.token());
        String actualMessage = callback.errorStatus();
        assertNotNull(actualMessage);
        assertTrue(actualMessage.contains(expectedMessageSubstring), String.format("The error message \"%s\" didn't contain the expected substring \"%s\"", actualMessage, expectedMessageSubstring));
    }

    private OAuthBearerValidatorCallbackHandler createHandler(Map<String, Object> options,
        AccessTokenBuilder builder) {
        OAuthBearerValidatorCallbackHandler handler = new OAuthBearerValidatorCallbackHandler();
        ValidatorCallbackHandlerConfiguration conf = new ValidatorCallbackHandlerConfiguration(options);

        CloseableVerificationKeyResolver verificationKeyResolver = new CloseableVerificationKeyResolver() {

            @Override
            public void init() {
                // Do nothing...
            }

            @Override
            public void close() {
                // Do nothing...
            }

            @Override
            public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) {
                return builder.jwk().getRsaPublicKey();
            }

        };

        handler.configure(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            conf,
            Collections.emptySet(),
            verificationKeyResolver);
        return handler;
    }

    private String createAccessKey(String header, String payload, String signature) {
        Base64.Encoder enc = Base64.getEncoder();
        header = enc.encodeToString(Utils.utf8(header));
        payload = enc.encodeToString(Utils.utf8(payload));
        signature = enc.encodeToString(Utils.utf8(signature));
        return String.format("%s.%s.%s", header, payload, signature);
    }

}
