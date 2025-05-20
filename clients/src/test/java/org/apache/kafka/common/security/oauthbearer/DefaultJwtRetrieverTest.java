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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class DefaultJwtRetrieverTest extends OAuthBearerTest {

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testConfigureRefreshingFileJwtRetriever() throws Exception {
        String expected = createJwt("jdoe");
        String file = tempFile(expected).toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = Collections.singletonMap(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, file);

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever()) {
            jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());
            assertEquals(expected, jwtRetriever.retrieve());
        }
    }

    @Test
    public void testConfigureRefreshingFileJwtRetrieverWithInvalidDirectory() throws IOException {
        // Should fail because the parent path doesn't exist.
        String file = new File("/tmp/this-directory-does-not-exist/foo.json").toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, file);

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever()) {
            assertThrowsWithMessage(
                ConfigException.class,
                () -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()),
                "that doesn't exist"
            );
        }
    }

    @Test
    public void testSaslOauthbearerTokenEndpointUrlIsNotAllowed() throws Exception {
        // Should fail because the URL was not allowed
        String file = tempFile("test data").toURI().toString();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, file);

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever()) {
            assertThrowsWithMessage(
                ConfigException.class,
                () -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()),
                ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG
            );
        }
    }

    @Test
    public void testConfigureWithAccessTokenFile() throws Exception {
        String expected = createJwt("jdoe");
        String file = tempFile(expected).toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, file);

        try (DefaultJwtRetriever jwtRetriever = new DefaultJwtRetriever()) {
            assertDoesNotThrow(() -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()));
            assertInstanceOf(FileJwtRetriever.class, jwtRetriever.delegate());
        }
    }

    @Test
    public void testConfigureWithAccessClientCredentials() throws Exception {
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "http://www.example.com");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "http://www.example.com");
        Map<String, Object> jaasConfigs = new HashMap<>();
        jaasConfigs.put(CLIENT_ID_CONFIG, "an ID");
        jaasConfigs.put(CLIENT_SECRET_CONFIG, "a secret");

        try (DefaultJwtRetriever jwtRetriever = new DefaultJwtRetriever()) {
            assertDoesNotThrow(() -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries(jaasConfigs)));
            assertInstanceOf(ClientCredentialsJwtRetriever.class, jwtRetriever.delegate());
        }
    }

    @ParameterizedTest
    @MethodSource("urlencodeHeaderSupplier")
    public void testUrlencodeHeader(Map<String, Object> configs, boolean expectedValue) {
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        boolean actualValue = ClientCredentialsJwtRetriever.validateUrlencodeHeader(cu);
        assertEquals(expectedValue, actualValue);
    }

    private static Stream<Arguments> urlencodeHeaderSupplier() {
        return Stream.of(
            Arguments.of(Collections.emptyMap(), DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE),
            Arguments.of(Collections.singletonMap(SASL_OAUTHBEARER_HEADER_URLENCODE, null), DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE),
            Arguments.of(Collections.singletonMap(SASL_OAUTHBEARER_HEADER_URLENCODE, true), true),
            Arguments.of(Collections.singletonMap(SASL_OAUTHBEARER_HEADER_URLENCODE, false), false)
        );
    }
}
