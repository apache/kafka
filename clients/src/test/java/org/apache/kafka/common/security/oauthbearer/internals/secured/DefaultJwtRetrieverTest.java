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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;

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
        String expected = "{}";

        File tmpDir = createTempDir("access-token");
        File accessTokenFile = createTempFile(tmpDir, "access-token-", ".json", expected);

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, accessTokenFile.toURI().toString());
        Map<String, ?> configs = Collections.singletonMap(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, accessTokenFile.toURI().toString());
        Map<String, Object> jaasConfig = Collections.emptyMap();

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaasConfig)) {
            jwtRetriever.init();
            assertEquals(expected, jwtRetriever.retrieve());
        }
    }

    @Test
    public void testConfigureRefreshingFileJwtRetrieverWithInvalidDirectory() throws IOException {
        // Should fail because the parent path doesn't exist.
        String file = new File("/tmp/this-directory-does-not-exist/foo.json").toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, file);
        Map<String, Object> jaasConfig = Collections.emptyMap();

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaasConfig)) {
            assertThrowsWithMessage(ConfigException.class, jwtRetriever::init, "that doesn't exist");
        }
    }

    @Test
    public void testConfigureRefreshingFileJwtRetrieverWithInvalidFile() throws Exception {
        // Should fail because while the parent path exists, the file itself doesn't.
        File tmpDir = createTempDir("this-directory-does-exist");
        File accessTokenFile = new File(tmpDir, "this-file-does-not-exist.json");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, accessTokenFile.toURI().toString());
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, accessTokenFile.toURI().toString());
        Map<String, Object> jaasConfig = Collections.emptyMap();

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaasConfig)) {
            assertThrowsWithMessage(ConfigException.class, jwtRetriever::init, "that doesn't exist");
        }
    }

    @Test
    public void testSaslOauthbearerTokenEndpointUrlIsNotAllowed() throws Exception {
        // Should fail if the URL is not allowed
        File tmpDir = createTempDir("not_allowed");
        File accessTokenFile = new File(tmpDir, "not_allowed.json");
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, accessTokenFile.toURI().toString());

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, Collections.emptyMap())) {
            assertThrowsWithMessage(ConfigException.class, jwtRetriever::init, ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        }
    }

    @Test
    public void testConfigureWithAccessTokenFile() throws Exception {
        String expected = "{}";

        File tmpDir = createTempDir("access-token");
        File accessTokenFile = createTempFile(tmpDir, "access-token-", ".json", expected);
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, accessTokenFile.toURI().toString());

        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, accessTokenFile.toURI().toString());

        DefaultJwtRetriever jwtRetriever = new DefaultJwtRetriever(
            configs,
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            Map.of()
        );
        assertDoesNotThrow(jwtRetriever::init);
        assertInstanceOf(FileJwtRetriever.class, jwtRetriever.delegate());
    }

    @Test
    public void testConfigureWithAccessClientCredentials() {
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "http://www.example.com");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "http://www.example.com");
        Map<String, Object> jaasConfigs = new HashMap<>();
        jaasConfigs.put(CLIENT_ID_CONFIG, "an ID");
        jaasConfigs.put(CLIENT_SECRET_CONFIG, "a secret");

        DefaultJwtRetriever jwtRetriever = new DefaultJwtRetriever(
            configs,
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            jaasConfigs
        );
        assertDoesNotThrow(jwtRetriever::init);
        assertInstanceOf(HttpJwtRetriever.class, jwtRetriever.delegate());
    }

    @ParameterizedTest
    @MethodSource("urlencodeHeaderSupplier")
    public void testUrlencodeHeader(Map<String, Object> configs, boolean expectedValue) {
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        boolean actualValue = DefaultJwtRetriever.validateUrlencodeHeader(cu);
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
